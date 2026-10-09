from mardi_importer.base import ADataSource
from .RPackage import RPackage, wikidata_r_packages
from . import archive
from .authors import fetch_packages_rds, package_entries, read_packages_rds
from .people import read_graph
from .resolve import MardiWiki, OrcidRegistry, People

import pandas as pd
import tempfile
import time
import json
import os
import logging
from pathlib import Path
log = logging.getLogger('CRANlogger')

class CRANSource(ADataSource):
    """Processes data from the Comprehensive R Archive Network.

    Metadata for each R package is scrapped from the CRAN Repository. Each 
    Wikibase item corresponding to each R package is subsequently updated
    or created, in case of a new package.
    
    Attributes:
        packages (Pandas dataframe): 
          Dataframe with **package name**, **title** and **date of publication** for
          each package in CRAN.
    """

    def __init__(self, user: str, password: str):
        super().__init__(user, password)
        self.packages = ""
        self.entries_by_package = {}
        self.people = None
        self.wikidata_ids = None

    def setup(self):
        """Create all necessary properties and entities for CRAN
        """
        # Import entities from Wikidata
        self.import_wikidata_entities("/wikidata_entities.txt")

        # Create new required local entities
        self.create_local_entities("/new_entities.json")

    def pull(self):
        """Reads **date**, **package name** and **title** from the CRAN Repository URL.

        The result is saved as a pandas dataframe in the attribute **packages**.

        Returns:
            Pandas dataframe: Attribute ``packages``
        """
        url = r"https://cran.r-project.org/web/packages/available_packages_by_date.html"

        tables = pd.read_html(url)
        self.packages = tables[0]
        self.load_people()
        self.wikidata_ids = wikidata_r_packages()
        return self.packages

    def load_people(self, packages_rds: str | None = None, sparql=None) -> None:
        """Read the people of every package from CRAN's package database, and their items.

        Mentions are joined into people across all packages and matched to the
        person items the packages link now (see :mod:`mardi_importer.cran.resolve`).
        If the database or the SPARQL store cannot be read, packages fall back to
        their page and to resolving people without that overview.
        """
        try:
            path = Path(packages_rds) if packages_rds else fetch_packages_rds(
                Path(tempfile.gettempdir()) / "mardi_importer" / "packages.rds")
            entries = []
            for row in read_packages_rds(path):
                found, notes = package_entries(row)
                self.entries_by_package[row["Package"]] = found
                entries += found
                for note in notes:
                    log.warning("%s: %s", row["Package"], note)
            graph = read_graph(sparql or _sparql)
            self.people = People.build(MardiWiki(self.api), entries, graph, OrcidRegistry())
            log.info("CRAN people: %d mentions in %d packages, %d person items linked",
                     len(entries), len(self.entries_by_package), len(graph))
        except Exception as exc:
            log.warning("CRAN people overview unavailable (%s); resolving per package", exc)
            self.people = People(MardiWiki(self.api), OrcidRegistry())

    def new_package(self, date: str, label: str, title: str) -> RPackage:
        return RPackage(date, label, title, entries=self.entries_by_package.get(label), people=self.people,
                        wikidata_ids=self.wikidata_ids)

    def push(self):
        """Updates the MaRDI Wikibase entities corresponding to R packages.

        For each **package name** in the attribute **packages** checks 
        if the date in CRAN coincides with the date in the MaRDI 
        knowledge graph. If not, the package is updated. If the package 
        is not found in the MaRDI knowledge graph, the corresponding 
        item is created.

        It creates a :class:`mardi_importer.cran.RPackage` instance
        for each package.
        """
        failed = []
        for n, (_, row) in enumerate(self.packages.iterrows(), 1):
            package_label = row["Package"]
            try:
                package = self.new_package(row["Date"], package_label, row["Title"])
                if package.exists():
                    if not package.is_updated():
                        log.info("[%d/%d] %s: not up to date, updating", n, len(self.packages), package_label)
                        package.update()
                    else:
                        log.info("[%d/%d] %s: up to date", n, len(self.packages), package_label)
                else:
                    log.info("[%d/%d] %s: not found, creating", n, len(self.packages), package_label)
                    package.create()
            except Exception:
                log.exception("[%d/%d] %s: failed", n, len(self.packages), package_label)
                failed.append(package_label)

            time.sleep(2)

        if failed:
            log.warning("%d packages failed: %s", len(failed), ", ".join(failed))
        self.sync_archive_status()
        return failed

    def sync_archive_status(self, dry_run: bool = False) -> dict:
        """Set or remove the *end time* of every *CRAN project* statement.

        See :mod:`mardi_importer.cran.archive`.
        """
        return archive.sync(self.api, dry_run=dry_run)


def _sparql(query: str) -> list[dict]:
    from wikibaseintegrator.wbi_helpers import execute_sparql_query
    res = execute_sparql_query(query)
    return [{k: v["value"] for k, v in b.items()} for b in res["results"]["bindings"]]
