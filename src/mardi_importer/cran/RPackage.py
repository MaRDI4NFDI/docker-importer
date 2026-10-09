from mardiclient import MardiClient, MardiItem
from mardi_importer import Importer
from mardi_importer.wikidata import WikidataImporter
from mardi_importer.arxiv import ArxivSource, ArxivPublication
from mardi_importer.crossref import CrossrefSource, CrossrefPublication
from mardi_importer.zenodo import ZenodoSource, ZenodoResource
from wikibaseintegrator.wbi_enums import ActionIfExists

from .authors import Entry, parse_author_text, parse_maintainer
from .resolve import MardiWiki, OrcidRegistry, People, Resolved, agrees

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from io import StringIO

from bs4 import BeautifulSoup
import pandas as pd
import requests
import re

import logging

log = logging.getLogger("CRANlogger")

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "mardi-importer (https://portal.mardi4nfdi.de; CRAN import)"


def wikidata_r_packages(session=None) -> Dict[str, str]:
    """CRAN package name → Wikidata QID, in one query per run.

    Items with the package's *CRAN project* (P5565) first, then items that are
    an *R package* (Q73539779) with the package's name as English label. A name
    shared by several items is left out. Empty if Wikidata cannot be reached:
    the Wikidata QID is then not added, nothing else changes.
    """
    session = session or requests.Session()
    query = ("SELECT ?item ?name ?kind WHERE { { ?item wdt:P5565 ?name BIND(1 AS ?kind) } UNION "
             "{ ?item wdt:P31 wd:Q73539779 ; rdfs:label ?name FILTER(LANG(?name) = \"en\") BIND(2 AS ?kind) } }")
    try:
        r = session.get(WIKIDATA_SPARQL, params={"query": query},
                        headers={"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT}, timeout=120)
        r.raise_for_status()
        rows = r.json()["results"]["bindings"]
    except Exception as exc:
        log.warning("Wikidata R packages not read (%s); no Wikidata QIDs will be added", exc)
        return {}
    found: Dict[str, Dict[int, set]] = {}
    for b in rows:
        qid = b["item"]["value"].rsplit("/", 1)[-1]
        found.setdefault(b["name"]["value"], {}).setdefault(int(b["kind"]["value"]), set()).add(qid)
    out = {}
    for name, by_kind in found.items():
        best = by_kind[min(by_kind)]
        if len(best) == 1:
            out[name] = next(iter(best))
    return out


@dataclass
class RPackage:
    """Class to manage R package items in the local Wikibase instance.

    Attributes:
        date:
          Date of publication
        label:
          Package name
        description:
          Title of the R package
        long_description:
          Detailed description of the R package
        url:
          URL to the CRAN repository
        version:
          Version of the R package
        versions:
          Previous published versions
        entries:
          People named by the package (authors, maintainer), from CRAN's
          package database or, failing that, the package page
        people:
          Resolution of people to items, shared by the packages of one run
        license:
          Software license
        dependency:
          Dependencies to R and other packages
        imports:
          Imported R packages
        _QID:
          Package QID
    """

    date: str
    label: str
    description: str
    long_description: str = ""
    url: str = ""
    version: str = ""
    versions: List[Tuple[str, str]] = field(default_factory=list)
    entries: Optional[List[Entry]] = None
    people: Optional[People] = None
    wikidata_ids: Optional[Dict[str, str]] = None
    license_data: List[Tuple[str, str]] = field(default_factory=list)
    dependencies: List[Tuple[str, str]] = field(default_factory=list)
    imports: List[Tuple[str, str]] = field(default_factory=list)
    crossref_publications: List[CrossrefPublication] = field(default_factory=list)
    arxiv_publications: List[ArxivPublication] = field(default_factory=list)
    zenodo_resources: List[ZenodoResource] = field(default_factory=list)
    _QID: str = ""
    _item: MardiItem = None
    api: Optional[MardiClient] = None
    wdi: Optional[WikidataImporter] = None
    crossref: Optional[CrossrefSource] = None
    arxiv: Optional[ArxivSource] = None
    zenodo: Optional[ZenodoSource] = None

    def __post_init__(self):
        if self.api is None:
            self.api = Importer.get_api("cran")
        if self.wdi is None:
            self.wdi = WikidataImporter()
        if self.crossref is None:
            self.crossref = Importer.create_source("crossref")
        if self.arxiv is None:
            self.arxiv = Importer.create_source("arxiv")
        if self.zenodo is None:
            self.zenodo = Importer.create_source("zenodo")
        if self.people is None:
            self.people = People(MardiWiki(self.api), OrcidRegistry())

    @property
    def QID(self) -> str:
        """Return the QID of the R package in the knowledge graph.

        Searches for an item with the package label in the Wikibase
        SQL tables and returns the QID if a matching result is found.

        Returns:
            str: The entity QID representing the R package.
        """
        self._QID = self._QID or self.item.is_instance_of("wd:Q73539779")
        return self._QID

    @property
    def item(self) -> MardiItem:
        """Return the Item representing the R package.

        Adds also the label and description of the package.

        Returns:
            MardiItem: MardiClient item
        """
        if not self._item:
            self._item = self.api.item.new()
            self._item.labels.set(language="en", value=self.label)
            description = self.description
            if self.label == self.description:
                description += " (R Package)"
            self._item.descriptions.set(language="en", value=description)
        return self._item

    def exists(self) -> str:
        """Checks if an item corresponding to the R package already exists.

        Returns:
          str: Entity ID
        """
        if self.QID:
            self._item = self.api.item.get(entity_id=self.QID)
        return self.QID

    def is_updated(self) -> bool:
        """Checks if the Item corresponding to the R package is up to date.

        Compares the last update property in the local knowledge graph with
        the publication date imported from CRAN.

        Returns:
          bool: **True** if both dates coincide, **False** otherwise.
        """
        return self.date == self.get_last_update()

    def pull(self):
        """Imports metadata from CRAN corresponding to the R package.

        Imports **Version**, **Dependencies**, **Imports** and **License**
        and saves them as instance attributes. Authors and maintainer come
        from :attr:`entries`; only when none were given are they read from
        the package page.
        """
        self.url = f"https://CRAN.R-project.org/package={self.label}"

        try:
            page = requests.get(self.url)
            soup = BeautifulSoup(page.content, "lxml")
        except:
            log.warning(f"Package {self.label} package not found in CRAN.")
            return None
        else:
            if soup.find_all("table"):
                self.long_description = soup.find_all("p")[0].get_text() or ""
                self.parse_publications(self.long_description)
                self.long_description = re.sub("\n", "", self.long_description).strip()
                self.long_description = re.sub("\t", "", self.long_description).strip()

                table = soup.find_all("table")[0]
                package_df = self.clean_package_list(table)

                if "Version" in package_df.columns:
                    self.version = package_df.loc[1, "Version"]
                if self.entries is None:
                    self.entries = self.page_entries(package_df)
                if "License" in package_df.columns:
                    self.license_data = package_df.loc[1, "License"]
                if "Depends" in package_df.columns:
                    self.dependencies = package_df.loc[1, "Depends"]
                if "Imports" in package_df.columns:
                    self.imports = package_df.loc[1, "Imports"]

                self.get_versions()
            else:
                log.warning(
                    "Metadata table not found in CRAN. Package has probably been archived."
                )
            return self

    def create(self) -> None:
        """Create a package in the Wikibase instance.

        This function pulls the package, inserts its claims, and writes
        it to the Wikibase instance.

        Returns:
            None
        """
        package = self.pull()

        if package:
            package = package.insert_claims().write()

        if package:
            log.info(f"Package created with QID: {package['QID']}.")
            # print('package created')
        else:
            log.info(f"Package could not be created.")
            # print('package not created')

    def write(self) -> Optional[Dict[str, str]]:
        """Write the package item to the Wikibase instance.

        If the item has claims, it will be written to the Wikibase instance.
        If the item is successfully written, a dictionary with the QID of the
        item will be returned.

        Returns:
            Optional[Dict[str, str]]:
                A dictionary with the QID of the written item if successful,
                or None otherwise.
        """
        if self.item.claims:
            item = self.item.write()
            if item:
                return {"QID": item.id}

    def insert_claims(self):
        # Instance of: R package
        self.item.add_claim("wdt:P31", "wd:Q73539779")
        self.item.add_claim("MaRDI profile type", "MaRDI software profile")

        # Programmed in: R
        self.item.add_claim("wdt:P277", "wd:Q206904")

        # Long description
        prop_nr = self.api.get_local_id_by_label("description", "property")
        self.item.add_claim(prop_nr, self.long_description)

        # Last update date
        self.item.add_claim("wdt:P5017", f"+{self.date}T00:00:00Z")

        # Software version identifiers
        for version, publication_date in self.versions:
            qualifier = [self.api.get_claim("wdt:P577", publication_date)]
            self.item.add_claim("wdt:P348", version, qualifiers=qualifier)

        if self.version:
            qualifier = [self.api.get_claim("wdt:P577", f"+{self.date}T00:00:00Z")]
            self.item.add_claim("wdt:P348", self.version, qualifiers=qualifier)

        # Authors and maintainer
        self.apply_people()

        # Licenses
        if self.license_data:
            claims = self.process_claims(self.license_data, "wdt:P275", "wdt:P9767")
            self.item.add_claims(claims)

        # Dependencies
        if self.dependencies:
            claims = self.process_claims(self.dependencies, "wdt:P1547", "wdt:P348")
            self.item.add_claims(claims)

        # Imports
        if self.imports:
            prop_nr = self.api.get_local_id_by_label("imports", "property")
            claims = self.process_claims(self.imports, prop_nr, "wdt:P348")
            self.item.add_claims(claims)

        # Related publications and sources
        cites_work = "wdt:P2860"
        for publications in [
            self.crossref_publications,
            self.arxiv_publications,
            self.zenodo_resources,
        ]:
            for publication in publications:
                publication.create()
                self.item.add_claim(cites_work, publication.QID)

        # CRAN Project
        self.item.add_claim("wdt:P5565", self.label)

        # Wikidata QID
        wikidata_QID = self.get_wikidata_QID()
        if wikidata_QID:
            self.item.add_claim("Wikidata QID", wikidata_QID)

        return self

    def update(self):
        """Updates existing WB item with the imported metadata from CRAN.

        The metadata corresponding to the package is first pulled from CRAN and
        saved as instance attributes through :meth:`pull`. The statements that
        do not coincide with the locally saved information are updated or
        subsituted with the updated information.

        Uses :class:`mardi_importer.wikibase.WBItem` to update the item
        corresponding to the R package.

        Returns:
          str: ID of the updated R package.
        """
        if self.pull():
            # Everything is changed in the item and written once at the end: an
            # error on the way (a source not answering) leaves the item as it was.
            if self.item.descriptions.values.get("en") != self.description:
                description = self.description
                if self.label == self.description:
                    description += " (R Package)"
                self.item.descriptions.set(language="en", value=description)

            self.item.add_claim("MaRDI profile type", "MaRDI software profile")

            # Long description
            self.item.add_claim(
                "description", self.long_description, action="replace_all"
            )

            # Last update date
            self.item.add_claim(
                "wdt:P5017", f"+{self.date}T00:00:00Z", action="replace_all"
            )

            # Software version identifiers
            for version, publication_date in self.versions:
                qualifier = [self.api.get_claim("wdt:P577", publication_date)]
                self.item.add_claim("wdt:P348", version, qualifiers=qualifier)

            if self.version:
                qualifier = [self.api.get_claim("wdt:P577", f"+{self.date}T00:00:00Z")]
                self.item.add_claim("wdt:P348", self.version, qualifiers=qualifier)

            # Authors and maintainer
            self.apply_people()

            # Licenses, dependencies, imports and cited works: replaced by CRAN's
            self.replace_statements("wdt:P275", self.process_claims(self.license_data, "wdt:P275", "wdt:P9767"))
            self.replace_statements("wdt:P1547", self.process_claims(self.dependencies, "wdt:P1547", "wdt:P348"))
            imports = self._prop("imports")
            self.replace_statements("imports", self.process_claims(self.imports, imports, "wdt:P348"))
            cited = []
            for publications in [
                self.crossref_publications,
                self.arxiv_publications,
                self.zenodo_resources,
            ]:
                for publication in publications:
                    publication.create()
                    if publication.QID:
                        cited.append(self.api.get_claim("wdt:P2860", publication.QID))
            self.replace_statements("wdt:P2860", cited)

            # CRAN Project
            self.item.add_claim("wdt:P5565", self.label, action="replace_all")

            # Wikidata QID
            wikidata_QID = self.get_wikidata_QID()
            if wikidata_QID:
                self.item.add_claim("Wikidata QID", wikidata_QID, action="replace_all")

            package = self.write()

            if package:
                print(f"Package with QID updated: {package['QID']}.")
            else:
                print(f"Package could not be updated.")

    def replace_statements(self, prop: str, claims: list) -> None:
        """Make ``claims`` the statements of ``prop`` in the item, to be written with it.

        Statements equal to one of ``claims`` (value and qualifiers) stay as
        they are; the others are removed, and the new ones added.
        """
        current = list(self.item.claims.get(self._prop(prop)))
        for claim in current:
            if claim not in claims:
                claim.remove()
        for claim in claims:
            if claim not in current:
                self.item.claims.add(claim, ActionIfExists.FORCE_APPEND)

    def process_claims(self, data, prop_nr, qualifier_nr=None):
        claims = []
        for value, qualifier_value in data:
            if not value:
                continue
            qualifier_prop_nr = (
                "wdt:P2699" if qualifier_value.startswith("https") else qualifier_nr
            )
            qualifier = (
                [self.api.get_claim(qualifier_prop_nr, qualifier_value)]
                if qualifier_value
                else []
            )
            claims.append(self.api.get_claim(prop_nr, value, qualifiers=qualifier))
        return claims

    def parse_publications(self, description):
        """Extracts the DOI identification of related publications.

        Identifies the DOI of publications that are mentioned using the
        format *doi:* or *arXiv:* in the long description of the
        R package.

        Returns:
          List:
            List containing the wikibase IDs of mentioned publications.
        """
        doi_references = re.findall("<doi:(.*?)>", description)
        arxiv_references = re.findall("<arXiv:(.*?)>", description)
        zenodo_references = re.findall("<zenodo:(.*?)>", description)

        doi_references = list(
            map(lambda x: x[:-1] if x.endswith(".") else x, doi_references)
        )
        arxiv_references = list(
            map(lambda x: x[:-1] if x.endswith(".") else x, arxiv_references)
        )
        zenodo_references = list(
            map(lambda x: x[:-1] if x.endswith(".") else x, zenodo_references)
        )

        crossref_references = []

        for doi in doi_references:
            doi = doi.strip().upper()
            if re.search("10.48550/", doi):
                arxiv_id = doi.replace(":", ".")
                arxiv_id = arxiv_id.replace("10.48550/arxiv.", "")
                arxiv_references.append(arxiv_id.strip())
            elif re.search("10.5281/", doi):
                zenodo_id = doi.replace(":", ".")
                zenodo_id = zenodo_id.replace("10.5281/zenodo.", "")
                zenodo_references.append(zenodo_id.strip())
            else:
                crossref_references.append(doi)

        for doi in crossref_references:
            publication = self.crossref.new_publication(doi.upper())
            self.crossref_publications.append(publication)

        for arxiv_id in arxiv_references:
            arxiv_id = arxiv_id.replace(":", ".")
            publication = self.arxiv.new_publication(arxiv_id)
            if publication.title != "Error":
                self.arxiv_publications.append(publication)

        for zenodo_id in zenodo_references:
            zenodo_id = zenodo_id.replace(":", ".")
            publication = self.zenodo.new_resource(zenodo_id)
            self.zenodo_resources.append(publication)

    def get_last_update(self):
        """Returns the package last update date saved in the Wikibase instance.

        Returns:
            str: Last update date in format DD-MM-YYYY.
        """
        last_update = self.item.get_value("wdt:P5017")
        return last_update[0][1:11] if last_update else None

    def clean_package_list(self, table_html):
        """Processes raw imported data from CRAN to enable the creation of items.

        - Package dependencies are splitted at the comma position.
        - License information is processed using the :meth:`parse_license` method.

        Args:
            table_html:
              HTML code obtained with BeautifulSoup corresponding to the table
              containing the metadata of the R package imported from CRAN.
        Returns:
            (Pandas dataframe):
              Dataframe with processed data from a single R package including columns:
              **Version**, **License**, **Depends** and **Imports** (processed),
              **Author** and **Maintainer** (as on the page).
        """
        package_df = pd.read_html(StringIO(str(table_html)))
        package_df = package_df[0].set_index(0).T
        package_df.columns = package_df.columns.str[:-1]
        if "Depends" in package_df.columns:
            package_df["Depends"] = package_df["Depends"].apply(self.parse_software)
        if "Imports" in package_df.columns:
            package_df["Imports"] = package_df["Imports"].apply(self.parse_software)
        if "License" in package_df.columns:
            package_df["License"] = package_df["License"].apply(self.parse_license)
        return package_df

    def parse_software(self, software_str: str) -> List[Tuple[str, str]]:
        """Processes the dependency and import information of each R package.

        This includes:
        - Extracting the version information of each dependency/import if provided.
        - Providing the Item QID given the dependency/import label.
        - Creating a new Item if the dependency/import is not found in the
          local knowledge graph.

        Returns:
            List[Tuple[str, str]]:
                List of tuples including software QID and version.
        """
        if pd.isna(software_str):
            return []

        software_list = str(software_str).split(", ")
        software_tuples = []

        for software_string in software_list:
            software_version = re.search("\((.*?)\)", software_string)
            software_version = software_version.group(1) if software_version else ""

            software_name = re.sub("\(.*?\)", "", software_string).strip()

            # Instance of R package
            if software_name == "R":
                # Software = R
                software_QID = self.wdi.query("local_id", "Q206904")
            else:
                item = self.api.item.new()
                item.labels.set(language="en", value=software_name)
                software_id = item.is_instance_of("wd:Q73539779")
                if software_id:
                    # Software = R package
                    software_QID = software_id
                else:
                    # Software = New instance of R package
                    item.add_claim("wdt:P31", "wd:Q73539779")
                    item.add_claim("wdt:P277", "wd:Q206904")
                    item.add_claim("MaRDI profile type", "MaRDI software profile")
                    software_QID = item.write().id

            software_tuples.append((software_QID, software_version))

        return software_tuples

    def parse_license(self, x: str) -> List[Tuple[str, str]]:
        """Splits string of licenses.

        Takes into account that licenses are often not uniformly listed.
        Characters \|, + and , are used to separate licenses. Further
        details on each license are often included in square brackets.

        The concrete License is identified and linked to the corresponding
        item that has previously been imported from Wikidata. Further license
        information, when provided between round or square brackets, is added
        as a qualifier.

        If a file license is mentioned, the linked to the file license
        in CRAN is added as a qualifier.

        Args:
            x (str): String imported from CRAN representing license
              information.

        Returns:
            List[Tuple[str, str]]:
                List of license tuples. Each tuple contains the license QID
                as the first element and the license qualifier as the
                second element.
        """
        if pd.isna(x):
            return []

        license_list = []
        licenses = str(x).split(" | ")

        i = 0
        while i in range(len(licenses)):
            if not re.findall(r"\[", licenses[i]) or (
                re.findall(r"\[", licenses[i]) and re.findall(r"\]", licenses[i])
            ):
                license_list.append(licenses[i])
                i += 1
            elif re.findall(r"\[", licenses[i]) and not re.findall(r"\]", licenses[i]):
                j = i + 1
                license_aux = licenses[i]
                closed = False
                while j < len(licenses) and not closed:
                    license_aux += " | "
                    license_aux += licenses[j]
                    if re.findall(r"\]", licenses[j]):
                        closed = True
                    j += 1
                license_list.append(license_aux)
                i = j

        split_list = []
        for item in license_list:
            items = item.split(" + ")
            i = 0
            while i in range(len(items)):
                if not re.findall(r"\[", items[i]) or (
                    re.findall(r"\[", items[i]) and re.findall(r"\]", items[i])
                ):
                    split_list.append(items[i])
                    i += 1
                elif re.findall(r"\[", items[i]) and not re.findall(r"\]", items[i]):
                    j = i + 1
                    items_aux = items[i]
                    closed = False
                    while j < len(items) and not closed:
                        items_aux += " + "
                        items_aux += items[j]
                        if re.findall(r"\]", items[j]):
                            closed = True
                        j += 1
                    split_list.append(items_aux)
                    i = j
        license_list = list(dict.fromkeys(split_list))

        license_tuples = []
        for license_str in license_list:
            license_qualifier = ""
            if re.findall(r"\(.*?\)", license_str):
                qualifier_groups = re.search(r"\((.*?)\)", license_str)
                license_qualifier = qualifier_groups.group(1)
                license_aux = re.sub(r"\(.*?\)", "", license_str)
                if re.findall(r"\[.*?\]", license_aux):
                    qualifier_groups = re.search(r"\[(.*?)\]", license_str)
                    license_qualifier = qualifier_groups.group(1)
                    license_str = re.sub(r"\[.*?\]", "", license_aux)
                else:
                    license_str = license_aux
            elif re.findall(r"\[.*?\]", license_str):
                qualifier_groups = re.search(r"\[(.*?)\]", license_str)
                license_qualifier = qualifier_groups.group(1)
                license_str = re.sub(r"\[.*?\]", "", license_str)

            license_str = license_str.strip()
            if license_str in ["file LICENSE", "file LICENCE"]:
                license_qualifier = (
                    f"https://cran.r-project.org/web/packages/{self.label}/LICENSE"
                )

            license_QID = self.get_license_QID(license_str)
            if license_QID:
                license_tuples.append((license_QID, license_qualifier))
            else:
                log.warning("%s: licence %r not known", self.label, license_str)
        return license_tuples

    # -- authors and maintainer ---------------------------------------------------------

    def page_entries(self, package_df) -> List[Entry]:
        """People named on the package page, for a package without database entries."""
        entries = []
        if "Author" in package_df.columns and isinstance(package_df.loc[1, "Author"], str):
            entries += parse_author_text(package_df.loc[1, "Author"], self.label)
        if "Maintainer" in package_df.columns and isinstance(package_df.loc[1, "Maintainer"], str):
            if (m := parse_maintainer(package_df.loc[1, "Maintainer"], self.label)):
                entries.append(m)
        return entries

    def _prop(self, prop: str) -> str:
        pid = self.api.get_local_id_by_label(prop, "property")
        return pid[0] if isinstance(pid, list) else pid

    def _linked(self, props: List[str]) -> Dict[str, List[Tuple[object, str, str]]]:
        """Current statements of ``props``: (claim, QID it resolves to, label) per property."""
        out = {}
        for prop in props:
            rows = []
            for claim in self.item.claims.get(self._prop(prop)):
                value = (claim.mainsnak.datavalue or {}).get("value")
                if isinstance(value, dict) and value.get("id"):
                    hit = self.people.wiki.lookup(value["id"])
                    rows.append((claim, hit[0], hit[1]) if hit else (claim, value["id"], ""))
            out[prop] = rows
        return out

    def plan_people(self, linked: List[Tuple[str, str]]) -> Tuple[List[Resolved], Optional[Resolved]]:
        """Resolved authors (Authors@R ``aut``, or everyone in a free-text Author field) and maintainer."""
        entries = self.entries or []
        maintainer, m = None, next((e for e in entries if e.source == "Maintainer"), None)
        is_maintainer = lambda e: m is not None and "cre" in e.roles and agrees(m.name, e.name, e.family)
        if m is not None:
            if not m.orcid:          # the maintainer's ORCID is on their Authors@R entry
                m.orcid = next((e.orcid for e in entries if e.source != "Maintainer" and is_maintainer(e)
                                and e.orcid), None)
            # First: the maintainer has an address, so is identifiable; their author entry
            # (often without one) then resolves to the same item.
            maintainer = self.people.resolve(m, linked)
        authors = [maintainer if maintainer is not None and maintainer.kind == "item" and is_maintainer(e)
                   else self.people.resolve(e, linked)
                   for e in entries if e.source != "Maintainer" and e.is_author]
        return authors, maintainer

    def apply_people(self) -> None:
        """Bring the author and maintainer statements in line with CRAN, only adding where possible.

        Links already on the package stay; an author name string is dropped
        when that person is now linked as an item; a maintainer link is
        removed only when CRAN names another maintainer or ORPHANED. Without
        any people for the package (none read), nothing is changed. Labels and
        aliases of person items are never changed.
        """
        if not self.entries:
            log.warning("%s: no authors or maintainer read; their statements are left as they are", self.label)
            return
        current = self._linked(["wdt:P50", "wdt:P126"]) if self.item.id else {"wdt:P50": [], "wdt:P126": []}
        linked = [(q, lab) for rows in current.values() for _, q, lab in rows]
        authors, maintainer = self.plan_people(linked)

        have = {q for _, q, _ in current["wdt:P50"]}
        for r in authors:
            if r.kind == "item" and r.qid not in have:
                self.item.add_claim("wdt:P50", r.qid)
                have.add(r.qid)
        linked_names = [r.name for r in authors if r.kind == "item"]
        strings = []
        for claim in self.item.claims.get(self._prop("wdt:P2093")):
            value = (claim.mainsnak.datavalue or {}).get("value")
            if isinstance(value, str) and any(agrees(value, n) for n in linked_names):
                claim.remove()
            elif isinstance(value, str):
                strings.append(value)
        for r in authors:
            if r.kind == "string" and r.name not in strings:
                self.item.add_claim("wdt:P2093", r.name)
                strings.append(r.name)

        for claim, q, lab in current["wdt:P126"]:
            replaced = maintainer is not None and maintainer.kind == "item" and \
                q != maintainer.qid and not agrees(maintainer.name, lab)
            if replaced or (maintainer is not None and maintainer.kind == "none"):
                claim.remove()
        if maintainer is not None and maintainer.kind == "item" and \
                maintainer.qid not in {q for _, q, _ in current["wdt:P126"]}:
            self.item.add_claim("wdt:P126", maintainer.qid)

    def get_license_QID(self, license_str: str) -> str:
        """Returns the Wikidata item ID corresponding to a software license.

        The same license is often denominated in CRAN using differents names.
        This function returns the wikidata item ID corresponding to a single
        unique license that is referenced in CRAN under different names (e.g.
        *Artistic-2.0* and *Artistic License 2.0* both refer to the same
        license, corresponding to item *Q14624826*).

        Args:
            license_str (str): String corresponding to a license imported from CRAN.

        Returns:
            (str): Wikidata item ID.
        """

        def get_license(label: str) -> str:
            license_item = self.api.item.new()
            license_item.labels.set(language="en", value=label)
            return license_item.is_instance_of("wd:Q207621")

        license_mapping = {
            "ACM": get_license("ACM Software License Agreement"),
            "AGPL": "wd:Q28130012",
            "AGPL-3": "wd:Q27017232",
            "Apache License": "wd:Q616526",
            "Apache License 2.0": "wd:Q13785927",
            "Apache License version 1.1": "wd:Q17817999",
            "Apache License version 2.0": "wd:Q13785927",
            "Artistic-2.0": "wd:Q14624826",
            "Artistic License 2.0": "wd:Q14624826",
            "BSD 2-clause License": "wd:Q18517294",
            "BSD 3-clause License": "wd:Q18491847",
            "BSD_2_clause": "wd:Q18517294",
            "BSD_3_clause": "wd:Q18491847",
            "BSL": "wd:Q2353141",
            "BSL-1.0": "wd:Q2353141",
            "CC0": "wd:Q6938433",
            "CC BY 4.0": "wd:Q20007257",
            "CC BY-SA 4.0": "wd:Q18199165",
            "CC BY-NC 4.0": "wd:Q34179348",
            "CC BY-NC-SA 4.0": "wd:Q42553662",
            "CeCILL": "wd:Q1052189",
            "CeCILL-2": "wd:Q19216649",
            "Common Public License Version 1.0": "wd:Q2477807",
            "CPL-1.0": "wd:Q2477807",
            "Creative Commons Attribution 4.0 International License": "wd:Q20007257",
            "EPL": "wd:Q1281977",
            "EUPL": "wd:Q1376919",
            "EUPL-1.1": "wd:Q1376919",
            "file LICENCE": get_license("File License"),
            "file LICENSE": get_license("File License"),
            "FreeBSD": "wd:Q34236",
            "GNU Affero General Public License": "wd:Q1131681",
            "GNU General Public License": "wd:Q7603",
            "GNU General Public License version 2": "wd:Q10513450",
            "GNU General Public License version 3": "wd:Q10513445",
            "GPL": "wd:Q7603",
            "GPL-2": "wd:Q10513450",
            "GPL-3": "wd:Q10513445",
            "LGPL": "wd:Q192897",
            "LGPL-2": "wd:Q23035974",
            "LGPL-2.1": "wd:Q18534390",
            "LGPL-3": "wd:Q18534393",
            "Lucent Public License": "wd:Q6696468",
            "MIT": "wd:Q334661",
            "MIT License": "wd:Q334661",
            "Mozilla Public License 1.1": "wd:Q26737735",
            "Mozilla Public License 2.0": "wd:Q25428413",
            "Mozilla Public License Version 2.0": "wd:Q25428413",
            "MPL": "wd:Q308915",
            "MPL version 1.0": "wd:Q26737738",
            "MPL version 1.1": "wd:Q26737735",
            "MPL version 2.0": "wd:Q25428413",
            "MPL-1.1": "wd:Q26737735",
            "MPL-2.0": "wd:Q25428413",
            "Unlimited": get_license("Unlimited License"),
        }

        license_info = license_mapping.get(license_str)
        if callable(license_info):
            return license_info()
        else:
            return license_info

    def get_wikidata_QID(self) -> Optional[str]:
        """The Wikidata QID of the R package, from :func:`wikidata_r_packages` (read once per run)."""
        if self.wikidata_ids is None:
            self.wikidata_ids = wikidata_r_packages()
        return self.wikidata_ids.get(self.label)

    def get_versions(self):
        url = f"https://cran.r-project.org/src/contrib/Archive/{self.label}"

        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.content, "lxml")
        except:
            log.warning(f"Version page for package {self.label} not found.")
        else:
            if soup.find_all("table"):
                table = soup.find_all("table")[0]
                versions_df = pd.read_html(StringIO(str(table)))
                versions_df = versions_df[0]
                versions_df = versions_df.drop(
                    columns=["Unnamed: 0", "Size", "Description"]
                )
                versions_df = versions_df.drop(index=[0, 1])

                for _, row in versions_df.iterrows():
                    name = row["Name"]
                    publication_date = row["Last modified"]
                    if isinstance(name, str):
                        version = re.sub(f"{self.label}_", "", name)
                        version = re.sub(".tar.gz", "", version)

                        publication_date = publication_date.split()[0]
                        publication_date = f"+{publication_date}T00:00:00Z"

                        self.versions.append((version, publication_date))
