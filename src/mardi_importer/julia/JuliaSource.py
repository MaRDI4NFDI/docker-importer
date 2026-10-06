"""Julia packages for numerical analysis, from the Julia General registry.

Scope: packages whose repository is under SciML, JuliaMath, JuliaLinearAlgebra,
JuliaNLSolvers or JuliaDiff (no ``_jll`` binary wrappers, no sub-directory
packages except StochasticDiffEq and DelayDiffEq). For each package the importer
writes the registry facts (name, repository, every registered version with the
day it was registered), the licence, authors, dependencies and the publications
its citation file names. Statements carry no references, as for CRAN; the run's
log and report say what was read from where.

Decisions encoded here (recorded in the MaRDI agents project, D016–D024):

* **Packages.** An existing item is updated only when it is the same software:
  it already carries this package's registry name; or it records the package's
  own repository; or it has the package's name and a repository named
  ``<Name>.jl`` (the same package before its repository moved). A name shared
  with other software (SUNDIALS vs. Sundials.jl) creates a new item and leaves
  the existing one untouched. Updates only add, never overwrite.
* **People.** An existing person item is reused only when it is certainly the
  same person: it carries the person's ORCID, or it is the same-named author of
  a paper the person's own package cites. Otherwise an identifiable person —
  joined by e-mail across packages, co-author of a cited paper being created,
  or holding an ORCID — gets a new item; similarly named items are logged as
  possible duplicates, never merged. Everyone else stays an author name string.
  E-mail addresses are used to join mentions only and are never written.
* **Publications.** A cited DOI or arXiv id already in MaRDI is linked (to every
  copy, if MaRDI holds duplicates); otherwise the journal DOI of an arXiv
  preprint or an exact title is tried; otherwise the publication is created
  through the Crossref or arXiv source. Zenodo DOIs are the software's own
  releases and are not treated as publications. Packages link to publications
  with *described by source*.
* **Licence.** A package whose licence cannot be determined is not written.
"""

from __future__ import annotations

import csv
import json
import logging
import re
import tempfile
from pathlib import Path
from typing import Iterable

import requests
from wikibaseintegrator.wbi_helpers import execute_sparql_query

from mardi_importer.base import ADataSource

from .JuliaPackage import (PERSON_PROFILE, PROFILE_TYPE, SOFTWARE_PROFILE, JuliaPackage, Planned,
                           add_planned, existing_values)
from .metadata import fetch_repo_metadata, is_zenodo
from .people import Mention, People, Person, assert_no_emails, norm_name, same_person_possible
from .registry import (SEED_ORGS, clone_registry, in_scope, read_registry, registration_dates,
                       repo_key, version_dates)

log = logging.getLogger("JuliaLogger")

USER_AGENT = "mardi-importer (https://portal.mardi4nfdi.de; Julia General import)"
JULIA_PACKAGE_CLASS = "Julia package"


# -- pure decision rules ---------------------------------------------------------------

def same_software(pkg: dict, by_repo: set[str], by_name: set[str],
                  urls_of: dict[str, list[str]]) -> list[str]:
    """Existing items that are *this* package, best evidence first. Empty → create.

    * the item records the package's registered repository and the package owns
      it (a sub-directory package shares it with its parent, so the name must
      agree too — DelayDiffEq is not OrdinaryDiffEq.jl);
    * the item has the package's name and a repository URL named ``<Name>.jl``.
    A shared name alone is never enough.
    """
    exact = by_repo & by_name if pkg.get("subdir") else set(by_repo)
    want = f"{pkg['name']}.jl".casefold()
    moved = {q for q in by_name
             if any((k := repo_key(u)) and k.rsplit("/", 1)[-1] == want for u in urls_of.get(q, []))}
    return sorted(exact & by_name) + sorted(exact - by_name) + sorted(moved - exact)


def decide_person(person: Person, by_orcid: dict[str, list[str]],
                  coauthor: dict[str, str]) -> None:
    """Reuse an item only when certain; otherwise create one for identifiable people."""
    found = sorted({q for o in person.orcids for q in by_orcid.get(o, [])})
    if found:
        keep = next((q for q in found if q in coauthor), found[0])
        person.qid, person.linked_by, person.action = keep, "orcid", "link"
        person.possible_duplicates += [q for q in found if q != keep]
        person.possible_duplicates += [q for q in coauthor if q not in found]
    elif len(coauthor) == 1:
        person.qid, person.linked_by, person.action = next(iter(coauthor)), "cited paper author", "link"
    else:
        person.possible_duplicates += list(coauthor)
        identifiable = (("email" in person.evidence and len(person.packages) > 1)
                        or person.new_papers or person.orcids or len(coauthor) > 1)
        if identifiable and not person.is_handle:
            person.action = "create"


def norm_title(title: str) -> str:
    return re.sub(r"[^\w]+", " ", title or "").strip().casefold()


# -- the source ---------------------------------------------------------------------

class JuliaSource(ADataSource):
    """Imports Julia numerical-analysis packages from the General registry."""

    def __init__(self, user: str, password: str):
        super().__init__(user, password)
        self.registry_sha = ""
        self.packages: list[JuliaPackage] = []
        self.persons: list[Person] = []
        self.publications: dict[str, object] = {}      # citation key → pending publication
        self.targets: dict[str, list[str]] = {}         # citation key → existing QIDs
        self.cited: dict[str, list[str]] = {}           # package → citation keys
        self.report: dict = {}
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT

    def setup(self):
        """Import the Wikidata entities used, and create the local ones."""
        self.import_wikidata_entities("/wikidata_entities.txt")
        self.create_local_entities("/new_entities.json")

    # -- pull: read everything, decide everything, write nothing --------------------

    def pull(self, names: Iterable[str] | None = None, registry_path: str | None = None,
             orcid_links: str | None = None) -> list[JuliaPackage]:
        """Read the registry and the packages' repositories, and decide every write.

        Args:
            names: Restrict to these package names (all in scope if omitted).
            registry_path: An existing clone of General (cloned if omitted).
            orcid_links: Optional CSV of ORCIDs found by an e-mail search
                (columns spellings, packages, orcid, name_agrees); used by name
                and package, never by address.
        """
        root = Path(registry_path) if registry_path else clone_registry(
            Path(tempfile.gettempdir()) / "mardi_importer" / "julia-general")
        self.registry_sha, all_pkgs = read_registry(root)
        wanted = set(names or [])
        pkgs = sorted((p for p in all_pkgs if in_scope(p) and (not wanted or p["name"] in wanted)),
                      key=lambda p: p["name"])
        log.info("Julia General @ %s: %d packages in scope", self.registry_sha[:7], len(pkgs))
        dates = registration_dates(root)
        versions = {p["name"]: version_dates(root, p, dates) for p in pkgs}
        n_all = sum(len(v) for v in versions.values())
        n_dated = sum(1 for v in versions.values() for _, day in v if day)
        log.info("  %d registered versions, %d with a registration day", n_all, n_dated)

        meta = {}
        for i, p in enumerate(pkgs, 1):
            meta[p["name"]] = fetch_repo_metadata(p, self.session)
            if i % 50 == 0:
                log.info("  read %d/%d repositories", i, len(pkgs))

        index = self._mardi_index()
        wikidata = self._wikidata_by_repo()

        self.packages = []
        for p in pkgs:
            md = meta[p["name"]]
            jp = JuliaPackage(name=p["name"], uuid=p["uuid"], repo=p["repo"], metadata=md,
                              subdir=p.get("subdir"), version=p.get("version"),
                              versions=versions[p["name"]],
                              wikidata_qid=wikidata.get(repo_key(p["repo"])), notes=list(md.notes))
            if undated := [v for v, day in jp.versions if not day]:
                jp.notes.append(f"{len(undated)} version(s) predate the registry's history: "
                                "written without publication date")
            self._decide_package(jp, p, index)
            self.packages.append(jp)

        self._resolve_citations(meta)
        self._cluster_people(pkgs, meta, orcid_links)
        return self.packages

    def _sparql(self, query: str) -> list[dict]:
        res = execute_sparql_query(query)
        return [{k: v["value"] for k, v in b.items()} for b in res["results"]["bindings"]]

    def _pid(self, prop: str) -> str | None:
        pid = self.api.get_local_id_by_label(prop, "property")
        return pid[0] if isinstance(pid, list) else pid

    def _mardi_index(self) -> dict:
        """Existing items by registry name, repository and label — whole sets, matched locally.

        No multi-value VALUES blocks: on the Blazegraph endpoint they return zero
        rows instead of failing.
        """
        qid = lambda uri: uri.rsplit("/", 1)[-1]
        index = {"by_package_name": {}, "by_repo": {}, "by_label": {}}
        name_pid = self._pid("Julia General registry package name")
        if name_pid:
            for r in self._sparql(f"SELECT ?item ?v WHERE {{ ?item wdt:{name_pid} ?v }}"):
                index["by_package_name"].setdefault(r["v"], set()).add(qid(r["item"]))
        repo_pid = self._pid("wdt:P1324")
        for r in self._sparql(f"SELECT ?item ?v WHERE {{ ?item wdt:{repo_pid} ?v }}"):
            if (k := repo_key(r["v"])):
                index["by_repo"].setdefault(k, set()).add(qid(r["item"]))
        for prop in ("swMATH work ID", "wdt:P1324"):
            pid = self._pid(prop)
            if not pid:
                continue
            for r in self._sparql(f"""SELECT ?item ?l WHERE {{ ?item wdt:{pid} ?x .
                ?item <http://www.w3.org/2000/01/rdf-schema#label> ?l FILTER(LANG(?l) = "en") }}"""):
                index["by_label"].setdefault(r["l"].casefold(), set()).add(qid(r["item"]))
        return index

    def _decide_package(self, jp: JuliaPackage, pkg: dict, index: dict) -> None:
        known = index["by_package_name"].get(jp.name, set())
        by_repo = index["by_repo"].get(repo_key(jp.repo), set())
        by_name = (index["by_label"].get(jp.name.casefold(), set())
                   | index["by_label"].get(f"{jp.name}.jl".casefold(), set()))
        urls_of = {q: self._item_urls(q) for q in by_name - by_repo}
        same = sorted(known) or same_software(pkg, by_repo, by_name, urls_of)
        if same:
            jp.action, jp.qid = "update", same[0]
            jp.matched_by = "registry name" if known else "same software"
        if jp.metadata.license_unclear:
            jp.action = "skip"
            jp.notes.append("licence unclear: not written")

    def _item_urls(self, qid: str) -> list[str]:
        item = self.api.item.get(entity_id=qid)
        planned = [Planned(p, None) for p in ("wdt:P1324", "wdt:P856")]
        vals = existing_values(self.api, item, planned)
        return vals.get("wdt:P1324", []) + vals.get("wdt:P856", [])

    def _wikidata_by_repo(self) -> dict[str, str]:
        rx = "|".join(o.lower() for o in SEED_ORGS)
        q = f"""SELECT ?i ?u WHERE {{ ?i wdt:P1324 ?u .
          FILTER(REGEX(LCASE(STR(?u)), "github\\\\.com/({rx})/")) }}"""
        try:
            r = self.session.get("https://query.wikidata.org/sparql",
                                 params={"query": q, "format": "json"}, timeout=120)
            r.raise_for_status()
        except requests.RequestException as exc:
            log.warning("Wikidata lookup failed, no Wikidata QIDs this run: %s", exc)
            return {}
        out = {}
        for b in r.json()["results"]["bindings"]:
            if (k := repo_key(b["u"]["value"])):
                out[k] = b["i"]["value"].rsplit("/", 1)[-1]
        return out

    # -- publications ------------------------------------------------------------------

    def _resolve_citations(self, meta: dict) -> None:
        """Map every cited DOI/arXiv id to existing items or a publication to create."""
        from mardi_importer import Importer

        self.cited, self.targets, self.publications = {}, {}, {}
        for name, md in meta.items():
            self.cited[name] = ([f"doi:{d}" for d in md.dois if not is_zenodo(d)]
                                + [f"arxiv:{a}" for a in md.arxiv])
        keys = sorted({k for ks in self.cited.values() for k in ks})
        pending = {}
        for key in keys:
            kind, ident = key.split(":", 1)
            found = self.api.search_entity_by_value("wdt:P356" if kind == "doi" else "wdt:P818", ident)
            if found:
                self.targets[key] = sorted(found)
            else:
                pending[key] = ident

        crossref = arxiv = None
        by_title: dict[str, str] = {}
        for key, ident in sorted(pending.items()):        # DOIs sort before arXiv ids
            try:
                if key.startswith("doi:"):
                    crossref = crossref or Importer.create_source("crossref")
                    pub = crossref.new_publication(ident)
                    if not getattr(pub, "crossref_ok", False):
                        log.warning("DOI %s not found in Crossref; not linked", ident)
                        continue
                else:
                    journal = self._arxiv_journal_doi(ident)
                    found = self.api.search_entity_by_value("wdt:P356", journal) if journal else []
                    if found:
                        self.targets[key] = sorted(found)
                        continue
                    arxiv = arxiv or Importer.create_source("arxiv")
                    pub = arxiv.new_publication(ident)
                    if getattr(pub, "title", "Error") in ("Error", None):
                        log.warning("arXiv %s not found; not linked", ident)
                        continue
            except Exception as exc:                         # one bad record must not stop the run
                log.warning("Could not read publication %s: %s", key, exc)
                continue
            t = norm_title(pub.title)
            if t in by_title:                                 # arXiv and journal version cited together
                self.publications[key] = self.publications[by_title[t]]
                continue
            item = self.api.item.new()
            item.labels.set(language="en", value=pub.title)
            existing = item.get_QID()
            if existing:
                self.targets[key] = sorted(existing)
                continue
            by_title[t] = key
            self.publications[key] = pub

    def _arxiv_journal_doi(self, arxiv_id: str) -> str | None:
        try:
            r = self.session.get("https://export.arxiv.org/api/query",
                                 params={"id_list": arxiv_id}, timeout=30)
        except requests.RequestException:
            return None
        m = re.search(r"<arxiv:doi[^>]*>(.*?)</arxiv:doi>", r.text) if r.ok else None
        return m.group(1).upper() if m else None

    # -- people ----------------------------------------------------------------------------

    def _cluster_people(self, pkgs: list[dict], meta: dict, orcid_links: str | None) -> None:
        people = People()
        for p in pkgs:
            md = meta[p["name"]]
            if md.project_url:
                for name, email in md.author_entries:
                    people.add(Mention(p["name"], name, email=email))
            if md.cff_url:
                for a in md.cff_authors:
                    people.add(Mention(p["name"], a["name"], email=a["email"], orcid=a["orcid"]))
        self.persons, by_mention = people.clusters()
        if orcid_links:
            self._apply_orcid_links(orcid_links)

        # co-authors of publications being created: same name, Crossref's ORCID if any
        for person in self.persons:
            names = {norm_name(n) for n in person.names}
            for pkg in person.packages:
                for key in self.cited.get(pkg, []):
                    pub = self.publications.get(key)
                    for author in getattr(pub, "authors", []) or []:
                        if norm_name(author.name) in names:
                            person.new_papers.add(key)
                            if author.orcid:
                                person.orcids.add(author.orcid)
                                person.evidence.add("orcid")

        by_orcid = {o: self.api.search_entity_by_value("wdt:P496", o)
                    for o in sorted({o for p in self.persons for o in p.orcids})}
        authors_of = self._authors_of_existing_papers()
        for person in self.persons:
            names = {norm_name(n) for n in person.names}
            coauthor = {}
            for pkg in sorted(person.packages):
                for key in self.cited.get(pkg, []):
                    for paper in self.targets.get(key, []):
                        for n in names & set(authors_of.get(paper, {})):
                            coauthor.setdefault(authors_of[paper][n], f"{paper} cited by {pkg}")
            decide_person(person, by_orcid, coauthor)
            if person.action == "create":
                item = self.api.item.new()
                item.labels.set(language="en", value=person.canonical)
                person.possible_duplicates += [q for q in item.get_QID() or []]

        for jp in self.packages:
            jp.authors = [(m, by_mention[i]) for i, m in enumerate(people.mentions)
                          if m.package == jp.name]

    def _authors_of_existing_papers(self) -> dict[str, dict[str, str]]:
        papers = sorted({q for qs in self.targets.values() for q in qs})
        out: dict[str, dict[str, str]] = {}
        for paper in papers:
            item = self.api.item.get(entity_id=paper)
            for author_qid in existing_values(self.api, item, [Planned("wdt:P50", None)]).get("wdt:P50", []):
                author = self.api.item.get(entity_id=author_qid)
                label = author.labels.get("en")
                if label:
                    out.setdefault(paper, {})[norm_name(str(label))] = author_qid
        return out

    def _apply_orcid_links(self, path: str) -> None:
        with open(path, newline="") as f:
            rows = [r for r in csv.DictReader(f) if r.get("name_agrees") in ("exact", "partial")]
        for row in rows:
            spell = {norm_name(s) for s in row["spellings"].split("; ") if s}
            pkgs = set(row["packages"].split("; "))
            for p in self.persons:
                if pkgs & p.packages and spell & {norm_name(x) for x in p.names}:
                    p.orcids.add(row["orcid"])
                    p.evidence.add("orcid")

    # -- push: write in dependency order -------------------------------------------------

    def push(self) -> dict:
        """Write people, then publications, then packages, then dependencies."""
        cls = self._local_item(JULIA_PACKAGE_CLASS)
        software_profile = self._local_item(SOFTWARE_PROFILE)

        for person in self.persons:
            try:
                self._write_person(person)
            except Exception as exc:
                log.error("Person %s not written: %s", person.canonical, exc, exc_info=True)

        pub_qid: dict[int, str] = {}
        for key, pub in self.publications.items():
            if id(pub) not in pub_qid:
                try:
                    self._attach_authors(pub, key)
                    pub_qid[id(pub)] = pub.create()
                except Exception as exc:
                    log.error("Publication %s not created: %s", key, exc, exc_info=True)
                    pub_qid[id(pub)] = None
            if pub_qid[id(pub)]:
                self.targets[key] = [pub_qid[id(pub)]]

        results = {}
        for jp in self.packages:
            if jp.action == "skip":
                results[jp.name] = {"qid": jp.qid, "status": "skipped", "notes": jp.notes}
                continue
            jp.papers = sorted({q for k in self.cited.get(jp.name, []) for q in self.targets.get(k, [])})
            try:
                qid = jp.write(self.api, jp.plan(cls, software_profile))
                results[jp.name] = {"qid": qid, "status": "updated" if jp.matched_by else "created",
                                    "conflicts": jp.conflicts}
            except Exception as exc:
                log.error("Julia package %s not written: %s", jp.name, exc, exc_info=True)
                results[jp.name] = {"qid": jp.qid, "status": "error", "error": str(exc)}

        qid_of = {jp.name: jp.qid for jp in self.packages if jp.qid and jp.action != "skip"}
        for jp in self.packages:
            deps = jp.plan_dependencies(qid_of) if jp.name in qid_of else []
            if deps:
                try:
                    item = self.api.item.get(entity_id=jp.qid)
                    add, _ = JuliaPackage.merge(deps, existing_values(self.api, item, deps))
                    for st in add:
                        add_planned(self.api, item, st)
                    if add:
                        item.write()
                except Exception as exc:
                    log.error("Dependencies of %s not written: %s", jp.name, exc, exc_info=True)

        self.report = self.summary(results)
        return self.report

    def _local_item(self, label: str) -> str:
        qid = self.api.get_local_id_by_label(label, "item")
        qid = qid[0] if isinstance(qid, list) else qid
        if not qid:
            raise RuntimeError(f"Local item '{label}' is missing; run setup()")
        return qid

    def _write_person(self, person: Person) -> None:
        if person.action == "link":
            # An existing item gains the ORCID only if it has none; a different
            # ORCID already there is a conflict for a person, not a second value.
            if len(person.orcids) != 1:
                return
            orcid = next(iter(person.orcids))
            item = self.api.item.get(entity_id=person.qid)
            planned = [Planned("wdt:P496", orcid)]
            have = existing_values(self.api, item, planned).get("wdt:P496", [])
            if not have:
                add_planned(self.api, item, planned[0])
                item.write()
            elif orcid not in have:
                log.warning("%s (%s) has ORCID %s, not %s; left unchanged",
                            person.canonical, person.qid, have, orcid)
        elif person.action == "create":
            assert_no_emails(person.canonical, "person label")
            item = self.api.item.new()
            item.labels.set(language="en", value=person.canonical)
            aliases = [n for n in person.names if n != person.canonical]
            for a in aliases:
                assert_no_emails(a, "person alias")
            if aliases:
                item.aliases.set(language="en", values=aliases)
            add_planned(self.api, item, Planned("wdt:P31", "wd:Q5"))
            for o in sorted(person.orcids):
                add_planned(self.api, item, Planned("wdt:P496", o))
            item.add_claim(PROFILE_TYPE, PERSON_PROFILE)
            person.qid = item.write().id
            if person.possible_duplicates:
                log.info("Created %s for %s; possible duplicates: %s",
                         person.qid, person.canonical, person.possible_duplicates)

    def _attach_authors(self, pub, key: str) -> None:
        """Give a new publication's authors the items of the people identified here."""
        citing = {pkg for pkg, keys in self.cited.items() if key in keys}
        for author in getattr(pub, "authors", []) or []:
            for person in self.persons:
                if (person.qid and person.packages & citing
                        and norm_name(author.name) in {norm_name(n) for n in person.names}):
                    author._QID = person.qid

    # -- reporting -------------------------------------------------------------------------

    def summary(self, results: dict | None = None) -> dict:
        """What was (or, before push, would be) written. Contains no e-mail address."""
        report = {
            "registry_commit": self.registry_sha,
            "packages": {jp.name: {"action": jp.action, "qid": jp.qid, "matched_by": jp.matched_by,
                                   "versions": len(jp.versions), "notes": jp.notes}
                         for jp in self.packages},
            "persons": [{"name": p.canonical, "action": p.action or "name string", "qid": p.qid,
                         "linked_by": p.linked_by, "packages": sorted(p.packages),
                         "possible_duplicates": p.possible_duplicates} for p in self.persons],
            "new_publications": sorted({getattr(p, "title", "") for p in self.publications.values()}),
        }
        if results is not None:
            report["results"] = results
        assert_no_emails(json.dumps(report), "report")
        return report
