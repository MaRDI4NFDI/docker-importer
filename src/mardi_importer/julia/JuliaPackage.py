"""One Julia package: the statements it should have, and writing them.

Statements are first planned as plain data (:class:`Planned`) so the rules can be
tested without a Wikibase; :meth:`JuliaPackage.write` then turns them into claims.

Every statement carries a reference — *stated in* the Julia General registry for
registry facts, *reference URL* of the exact file it was read from (pinned to a
commit), and *retrieved*. Properties and items are given as Wikidata IDs and
resolved to local IDs by mardiclient.

Every registered (non-yanked) version becomes a *software version identifier*
statement qualified with its *publication date*, the day the version was
registered in General, as the CRAN source does for R packages; its reference URL
is the registering commit.

An existing item is only ever added to (never overwritten or pruned): a value
already present is skipped, and a single-valued property that already holds a
different value is left alone and reported as a conflict.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .metadata import LICENSES, RepoMetadata
from .people import Mention, Person, assert_no_emails
from .registry import GENERAL_COMMIT_URL, registry_url, repo_key

log = logging.getLogger("JuliaLogger")

# Wikidata properties used by the Julia source (see wikidata_entities.txt).
INSTANCE_OF = "wdt:P31"
PROGRAMMED_IN = "wdt:P277"
SOURCE_REPOSITORY = "wdt:P1324"
VERSION = "wdt:P348"
PUBLICATION_DATE = "wdt:P577"
LICENSE = "wdt:P275"
AUTHOR = "wdt:P50"
AUTHOR_NAME_STRING = "wdt:P2093"
OBJECT_NAMED_AS = "wdt:P1932"
DEPENDS_ON = "wdt:P1547"
DESCRIBED_BY_SOURCE = "wdt:P1343"
STATED_IN = "wdt:P248"
REFERENCE_URL = "wdt:P854"
RETRIEVED = "wdt:P813"
# Local properties and items (see new_entities.json).
PACKAGE_NAME = "Julia General registry package name"
WIKIDATA_QID = "Wikidata QID"
PROFILE_TYPE = "MaRDI profile type"
JULIA = "wd:Q2613697"

# Properties an update may only fill when empty.
SINGLE_VALUED = frozenset({INSTANCE_OF, PROGRAMMED_IN, SOURCE_REPOSITORY, LICENSE,
                           PACKAGE_NAME, WIKIDATA_QID})


@dataclass
class Planned:
    """One statement to write: property, value, qualifiers, and its reference.

    A qualifier is ``(property, value)`` or ``(property, value, claim kwargs)``,
    the kwargs going to ``get_claim`` (e.g. a time precision).
    """
    prop: str
    value: Any
    ref_url: str | None
    retrieved: str | None
    stated_in_registry: bool = False
    qualifiers: list[tuple] = field(default_factory=list)

    @property
    def referenced(self) -> bool:
        return bool(self.ref_url and self.retrieved)


def time_value(day: str) -> str:
    return f"+{day}T00:00:00Z"


@dataclass
class JuliaPackage:
    name: str
    uuid: str
    repo: str
    path: str
    registry_sha: str
    retrieved: str
    metadata: RepoMetadata
    subdir: str | None = None
    version: str | None = None        # latest live version
    versions: list[tuple[str, str | None, str | None]] = field(default_factory=list)
    # every live version: (version, registration day, registering commit); the
    # last two are None for versions older than the registry's history
    action: str = "create"            # create | update | skip
    qid: str | None = None            # existing item (update) or the created one
    matched_by: str | None = None
    wikidata_qid: str | None = None
    wikidata_retrieved: str | None = None
    papers: list[str] = field(default_factory=list)    # QIDs of cited publications
    authors: list[tuple[Mention, Person]] = field(default_factory=list)
    conflicts: list[dict] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.name}.jl"

    # -- planning (pure) -------------------------------------------------------

    def plan(self, julia_package_class: str) -> list[Planned]:
        """Every statement for this package except dependencies (planned later)."""
        pkg = {"path": self.path}
        reg = registry_url(self.registry_sha, pkg, "Package.toml")
        md = self.metadata
        out = [
            Planned(PACKAGE_NAME, self.name, reg, self.retrieved, True),
            Planned(INSTANCE_OF, julia_package_class, reg, self.retrieved, True),
            Planned(PROGRAMMED_IN, JULIA, reg, self.retrieved, True),
            Planned(SOURCE_REPOSITORY, self.repo, reg, self.retrieved, True),
        ]
        versions = self.versions or ([(self.version, None, None)] if self.version else [])
        for v, day, sha in versions:
            url = (GENERAL_COMMIT_URL.format(sha=sha) if sha
                   else registry_url(self.registry_sha, pkg, "Versions.toml"))
            quals = [(PUBLICATION_DATE, time_value(day), {"precision": 11})] if day else []
            out.append(Planned(VERSION, v, url, self.retrieved, True, quals))
        if md.license and md.license_url:
            out.append(Planned(LICENSE, LICENSES[md.license], md.license_url, md.retrieved))
        if md.citation_url:
            out += [Planned(DESCRIBED_BY_SOURCE, q, md.citation_url, md.retrieved)
                    for q in self.papers]
        if self.wikidata_qid:
            out.append(Planned(WIKIDATA_QID, self.wikidata_qid,
                               f"https://www.wikidata.org/wiki/{self.wikidata_qid}",
                               self.wikidata_retrieved))
        out += self.plan_authors()
        return out

    def plan_authors(self) -> list[Planned]:
        """One statement per person: an item where the person has one, else a name string.

        The name as the source states it is kept in *object named as*.
        """
        out, done = [], set()
        for m, person in sorted(self.authors, key=lambda mp: mp[0].orcid is None):
            if person.id in done:
                continue
            done.add(person.id)
            if person.qid:
                out.append(Planned(AUTHOR, person.qid, m.ref_url, m.retrieved,
                                   qualifiers=[(OBJECT_NAMED_AS, m.name)]))
            else:
                out.append(Planned(AUTHOR_NAME_STRING, m.name, m.ref_url, m.retrieved))
        return out

    def plan_dependencies(self, qid_of: dict[str, str]) -> list[Planned]:
        """``depends on software`` to packages that exist or were created in this run."""
        md = self.metadata
        if not md.project_url:
            return []
        return [Planned(DEPENDS_ON, qid_of[d], md.project_url, md.retrieved)
                for d in md.deps if d in qid_of]

    # -- update rule (pure) -------------------------------------------------------

    @staticmethod
    def merge(planned: list[Planned], existing: dict[str, list[str]]) -> tuple[list[Planned], list[dict]]:
        """Add-only: drop what is already there; keep single-valued conflicts out."""
        add, conflicts = [], []
        for st in planned:
            have = existing.get(st.prop, [])
            val = str(st.value)
            if val in have or (st.prop == SOURCE_REPOSITORY and
                               repo_key(val) in {repo_key(h) for h in have}):
                continue
            if have and st.prop in SINGLE_VALUED:
                conflicts.append({"property": st.prop, "planned": val, "existing": have})
                continue
            add.append(st)
        return add, conflicts

    # -- writing --------------------------------------------------------------------

    def write(self, api, planned: list[Planned], registry_item: str) -> str | None:
        """Create the item, or add the planned statements to the existing one."""
        planned = resolve_items(api, planned)
        if self.qid:
            item = api.item.get(entity_id=self.qid)
            planned, conflicts = self.merge(planned, existing_values(api, item, planned))
            self.conflicts += conflicts
        else:
            item = api.item.new()
            item.labels.set(language="en", value=self.label)
            item.descriptions.set(language="en", value="Julia package")
            item.aliases.set(language="en", values=[self.name])
            planned = planned + [Planned(PROFILE_TYPE, "MaRDI software profile", None, None)]
        if not planned:
            return self.qid
        for st in planned:
            add_planned(api, item, st, registry_item)
        written = item.write()
        self.qid = written.id
        return self.qid


def resolve_items(api, planned: list[Planned]) -> list[Planned]:
    """Replace ``wd:Q…`` values by local QIDs, so they compare with existing claims."""
    out = []
    for st in planned:
        if isinstance(st.value, str) and st.value.startswith("wd:"):
            local = api.get_local_id_by_label(st.value, "item")
            local = local[0] if isinstance(local, list) else local
            st = Planned(st.prop, local, st.ref_url, st.retrieved, st.stated_in_registry, st.qualifiers)
        out.append(st)
    return out


def _pid(api, prop: str) -> str | None:
    pid = api.get_local_id_by_label(prop, "property")
    return pid[0] if isinstance(pid, list) else pid


def existing_values(api, item, planned: list[Planned]) -> dict[str, list[str]]:
    """Current values of the planned properties, including URL values.

    ``MardiItem.get_value`` skips URL-typed properties, which would make an
    existing repository look absent and be added twice.

    A version without its publication date counts as absent, so that planning it
    again completes the existing statement (append-or-replace keeps the value and
    adds the qualifier) instead of skipping it.
    """
    claims = item.get_json().get("claims", {})
    props = {st.prop for st in planned}
    date_pid = _pid(api, PUBLICATION_DATE) if VERSION in props else None
    out: dict[str, list[str]] = {}
    for prop in props:
        pid = _pid(api, prop)
        values = []
        for c in claims.get(pid, []) if pid else []:
            dv = c.get("mainsnak", {}).get("datavalue")
            if not dv:
                continue
            if prop == VERSION and date_pid and date_pid not in c.get("qualifiers", {}):
                continue
            v = dv["value"]
            values.append(v["id"] if isinstance(v, dict) and "id" in v
                          else v.get("time") if isinstance(v, dict) else str(v))
        out[prop] = values
    return out


def _wbi_models():
    """WikibaseIntegrator's qualifier and reference containers (replaceable in tests)."""
    from wikibaseintegrator.models import Qualifiers, Reference, References
    return Qualifiers, Reference, References


def add_planned(api, item, st: Planned, registry_item: str) -> None:
    """Add one planned statement with its qualifiers and reference."""
    Qualifiers, Reference, References = _wbi_models()

    value = st.value
    if isinstance(value, str):
        assert_no_emails(value, f"{st.prop} of {item.labels.get('en')}")
    kwargs: dict[str, Any] = {}
    if st.qualifiers:
        q = Qualifiers()
        for prop, v, *extra in st.qualifiers:
            assert_no_emails(str(v), f"qualifier {prop}")
            q.add(api.get_claim(prop, v, **(extra[0] if extra else {})))
        kwargs["qualifiers"] = q
    if st.referenced:
        ref = Reference()
        if st.stated_in_registry:
            ref.add(api.get_claim(STATED_IN, registry_item))
        ref.add(api.get_claim(REFERENCE_URL, st.ref_url))
        ref.add(api.get_claim(RETRIEVED, time_value(st.retrieved), precision=11))
        refs = References()
        refs.add(ref)
        kwargs["references"] = refs
    item.add_claim(st.prop, value, **kwargs)
