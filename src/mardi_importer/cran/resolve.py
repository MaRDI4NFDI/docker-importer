"""Which person item stands for a person named by a CRAN package.

Used by :class:`mardi_importer.cran.RPackage.RPackage` for authors and
maintainers. The rules keep what was fixed by hand in the Wikibase and do not
repeat the old importer's mistakes:

1. **An item the package already links** is kept when its label agrees with
   the name: a link corrected by hand stays as it is.
2. **A verified ORCID.** A DESCRIPTION file may carry someone else's ORCID, so an
   ORCID is used only if the name registered for it agrees with the name next to
   it (a private record cannot be checked and is accepted), and it finds an item
   only if that item's label agrees too.
3. **The same person elsewhere on CRAN**: joined across packages by ORCID or
   e-mail address (:func:`mardi_importer.cran.people.cluster`), whose item is
   known from the links of the other packages. Links to merged items lead to
   the item they were merged into.
4. Otherwise a **new item** for a maintainer, someone with a verified ORCID, or
   someone named by several packages; else an **author name string**.

Existing person items are never relabelled or given aliases. ``ORPHANED`` has no
maintainer; an organisation is a name string. E-mail addresses only join
mentions; they are never written.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, Protocol

from .authors import Entry
from .people import GraphPerson, Person, cluster, compatible, match_items, same_name

log = logging.getLogger("CRANlogger")

ORCID_API = "https://pub.orcid.org/v3.0"
USER_AGENT = "mardi-importer (https://portal.mardi4nfdi.de; CRAN import)"


def agrees(name: str, label: str, family: str | None = None) -> bool:
    return bool(label) and (compatible(name, label, family) or compatible(label, name))


class Wiki(Protocol):
    def lookup(self, qid: str) -> tuple[str, str] | None:
        """(QID, English label) of the item, following a redirect; None if it is gone."""

    def items_with_orcid(self, orcid: str) -> list[str]: ...

    def create_person(self, name: str, orcid: str | None) -> str: ...


class OrcidRegistry:
    """Names registered for ORCIDs, from the public ORCID API (cached; None if private or unknown)."""

    def __init__(self, session=None):
        self.session, self.cache = session, {}

    def name(self, orcid: str) -> str | None:
        if orcid not in self.cache:
            self.cache[orcid] = self._fetch(orcid)
        return self.cache[orcid]

    def _fetch(self, orcid: str) -> str | None:
        import requests
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({"Accept": "application/json", "User-Agent": USER_AGENT})
        try:
            r = self.session.get(f"{ORCID_API}/{orcid}/person", timeout=30)
            if r.status_code != 200:
                return None
            n = r.json().get("name") or {}
        except Exception as exc:  # the registry being down must not stop an import
            log.warning("ORCID %s not checked: %s", orcid, exc)
            return None
        given = (n.get("given-names") or {}).get("value")
        family = (n.get("family-name") or {}).get("value")
        return " ".join(filter(None, (given, family))) or None


@dataclass
class Resolved:
    kind: str                  # "item" | "string" | "none"
    name: str
    qid: str | None = None
    how: str = ""


@dataclass
class People:
    """Resolution of the people named by CRAN packages, for one import run."""
    wiki: Wiki
    registry: OrcidRegistry
    person_of: dict[int, Person] = field(default_factory=dict)       # id(entry) → person
    items_of_person: dict[str, set[str]] = field(default_factory=dict)   # person id → QIDs linked for them
    created_by_orcid: dict[str, str] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    @classmethod
    def build(cls, wiki: Wiki, entries: list[Entry], graph: dict[str, GraphPerson],
              registry: OrcidRegistry | None = None) -> "People":
        """Join the mentions of all packages into people, and find their items from the graph."""
        people, of = cluster(entries)
        matches, _ = match_items(graph, entries, of)
        items: dict[str, set[str]] = {}
        for qid, ms in matches.items():
            ps = {m.person.id: m.person for m in ms}
            names = [p.name for p in ps.values()]
            # An item matched to differently named people (a conflation) is no evidence;
            # one person CRAN knows under two addresses is.
            if all(same_name(a, b) for a in names for b in names):
                for pid in ps:
                    items.setdefault(pid, set()).add(qid)
        return cls(wiki, registry or OrcidRegistry(), {id(entries[i]): p for i, p in of.items()}, items)

    def verified_orcid(self, entry: Entry) -> str | None:
        if not entry.orcid:
            return None
        registered = self.registry.name(entry.orcid)
        if registered and not agrees(entry.name, registered, entry.family):
            self.notes.append(f"{entry.package}: ORCID {entry.orcid} of {entry.name!r} is registered to "
                              f"{registered!r}; not used")
            return None
        return entry.orcid

    def resolve(self, entry: Entry, linked: Iterable[tuple[str, str]] = ()) -> Resolved:
        """The item (or name string) for ``entry``; ``linked`` are the (QID, label) the package links now."""
        if entry.kind == "orphaned":
            return Resolved("none", entry.name, how="orphaned")
        if entry.kind == "organisation":
            return Resolved("string", entry.name, how="organisation")

        same = {q for q, lab in linked if agrees(entry.name, lab, entry.family)}
        if len(same) == 1:
            return Resolved("item", entry.name, same.pop(), "already linked")

        orcid = self.verified_orcid(entry)
        if orcid:
            if orcid in self.created_by_orcid:
                return Resolved("item", entry.name, self.created_by_orcid[orcid], "created in this run")
            found = [hit for q in self.wiki.items_with_orcid(orcid) if (hit := self.wiki.lookup(q))]
            good = {q for q, lab in found if agrees(entry.name, lab, entry.family)}
            if len(good) == 1:
                return Resolved("item", entry.name, good.pop(), "ORCID")
            if found and not good:
                self.notes.append(f"{entry.package}: ORCID {orcid} of {entry.name!r} is on "
                                  f"{', '.join(f'{q} {lab!r}' for q, lab in found)}; not used")
                orcid = None

        person = self.person_of.get(id(entry))
        if person and person.id in self.items_of_person:
            # several items may be linked for one person, merged since into one
            hits = {hit for q in self.items_of_person[person.id] if (hit := self.wiki.lookup(q))}
            good = {q for q, lab in hits if agrees(entry.name, lab, entry.family)}
            if len(good) == 1:
                return Resolved("item", entry.name, good.pop(), "same person on CRAN")

        several_packages = person is not None and len(person.packages) > 1
        if orcid or entry.source == "Maintainer" or several_packages:
            name = person.name if person else entry.name
            qid = self.wiki.create_person(name, orcid)
            if orcid:
                self.created_by_orcid[orcid] = qid
            if person:
                self.items_of_person[person.id] = {qid}
            return Resolved("item", name, qid, "created")
        return Resolved("string", entry.name, how="name only")


class MardiWiki:
    """:class:`Wiki` over a MardiClient."""

    def __init__(self, api):
        self.api, self.cache = api, {}

    def lookup(self, qid: str) -> tuple[str, str] | None:
        if qid not in self.cache:
            try:
                item = self.api.item.get(entity_id=qid)          # a redirect gives its target
                lab = item.labels.get("en")
                self.cache[qid] = (item.id, str(lab) if lab else "")
            except Exception:
                self.cache[qid] = None
        return self.cache[qid]

    def items_with_orcid(self, orcid: str) -> list[str]:
        return self.api.search_entity_by_value("wdt:P496", orcid)

    def create_person(self, name: str, orcid: str | None) -> str:
        item = self.api.item.new()
        item.labels.set(language="en", value=name)
        item.add_claim("wdt:P31", "wd:Q5")
        item.add_claim("MaRDI profile type", "MaRDI person profile")
        if orcid:
            item.add_claim("wdt:P496", orcid)
        return item.write().id
