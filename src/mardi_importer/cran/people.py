"""People named by CRAN packages, and the person items that stand for them.

Read only: used by the CRAN importer (:mod:`mardi_importer.cran.resolve`) to
recognise the people of a package across packages and in the Wikibase.

1. **People.** Every person named by a package on CRAN (from
   :mod:`mardi_importer.cran.authors`) is a mention. Mentions are joined into
   one person by the same ORCID, the same e-mail address, or as a package's
   maintainer and its ``cre`` author, each only when the names agree (a
   DESCRIPTION file may carry someone else's ORCID). A name alone never joins
   mentions.
2. **Items.** Each person item linked to an R package (*author*, *maintained
   by*) is matched to a mention of that package with an agreeing name, and so to
   a person. An item carrying an ORCID is also matched to the person with it.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Callable

from .authors import Entry

log = logging.getLogger("CRANlogger")

PROPERTY_LABELS = {"cran": "CRAN project", "author": "author", "maintainer": "maintained by",
                   "orcid": "ORCID iD"}
# Letters NFKD does not decompose, and German umlauts as written without them (Grün, Gruen).
_TRANSLIT = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "Ä": "Ae", "Ö": "Oe", "Ü": "Ue", "ß": "ss",
                           "ø": "o", "Ø": "O", "ł": "l", "Ł": "L", "đ": "d", "Đ": "D", "æ": "ae",
                           "Æ": "Ae", "œ": "oe", "Œ": "Oe", "ı": "i", "þ": "th", "ð": "d"})
_TITLES = {"mr", "mrs", "ms", "miss", "dr", "prof", "professor", "sir", "phd", "md", "jr", "sr"}


# -- names -----------------------------------------------------------------------------

def norm(name: str) -> str:
    name = unicodedata.normalize("NFKD", unicodedata.normalize("NFC", name or "").translate(_TRANSLIT))
    name = "".join(c for c in name if not unicodedata.combining(c)).casefold()
    words = re.sub(r"[.\-'’`´,_]", " ", name).split()
    return " ".join(w for w in words if w not in _TITLES) or " ".join(words)


def compatible(name: str, other: str, family: str | None = None) -> bool:
    """Could ``other`` (an item label, another spelling) be the person called ``name``?

    The family name (``family``, or the last word of ``name``) must appear in
    ``other``; the first remaining names must agree: equal, one a prefix of the
    other (Tim, Timothy) or an initial. Word order does not matter ("Brault
    Vincent"). A one-word name agrees only with the same word.
    """
    a, b = norm(name).split(), norm(other).split()
    if not a or not b:
        return False
    fam = norm(family).split() if family else a[-1:]
    if not fam or not all(t in b for t in fam):
        return False
    ra, rb = [t for t in a if t not in fam], [t for t in b if t not in fam]
    if not ra or not rb:
        return not ra and not rb
    fa, fb = ra[0], rb[0]
    if len(fa) == 1 or len(fb) == 1:
        return fa[0] == fb[0]
    return fa.startswith(fb) or fb.startswith(fa)


def agree(a: Entry, b: Entry) -> bool:
    return compatible(a.name, b.name, a.family) or compatible(b.name, a.name, b.family)


def same_name(a: str, b: str) -> bool:
    """First and last name equal (in either order): no initials, no prefixes."""
    x, y = norm(a).split(), norm(b).split()
    return bool(x and y) and ({x[0], x[-1]} == {y[0], y[-1]})


# -- people ----------------------------------------------------------------------------

class _UF:
    def __init__(self):
        self.parent: dict = {}

    def find(self, k):
        self.parent.setdefault(k, k)
        while self.parent[k] != k:
            self.parent[k] = self.parent[self.parent[k]]
            k = self.parent[k]
        return k

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb, key=str)] = min(ra, rb, key=str)


@dataclass
class Person:
    id: str
    entries: list[Entry]
    joined_by: set[str] = field(default_factory=set)      # "orcid" | "email" | "maintainer"
    shared_id_conflicts: int = 0        # ORCID or address also given for a differently named person

    @property
    def orcids(self) -> set[str]:
        return {e.orcid for e in self.entries if e.orcid}

    @property
    def packages(self) -> set[str]:
        return {e.package for e in self.entries}

    @property
    def name(self) -> str:
        """Most frequent full name, Authors@R spellings first."""
        names = Counter(e.name for e in self.entries if len(e.name.split()) > 1) \
            or Counter(e.name for e in self.entries)
        structured = Counter(e.name for e in self.entries if e.source == "Authors@R" and e.family)
        return (structured or names).most_common(1)[0][0]

    @property
    def names(self) -> set[str]:
        return {e.name for e in self.entries}


def cluster(entries: list[Entry]) -> tuple[list[Person], dict[int, Person]]:
    """People named by the packages, and the person of each mention (by index)."""
    idx = [i for i, e in enumerate(entries) if e.kind == "person"]
    uf, reasons = _UF(), []
    by_orcid, by_email, by_pkg = defaultdict(list), defaultdict(list), defaultdict(list)
    for i in idx:
        uf.find(i)
        e = entries[i]
        if e.orcid:
            by_orcid[e.orcid].append(i)
        if e.email:
            by_email[e.email].append(i)
        by_pkg[e.package].append(i)
    conflicts = Counter()
    for ids in by_orcid.values():
        for j in ids[1:]:
            if agree(entries[ids[0]], entries[j]):
                uf.union(ids[0], j)
                reasons.append((ids[0], "orcid"))
            else:
                conflicts[ids[0]] += 1
    for ids in by_email.values():
        for j in ids[1:]:
            if agree(entries[ids[0]], entries[j]):
                uf.union(ids[0], j)
                reasons.append((ids[0], "email"))
            else:
                conflicts[ids[0]] += 1
    for ids in by_pkg.values():
        mts = [i for i in ids if entries[i].source == "Maintainer"]
        cres = [i for i in ids if entries[i].source != "Maintainer" and "cre" in entries[i].roles]
        for m in mts:
            for c in cres:
                if agree(entries[m], entries[c]):
                    uf.union(m, c)
                    reasons.append((m, "maintainer"))

    groups: dict[int, list[int]] = defaultdict(list)
    for i in idx:
        groups[uf.find(i)].append(i)
    why: dict[int, set[str]] = defaultdict(set)
    for i, r in reasons:
        why[uf.find(i)].add(r)
    bad: Counter = Counter()
    for i, n in conflicts.items():
        bad[uf.find(i)] += n
    people, of = [], {}
    for n, (root, members) in enumerate(sorted(groups.items(), key=lambda kv: min(kv[1])), 1):
        p = Person(f"person-{n:05d}", [entries[i] for i in members], why[root], bad[root])
        people.append(p)
        for i in members:
            of[i] = p
    return people, of


# -- the Wikibase ----------------------------------------------------------------------

@dataclass
class GraphPerson:
    qid: str
    label: str
    orcids: set[str] = field(default_factory=set)
    links: list[tuple[str, str]] = field(default_factory=list)     # (package, "author" | "maintainer")


def _prefixes(entity_base: str | None) -> str:
    """Prefix declarations; none when the endpoint declares them itself (the importer's WDQS)."""
    if not entity_base:
        return ""
    base = entity_base.rstrip("/").rsplit("/entity", 1)[0]
    return (f"PREFIX wd: <{base}/entity/> PREFIX wdt: <{base}/prop/direct/> "
            "PREFIX wikibase: <http://wikiba.se/ontology#> "
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> ")


def read_graph(sparql: Callable[[str], list[dict]], entity_base: str | None = None) -> dict[str, GraphPerson]:
    """Person items linked to R packages, with their labels, ORCIDs and links."""
    pre = _prefixes(entity_base)
    labels = " ".join(f'"{v}"@en' for v in PROPERTY_LABELS.values())
    found = {r["l"]: r["p"].rsplit("/", 1)[-1] for r in sparql(
        pre + f"SELECT ?p ?l WHERE {{ ?p a wikibase:Property ; rdfs:label ?l . VALUES ?l {{ {labels} }} }}")}
    pid = {k: found.get(v) for k, v in PROPERTY_LABELS.items()}
    if not all(pid.values()):
        raise RuntimeError(f"properties not found by label: {pid}")
    cran, aut, mnt, orc = pid["cran"], pid["author"], pid["maintainer"], pid["orcid"]
    linked = f"{{ SELECT DISTINCT ?person WHERE {{ ?pkg wdt:{cran} ?n . ?pkg wdt:{aut}|wdt:{mnt} ?person }} }}"

    people: dict[str, GraphPerson] = {}
    q = lambda u: u.rsplit("/", 1)[-1]
    for r in sparql(pre + f"SELECT ?person ?label ?orcid WHERE {{ {linked} "
                          f"OPTIONAL {{ ?person rdfs:label ?label FILTER(LANG(?label) = \"en\") }} "
                          f"OPTIONAL {{ ?person wdt:{orc} ?orcid }} }}"):
        p = people.setdefault(q(r["person"]), GraphPerson(q(r["person"]), r.get("label", "")))
        if r.get("orcid"):
            p.orcids.add(r["orcid"])
    for r in sparql(pre + f"SELECT ?name ?role ?person WHERE {{ ?pkg wdt:{cran} ?name . "
                          f"{{ ?pkg wdt:{aut} ?person BIND(\"author\" AS ?role) }} UNION "
                          f"{{ ?pkg wdt:{mnt} ?person BIND(\"maintainer\" AS ?role) }} }}"):
        if (p := people.get(q(r["person"]))):
            p.links.append((r["name"], r["role"]))
    return people


# -- matching items to people ------------------------------------------------------------

@dataclass
class Match:
    person: Person
    how: str                          # "ORCID" | "maintainer of X" | "author of X"


def match_items(graph: dict[str, GraphPerson], entries: list[Entry],
                of: dict[int, Person]) -> tuple[dict[str, list[Match]], dict[str, list[str]]]:
    """The people each item is matched to, and why items could not be matched."""
    by_pkg: dict[str, list[int]] = defaultdict(list)
    by_orcid: dict[str, list[Entry]] = defaultdict(list)
    for i, e in enumerate(entries):
        by_pkg[e.package].append(i)
        if i in of and e.orcid:
            by_orcid[e.orcid].append(i)
    matches: dict[str, list[Match]] = defaultdict(list)
    unmatched: dict[str, list[str]] = defaultdict(list)
    for item in graph.values():
        for o in sorted(item.orcids):
            people = {of[i].id: of[i] for i in by_orcid.get(o, [])
                      if compatible(entries[i].name, item.label, entries[i].family)}
            if len(people) == 1:
                matches[item.qid].append(Match(next(iter(people.values())), "ORCID"))
        for package, role in item.links:
            if package not in by_pkg:
                unmatched[item.qid].append(f"{role} of {package}: package not on CRAN")
                continue
            cands = [i for i in by_pkg[package] if i in of and
                     (role == "author" or entries[i].is_maintainer)]
            agreeing = [i for i in cands if compatible(entries[i].name, item.label, entries[i].family)]
            exact = [i for i in agreeing if norm(entries[i].name) == norm(item.label)]
            people = {of[i].id: of[i] for i in (exact or agreeing)}
            if len(people) == 1:
                matches[item.qid].append(Match(next(iter(people.values())), f"{role} of {package}"))
            elif not people:
                unmatched[item.qid].append(f"{role} of {package}: no {role} of that name on CRAN now")
            else:
                unmatched[item.qid].append(f"{role} of {package}: several people of that name")
    return matches, unmatched
