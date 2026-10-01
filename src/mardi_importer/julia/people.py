"""Grouping Julia package authors into people.

Author mentions come from ``Project.toml`` and ``CITATION.cff``, sometimes with
an e-mail address or an ORCID. Mentions that are demonstrably the same person
form one cluster across packages, so that one person is one item.

E-mail addresses are a **clustering signal only**. They join mentions and are
then dropped: a :class:`Person` has no address field, and
:func:`assert_no_emails` is run over every value before it is written.

Joining evidence: same ORCID; same e-mail address; same full name
(case-folded). Whether a cluster reuses an existing item or gets a new one is
decided in :mod:`mardi_importer.julia.JuliaSource`.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

from .metadata import EMAIL


def norm_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip().casefold()


@dataclass
class Mention:
    package: str
    name: str                      # as stated in the source
    ref_url: str                   # file the name was read from
    retrieved: str
    email: str | None = field(default=None, repr=False)
    orcid: str | None = None


@dataclass
class Person:
    id: str                        # batch-local, stable for identical input
    names: Counter
    packages: set[str]
    orcids: set[str]
    evidence: set[str]             # kinds only: {"orcid", "email", "name"}
    first_ref: tuple[str, str] = ("", "")
    qid: str | None = None         # existing item, or set once created
    linked_by: str | None = None   # "orcid" | "cited paper author"
    action: str | None = None      # "link" | "create"; None → author name string
    new_papers: set[str] = field(default_factory=set)   # cited papers created in this run
    possible_duplicates: list[str] = field(default_factory=list)   # logged, never merged

    @property
    def canonical(self) -> str:
        """Most frequent spelling that looks like a full name, else the most frequent."""
        full = [(n, c) for n, c in self.names.most_common() if " " in n]
        return (full or self.names.most_common())[0][0]

    @property
    def is_handle(self) -> bool:
        """Known only by a single token such as a GitHub handle ("jClugstor")."""
        return " " not in self.canonical.strip()


class People:
    def __init__(self) -> None:
        self.mentions: list[Mention] = []
        self._parent: dict[str, str] = {}

    def _find(self, k: str) -> str:
        self._parent.setdefault(k, k)
        while self._parent[k] != k:
            self._parent[k] = self._parent[self._parent[k]]
            k = self._parent[k]
        return k

    def _union(self, a: str, b: str) -> None:
        ra, rb = self._find(a), self._find(b)
        if ra != rb:
            self._parent[max(ra, rb)] = min(ra, rb)

    def add(self, m: Mention) -> None:
        node = f"m:{len(self.mentions)}"
        self.mentions.append(m)
        self._union(node, "n:" + norm_name(m.name))
        if m.email:
            self._union(node, "e:" + m.email)
        if m.orcid:
            self._union(node, "o:" + m.orcid)

    def clusters(self) -> tuple[list[Person], dict[int, Person]]:
        """People sorted by canonical name, and the person of each mention."""
        groups: dict[str, list[int]] = {}
        for i in range(len(self.mentions)):
            groups.setdefault(self._find(f"m:{i}"), []).append(i)
        built = []
        for idxs in groups.values():
            ms = [self.mentions[i] for i in idxs]
            keys = Counter()
            for m in ms:
                keys.update({("name", norm_name(m.name))})
                keys.update({("email", m.email)} if m.email else set())
                keys.update({("orcid", m.orcid)} if m.orcid else set())
            evidence = {kind for (kind, _), n in keys.items() if n > 1}
            if any(m.orcid for m in ms):
                evidence.add("orcid")
            built.append((idxs, Person(
                id="", names=Counter(m.name for m in ms), packages={m.package for m in ms},
                orcids={m.orcid for m in ms if m.orcid}, evidence=evidence,
                first_ref=(ms[0].ref_url, ms[0].retrieved))))
        built.sort(key=lambda b: (b[1].canonical.casefold(), min(b[0])))
        by_mention: dict[int, Person] = {}
        for n, (idxs, person) in enumerate(built, 1):
            person.id = f"person-{n:04d}"
            for i in idxs:
                by_mention[i] = person
        return [p for _, p in built], by_mention


def same_person_possible(ours: str, theirs: str) -> bool:
    """Could an item labelled ``theirs`` be the person we call ``ours``?

    Used only to log possible duplicates. Same family name, and compatible first
    names: equal, one a prefix of the other (Tim/Timothy), or either an initial.
    """
    a, b = ours.split(), theirs.split()
    if len(a) < 2 or len(b) < 2 or norm_name(a[-1]) != norm_name(b[-1]):
        return False
    fa, fb = a[0].rstrip(".").casefold(), b[0].rstrip(".").casefold()
    if len(fa) == 1 or len(fb) == 1:
        return fa[0] == fb[0]
    return fa.startswith(fb) or fb.startswith(fa)


def assert_no_emails(value: str, where: str) -> None:
    """Fail closed: a value that would carry an e-mail address is not written."""
    if EMAIL.search(value or ""):
        raise ValueError(f"refusing to write {where}: it contains an e-mail address")
