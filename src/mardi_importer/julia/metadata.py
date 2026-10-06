"""Metadata read from each Julia package's own repository.

Files are read at a commit pinned with ``git ls-remote`` through
raw.githubusercontent.com — no GitHub API, so no rate limit — and every fact
keeps the URL it came from, for its reference.

E-mail addresses found in ``Project.toml`` are returned to the caller for
clustering people only. They must never be written to the knowledge graph;
see :mod:`mardi_importer.julia.people`.
"""

from __future__ import annotations

import re
import subprocess
import tomllib
from dataclasses import dataclass, field

import requests

from .registry import repo_key

EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
ORCID = re.compile(r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]")

LICENSE_FILES = ("LICENSE", "LICENSE.md", "LICENSE.txt", "LICENCE", "LICENCE.md",
                 "COPYING", "COPYING.md")
CITATION_FILES = ("CITATION.bib", "CITATION.cff")

# SPDX id → Wikidata item, resolved to the local item by mardiclient.
LICENSES = {
    "MIT": "wd:Q334661",
    "BSD-3-Clause": "wd:Q18491847",
    "BSD-2-Clause": "wd:Q18517294",
    "Apache-2.0": "wd:Q13785927",
    "MPL-2.0": "wd:Q25428413",
    "GPL-3.0": "wd:Q10513445",
    "GPL-2.0": "wd:Q10513450",
    "LGPL-3.0": "wd:Q18534393",
    "LGPL-2.1": "wd:Q18534390",
    "GPL-3.0-or-later": "wd:Q27016754",
    "GPL-2.0-or-later": "wd:Q27016752",
    "LGPL-3.0-or-later": "wd:Q27016762",
    "LGPL-2.1-or-later": "wd:Q27016757",
}


# -- licences -----------------------------------------------------------------

_LICENSE_MARKERS = [  # (family, pattern) — matched against normalised lower-case text
    ("MPL", r"mozilla public license"),
    ("Apache", r"apache license"),
    ("AGPL", r"gnu affero"),
    ("LGPL", r"gnu (?:lesser|library) general public license|\blgpl"),
    ("GPL", r"gnu (?:general )?public license|\bgpl\b|\bgplv?\d"),
    ("BSD", r"\bbsd\b|redistribution and use in source and binary forms"),
    ("MIT", r"\bmit (?:\"?expat\"? )?licen[cs]e|\bmit \"expat\"|\bexpat licen[cs]e|"
            r"permission is hereby granted, free of charge"),
]
_HEADER_CHARS = 400
# Copyleft licence texts name one another (the GPL mentions the LGPL and AGPL,
# the MPL mentions the GPL); mentions within this group are not a second licence.
_CROSS_REFERENCING = frozenset({"GPL", "LGPL", "AGPL", "MPL"})


def _license_markers(t: str) -> list[tuple[int, str]]:
    found = []
    for family, pat in _LICENSE_MARKERS:
        m = re.search(pat, t)
        if m:
            found.append((m.start(), family))
    return sorted(found)


def _mixed(families: set[str]) -> bool:
    return len(families - _CROSS_REFERENCING) + bool(families & _CROSS_REFERENCING) > 1


def classify_license(text: str) -> str | None:
    """SPDX id for a licence file, or None when unsure.

    A missing licence is cheap, a wrong one is not. Julia licence files often
    state the package's licence and then append the notices of bundled
    third-party code, so the first licence named decides; a file mixing licence
    families with none named in its opening returns None.
    """
    t = re.sub(r"[\s>*#`]+", " ", text.lower()).strip()
    markers = _license_markers(t)
    if not markers:
        return None
    pos, family = markers[0]
    if pos >= _HEADER_CHARS and _mixed({f for _, f in markers}):
        return None
    head = t[max(0, pos - 60):pos + 600]          # the licence statement itself
    block = t[max(0, pos - 60):pos + 2500]        # enough to hold a BSD clause list
    # "or later" only counts near the statement: the GPL's own appendix says it too.
    or_later = "any later version" in head or re.search(r"\d\.\d\+|\bv?\d\+", head)

    if family == "MIT":
        return "MIT"
    if family == "MPL":
        return "MPL-2.0" if "2.0" in head else None
    if family == "Apache":
        return "Apache-2.0" if "2.0" in head else None
    if family == "BSD":
        two = re.search(r"2-clause|two-clause|simplified", head)
        three = re.search(r"3-clause|three-clause|new bsd|revised bsd|modified bsd", head)
        if two or three:
            return "BSD-2-Clause" if two and (not three or two.start() < three.start()) else "BSD-3-Clause"
        return "BSD-3-Clause" if re.search(r"neither the name|endorse or promote", block) else "BSD-2-Clause"
    if family in ("GPL", "LGPL"):
        if family == "LGPL":
            ver = "3.0" if re.search(r"version 3|v3|3\.0|lgpl-?3", head) else \
                  "2.1" if re.search(r"version 2\.1|v2\.1|2\.1", head) else None
        else:
            ver = "3.0" if re.search(r"version 3|v3|3\.0|gpl-?3", head) else \
                  "2.0" if re.search(r"version 2|v2|2\.0|gpl-?2", head) else None
        if ver is None:
            return None
        spdx = f"{family}-{ver}"
        return spdx + "-or-later" if or_later and spdx + "-or-later" in LICENSES else spdx
    return None


# -- citations ------------------------------------------------------------------

_DOI_FIELDS = [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in (
    r"\bdoi\s*=\s*[{\"]\s*(?:https?://(?:dx\.)?doi\.org/)?(10\.\d{4,9}/[^\s\"{}]+)",   # BibTeX field
    r"https?://(?:dx\.)?doi\.org/(10\.\d{4,9}/[^\s\"'{}<>,]+)",                        # doi.org link
    r"^\s*doi\s*:\s*['\"]?(10\.\d{4,9}/[^\s'\"]+)",                                     # CFF key
    r"type\s*:\s*doi\s*\n\s*value\s*:\s*['\"]?(10\.\d{4,9}/[^\s'\"]+)",                 # CFF identifier
)]
_ARXIV = re.compile(
    r"(?:arxiv\.org/(?:abs|pdf)/|arxiv[.:]\s*|eprint\s*=\s*[{\"]\s*)(\d{4}\.\d{4,5})(?:v\d+)?",
    re.IGNORECASE)


def citation_ids(text: str) -> tuple[list[str], list[str]]:
    """DOIs (upper-cased, as stored in the knowledge graph) and new-style arXiv ids.

    DOIs are read only from DOI fields and doi.org links: a publisher URL that
    merely contains a DOI (``.../10.5334/jors.151/galley/245/download/``) is not one.
    """
    dois, arxiv = [], []
    for rx in _DOI_FIELDS:
        for d in rx.findall(text):
            d = d.rstrip(".;)/").upper()
            if d.startswith("10.48550/ARXIV."):
                a = d.split("ARXIV.", 1)[1]
                if a not in arxiv:
                    arxiv.append(a)
            elif d not in dois:
                dois.append(d)
    for a in _ARXIV.findall(text):
        if a not in arxiv:
            arxiv.append(a)
    return dois, arxiv


def is_zenodo(doi: str) -> bool:
    """A Zenodo DOI in a package's citation file is the software's own release."""
    return doi.upper().startswith("10.5281/ZENODO.")


# -- authors ---------------------------------------------------------------------

_CONTRIBUTORS = re.compile(
    r"^(?:and\s+)?(?:the\s+)?(?:other\s+|all\s+)?(?:[\w.\-]+\s+)?contributors?$", re.IGNORECASE)
_COMPANY_SUFFIX = re.compile(r"^(?:inc|ltd|llc|gmbh|co)\b\.?", re.IGNORECASE)


def parse_author_entries(authors) -> list[tuple[str, str | None]]:
    """(name, e-mail or None) per person in ``Project.toml`` authors.

    An address binds to the name immediately before it: in
    "A <a@x>, B and contributors" only A has one. "contributors" filler is
    dropped, and a company suffix ("JuliaHub, Inc.") is not a second person.
    """
    out: list[tuple[str, str | None]] = []
    seen: set[str] = set()
    for entry in authors:
        s = re.sub(r"\s+", " ", entry).replace(" ,", ",").strip(" ,;")
        parts: list[str] = []
        for chunk in re.split(r",\s*", s):
            if parts and _COMPANY_SUFFIX.match(chunk):
                parts[-1] = f"{parts[-1]}, {chunk}"
            else:
                parts.append(chunk)
        for part in parts:
            for piece in re.split(r"\s+(?:and|&)\s+|^and\s+", part):
                m = EMAIL.search(piece)
                email = m.group(0).lower() if m else None
                name = re.sub(r"<[^>]*>", "", piece)
                name = re.sub(r"\s+", " ", EMAIL.sub("", name).strip(" ,;()"))
                if name and not _CONTRIBUTORS.match(name) and name not in seen:
                    seen.add(name)
                    out.append((name, email))
    return out


def cff_authors(text: str) -> list[dict]:
    """Top-level ``authors:`` of a CITATION.cff — the software's authors.

    ``preferred-citation: authors:`` are the paper's authors and are ignored.
    """
    lines = text.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if re.match(r"^authors\s*:", line))
    except StopIteration:
        return []
    people: list[dict] = []
    cur: dict | None = None
    for line in lines[start + 1:]:
        if line.strip() == "" or line.lstrip().startswith("#"):
            continue
        if not line[:1].isspace() and not line.startswith("-"):
            break                                   # next top-level key
        m = re.match(r"^\s*(-\s+)?([\w-]+)\s*:\s*(.*?)\s*$", line)
        if not m:
            continue
        if m.group(1):
            cur = {}
            people.append(cur)
        if cur is not None:
            cur[m.group(2)] = m.group(3).strip("'\"")
    out = []
    for p in people:
        name = p.get("name") or " ".join(
            x for x in (p.get("given-names"), p.get("name-particle"), p.get("family-names")) if x)
        orcid = ORCID.search(p.get("orcid", ""))
        email = p.get("email", "").lower() or None
        if name:
            out.append({"name": name, "orcid": orcid.group(0) if orcid else None,
                        "email": email if email and EMAIL.fullmatch(email) else None})
    return out


# -- fetching -----------------------------------------------------------------------

@dataclass
class RepoMetadata:
    """What one package's repository says about it, each fact with its source URL."""
    commit: str | None = None
    project_url: str | None = None
    author_entries: list[tuple[str, str | None]] = field(default_factory=list, repr=False)
    deps: list[str] = field(default_factory=list)
    license: str | None = None
    license_url: str | None = None
    license_unclear: bool = False
    dois: list[str] = field(default_factory=list)
    arxiv: list[str] = field(default_factory=list)
    citation_url: str | None = None
    cff_url: str | None = None
    cff_authors: list[dict] = field(default_factory=list, repr=False)
    notes: list[str] = field(default_factory=list)


def head_commit(repo: str) -> str | None:
    """Pin a repository to its current commit without the GitHub API."""
    try:
        out = subprocess.run(["git", "ls-remote", repo, "HEAD"], capture_output=True, text=True,
                             timeout=60, env={"GIT_TERMINAL_PROMPT": "0", "PATH": "/usr/bin:/bin"})
    except subprocess.TimeoutExpired:
        return None
    return out.stdout.split()[0] if out.returncode == 0 and out.stdout.strip() else None


def _path(subdir: str | None, fname: str) -> str:
    return f"{subdir}/{fname}" if subdir else fname


def raw_url(repo: str, sha: str, subdir: str | None, fname: str) -> str:
    return f"https://raw.githubusercontent.com/{repo_key(repo).removeprefix('github.com/')}/{sha}/{_path(subdir, fname)}"


def blob_url(repo: str, sha: str, subdir: str | None, fname: str) -> str:
    return f"https://github.com/{repo_key(repo).removeprefix('github.com/')}/blob/{sha}/{_path(subdir, fname)}"


def fetch_repo_metadata(pkg: dict, session: requests.Session) -> RepoMetadata:
    """Project.toml, licence and citation files of one package at a pinned commit."""
    md = RepoMetadata()
    sha = head_commit(pkg["repo"])
    if not sha:
        md.notes.append("repository unreachable")
        md.license_unclear = True
        return md
    md.commit = sha
    sub = pkg.get("subdir")

    def get(fname):
        r = session.get(raw_url(pkg["repo"], sha, sub, fname), timeout=30)
        return r.text if r.status_code == 200 else None

    proj = get("Project.toml")
    if proj is not None:
        p = tomllib.loads(proj)
        md.author_entries = parse_author_entries(p.get("authors", []))
        md.deps = sorted(p.get("deps", {}))
        md.project_url = blob_url(pkg["repo"], sha, sub, "Project.toml")

    for fname in LICENSE_FILES:
        text = get(fname)
        if text is not None:
            md.license = classify_license(text)
            md.license_url = blob_url(pkg["repo"], sha, sub, fname)
            if md.license is None:
                md.notes.append(f"licence in {fname} not recognised")
                md.license_unclear = True
            break
    else:
        md.notes.append("no licence file found")
        md.license_unclear = True

    for fname in CITATION_FILES:
        text = get(fname)
        if text is None:
            continue
        d, a = citation_ids(text)
        md.dois += [x for x in d if x not in md.dois]
        md.arxiv += [x for x in a if x not in md.arxiv]
        url = blob_url(pkg["repo"], sha, sub, fname)
        if md.citation_url is None and (d or a):
            md.citation_url = url
        if fname == "CITATION.cff":
            md.cff_url = url
            md.cff_authors = cff_authors(text)
    return md
