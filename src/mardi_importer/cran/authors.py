"""Authors of CRAN packages, read from CRAN's package database.

``https://cran.r-project.org/web/packages/packages.rds`` (what R's
``tools::CRAN_package_db()`` reads) holds the DESCRIPTION fields of every
package on CRAN. Authors come from ``Authors@R`` when the package has it: R code
of ``person()`` calls with given and family name, e-mail address, roles and
ORCID. Otherwise from the free-text ``Author`` field. The maintainer comes from
``Maintainer`` ("Name <address>"), whose address is the most reliable way of
recognising one person across packages.

E-mail addresses are kept on :class:`Entry` only to join mentions of the same
person; they are never written to the Wikibase or to a report.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

PACKAGES_RDS = "https://cran.r-project.org/web/packages/packages.rds"

ORCID = re.compile(r"\b(\d{4}-\d{4}-\d{4}-\d{3}[\dX])\b")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_ORG = re.compile(
    r"\b(team|inc|ltd|llc|gmbh|corp|corporation|company|universit\w*|univ|institut\w*|foundation|"
    r"consortium|project|group|lab|laborator\w*|cent(?:er|re)|department|council|agency|ministry|"
    r"authors|contributors|developers|maintainers|core|plc|limited|"
    r"organi[sz]ation|association|society|network|initiative|office|bureau|service|services|"
    r"software|technologies|solutions|google|microsoft|posit|rstudio|oracle|ibm|amazon|nvidia|"
    r"college|school|academy|accademia|trustees|hospital|government|majesty|commission)\b",
    re.I)
PERSON_ARGS = ("given", "family", "middle", "email", "role", "comment", "first", "last")


@dataclass
class Entry:
    """One person (or organisation) named by one package."""
    package: str
    name: str
    source: str                          # "Authors@R" | "Author" | "Maintainer"
    roles: tuple[str, ...] = ()
    given: str | None = None
    family: str | None = None
    orcid: str | None = None
    email: str | None = field(default=None, repr=False)
    kind: str = "person"                 # "person" | "organisation" | "orphaned"

    @property
    def is_author(self) -> bool:
        return "aut" in self.roles or (self.source == "Author" and not self.roles)

    @property
    def is_maintainer(self) -> bool:
        return self.source == "Maintainer" or "cre" in self.roles


def norm_orcid(text: str | None) -> str | None:
    m = ORCID.search(text or "")
    return m.group(1) if m else None


def looks_like_organisation(name: str) -> bool:
    return bool(_ORG.search(name))


def clean_name(name: str) -> str:
    name = unicodedata.normalize("NFC", name)
    name = re.sub(r"\s+", " ", name.replace('"', "").replace("\\", "")).strip(" ,;")
    return name


# -- a small reader for the R code in Authors@R --------------------------------------

_TOKEN = re.compile(r"""
    (?P<ws>\s+|\#[^\n]*)
  | (?P<str>"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')
  | (?P<id>[A-Za-z._][A-Za-z0-9._]*(?:::[A-Za-z._][A-Za-z0-9._]*)?)
  | (?P<num>\d+(?:\.\d+)?L?)
  | (?P<op><-|[(),=+])
  | (?P<other>.)
""", re.S | re.X)

_ESC = re.compile(r"\\(u\{[0-9A-Fa-f]+\}|U\{[0-9A-Fa-f]+\}|u[0-9A-Fa-f]{1,4}|U[0-9A-Fa-f]{1,8}|x[0-9A-Fa-f]{1,2}|.)", re.S)
_SIMPLE = {"n": "\n", "t": "\t", "r": "\r", "0": ""}


def _unescape(s: str) -> str:
    def sub(m):
        e = m.group(1)
        if e[0] in "uUx" and len(e) > 1:
            return chr(int(e[1:].strip("{}"), 16))
        return _SIMPLE.get(e, e)
    return _ESC.sub(sub, s)


class RSyntaxError(ValueError):
    pass


class _Call:
    def __init__(self, name: str, args: list[tuple[str | None, object]]):
        self.name, self.args = name.split("::")[-1], args


def _tokens(text: str) -> list[tuple[str, str]]:
    out = []
    for m in _TOKEN.finditer(text):
        kind = m.lastgroup
        if kind == "ws":
            continue
        if kind == "other":
            raise RSyntaxError(f"unexpected {m.group()!r}")
        out.append((kind, m.group()))
    return out


class _Parser:
    def __init__(self, text: str):
        self.toks, self.i = _tokens(text), 0

    def peek(self, value=None):
        if self.i >= len(self.toks):
            return None
        tok = self.toks[self.i]
        return tok if value is None or tok[1] == value else None

    def take(self, value=None):
        tok = self.peek()
        if tok is None or (value is not None and tok[1] != value):
            raise RSyntaxError(f"expected {value!r}, got {tok!r}")
        self.i += 1
        return tok

    def expr(self):
        left = self.atom()
        while self.peek("+"):
            self.take("+")
            left = _Call("c", [(None, left), (None, self.atom())])
        return left

    def atom(self):
        kind, val = self.take()
        if kind == "str":
            return _unescape(val[1:-1])
        if kind == "num":
            return val
        if kind == "id":
            if self.peek("("):
                return self.call(val)
            return {"NULL": None, "NA": None, "TRUE": True, "FALSE": False}.get(val, val)
        if val == "(":
            inner = self.expr()
            self.take(")")
            return inner
        raise RSyntaxError(f"unexpected {val!r}")

    def call(self, name):
        self.take("(")
        args = []
        while not self.peek(")"):
            if self.peek(","):                       # empty positional argument
                self.take(",")
                args.append((None, None))
                continue
            key = None
            if self.peek() and self.peek()[0] in ("id", "str") and \
                    self.i + 1 < len(self.toks) and self.toks[self.i + 1][1] == "=":
                key = self.take()[1].strip("\"'")
                self.take("=")
            args.append((key, self.expr()))
            if self.peek(","):
                self.take(",")
                if self.peek(")"):
                    args.append((None, None))
        self.take(")")
        return _Call(name, args)


def _values(v) -> list[tuple[str | None, object]]:
    """Flatten ``c(...)`` into (name, value) pairs; anything else is one unnamed value."""
    if isinstance(v, _Call) and v.name == "c":
        out = []
        for k, x in v.args:
            inner = _values(x)
            out += [(k or ik, iv) for ik, iv in inner] if k else inner
        return out
    if isinstance(v, _Call) and v.name in ("paste", "paste0"):
        sep = next((x for k, x in v.args if k == "sep"), " " if v.name == "paste" else "")
        return [(None, str(sep).join(str(x) for k, x in v.args if k != "sep" and isinstance(x, str)))]
    return [] if v is None else [(None, v)]


def _strings(v) -> list[str]:
    return [x for _, x in _values(v) if isinstance(x, str) and x.strip()]


def _person(call: _Call) -> dict:
    args: dict[str, object] = {}
    pos = iter(PERSON_ARGS)
    for k, v in call.args:
        if k is None:
            k = next(pos, None)
            if k is None:
                continue
        args.setdefault(k, v)
    given = " ".join(_strings(args.get("given")) or _strings(args.get("first")))
    middle = " ".join(_strings(args.get("middle")))
    family = " ".join(_strings(args.get("family")) or _strings(args.get("last")))
    comment = _values(args.get("comment"))
    orcid = next((norm_orcid(str(x)) for k, x in comment if k and k.lower() == "orcid"), None) \
        or next((o for _, x in comment if (o := norm_orcid(str(x)))), None)
    emails = _strings(args.get("email"))
    return {"given": " ".join(filter(None, (given, middle))) or None, "family": family or None,
            "email": emails[0] if emails else None, "roles": tuple(_strings(args.get("role"))),
            "orcid": orcid}


def _persons(v) -> list[dict]:
    if isinstance(v, _Call) and v.name == "person":
        return [_person(v)]
    if isinstance(v, _Call) and v.name == "as.person":
        return [{"text": s} for s in _strings(v.args[0][1] if v.args else None)]
    if isinstance(v, _Call) and v.name in ("c", "list"):
        return [p for _, x in v.args for p in _persons(x)]
    if isinstance(v, str):
        return [{"text": v}]
    return []


def parse_authors_r(text: str, package: str = "") -> list[Entry]:
    """Entries of an ``Authors@R`` field. Raises :class:`RSyntaxError` if unreadable."""
    p = _Parser(text)
    value = p.expr()
    if p.peek() is not None:
        raise RSyntaxError(f"trailing {p.peek()!r}")
    out = []
    for d in _persons(value):
        if "text" in d:
            out += parse_author_text(d["text"], package, source="Authors@R")
            continue
        given, family = d["given"], d["family"]
        name = clean_name(" ".join(filter(None, (given, family))))
        if not name:
            continue
        kind = "organisation" if not (given and family) and looks_like_organisation(name) else "person"
        if not family and kind == "person" and len(name.split()) == 1 and not d["orcid"]:
            kind = "organisation" if "cph" in d["roles"] or "fnd" in d["roles"] else "person"
        email = d["email"].strip().lower() if d["email"] and EMAIL.fullmatch(d["email"].strip()) else None
        out.append(Entry(package, name, "Authors@R", d["roles"], given, family, d["orcid"], email, kind))
    return out


# -- free text: Author and Maintainer fields --------------------------------------------

def _split_top(text: str) -> list[str]:
    """Split at commas, semicolons and " and " outside brackets."""
    parts, depth, cur, i = [], 0, [], 0
    while i < len(text):
        ch = text[i]
        if ch in "([<{":
            depth += 1
        elif ch in ")]>}":
            depth = max(0, depth - 1)
        if depth == 0 and (ch in ",;" or text.startswith(" and ", i) or text.startswith("\nand ", i)):
            parts.append("".join(cur))
            cur = []
            i += 5 if ch in " \n" else 1
            continue
        cur.append(ch)
        i += 1
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def parse_author_text(text: str, package: str = "", source: str = "Author") -> list[Entry]:
    """Entries of a free-text ``Author`` field ("Jane Roe [aut, cre] (ORCID: ...), ...")."""
    out = []
    for part in _split_top(" ".join(text.split())):
        roles = tuple(r.strip() for m in re.findall(r"\[([^\]]*)\]", part) for r in m.split(","))
        orcid = norm_orcid(part)
        m = EMAIL.search(part)
        email = m.group().lower() if m else None
        name = re.sub(r"\[[^\]]*\]|\([^)]*\)|<[^>]*>|\{[^}]*\}", " ", part)
        name = clean_name(EMAIL.sub(" ", name))
        name = re.sub(r"^.*?\b(contributed|contributions?|written|ported|translated|maintained|packaged)"
                      r"\s+(by|from)\s+", "", name, flags=re.I)
        name = re.sub(r"^((with |and )?(contributions? )?(from|by)|and|with)\s+", "", name, flags=re.I)
        if not name or not re.search(r"[^\W\d_]", name):
            continue
        kind = "organisation" if looks_like_organisation(name) else "person"
        out.append(Entry(package, name, source, roles, orcid=orcid, email=email, kind=kind))
    return out


def parse_maintainer(text: str, package: str = "") -> Entry | None:
    """The ``Maintainer`` field: "Name <address>", or "ORPHANED"."""
    text = " ".join((text or "").split())
    if not text:
        return None
    if text.upper().startswith("ORPHANED"):
        return Entry(package, "ORPHANED", "Maintainer", ("cre",), kind="orphaned")
    m = EMAIL.search(text)
    name = clean_name(re.sub(r"<[^>]*>|\([^)]*\)", " ", text))
    if not name:
        return None
    kind = "organisation" if looks_like_organisation(name) else "person"
    return Entry(package, name, "Maintainer", ("cre",), email=m.group().lower() if m else None, kind=kind)


def package_entries(row: dict) -> tuple[list[Entry], list[str]]:
    """Entries of one package database row, and notes on what could not be read."""
    pkg, notes = row["Package"], []
    entries: list[Entry] = []
    authors_r = row.get("Authors@R")
    if authors_r and str(authors_r).strip():
        try:
            entries = parse_authors_r(str(authors_r), pkg)
        except RSyntaxError as exc:
            notes.append(f"Authors@R unreadable ({exc}); Author text used")
    if not entries and row.get("Author"):
        entries = parse_author_text(str(row["Author"]), pkg)
    if (mt := parse_maintainer(str(row.get("Maintainer") or ""), pkg)):
        entries.append(mt)
    return entries, notes


# -- the package database ----------------------------------------------------------------

def read_packages_rds(path: str | Path) -> list[dict]:
    """Rows of CRAN's package database (a character matrix saved by R)."""
    import rdata
    parsed = rdata.parser.parse_file(path)
    conv = rdata.conversion.SimpleConverter()
    obj, attrs, a = parsed.object, {}, parsed.object.attributes
    while a is not None and a.info.type.name == "LIST":
        attrs[conv.convert(a.tag)] = a.value[0]
        a = a.value[1]
    nrow, ncol = conv.convert(attrs["dim"])
    columns = conv.convert(attrs["dimnames"])[1]
    flat = list(conv.convert(obj))
    rows = []
    for i in range(nrow):
        row = {}
        for j, col in enumerate(columns):
            v = flat[j * nrow + i]
            row[col] = None if v is None or (isinstance(v, float) and v != v) else v
        rows.append(row)
    return rows


def fetch_packages_rds(dest: str | Path, session=None) -> Path:
    import requests
    session = session or requests.Session()
    res = session.get(PACKAGES_RDS, timeout=300)
    res.raise_for_status()
    dest = Path(dest)
    dest.write_bytes(res.content)
    return dest
