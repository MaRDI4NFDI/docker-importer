"""Turn OpenML API responses into dataset records, and records into statements.

A *record* is the plain dict the importer stores, one per line, in a
download file. It holds the OpenML values the portal item is built from, in
the form they are written to the portal, so two downloads can be compared
record by record.

:func:`statements` turns a record into the list of :class:`Field` objects
that describe the item's statements. Values that point to other items are
references that the source resolves to portal IDs before writing:

- ``wd:Q…``: a Wikidata item, translated by mardiclient,
- ``label:…``: a portal-only item, found by its English label,
- ``doi:…`` and ``arxiv:…``: a publication, found or created by the source.

This module does not talk to the portal or to OpenML, so it can be tested
on its own.
"""

from __future__ import annotations

import re
from typing import NamedTuple, Optional
from urllib.parse import unquote

INSTANCE = "wd:Q1172284"            # dataset
PROFILE = "label:MaRDI dataset profile"
OLD_PROFILE = "label:Dataset"       # what the previous importer wrote
CREATOR = "wd:Q59275219"            # object has role: creator
CONTRIBUTOR = "wd:Q20204892"        # object has role: contributor
MD5 = "wd:Q185235"

ARFF_FORMATS = {"arff": "wd:Q4489412", "sparse_arff": "label:Sparse ARFF"}

# OpenML qualities stored on the item, with the portal property for each.
# NumberOfBinaryFeatures is left out: OpenML's dataset list does not give it,
# and fetching it would take one more request per dataset. Values the earlier
# importer wrote stay on the items.
QUALITIES = {
    "NumberOfClasses": "number of classes",
    "NumberOfFeatures": "number of features",
    "NumberOfInstances": "number of instances",
    "NumberOfInstancesWithMissingValues": "number of instances with missing values",
    "NumberOfMissingValues": "number of missing values",
    "NumberOfNumericFeatures": "number of numeric features",
    "NumberOfSymbolicFeatures": "number of symbolic features",
}

# OpenML's subject tags; other tags (study_1, uci, ...) are not stored.
SEMANTIC_TAGS = {
    "Agriculture", "Astronomy", "Chemistry", "Computational Universe",
    "Computer Systems", "Culture", "Demographics", "Earth Science",
    "Economics", "Education", "Geography", "Government", "Health", "History",
    "Human Activities", "Images", "Language", "Life Science",
    "Machine Learning", "Manufacturing", "Mathematics", "Medicine",
    "Meteorology", "Physical Sciences", "Politics", "Social Media",
    "Sociology", "Statistics", "Text & Literature", "Transportation",
}

# Licence names as OpenML gives them. A licence that is not listed is not
# written; the source counts and logs it.
LICENSES = {
    "CC0": "wd:Q6938433",
    "CC BY 4.0": "wd:Q20007257",
    "CC BY-SA 4.0": "wd:Q18199165",
    "CC BY-NC 4.0": "wd:Q34179348",
    "CC BY-NC-SA 4.0": "wd:Q42553662",
    "MIT": "wd:Q334661",
    "GPL-2": "wd:Q10513450",
    "GPL-3": "wd:Q10513445",
}

DOI = re.compile(r"\b(10\.\d{4,9}/[^\s?#&\"<>]+)")
ARXIV = re.compile(r"arxiv\.org/(?:abs|pdf)/([^\s?#]+?)(?:v\d+)?(?:\.pdf)?/?$", re.I)
URL = re.compile(r"^https?://\S+$")
# OpenML's own download link, which has had several hosts over the years
# (api.openml.org, www.openml.org, openml.org).
OPENML_DOWNLOAD = re.compile(r"^https?://(?:www\.|api\.)?openml\.org/data/v1/download/(\d+)(?:/|$)", re.I)


class Field(NamedTuple):
    """The values one property should have on a dataset item.

    Attributes:
        prop: the property, as ``wdt:P…`` or its English label.
        values: the values; item values are references (see module doc).
        qualifier: optional ``(property, value)`` added to every new claim.
        by_qualifier: only claims that carry ``qualifier`` belong to this
            field; used for author name strings, where creators and
            contributors share one property.
        compare: ``"text"`` compares descriptions by their words only;
            ``"url"`` treats OpenML download links as one link whatever
            their host, so an old form is replaced instead of kept.
        replaces: values the previous importer wrote in place of these;
            they are removed from the item.
    """

    prop: str
    values: list
    qualifier: Optional[tuple] = None
    by_qualifier: bool = False
    compare: str = "exact"
    replaces: tuple = ()


def as_list(value) -> list:
    """OpenML gives a single value instead of a one-element list; undo that."""
    if value is None or value == "":
        return []
    return value if isinstance(value, list) else [value]


def clean_text(text: str) -> str:
    """Put a text on one line: every run of whitespace becomes one space.

    OpenML escapes some quotes with a backslash (``King-Rook\\'s``); the
    backslash is dropped.
    """
    return " ".join(text.replace("\\'", "'").replace('\\"', '"').split())


def text_key(text: str) -> str:
    """The words of a text, ignoring line breaks and ``\\N``/``\\T`` codes."""
    return clean_text(re.sub(r"\\(?:NEWLINE|TAB|N|T|R)", " ", text))


def openml_download(url) -> Optional[str]:
    """The file id of an OpenML download link, ``None`` for other links."""
    match = OPENML_DOWNLOAD.match(str(url))
    return match.group(1) if match else None


def to_count(value) -> Optional[str]:
    """``"898.0"`` -> ``"898"``; ``None`` for missing or non-numeric values."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:     # NaN
        return None
    return str(int(number))


def paper(url: Optional[str]) -> Optional[str]:
    """``doi:…`` or ``arxiv:…`` for a paper URL, ``None`` if it has neither."""
    if not url:
        return None
    url = unquote(unquote(url))     # some links are encoded twice: 10.1007%252FBF00114162
    arxiv = ARXIV.search(url)
    if arxiv:
        return f"arxiv:{arxiv.group(1)}"
    doi = DOI.search(url)
    if doi:
        return f"doi:{doi.group(1).rstrip('.,;)').upper()}"
    return None


def record_from_api(description: dict, qualities: list) -> dict:
    """Build a record from ``/data/{id}`` and the dataset's qualities.

    Args:
        description: the ``data_set_description`` object of ``/data/{id}``.
        qualities: the dataset's ``quality`` list from ``/data/list``
            (``{"name": …, "value": …}`` entries).
    """
    d = description

    def text(key):
        value = d.get(key)
        if value is None or isinstance(value, (dict, list)):
            return None
        value = clean_text(str(value))
        return value or None

    by_name = {q.get("name"): q.get("value") for q in as_list(qualities)}
    upload = text("upload_date")
    return {
        "id": str(d["id"]),
        "name": text("name"),
        "version": text("version"),
        "description": text("description"),
        "creators": [clean_text(c) for c in as_list(d.get("creator")) if clean_text(c)],
        "contributors": [clean_text(c) for c in as_list(d.get("contributor")) if clean_text(c)],
        "collection_date": text("collection_date"),
        "upload_date": upload[:10] if upload else None,
        "licence": text("licence"),
        "url": text("url"),
        "original_data_url": text("original_data_url"),
        "default_target_attribute": text("default_target_attribute"),
        "row_id_attribute": text("row_id_attribute"),
        "tags": sorted({t for t in as_list(d.get("tag")) if t in SEMANTIC_TAGS}),
        "paper_url": text("paper_url"),
        "md5_checksum": text("md5_checksum"),
        "format": text("format"),
        "qualities": {k: to_count(by_name.get(k)) for k in QUALITIES if to_count(by_name.get(k)) is not None},
    }


def statements(record: dict) -> list[Field]:
    """The statements a dataset item should have for this record."""
    r = record
    one = lambda value: [value] if value else []  # noqa: E731
    fmt = ARFF_FORMATS.get((r.get("format") or "").lower())
    fields = [
        Field("wdt:P31", [INSTANCE]),
        Field("MaRDI profile type", [PROFILE], replaces=(OLD_PROFILE,)),
        Field("wdt:P11238", [r["id"]]),
        Field("dataset version identifier", one(r.get("version"))),
        Field("description", one(r.get("description")), compare="text"),
        Field("wdt:P2093", r.get("creators", []), ("wdt:P3831", CREATOR), by_qualifier=True),
        Field("wdt:P2093", r.get("contributors", []), ("wdt:P3831", CONTRIBUTOR), by_qualifier=True),
        Field("collection date", one(r.get("collection_date"))),
        Field("upload date", [f"+{r['upload_date']}T00:00:00Z"] if r.get("upload_date") else []),
        Field("wdt:P275", one(LICENSES.get(r.get("licence")))),
        Field("wdt:P953", [u for u in (r.get("url"), r.get("original_data_url")) if u and URL.match(u)],
              compare="url"),
        Field("default target attribute", one(r.get("default_target_attribute"))),
        Field("row id attribute", one(r.get("row_id_attribute"))),
        Field("OpenML semantic tag", [f"label:{t}" for t in r.get("tags", [])]),
        Field("wdt:P2860", one(paper(r.get("paper_url")))),
        Field("citation text", one(r.get("paper_url"))),
        Field("wdt:P4092", one(r.get("md5_checksum")), ("wdt:P459", MD5)),
        Field("wdt:P2701", one(fmt)),
    ]
    for quality, prop in QUALITIES.items():
        fields.append(Field(prop, one(r.get("qualities", {}).get(quality))))
    return fields
