"""Apply a zbMATH diff file to the MaRDI Wikibase.

The diff file is produced by :mod:`mardi_importer.zbmath.diff`: one row per
changed record, holding every column of the old dump plus a ``<col>_new``
column with the new value (or :data:`~mardi_importer.zbmath.diff.DELETED_SENTINEL`
when zbMATH emptied the field).

Merge policy
------------
Claims are merged surgically rather than replaced: for each property, the
value that the *old dump* carried is removed **only if** it is still on the
item and is not part of the new data, and the new values are appended only if
they are not already there. Anything else on the item — a value somebody added
by hand, or one the importer took from Crossref — is left untouched. A field
that zbMATH emptied results in a removal with no addition.

arXiv records
-------------
Records whose ``zbl_id`` contains ``arXiv`` do not live on a publication item
with a zbMATH DE number; the importer keeps them as separate arXiv items
(``ZBMathSource.create_arxiv_item``) that are found by their arXiv ID and are
linked to a zbMATH publication with "is preprint of". This module follows that
split: :meth:`ZBMathUpdater.apply` takes a ``mode`` of ``"non_arxiv"`` or
``"arxiv"`` and only touches the properties the importer maintains for that
kind of item.

Records not in the portal
-------------------------
A diff row whose item cannot be found (never imported, merged by hand, ...)
cannot be applied. Given a ``missing_path``, the pass writes such rows to that
file in the diff's own format and carries on; the flow fails the run once all
its steps are done, pointing at those files.
"""

from __future__ import annotations

import csv
import os
import re
from typing import Callable, Iterator, NamedTuple, Optional

from wikibaseintegrator.wbi_enums import ActionIfExists

from .diff import (
    DELETED_SENTINEL,
    DE_COL,
    has_conflict,
    is_blank,
    split_list,
    year_value,
)
from .misc import _with_backoff, get_info_from_doi
from .ZBMathAuthor import ZBMathAuthor
from .ZBMathJournal import ZBMathJournal
from .ZBMathPublication import license_dict


class ZBMathUpdateError(RuntimeError):
    """Fatal problem while applying the diff.

    Raised instead of ``sys.exit`` so the surrounding Prefect task fails
    visibly with the de_number and the reason attached.
    """


class ZBMathItemNotFound(ZBMathUpdateError):
    """The record is in the diff but its item is not in the portal.

    When the pass is given a ``missing_path`` this is caught per row: the row
    is set aside in that file and the pass carries on, so one record that never
    made it into the portal cannot block every record after it.
    """


#: Property identifiers per diff column, spelled exactly as the importer
#: spells them so they resolve through the same entity mappings.
#: ``label_id_dict`` entries are filled in from the source at runtime.
PROPERTY_IDS = {
    "document_title": "wdt:P1476",
    "classifications": "wdt:P3285",
    "publication_year": "wdt:P577",
    "serial": "wdt:P1433",
    "links": "wdt:P953",
    "doi": "wdt:P356",
    "author": "wdt:P50",
    # authors without a zbMATH code, as ZBMathPublication.insert_claims writes them
    "author_name_string": "P43",
    "reviewers": "wdt:P4032",
    "license": "P163",
    "references": "P223",
    # arXiv items, as written by ZBMathSource.create_arxiv_item
    "arxiv_id": "P21",
    "source": "P22",
    "arxiv_classifications": "P226",
    "arxiv_publication_year": "P28",
    "preprint": "P1676",
}

#: Columns the importer maintains on a *publication* item.
NON_ARXIV_COLUMNS = [
    "document_title",
    "classifications",
    "keywords",
    "publication_year",
    "serial",
    "links",
    "doi",
    "author",
    "author_ids",
    "review_sign",
    "reviewer_id",
    "review_text",
    "license",
]

#: Columns the importer maintains on an *arXiv* item.
ARXIV_COLUMNS = [
    "document_title",
    "classifications",
    "author",
    "author_ids",
    "publication_year",
    "source",
    'doi'
]

_LINK_PATTERN = re.compile(r"^([a-z][a-z\d+.-]*):([^][<>\"\x00-\x20\x7F])+$")
_ARXIV_PREFIX = "https://arxiv.org/abs/"


# ── value parsing ────────────────────────────────────────────────────────────

def parse_list(value: Optional[str]) -> list[str]:
    """Semicolon-separated list, individually stripped, blanks dropped.

    Shared with the diff builder so what it calls unchanged and what this
    applies can never drift apart.
    """
    return split_list(value)


def parse_positional(value: Optional[str]) -> list[str]:
    """Split a column that is aligned by position with another column.

    ``process_data`` writes a literal ``None`` for an author who has no name or
    no zbMATH code, precisely so that ``author`` and ``author_ids`` stay lined
    up, and ``ZBMathSource.push`` reads them back keeping those slots. Here they
    are kept as empty strings. Dropping them, as :func:`parse_list` does, would
    shift every later entry along and pair names with the wrong IDs.
    """
    if is_blank(value):
        return []
    return [
        "" if is_blank(x) or has_conflict(x) else x.strip()
        for x in value.split(";")
    ]


def parse_single(value: Optional[str]) -> list[str]:
    if is_blank(value):
        return []
    return [value.strip()]


def parse_doi(value: Optional[str]) -> list[str]:
    """DOIs are stored upper-cased by ``ZBMathPublication``."""
    if is_blank(value):
        return []
    return [value.strip().upper()]


def parse_year(value: Optional[str]) -> list[str]:
    """zbMATH year -> a year-precision Wikibase time string."""
    if is_blank(value):
        return []
    # ``2024.0`` is the same year; older processed dumps can carry that form.
    year = year_value(value)
    if not re.fullmatch(r"[+-]?\d{1,4}", year):
        return []
    if not year.startswith(("+", "-")):
        year = "+" + year
    return [f"{year}-00-00T00:00:00Z"]


def importer_written_date(creation_date: Optional[str]) -> list[str]:
    """The publication date the importer wrote from a zbMATH datestamp.

    ``ZBMathPublication.insert_claims`` wrote the datestamp in preference to
    the year, but the datestamp is when zbMATH processed the record, not when
    the article was published. ``ZBMathSource.push`` also compares against
    ``"0001-01-01T00:00:00"`` without the trailing ``Z`` the API sends, so the
    placeholder slipped through and was written as 1 January 0001.

    This reproduces both exactly, so those values can be recognised on an item
    and replaced by the year. It is only ever used to decide what to *remove*.
    """
    if creation_date is None:
        return []
    value = creation_date.strip()
    # push's own check: this exact spelling was caught and the year written.
    if value in ("", "None", "nan", "NaN", "0001-01-01T00:00:00"):
        return []
    date = value.split("T")[0]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        return []
    return [f"+{date}T00:00:00Z"]


def parse_source(value: Optional[str]) -> list[str]:
    """arXiv classification, i.e. the part in square brackets."""
    if is_blank(value):
        return []
    match = re.search(r"\[(.*?)\]", value)
    return [match.group(1)] if match else []


def parse_links(value: Optional[str]) -> tuple[list[str], Optional[str]]:
    """Return ``(non_arxiv_links, arxiv_id)`` for a raw links string."""
    if is_blank(value):
        return [], None
    links = [
        x.strip() for x in value.split(";")
        if _LINK_PATTERN.match(x.strip()) and "http" in x
    ]
    arxiv_id = None
    remaining = []
    for link in links:
        if _ARXIV_PREFIX in link:
            arxiv_id = link.removeprefix(_ARXIV_PREFIX)
        else:
            remaining.append(link)
    return remaining, arxiv_id


def parse_licenses(value: Optional[str]) -> list[str]:
    """Licence URLs mapped to the QIDs the importer uses."""
    qids = []
    for raw in parse_list(value):
        qid = license_dict.get(raw)
        if qid:
            qids.append(qid)
    return qids


def author_label(name: Optional[str]) -> str:
    """``"Last, First"`` -> ``"First Last"``, as ``ZBMathAuthor`` does."""
    if is_blank(name):
        return ""
    parts = name.strip().split(",")
    return ((" ").join(parts[1:]) + " " + parts[0]).strip()


def reviewer_label(review_sign: Optional[str]) -> str:
    """Reviewer name, normalised the way ``ZBMathSource.push`` normalises it."""
    if is_blank(review_sign):
        return ""
    return review_sign.strip().split("/")[0].strip().split("(")[0].strip()


def person_label(raw_name: Optional[str], label_fn) -> str:
    """The label the importer gives a person's item for a raw dump name.

    ``label_fn`` first reduces the raw value to a name (for a reviewer that
    strips the affiliation, as ``ZBMathSource.push`` does), then the name is
    turned around the way ``ZBMathAuthor`` turns it around. For authors
    ``label_fn`` is :func:`author_label` itself, which is idempotent, so both
    kinds of person end up with exactly the label the import would give them.
    """
    if not raw_name or is_blank(raw_name):
        return ""
    return author_label(label_fn(raw_name))


class PersonChanges(NamedTuple):
    """What a changed author/reviewer pair of columns means for one item."""

    new_qids: list
    old_qids: list
    #: ``(qid, label)`` for people whose name changed; ``label`` is final.
    relabel: list
    #: Name strings of people without a zbMATH code, as the importer writes
    #: them to the author-name-string property.
    new_strings: list
    old_strings: list


def claim_scalar(claim) -> Optional[str]:
    """Reduce an on-item claim to a value comparable with a parsed one."""
    snak = claim.mainsnak
    datavalue = snak.datavalue or {}
    value = datavalue.get("value")
    if value is None:
        return None
    datatype = snak.datatype
    if datatype == "wikibase-item":
        return value.get("id")
    if datatype == "time":
        return value.get("time")
    if datatype == "monolingualtext":
        return value.get("text")
    if datatype == "quantity":
        return value.get("amount")
    return value


class MissingRows:
    """Diff rows whose item is not in the portal, set aside in a file.

    The file has the diff's own columns, so it can be read like a diff file.
    Rows already in it (from an interrupted run of the same pass) are not
    written twice.
    """

    def __init__(self, path: str, fieldnames: list[str]):
        self.path = path
        self.fieldnames = list(fieldnames)
        self.seen: set[str] = set()
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
                for row in reader:
                    self.seen.add((row.get(DE_COL) or "").strip())
        else:
            with open(path, "w", encoding="utf-8", newline="") as f:
                self._writer(f).writerow(self.fieldnames)

    @staticmethod
    def _writer(f):
        return csv.writer(
            f, delimiter="\t", quoting=csv.QUOTE_NONE, quotechar=None,
            lineterminator="\n",
        )

    def add(self, row: dict) -> bool:
        """Append ``row``; returns False if it was already there."""
        de_number = (row.get(DE_COL) or "").strip()
        if de_number in self.seen:
            return False
        with open(self.path, "a", encoding="utf-8", newline="") as f:
            self._writer(f).writerow([row.get(c) or "" for c in self.fieldnames])
            f.flush()
            os.fsync(f.fileno())
        self.seen.add(de_number)
        return True


def count_rows(path: Optional[str]) -> int:
    """Data rows in a tab-separated file with a header, 0 if it is absent."""
    if not path or not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        return max(0, sum(1 for line in f if line.strip()) - 1)


# ── the updater ──────────────────────────────────────────────────────────────

class ZBMathUpdater:
    """Applies a diff file to the MaRDI Wikibase.

    Args:
        source: A live :class:`mardi_importer.zbmath.ZBMathSource`. Its ``api``
            and ``label_id_dict`` are reused, so properties and profile items
            resolve through the same entity mappings the import uses.
        logger: Anything with ``info``/``warning``/``error``. A Prefect run
            logger is passed in by the flow.
    """

    def __init__(self, source, logger):
        self.source = source
        self.api = source.api
        self.log = logger
        self.label_id_dict = source.label_id_dict

        self._prop_cache: dict[str, str] = {}
        self._author_cache: dict[str, Optional[str]] = {}
        self._journal_cache: dict[str, Optional[str]] = {}

        # Properties that are only known by label live in label_id_dict.
        self._label_props = {
            "keywords": self.label_id_dict["keyword_prop"],
            "review_text": self.label_id_dict["review_prop"],
            "de_number": self.label_id_dict["de_number_prop"],
        }

        self.stats = self._new_stats()

    @staticmethod
    def _new_stats() -> dict:
        """Counters for one pass. ``apply`` starts each pass from zero, so the
        arXiv pass does not report the publication pass's numbers as its own
        (the updater is shared between the two)."""
        return {
            "rows_seen": 0,
            "rows_changed": 0,
            "items_written": 0,
            "claims_added": 0,
            "claims_removed": 0,
            "authors_created": 0,
            "journals_created": 0,
            "labels_updated": 0,
            "preprint_links_added": 0,
            "missing_in_portal": 0,
            # True when the pass stopped at ``max_rows`` with rows still to go
            "stopped_early": False,
        }

    # ── property resolution ──────────────────────────────────────────────

    def prop(self, key: str) -> str:
        """Local property ID for a diff column or a ``PROPERTY_IDS`` key.

        ``MardiClient.search_entity_by_value`` bails out when handed a list,
        which is what a label lookup returns, so everything is collapsed to a
        single ID here.
        """
        if key in self._prop_cache:
            return self._prop_cache[key]

        identifier = self._label_props.get(key) or PROPERTY_IDS.get(key)
        if identifier is None:
            raise ZBMathUpdateError(f"no property configured for {key!r}")

        resolved = identifier
        if not re.fullmatch(r"P\d+", str(identifier)):
            resolved = _with_backoff(
                lambda: self.api.get_local_id_by_label(str(identifier), "property"),
                self.log, f"resolving property {identifier!r}",
            )
        if isinstance(resolved, list):
            resolved = resolved[0] if resolved else None
        if not resolved or not re.fullmatch(r"P\d+", str(resolved)):
            raise ZBMathUpdateError(
                f"could not resolve property {identifier!r} for {key!r} "
                f"(got {resolved!r})"
            )
        resolved = str(resolved)
        self._prop_cache[key] = resolved
        return resolved

    def log_resolved_properties(self) -> None:
        """Log the local ID every property key resolved to, once.

        Your update script hardcoded these; this flow resolves them through the
        entity mappings instead. Logging them makes the first run's output the
        place to check they came out as the IDs you used.
        """
        if getattr(self, "_logged_properties", False):
            return
        keys = list(PROPERTY_IDS) + list(self._label_props)
        resolved = {key: self.prop(key) for key in keys}
        self.log.info(
            "resolved properties: %s",
            ", ".join(f"{k}={v}" for k, v in resolved.items()),
        )
        self._logged_properties = True

    def check_property_consistency(self) -> None:
        """Log a warning if the arXiv-specific literals the importer uses do
        not resolve to the same properties as their ``wdt:`` counterparts."""
        pairs = [
            ("arxiv_classifications", "classifications"),
            ("arxiv_publication_year", "publication_year"),
        ]
        for literal_key, wdt_key in pairs:
            try:
                a, b = self.prop(literal_key), self.prop(wdt_key)
            except ZBMathUpdateError:
                continue
            if a != b:
                self.log.warning(
                    "%s resolves to %s but %s resolves to %s; the importer uses "
                    "the former for arXiv items and the latter for publications",
                    PROPERTY_IDS[literal_key], a, PROPERTY_IDS[wdt_key], b,
                )

    # ── authors and journals ─────────────────────────────────────────────

    def resolve_author(
        self, zbmath_author_id: str, name: str, create: bool
    ) -> Optional[str]:
        """QID for a zbMATH author ID, optionally creating the item.

        The cache is what keeps a long run from re-running the same SPARQL
        lookup for every row an author appears in, and from creating the same
        author twice before the query service has caught up.
        """
        author_id = (zbmath_author_id or "").strip()
        if is_blank(author_id) or has_conflict(author_id):
            return None
        if self._author_cache.get(author_id):
            return self._author_cache[author_id]

        def _resolve():
            author = ZBMathAuthor(
                name=name,
                zbmath_author_id=author_id,
                label_id_dict=self.label_id_dict,
            )
            existing = author.exists()
            if existing:
                return existing, False
            if not create:
                return None, False
            return author.create(), True

        qid, created = _with_backoff(_resolve, self.log, f"resolving author {author_id}")
        if created:
            self.stats["authors_created"] += 1
            self.log.info("created author %s (%s) as %s", name, author_id, qid)
        if qid:
            self._author_cache[author_id] = qid
        return qid

    def relabel_author(self, qid: str, label: str) -> None:
        """Follow a name change from zbMATH through to the item's label.

        ``label`` is the finished label (see :func:`person_label`); it is set
        as given.
        """
        if not label:
            return

        def _write():
            item = self.api.item.get(entity_id=qid)
            try:
                current = item.labels.values["en"].value
            except (KeyError, AttributeError):
                current = ""
            if current == label:
                return False
            item.labels.set(language="en", value=label)
            item.write()
            self.log.info("relabelled %s: %r -> %r", qid, current, label)
            return True

        if _with_backoff(_write, self.log, f"relabelling author {qid}"):
            self.stats["labels_updated"] += 1

    def resolve_journal(self, name: str, create: bool) -> Optional[str]:
        """QID for a journal name, optionally creating the item."""
        if is_blank(name) or has_conflict(name):
            return None
        key = " ".join(name.strip().split())
        if self._journal_cache.get(key):
            return self._journal_cache[key]

        def _resolve():
            journal = ZBMathJournal(key)
            existing = journal.exists()
            if existing:
                return existing, False
            if not create:
                return None, False
            return journal.create(), True

        qid, created = _with_backoff(_resolve, self.log, f"resolving journal {key!r}")
        if created:
            self.stats["journals_created"] += 1
            self.log.info("created journal %r as %s", key, qid)
        if qid:
            self._journal_cache[key] = qid
        return qid

    # ── claim merging ────────────────────────────────────────────────────

    def merge_claims(
        self,
        item,
        prop_nr: str,
        new_values: list[str],
        old_values: list[str],
        removable=None,
        **claim_kwargs,
    ) -> bool:
        """Surgically merge one property. Returns True if the item changed.

        Args:
            new_values: Values the new dump carries; appended when absent.
            old_values: Values the old dump carried; removed when still on the
                item and not part of ``new_values``.
            removable: Optional ``claim -> bool`` predicate narrowing which
                existing claims may be removed at all.
        """
        new_set = {v for v in new_values if v}
        old_set = {v for v in old_values if v}
        changed = False

        for existing in item.claims.get(prop_nr):
            if existing.removed:
                continue
            scalar = claim_scalar(existing)
            if scalar is None or scalar not in old_set or scalar in new_set:
                continue
            if removable is not None and not removable(existing):
                continue
            existing.remove()
            self.stats["claims_removed"] += 1
            changed = True

        present = {
            claim_scalar(c) for c in item.claims.get(prop_nr) if not c.removed
        }
        for value in new_values:
            if not value or value in present:
                continue
            claim = _with_backoff(
                lambda: self.api.get_claim(prop_nr, value, **claim_kwargs),
                self.log, f"building claim {prop_nr} = {value!r}",
            )
            if not claim:
                self.log.warning(
                    "could not build a claim for %s = %r", prop_nr, value
                )
                continue
            item.add_claims([claim], ActionIfExists.FORCE_APPEND)
            present.add(value)
            self.stats["claims_added"] += 1
            changed = True

        return changed

    # ── item lookup ──────────────────────────────────────────────────────

    def get_item(self, qid: str):
        return _with_backoff(
            lambda: self.api.item.get(entity_id=qid), self.log, f"fetching {qid}"
        )

    def find_publication(self, de_number: str) -> str:
        prop_nr = self.prop("de_number")
        qids = _with_backoff(
            lambda: self.api.search_entity_by_value(prop_nr, de_number),
            self.log, f"looking up de_number {de_number}",
        )
        if not qids:
            raise ZBMathItemNotFound(
                f"no publication item with {self.prop('de_number')} = {de_number}; "
                "the record is in the diff but not in the portal, so the update "
                "cannot be applied"
            )
        if len(qids) > 1:
            self.log.warning(
                "de_number %s matches %d items (%s); using the first",
                de_number, len(qids), ", ".join(qids),
            )
        return qids[0]

    def find_arxiv_item(self, arxiv_id: str) -> str:
        prop_nr = self.prop("arxiv_id")
        qids = _with_backoff(
            lambda: self.api.search_entity_by_value(prop_nr, arxiv_id),
            self.log, f"looking up arXiv id {arxiv_id}",
        )
        if not qids:
            raise ZBMathItemNotFound(
                f"no arXiv item with {self.prop('arxiv_id')} = {arxiv_id}; "
                "the record is in the diff but not in the portal, so the update "
                "cannot be applied"
            )
        if len(qids) > 1:
            self.log.warning(
                "arXiv id %s matches %d items (%s); using the first",
                arxiv_id, len(qids), ", ".join(qids),
            )
        return qids[0]

    def arxiv_id_for_row(self, row: dict, de_number: str) -> str:
        """The arXiv ID an arXiv record's item is found by."""
        zbl_id = (row.get("zbl_id") or "").strip()
        arxiv_id = zbl_id.split(":")[-1]
        if not arxiv_id:
            raise ZBMathUpdateError(
                f"de_number {de_number}: cannot derive an arXiv id from "
                f"zbl_id {zbl_id!r}"
            )
        return arxiv_id

    def root_qid(self, row: dict, de_number: str) -> str:
        """The item this record's own claims live on.

        An arXiv record has no de_number on its item, so it is found by arXiv
        ID; everything else is found by zbMATH DE number.
        """
        if self.is_arxiv_row(row):
            return self.find_arxiv_item(self.arxiv_id_for_row(row, de_number))
        return self.find_publication(de_number)

    # ── row helpers ──────────────────────────────────────────────────────

    @staticmethod
    def is_arxiv_row(row: dict) -> bool:
        zbl_id = row.get("zbl_id")
        return bool(zbl_id) and not is_blank(zbl_id) and "arXiv" in zbl_id

    @staticmethod
    def changed(row: dict, column: str) -> Optional[str]:
        """The ``<col>_new`` value, or None when the column did not change."""
        raw = row.get(column + "_new")
        if raw is None or raw == "":
            return None
        return raw

    @staticmethod
    def deleted(value: Optional[str]) -> bool:
        return value == DELETED_SENTINEL

    def _merge_publication_date(
        self, item, row: dict, prop_nr: str, de_number: str
    ) -> bool:
        """Write the year of publication when it changed.

        The publication date is the year the article was published, which is
        all zbMATH records. What gets replaced is everything this record could
        have put on the item: the old year (on items already corrected), and
        what the importer wrote from the datestamp (on items not yet corrected)
        — see :func:`importer_written_date`. A date added by hand is neither,
        so it stays.
        """
        change = self.changed(row, "publication_year")
        new_year = [] if self.deleted(change) else parse_year(change)
        if not new_year and not self.deleted(change):
            self.log.warning(
                "de_number %s: publication_year %r is not a usable year; "
                "leaving the publication date alone", de_number, change,
            )
            return False
        old_values = parse_year(row.get("publication_year")) + importer_written_date(
            row.get("creation_date")
        )
        return self.merge_claims(item, prop_nr, new_year, old_values)

    def _person_values(
        self, row: dict, name_col: str, id_col: str, label_fn
    ) -> Optional[PersonChanges]:
        """Resolve the old and new people for authors or reviewers.

        People with a zbMATH code become person QIDs; people without one
        become name strings, which is how ``ZBMathSource.push`` writes them.
        A person who gained a code therefore moves from the name strings to the
        person QIDs (the old string is removed, the person added) and one who
        lost it the other way round. Returns ``None`` when neither column
        changed, or when the change should not be applied at all.
        """
        name_change = self.changed(row, name_col)
        id_change = self.changed(row, id_col)
        if name_change is None and id_change is None:
            return None

        old_ids = parse_positional(row.get(id_col))
        old_names = parse_positional(row.get(name_col))

        # The IDs decide who the people are; the names decide what their items
        # are labelled, and are the whole record for people without a code.
        if self.deleted(id_change):
            new_ids = []
        elif id_change is not None:
            new_ids = parse_positional(id_change)
        else:
            new_ids = old_ids

        if self.deleted(name_change):
            new_names = []
        elif name_change is not None:
            new_names = parse_positional(name_change)
        else:
            new_names = old_names

        # The new side has to line up exactly. Your update script stopped the
        # run here (sys.exit), because pairing on misaligned lists would put a
        # name on the wrong person; this stops the task the same way.
        if new_names and new_ids and len(new_names) != len(new_ids):
            raise ZBMathUpdateError(
                f"de_number {row.get(DE_COL)}: {name_col} has {len(new_names)} "
                f"entries but {id_col} has {len(new_ids)}, so names cannot be "
                "matched to IDs"
            )

        # The old side is only used to look people up and to know which name
        # strings this record put on the item, so it is padded, not rejected.
        def pairs(ids, names):
            n = max(len(ids), len(names))
            ids = list(ids) + [""] * (n - len(ids))
            names = list(names) + [""] * (n - len(names))
            return list(zip(ids, names))

        new_all = pairs(new_ids, new_names)
        old_all = pairs(old_ids, old_names)
        new_coded = [(i, n) for i, n in new_all if i]
        old_coded = [(i, n) for i, n in old_all if i]
        old_name_by_id = dict(old_coded)

        # Your script only touched the property when at least one new person
        # could be resolved (``if authors: change_dict["author"] = ...``). Kept:
        # if the new side still lists people but none of them has a zbMATH code,
        # that looks like zbMATH losing the codes rather than the people leaving,
        # so neither the people nor the name strings are changed. Only a new
        # side with nobody on it at all is a real removal. A record whose people
        # never had codes is not affected: its name strings are merged as usual.
        if not new_coded and old_coded and (any(new_ids) or any(new_names)):
            self.log.warning(
                "de_number %s: %s still lists people but none has a zbMATH "
                "code; leaving the people on the item alone",
                row.get(DE_COL), name_col,
            )
            return None

        new_qids: list[str] = []
        relabel: list[tuple[str, str]] = []
        for author_id, raw_name in new_coded:
            label = person_label(raw_name, label_fn)
            qid = self.resolve_author(author_id, label, create=True)
            if not qid:
                continue
            new_qids.append(qid)
            previous = old_name_by_id.get(author_id)
            if label and previous is not None and person_label(previous, label_fn) != label:
                relabel.append((qid, label))

        old_qids = []
        for author_id, raw_name in old_coded:
            qid = self.resolve_author(
                author_id, person_label(raw_name, label_fn), create=False
            )
            if qid:
                old_qids.append(qid)

        def strings(all_pairs):
            return [author_label(n) for i, n in all_pairs if not i and n]

        return PersonChanges(
            new_qids, old_qids, relabel, strings(new_all), strings(old_all)
        )

    # ── the main loop ────────────────────────────────────────────────────

    def apply(
        self,
        diff_path: str,
        mode: str = "non_arxiv",
        resume_after_de: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        missing_path: Optional[str] = None,
        max_rows: Optional[int] = None,
    ) -> dict:
        """Apply every row of the diff that belongs to ``mode``.

        The file is streamed, so memory use does not depend on its size.

        Args:
            diff_path: Diff file from :func:`mardi_importer.zbmath.diff.build_diff`.
            mode: ``"non_arxiv"`` or ``"arxiv"``.
            resume_after_de: Continue after this de_number. Raises if it never
                turns up, rather than quietly processing nothing.
            progress_callback: Called with a de_number once that record is
                fully written, including its preprint link.
            missing_path: File that collects rows whose item is not in the
                portal; the pass then carries on with the next row. Without
                it such a row raises :class:`ZBMathItemNotFound`.
            max_rows: Stop after this many rows of ``mode`` (0 applies
                nothing). ``stopped_early`` in the result tells whether rows
                were left; resuming after the last reported de_number picks
                them up. ``None`` processes everything.

        Returns:
            The statistics of this pass.
        """
        if mode not in ("non_arxiv", "arxiv"):
            raise ZBMathUpdateError(f"unknown mode {mode!r}")
        self.stats = self._new_stats()
        missing: Optional[MissingRows] = None

        self.log_resolved_properties()
        self.check_property_consistency()

        looking_for_resume = resume_after_de is not None
        found_resume = not looking_for_resume

        for row in self._rows(diff_path):
            de_number = (row.get(DE_COL) or "").strip()

            if not found_resume:
                if de_number == str(resume_after_de):
                    found_resume = True
                continue

            is_arxiv = self.is_arxiv_row(row)
            if (mode == "arxiv") != is_arxiv:
                continue

            if max_rows is not None and self.stats["rows_seen"] >= max_rows:
                self.stats["stopped_early"] = True
                self.log.info(
                    "%s pass stopped after %d row(s) as requested; the next row "
                    "would have been de_number %s", mode, max_rows, de_number,
                )
                break

            self.stats["rows_seen"] += 1
            try:
                wrote = (
                    self._apply_arxiv_row(row, de_number)
                    if is_arxiv
                    else self._apply_publication_row(row, de_number)
                )
            except ZBMathItemNotFound as e:
                if missing_path is None:
                    raise
                missing = missing or MissingRows(missing_path, self._fieldnames)
                if missing.add(row):
                    self.stats["missing_in_portal"] += 1
                self.log.warning("%s; set aside in %s", e, missing_path)
                wrote = False
            if wrote:
                self.stats["items_written"] += 1

            if progress_callback:
                progress_callback(de_number)

        if looking_for_resume and not found_resume:
            raise ZBMathUpdateError(
                f"resume position de_number={resume_after_de} is not in "
                f"{diff_path}; refusing to continue because that would silently "
                "process nothing or restart from the beginning"
            )

        return self.stats

    def _rows(self, diff_path: str) -> Iterator[dict]:
        import csv

        with open(diff_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            if reader.fieldnames is None or DE_COL not in reader.fieldnames:
                raise ZBMathUpdateError(f"{diff_path} has no {DE_COL} column")
            missing = [
                c for c in reader.fieldnames
                if not c.endswith("_new") and c != DE_COL
                and f"{c}_new" not in reader.fieldnames
            ]
            if missing:
                raise ZBMathUpdateError(
                    f"{diff_path} has columns without a _new counterpart: {missing}"
                )
            self._fieldnames = list(reader.fieldnames)
            yield from reader

    # ── publication items ────────────────────────────────────────────────

    def _apply_publication_row(self, row: dict, de_number: str) -> bool:
        touched = [c for c in NON_ARXIV_COLUMNS if self.changed(row, c) is not None]
        if not touched:
            return False
        self.stats["rows_changed"] += 1

        qid = self.find_publication(de_number)
        item = self.get_item(qid)
        changed = False
        relabels: list[tuple[str, str]] = []
        arxiv_id: Optional[str] = None

        # --- title: label and title claim -------------------------------
        title_change = self.changed(row, "document_title")
        if title_change is not None and not self.deleted(title_change):
            item.labels.set(language="en", value=title_change)
            changed = True
            changed |= self.merge_claims(
                item, self.prop("document_title"),
                parse_single(title_change), parse_single(row.get("document_title")),
                language="en",
            )
        elif self.deleted(title_change):
            self.log.warning(
                "de_number %s: zbMATH dropped the title; the label is kept and "
                "only the title claim is removed", de_number,
            )
            changed |= self.merge_claims(
                item, self.prop("document_title"),
                [], parse_single(row.get("document_title")), language="en",
            )

        # --- simple list/scalar properties ------------------------------
        simple = [
            ("classifications", "classifications", parse_list, {}),
            ("keywords", "keywords", parse_list, {}),
            ("links", "links", None, {}),          # handled below
            ("doi", "doi", parse_doi, {}),
            ("review_text", "review_text", parse_single, {}),
            ("license", "license", parse_licenses, {}),
        ]
        for column, prop_key, parser, kwargs in simple:
            change = self.changed(row, column)
            if change is None:
                continue
            if column == "links":
                continue
            new_values = [] if self.deleted(change) else parser(change)
            if not new_values and not self.deleted(change):
                self.log.warning(
                    "de_number %s: %s changed to %r but nothing usable came out "
                    "of it; leaving the property alone", de_number, column, change,
                )
                continue
            changed |= self.merge_claims(
                item, self.prop(prop_key), new_values, parser(row.get(column)),
                **kwargs,
            )

        # --- links (and the arXiv id riding along) ----------------------
        links_change = self.changed(row, "links")
        if links_change is not None:
            old_links, _ = parse_links(row.get("links"))
            usable = True
            if self.deleted(links_change):
                new_links = []
            else:
                new_links, arxiv_id = parse_links(links_change)
                # Neither a valid link nor an arXiv ID came out of it: that is a
                # parse failure, not zbMATH removing the links, so leave them.
                # (Only an arXiv link left is a real change, and removes the rest.)
                if not new_links and arxiv_id is None:
                    usable = False
                    self.log.warning(
                        "de_number %s: links changed to %r but nothing usable came "
                        "out of it; leaving the property alone",
                        de_number, links_change,
                    )
            if usable:
                changed |= self.merge_claims(
                    item, self.prop("links"), new_links, old_links
                )
        if arxiv_id is None:
            # A link list that did not change can still point at an arXiv item
            # whose preprint link was never established.
            _, arxiv_id = parse_links(row.get("links"))

        # --- publication date: the year of publication -------------------
        if self.changed(row, "publication_year") is not None:
            changed |= self._merge_publication_date(
                item, row, self.prop("publication_year"), de_number
            )

        # --- journal ----------------------------------------------------
        serial_change = self.changed(row, "serial")
        if serial_change is not None:
            old_journal = self.resolve_journal(
                (row.get("serial") or "").split(";")[-1], create=False
            )
            if self.deleted(serial_change):
                new_journal = None
            else:
                new_journal = self.resolve_journal(
                    serial_change.split(";")[-1], create=True
                )
            if new_journal or self.deleted(serial_change):
                changed |= self.merge_claims(
                    item, self.prop("serial"),
                    [new_journal] if new_journal else [],
                    [old_journal] if old_journal else [],
                )
            else:
                self.log.warning(
                    "de_number %s: could not resolve the new journal %r; "
                    "leaving the existing one in place", de_number, serial_change,
                )

        # --- authors and reviewers --------------------------------------
        authors = self._person_values(row, "author", "author_ids", author_label)
        if authors:
            relabels.extend(authors.relabel)
            changed |= self.merge_claims(
                item, self.prop("author"), authors.new_qids, authors.old_qids
            )
            # People without a zbMATH code, kept as name strings. A name that
            # gained a code is in old_strings but not new_strings, so it goes
            # here while the person was added just above.
            changed |= self.merge_claims(
                item, self.prop("author_name_string"),
                authors.new_strings, authors.old_strings,
            )

        # Reviewers without a code are not written by the importer at all, so
        # only the reviewer property is merged.
        reviewers = self._person_values(
            row, "review_sign", "reviewer_id", reviewer_label
        )
        if reviewers:
            relabels.extend(reviewers.relabel)
            changed |= self.merge_claims(
                item, self.prop("reviewers"),
                reviewers.new_qids, reviewers.old_qids,
            )

        # --- a DOI can fill in a title or journal that was never known ---
        changed |= self._fill_from_doi(row, item, de_number)

        if changed:
            _with_backoff(item.write, self.log, f"writing item {qid} (de_number {de_number})")
            self.log.info(
                "updated %s (de_number %s): %s", qid, de_number, ", ".join(touched)
            )

        for author_qid, label in relabels:
            self.relabel_author(author_qid, label)

        if arxiv_id:
            self._ensure_preprint_link(arxiv_id, qid)

        return changed

    def _fill_from_doi(self, row: dict, item, de_number: str) -> bool:
        """Ask Crossref for a title or journal the dump never had.

        Mirrors what ``ZBMathSource.push`` does on import: only fields that are
        empty in *both* dumps are filled, so this never overwrites zbMATH data.
        """
        doi_change = self.changed(row, "doi")
        if doi_change is None or self.deleted(doi_change):
            return False
        doi = doi_change.strip()
        changed = False

        # The licence-conflict placeholder is the main reason a title is
        # missing: ``ZBMathSource.push`` asks Crossref exactly when it sees it.
        old_title = row.get("document_title")
        title_blank = (
            is_blank(old_title) or has_conflict(old_title)
        ) and not self.changed(row, "document_title")
        if title_blank:
            title = get_info_from_doi(doi=doi, key="document_title")
            if title:
                self.log.info(
                    "de_number %s: took the title from the DOI: %r", de_number, title
                )
                item.labels.set(language="en", value=title)
                changed = True
                changed |= self.merge_claims(
                    item, self.prop("document_title"), [title], [], language="en"
                )

        old_serial = row.get("serial")
        serial_blank = (
            is_blank(old_serial) or has_conflict(old_serial)
        ) and not self.changed(row, "serial")
        if serial_blank:
            journal_string = get_info_from_doi(doi=doi, key="journal")
            if journal_string:
                journal = self.resolve_journal(journal_string, create=True)
                if journal:
                    self.log.info(
                        "de_number %s: took the journal from the DOI: %r",
                        de_number, journal_string,
                    )
                    changed |= self.merge_claims(
                        item, self.prop("serial"), [journal], []
                    )
        return changed

    def _ensure_preprint_link(self, arxiv_id: str, publication_qid: str) -> None:
        """Make sure the arXiv item points at the publication, exactly once."""
        prop_nr = self.prop("arxiv_id")
        qids = _with_backoff(
            lambda: self.api.search_entity_by_value(prop_nr, arxiv_id),
            self.log, f"looking up arXiv id {arxiv_id}",
        )
        if not qids:
            self.log.info(
                "no arXiv item for %s yet; no preprint link to establish", arxiv_id
            )
            return
        prop_nr = self.prop("preprint")
        for arxiv_qid in qids:
            arxiv_item = self.get_item(arxiv_qid)
            present = {
                claim_scalar(c) for c in arxiv_item.claims.get(prop_nr)
                if not c.removed
            }
            if publication_qid in present:
                continue
            claim = _with_backoff(
                lambda: self.api.get_claim(prop_nr, publication_qid),
                self.log, f"building claim {prop_nr} = {publication_qid}",
            )
            if not claim:
                self.log.warning(
                    "could not build the preprint claim %s -> %s",
                    arxiv_qid, publication_qid,
                )
                continue
            arxiv_item.add_claims([claim], ActionIfExists.FORCE_APPEND)
            _with_backoff(arxiv_item.write, self.log, f"writing arXiv item {arxiv_qid}")
            self.stats["preprint_links_added"] += 1
            self.log.info("linked arXiv item %s -> %s", arxiv_qid, publication_qid)

    # ── arXiv items ──────────────────────────────────────────────────────

    def _apply_arxiv_row(self, row: dict, de_number: str) -> bool:
        touched = [c for c in ARXIV_COLUMNS if self.changed(row, c) is not None]
        if not touched:
            return False
        self.stats["rows_changed"] += 1

        qid = self.find_arxiv_item(self.arxiv_id_for_row(row, de_number))
        item = self.get_item(qid)
        changed = False
        relabels: list[tuple[str, str]] = []

        # The importer keeps the title of an arXiv item as a label only.
        title_change = self.changed(row, "document_title")
        if title_change is not None and not self.deleted(title_change):
            item.labels.set(language="en", value=title_change)
            changed = True

        for column, prop_key, parser in (
            ("classifications", "arxiv_classifications", parse_list),
            ("source", "source", parse_source),
            ("doi", "doi", lambda v: [d for d in parse_doi(v) if "ARXIV" in d]),
        ):
            change = self.changed(row, column)
            if change is None:
                continue
            new_values = [] if self.deleted(change) else parser(change)
            if not new_values and not self.deleted(change):
                # e.g. a source with no [classification] in it: your script
                # skipped those, and removing the old value would be worse.
                self.log.warning(
                    "de_number %s: %s changed to %r but nothing usable came out "
                    "of it; leaving the property alone", de_number, column, change,
                )
                continue
            changed |= self.merge_claims(
                item, self.prop(prop_key), new_values, parser(row.get(column)),
            )

        # ``create_arxiv_item`` writes the year and never the creation date,
        # so an arXiv item's date claim is year-precision only.
        year_change = self.changed(row, "publication_year")
        if year_change is not None:
            new_year = [] if self.deleted(year_change) else parse_year(year_change)
            if not new_year and not self.deleted(year_change):
                self.log.warning(
                    "de_number %s: publication_year %r is not a usable year",
                    de_number, year_change,
                )
            else:
                changed |= self.merge_claims(
                    item, self.prop("arxiv_publication_year"),
                    new_year, parse_year(row.get("publication_year")),
                )


        authors = self._person_values(row, "author", "author_ids", author_label)
        if authors:
            relabels.extend(authors.relabel)
            changed |= self.merge_claims(
                item, self.prop("author"), authors.new_qids, authors.old_qids
            )
            changed |= self.merge_claims(
                item, self.prop("author_name_string"),
                authors.new_strings, authors.old_strings,
            )

        if changed:
            _with_backoff(item.write, self.log, f"writing arXiv item {qid} (de_number {de_number})")
            self.log.info(
                "updated arXiv item %s (de_number %s): %s",
                qid, de_number, ", ".join(touched),
            )

        for author_qid, label in relabels:
            self.relabel_author(author_qid, label)

        return changed

    # ── references ───────────────────────────────────────────────────────

    def apply_references(
        self,
        diff_path: str,
        resume_after_de: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        batch_size: int = 100,
        missing_path: Optional[str] = None,
        max_rows: Optional[int] = None,
    ) -> dict:
        """Merge changed reference lists onto the publication items.

        The import flow's reference pass only ever appends; here the old list
        is known, so references zbMATH dropped are removed as well — again
        without touching references that came from somewhere else.

        Both kinds of record are covered: an arXiv record's references go on
        its arXiv item, everything else on the item carrying the DE number.

        ``missing_path`` and ``max_rows`` work as in :meth:`apply`; only rows
        whose references changed count towards ``max_rows``.
        """
        prop_nr = self.prop("references")
        stats = {"rows_seen": 0, "items_written": 0, "added": 0, "removed": 0,
                 "arxiv_roots": 0, "missing_in_portal": 0, "stopped_early": False}
        missing: Optional[MissingRows] = None

        looking_for_resume = resume_after_de is not None
        found_resume = not looking_for_resume

        for row in self._rows(diff_path):
            de_number = (row.get(DE_COL) or "").strip()
            if not found_resume:
                if de_number == str(resume_after_de):
                    found_resume = True
                continue

            change = self.changed(row, "references")
            if change is None:
                continue
            if max_rows is not None and stats["rows_seen"] >= max_rows:
                stats["stopped_early"] = True
                self.log.info(
                    "references pass stopped after %d row(s) as requested; the "
                    "next row would have been de_number %s", max_rows, de_number,
                )
                break
            stats["rows_seen"] += 1

            old_refs = parse_list(row.get("references"))
            new_refs = [] if self.deleted(change) else parse_list(change)

            new_qids = self._reference_qids(new_refs, batch_size)
            old_qids = self._reference_qids(old_refs, batch_size)

            # An arXiv record's references belong on its arXiv item, which is
            # found by arXiv ID; a publication's belong on the item carrying
            # its DE number.
            if self.is_arxiv_row(row):
                stats["arxiv_roots"] += 1
            try:
                qid = self.root_qid(row, de_number)
            except ZBMathItemNotFound as e:
                if missing_path is None:
                    raise
                missing = missing or MissingRows(missing_path, self._fieldnames)
                if missing.add(row):
                    stats["missing_in_portal"] += 1
                self.log.warning("%s; set aside in %s", e, missing_path)
                if progress_callback:
                    progress_callback(de_number)
                continue
            item = self.get_item(qid)
            before_added = self.stats["claims_added"]
            before_removed = self.stats["claims_removed"]
            if self.merge_claims(item, prop_nr, new_qids, old_qids):
                _with_backoff(item.write, self.log, f"writing references for {qid}")
                stats["items_written"] += 1
                self.log.info("updated references of %s (de_number %s)", qid, de_number)
            stats["added"] += self.stats["claims_added"] - before_added
            stats["removed"] += self.stats["claims_removed"] - before_removed

            if progress_callback:
                progress_callback(de_number)

        if looking_for_resume and not found_resume:
            raise ZBMathUpdateError(
                f"resume position de_number={resume_after_de} is not in {diff_path}"
            )
        return stats

    def _reference_qids(self, de_numbers: list[str], batch_size: int) -> list[str]:
        if not de_numbers:
            return []
        prop_nr = self.prop("de_number")
        mapping: dict = {}
        for start in range(0, len(de_numbers), batch_size):
            chunk = de_numbers[start:start + batch_size]
            mapping.update(_with_backoff(
                lambda: self.api.batch_search_by_value(prop_nr, chunk),
                self.log, f"looking up {len(chunk)} referenced de_numbers",
            ))
        return [mapping[de][0] for de in de_numbers if mapping.get(de)]
