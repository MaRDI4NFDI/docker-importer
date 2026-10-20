"""Offline tests for the zbMATH update pipeline.

Covers the diff builder end to end (hashes -> diff -> deleted/new) and the
claim-merge policy of ``ZBMathUpdater``, which is the part that decides what
gets removed from a real item.
"""

import os
import sys
import tempfile
import types
import unittest
import unittest.mock
from unittest.mock import MagicMock

from tests.prefect_stub import install_prefect_stub

install_prefect_stub(force=True)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(REPO_ROOT, "src")
for path in (REPO_ROOT, SRC):
    if path not in sys.path:
        sys.path.insert(0, path)


def _install_stubs() -> None:
    """Provide the minimum needed to import the zbMATH submodules directly.

    ``mardi_importer.__init__`` pulls in every source and ``mardiclient``; the
    modules under test only need ``Importer`` to exist.
    """
    if "mardi_importer" not in sys.modules:
        package = types.ModuleType("mardi_importer")
        package.__path__ = [os.path.join(SRC, "mardi_importer")]

        class Importer:
            _apis = {}

            @classmethod
            def get_api(cls, name):
                return cls._apis.get(name)

        package.Importer = Importer
        sys.modules["mardi_importer"] = package

    # The zbMATH package's __init__ imports ZBMathSource, which needs the full
    # dependency stack. Register the package with an empty body so the modules
    # under test can be imported directly while keeping relative imports working.
    if "mardi_importer.zbmath" not in sys.modules:
        zbmath = types.ModuleType("mardi_importer.zbmath")
        zbmath.__path__ = [os.path.join(SRC, "mardi_importer", "zbmath")]
        sys.modules["mardi_importer.zbmath"] = zbmath
        sys.modules["mardi_importer"].zbmath = zbmath

    if "habanero" not in sys.modules:
        habanero = types.ModuleType("habanero")
        habanero.Crossref = MagicMock()
        sys.modules["habanero"] = habanero

    if "wikibaseintegrator" not in sys.modules:
        wbi = types.ModuleType("wikibaseintegrator")
        wbi.__path__ = []
        sys.modules["wikibaseintegrator"] = wbi
    if "wikibaseintegrator.wbi_enums" not in sys.modules:
        sys.modules["wikibaseintegrator.wbi_enums"] = types.ModuleType(
            "wikibaseintegrator.wbi_enums"
        )
    if "wikibaseintegrator.wbi_helpers" not in sys.modules:
        helpers = types.ModuleType("wikibaseintegrator.wbi_helpers")
        helpers.execute_sparql_query = MagicMock()
        helpers.merge_items = MagicMock()
        sys.modules["wikibaseintegrator.wbi_helpers"] = helpers

    # Other test modules install their own stub of this enum, and the suite
    # shares one interpreter, so fill in only what is missing instead of
    # depending on which module got there first.
    enums = sys.modules["wikibaseintegrator.wbi_enums"]
    action = getattr(enums, "ActionIfExists", None)
    if action is None:
        class ActionIfExists:
            pass

        action = ActionIfExists
        enums.ActionIfExists = action
    for member in ("FORCE_APPEND", "REPLACE_ALL", "APPEND_OR_REPLACE", "KEEP"):
        if not hasattr(action, member):
            setattr(action, member, member)


_install_stubs()

from mardi_importer.zbmath import diff as diffmod  # noqa: E402
from mardi_importer.zbmath.ZBMathUpdate import (  # noqa: E402
    ZBMathUpdateError,
    ZBMathUpdater,
    author_label,
    importer_written_date,
    parse_doi,
    parse_licenses,
    parse_list,
    parse_year,
    reviewer_label,
)

HEADER = [
    "de_number", "creation_date", "document_title", "classifications",
    "keywords", "publication_year", "serial", "links", "doi", "author",
    "author_ids", "zbl_id", "references", "review_text", "review_sign",
    "reviewer_id", "license", "source", "language",
]


def _row(**overrides):
    row = {c: "None" for c in HEADER}
    row.update(overrides)
    return row


def _write_dump(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(HEADER) + "\n")
        for row in rows:
            f.write("\t".join(str(row[c]) for c in HEADER) + "\n")


def _read_diff(path):
    import csv

    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


class DiffBuilderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        self.addCleanup(self.tmp.cleanup)

    def _build(self, old_rows, new_rows):
        old = os.path.join(self.dir, "zbmath_data_dump_old.csv")
        new = os.path.join(self.dir, "zbmath_data_dump_new.csv")
        _write_dump(old, old_rows)
        _write_dump(new, new_rows)
        old_hash = diffmod.hash_dump(old)
        new_hash = diffmod.hash_dump(new)
        out = os.path.join(self.dir, "differences.csv")
        path, count = diffmod.build_diff(old, new, old_hash, new_hash, out)
        return _read_diff(path), count, (old, new, old_hash, new_hash)

    def test_unchanged_record_is_not_in_the_diff(self):
        row = _row(de_number="1", document_title="Title", classifications="11A05")
        rows, count, _ = self._build([row], [dict(row)])
        self.assertEqual(count, 0)
        self.assertEqual(rows, [])

    def test_changed_field_carries_the_new_value(self):
        old = _row(de_number="1", document_title="Old", classifications="11A05")
        new = _row(de_number="1", document_title="New", classifications="11A05")
        rows, count, _ = self._build([old], [new])
        self.assertEqual(count, 1)
        self.assertEqual(rows[0]["document_title"], "Old")
        self.assertEqual(rows[0]["document_title_new"], "New")
        # untouched columns stay empty on the _new side
        self.assertEqual(rows[0]["classifications_new"], "")

    def test_emptied_field_is_marked_as_deleted(self):
        old = _row(de_number="1", keywords="prime;number", document_title="T")
        new = _row(de_number="1", keywords="None", document_title="T")
        rows, _, _ = self._build([old], [new])
        self.assertEqual(rows[0]["keywords_new"], diffmod.DELETED_SENTINEL)
        self.assertEqual(rows[0]["keywords"], "prime;number")

    def test_creation_date_alone_does_not_produce_a_row(self):
        """The datestamp is not written anywhere, so a change to it alone is
        not a change worth a diff row."""
        old = _row(de_number="1", creation_date="2024-01-01T00:00:00Z", document_title="T")
        new = _row(de_number="1", creation_date="2025-01-01T00:00:00Z", document_title="T")
        rows, count, _ = self._build([old], [new])
        self.assertEqual(count, 0)

    def test_creation_date_still_travels_with_a_real_change(self):
        """It stays in the diff file as information, it just never triggers one."""
        old = _row(de_number="1", creation_date="2024-01-01T00:00:00Z", document_title="T")
        new = _row(de_number="1", creation_date="2025-01-01T00:00:00Z", document_title="T2")
        rows, count, _ = self._build([old], [new])
        self.assertEqual(count, 1)
        self.assertEqual(rows[0]["document_title_new"], "T2")

    def test_conflict_placeholder_is_never_written_as_a_value(self):
        old = _row(de_number="1", document_title="Real title")
        new = _row(de_number="1", document_title=diffmod.CONFLICT_STRING + ".")
        rows, count, _ = self._build([old], [new])
        self.assertEqual(count, 0, "a licence conflict is not a change")

    def test_conflict_placeholder_is_not_treated_as_a_deletion(self):
        old = _row(de_number="1", document_title=diffmod.CONFLICT_STRING + ".")
        new = _row(de_number="1", document_title="None", keywords="k")
        rows, _, _ = self._build([old], [new])
        self.assertEqual(rows[0]["document_title_new"], "")

    def test_numbers_survive_as_written(self):
        old = _row(de_number="1", publication_year="2023", document_title="T")
        new = _row(de_number="1", publication_year="2024", document_title="T")
        rows, _, _ = self._build([old], [new])
        self.assertEqual(rows[0]["publication_year_new"], "2024")
        self.assertNotIn(".0", rows[0]["publication_year_new"])

    def test_every_column_has_a_new_counterpart(self):
        old = _row(de_number="1", document_title="A")
        new = _row(de_number="1", document_title="B")
        rows, _, _ = self._build([old], [new])
        for column in HEADER:
            if column == "de_number":
                continue
            self.assertIn(f"{column}_new", rows[0])

    def test_mismatched_headers_raise(self):
        old = os.path.join(self.dir, "old.csv")
        new = os.path.join(self.dir, "new.csv")
        _write_dump(old, [_row(de_number="1")])
        with open(new, "w", encoding="utf-8") as f:
            f.write("de_number\tdocument_title\n1\tT\n")
        old_hash = diffmod.hash_dump(old)
        new_hash = diffmod.hash_dump(new)
        with self.assertRaises(diffmod.ZBMathDiffError):
            diffmod.build_diff(
                old, new, old_hash, new_hash, os.path.join(self.dir, "d.csv")
            )

    def test_reordered_unordered_column_is_not_a_change(self):
        """The order zbMATH lists keywords or MSC codes in carries no meaning."""
        old = _row(de_number="1", keywords="prime;number;sieve",
                   classifications="11A05;11B13")
        new = _row(de_number="1", keywords="sieve;prime;number",
                   classifications="11B13;11A05")
        rows, count, _ = self._build([old], [new])
        self.assertEqual(count, 0, "a reorder must not reach the portal")
        self.assertEqual(rows, [])

    def test_reordered_column_hashes_the_same(self):
        """So a reordered record is not even a comparison candidate."""
        a = os.path.join(self.dir, "a.csv")
        b = os.path.join(self.dir, "b.csv")
        _write_dump(a, [_row(de_number="1", keywords="prime;number")])
        _write_dump(b, [_row(de_number="1", keywords="number;prime")])
        self.assertEqual(
            diffmod.read_hashes(diffmod.hash_dump(a)),
            diffmod.read_hashes(diffmod.hash_dump(b)),
        )

    def test_reordered_authors_is_still_a_change(self):
        """author and author_ids are aligned by position, so a reorder of one
        is a genuine re-pairing and must not be collapsed."""
        old = _row(de_number="1", author="A, A;B, B", author_ids="a1;b1")
        new = _row(de_number="1", author="B, B;A, A", author_ids="a1;b1")
        rows, count, _ = self._build([old], [new])
        self.assertEqual(count, 1)
        self.assertEqual(rows[0]["author_new"], "B, B;A, A")

    def test_placeholder_and_empty_cell_compare_equal(self):
        old = _row(de_number="1", keywords="None", document_title="T")
        new = _row(de_number="1", keywords="", document_title="T")
        rows, count, _ = self._build([old], [new])
        self.assertEqual(count, 0)

    def test_a_real_change_in_an_unordered_column_is_still_caught(self):
        old = _row(de_number="1", keywords="prime;number")
        new = _row(de_number="1", keywords="prime;number;sieve")
        rows, count, _ = self._build([old], [new])
        self.assertEqual(count, 1)
        self.assertEqual(rows[0]["keywords_new"], "prime;number;sieve")

    def test_hash_file_name_carries_the_scheme_version(self):
        """A scheme change must not silently reuse an incompatible hash file."""
        path = os.path.join(self.dir, "zbmath_data_dump_x.csv")
        _write_dump(path, [_row(de_number="1")])
        produced = diffmod.hash_dump(path)
        self.assertEqual(produced, diffmod.hash_path_for(path))
        self.assertIn(f"hashes-v{diffmod.HASH_SCHEME}", produced)

    def test_duplicate_de_number_raises(self):
        path = os.path.join(self.dir, "dup.csv")
        _write_dump(path, [_row(de_number="1"), _row(de_number="1")])
        with self.assertRaises(diffmod.ZBMathDiffError):
            diffmod.hash_dump(path)

    def test_batching_finds_every_changed_record(self):
        old_rows = [_row(de_number=str(i), document_title=f"old{i}") for i in range(25)]
        new_rows = [_row(de_number=str(i), document_title=f"new{i}") for i in range(25)]
        old = os.path.join(self.dir, "zbmath_data_dump_old.csv")
        new = os.path.join(self.dir, "zbmath_data_dump_new.csv")
        _write_dump(old, old_rows)
        _write_dump(new, new_rows)
        out = os.path.join(self.dir, "differences.csv")
        _, count = diffmod.build_diff(
            old, new, diffmod.hash_dump(old), diffmod.hash_dump(new), out,
            batch_size=4,
        )
        self.assertEqual(count, 25)
        seen = {r["de_number"] for r in _read_diff(out)}
        self.assertEqual(seen, {str(i) for i in range(25)})

    def test_deleted_records(self):
        old = os.path.join(self.dir, "old.csv")
        new = os.path.join(self.dir, "new.csv")
        _write_dump(old, [_row(de_number="1"), _row(de_number="2")])
        _write_dump(
            new,
            [_row(de_number="2"), _row(de_number="3", document_title="Fresh")],
        )
        deleted, n_deleted = diffmod.build_deleted(
            diffmod.hash_dump(old), diffmod.hash_dump(new),
            os.path.join(self.dir, "deleted.tsv"),
        )
        self.assertEqual(n_deleted, 1)
        with open(deleted, encoding="utf-8") as f:
            self.assertEqual(f.read().split(), ["de_number", "1"])

    def test_new_records_are_not_in_the_deleted_list(self):
        old = os.path.join(self.dir, "old.csv")
        new = os.path.join(self.dir, "new.csv")
        _write_dump(old, [_row(de_number="1")])
        _write_dump(new, [_row(de_number="1"), _row(de_number="2")])
        _, n_deleted = diffmod.build_deleted(
            diffmod.hash_dump(old), diffmod.hash_dump(new),
            os.path.join(self.dir, "deleted.tsv"),
        )
        self.assertEqual(n_deleted, 0)


class ParsingTest(unittest.TestCase):
    def test_list_values_are_stripped(self):
        self.assertEqual(parse_list("11A05; 11B13 ;"), ["11A05", "11B13"])

    def test_placeholders_are_blank(self):
        self.assertEqual(parse_list("None"), [])
        self.assertEqual(parse_list(""), [])

    def test_doi_is_upper_cased(self):
        self.assertEqual(parse_doi("10.1234/AbC"), ["10.1234/ABC"])

    def test_year_becomes_a_year_precision_time(self):
        self.assertEqual(parse_year("2024"), ["+2024-00-00T00:00:00Z"])

    def test_non_year_is_rejected_rather_than_written(self):
        self.assertEqual(parse_year("in press"), [])
        self.assertEqual(parse_year("2024.5"), [])

    def test_float_formatted_year_is_the_same_year(self):
        self.assertEqual(parse_year("2024.0"), ["+2024-00-00T00:00:00Z"])

    def test_author_label_reorders_the_name(self):
        self.assertEqual(author_label("Mustermann, Max"), "Max Mustermann")

    def test_reviewer_label_matches_the_importer(self):
        self.assertEqual(reviewer_label("Müller, Hans (Berlin)"), "Müller, Hans")
        self.assertEqual(reviewer_label("Müller, Hans / Berlin"), "Müller, Hans")

    def test_importer_written_date_reproduces_push(self):
        # a real datestamp: what insert_claims wrote instead of the year
        self.assertEqual(importer_written_date("2024-08-21T14:32:25Z"),
                         ["+2024-08-21T00:00:00Z"])
        # the API's placeholder ends in Z, push's check does not, so it was
        # written as a date
        self.assertEqual(importer_written_date("0001-01-01T00:00:00Z"),
                         ["+0001-01-01T00:00:00Z"])
        # push caught this spelling and wrote the year
        self.assertEqual(importer_written_date("0001-01-01T00:00:00"), [])
        self.assertEqual(importer_written_date("None"), [])

    def test_canonical_collapses_order_only_when_unordered(self):
        self.assertEqual(
            diffmod.canonical("b;a", unordered=True),
            diffmod.canonical("a;b", unordered=True),
        )
        self.assertNotEqual(
            diffmod.canonical("b;a"), diffmod.canonical("a;b")
        )

    def test_canonical_collapses_placeholders_and_whitespace(self):
        self.assertEqual(diffmod.canonical("None"), "")
        self.assertEqual(diffmod.canonical("  T  "), "T")
        self.assertEqual(diffmod.canonical("a; b ;;None", unordered=True), "a;b")

    def test_parse_list_is_the_diff_builders_split(self):
        self.assertEqual(parse_list("a; b ;;None"), diffmod.split_list("a; b ;;None"))

    def test_licenses_map_to_qids(self):
        self.assertEqual(
            parse_licenses("https://creativecommons.org/licenses/by/4.0/"),
            ["Q57056"],
        )
        self.assertEqual(parse_licenses("https://example.invalid/nope"), [])


class FakeClaim:
    def __init__(self, value, datatype="string", precision=None):
        self.removed = False
        payload = {"value": value}
        if datatype == "time":
            payload = {"value": {"time": value, "precision": precision or 9}}
        elif datatype == "wikibase-item":
            payload = {"value": {"id": value}}
        elif datatype == "monolingualtext":
            payload = {"value": {"text": value}}
        self.mainsnak = types.SimpleNamespace(datavalue=payload, datatype=datatype)

    def remove(self):
        self.removed = True


class FakeClaims:
    def __init__(self):
        self._by_prop = {}

    def get(self, prop):
        return self._by_prop.get(prop, [])

    def add(self, prop, claim):
        self._by_prop.setdefault(prop, []).append(claim)


class FakeItem:
    def __init__(self, claims=None):
        self.id = "Q1"
        self.claims = FakeClaims()
        self.labels = MagicMock()
        self.written = 0
        for prop, items in (claims or {}).items():
            for claim in items:
                self.claims.add(prop, claim)

    def add_claims(self, claims, action=None):
        for claim in claims:
            self.claims.add(claim.prop_nr, claim)

    def write(self):
        self.written += 1
        return self


#: Stand-in for what the importer API resolves the Wikidata-style identifiers
#: to locally. Mirrors the shape of the real mapping, not its values.
LOCAL_PIDS = {
    "wdt:P1476": "P159",
    "wdt:P3285": "P226",
    "wdt:P577": "P28",
    "wdt:P1433": "P200",
    "wdt:P953": "P205",
    "wdt:P356": "P27",
    "wdt:P50": "P16",
    "wdt:P4032": "P1447",
}


def _updater():
    api = MagicMock()
    api.get_local_id_by_label.side_effect = lambda ident, kind: LOCAL_PIDS.get(
        ident, ident
    )
    api.get_claim.side_effect = lambda prop, value, **kw: types.SimpleNamespace(
        prop_nr=prop,
        removed=False,
        mainsnak=types.SimpleNamespace(
            datavalue={"value": value}, datatype="string"
        ),
    )
    source = types.SimpleNamespace(
        api=api,
        label_id_dict={
            "keyword_prop": "P1450",
            "review_prop": "P1448",
            "de_number_prop": "P1451",
            "mardi_profile_type_prop": "P1460",
            "mardi_person_profile_item": "Q5976445",
        },
        logger=MagicMock(),
    )
    return ZBMathUpdater(source, MagicMock())


class MergePolicyTest(unittest.TestCase):
    def test_new_value_is_added_and_old_one_removed(self):
        up = _updater()
        existing = FakeClaim("old")
        item = FakeItem({"P1": [existing]})
        changed = up.merge_claims(item, "P1", ["new"], ["old"])
        self.assertTrue(changed)
        self.assertTrue(existing.removed)
        self.assertIn("new", [c.mainsnak.datavalue["value"] for c in item.claims.get("P1")])

    def test_manually_added_value_is_preserved(self):
        up = _updater()
        from_zbmath = FakeClaim("old")
        by_hand = FakeClaim("added by a human")
        item = FakeItem({"P1": [from_zbmath, by_hand]})
        up.merge_claims(item, "P1", ["new"], ["old"])
        self.assertTrue(from_zbmath.removed)
        self.assertFalse(by_hand.removed, "only the old zbMATH value may go")

    def test_deletion_removes_without_adding(self):
        up = _updater()
        existing = FakeClaim("gone")
        other = FakeClaim("untouched")
        item = FakeItem({"P1": [existing, other]})
        changed = up.merge_claims(item, "P1", [], ["gone"])
        self.assertTrue(changed)
        self.assertTrue(existing.removed)
        self.assertFalse(other.removed)

    def test_value_kept_when_it_is_in_both_old_and_new(self):
        up = _updater()
        existing = FakeClaim("same")
        item = FakeItem({"P1": [existing]})
        changed = up.merge_claims(item, "P1", ["same"], ["same"])
        self.assertFalse(existing.removed)
        self.assertFalse(changed, "nothing to do means no write")

    def test_no_duplicate_when_the_value_is_already_there(self):
        up = _updater()
        item = FakeItem({"P1": [FakeClaim("already")]})
        changed = up.merge_claims(item, "P1", ["already"], [])
        self.assertFalse(changed)
        self.assertEqual(len(item.claims.get("P1")), 1)

    def test_removable_predicate_blocks_a_removal(self):
        up = _updater()
        existing = FakeClaim("old")
        item = FakeItem({"P1": [existing]})
        up.merge_claims(item, "P1", ["new"], ["old"], removable=lambda c: False)
        self.assertFalse(existing.removed)


class ArxivRowTest(unittest.TestCase):
    def setUp(self):
        patcher = unittest.mock.patch(
            "mardi_importer.zbmath.ZBMathUpdate.get_info_from_doi", return_value=None
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.up = _updater()
        self.item = FakeItem({"P22": [FakeClaim("math.NT")]})
        self.up.api.item.get.return_value = self.item
        self.up.api.search_entity_by_value.side_effect = (
            lambda prop, value: ["Q900"] if prop == "P21" else []
        )

    def test_source_without_a_classification_keeps_the_old_one(self):
        """Your script skipped a source with no [..]; removing would be worse."""
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "diff.csv")
            with open(path, "w", encoding="utf-8") as f:
                f.write("de_number\tzbl_id\tsource\tzbl_id_new\tsource_new\n")
                f.write("1\tarXiv:2401.00001\tarXiv [math.NT]\t\tarXiv no brackets\n")
            self.up.apply(path, mode="arxiv")
        self.assertFalse(self.item.claims.get("P22")[0].removed)


class RowHelperTest(unittest.TestCase):
    def test_arxiv_rows_are_recognised(self):
        self.assertTrue(ZBMathUpdater.is_arxiv_row({"zbl_id": "arXiv:2401.00001"}))
        self.assertFalse(ZBMathUpdater.is_arxiv_row({"zbl_id": "Zbl 1234.56789"}))
        self.assertFalse(ZBMathUpdater.is_arxiv_row({"zbl_id": "None"}))

    def test_changed_ignores_empty_new_columns(self):
        self.assertIsNone(ZBMathUpdater.changed({"doi_new": ""}, "doi"))
        self.assertEqual(ZBMathUpdater.changed({"doi_new": "10.1/x"}, "doi"), "10.1/x")

    def test_missing_resume_position_raises(self):
        up = _updater()
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "diff.csv")
            with open(path, "w", encoding="utf-8") as f:
                f.write("de_number\tdoi\tdoi_new\n1\ta\tb\n")
            up.find_publication = lambda de: "Q1"
            up.api.item.get.return_value = FakeItem()
            with self.assertRaises(ZBMathUpdateError) as ctx:
                up.apply(path, mode="non_arxiv", resume_after_de="999999")
            self.assertIn("resume position", str(ctx.exception))

    def test_diff_without_new_columns_raises(self):
        up = _updater()
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "diff.csv")
            with open(path, "w", encoding="utf-8") as f:
                f.write("de_number\tdoi\n1\ta\n")
            with self.assertRaises(ZBMathUpdateError):
                up.apply(path, mode="non_arxiv")

    def test_property_resolution_collapses_a_label_list(self):
        up = _updater()
        up.api.get_local_id_by_label.side_effect = lambda ident, kind: ["P159"]
        self.assertEqual(up.prop("document_title"), "P159")

    def test_unresolvable_property_raises(self):
        up = _updater()
        up.api.get_local_id_by_label.side_effect = lambda ident, kind: None
        with self.assertRaises(ZBMathUpdateError):
            up.prop("document_title")


class RowApplicationTest(unittest.TestCase):
    """Drive a whole diff row through ``apply`` against a fake Wikibase."""

    def setUp(self):
        # These tests must never reach out to Crossref; the DOI fallback is
        # exercised separately.
        patcher = unittest.mock.patch(
            "mardi_importer.zbmath.ZBMathUpdate.get_info_from_doi",
            return_value=None,
        )
        self.crossref = patcher.start()
        self.addCleanup(patcher.stop)

        self.up = _updater()
        self.item = FakeItem({
            LOCAL_PIDS["wdt:P1476"]: [
                FakeClaim("Old title", datatype="monolingualtext")
            ],
            "P1450": [FakeClaim("prime"), FakeClaim("hand-added keyword")],
            LOCAL_PIDS["wdt:P356"]: [FakeClaim("10.1234/OLD")],
        })
        self.up.api.item.get.return_value = self.item
        self.up.api.search_entity_by_value.side_effect = self._search
        self.up.resolve_author = lambda aid, name, create: f"QA{aid}"
        self.up.resolve_journal = lambda name, create: None
        self.up.relabel_author = MagicMock()
        self.searches = []

    def _search(self, prop, value):
        self.searches.append((prop, value))
        if prop == "P1451":
            return ["Q100"]
        return []

    def _run(self, row):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "diff.csv")
            columns = list(row)
            with open(path, "w", encoding="utf-8") as f:
                f.write("\t".join(columns) + "\n")
                f.write("\t".join(row[c] for c in columns) + "\n")
            return self.up.apply(path, mode="non_arxiv")

    @staticmethod
    def _diff_row(**values):
        row = {"de_number": "1"}
        for column in (
            "document_title", "creation_date", "keywords", "doi", "author", "author_ids",
            "zbl_id", "links", "publication_year", "serial", "classifications",
            "review_text", "review_sign", "reviewer_id", "license",
            "references", "source",
        ):
            row[column] = values.get(column, "None")
            row[f"{column}_new"] = values.get(f"{column}_new", "")
        return row

    def test_title_change_updates_label_and_claim(self):
        stats = self._run(self._diff_row(
            document_title="Old title", document_title_new="New title",
        ))
        self.item.labels.set.assert_called_once_with(
            language="en", value="New title"
        )
        values = [
            c.mainsnak.datavalue["value"] for c in self.item.claims.get(LOCAL_PIDS["wdt:P1476"])
            if not c.removed
        ]
        self.assertEqual(values, ["New title"])
        self.assertEqual(stats["items_written"], 1)
        self.assertEqual(self.item.written, 1, "one write per item")

    def test_keyword_deletion_removes_only_the_zbmath_value(self):
        self._run(self._diff_row(
            keywords="prime", keywords_new=diffmod.DELETED_SENTINEL,
        ))
        remaining = [
            c.mainsnak.datavalue["value"] for c in self.item.claims.get("P1450")
            if not c.removed
        ]
        self.assertEqual(remaining, ["hand-added keyword"])

    def test_doi_is_compared_and_written_upper_case(self):
        self._run(self._diff_row(doi="10.1234/old", doi_new="10.1234/new"))
        claims = self.item.claims.get(LOCAL_PIDS["wdt:P356"])
        self.assertTrue(
            claims[0].removed, "the old DOI is stored upper-cased and must match"
        )
        self.assertIn(
            "10.1234/NEW",
            [c.mainsnak.datavalue["value"] for c in claims if not c.removed],
        )

    def test_author_change_resolves_and_merges(self):
        self._run(self._diff_row(
            author="Old, A", author_ids="a1",
            author_new="New, N;Second, S", author_ids_new="a1;a2",
        ))
        authors = [
            c.mainsnak.datavalue["value"] for c in self.item.claims.get(LOCAL_PIDS["wdt:P50"])
            if not c.removed
        ]
        self.assertEqual(sorted(authors), ["QAa1", "QAa2"])

    def _dates(self):
        # read claims the way the updater does, so seeded time claims and
        # newly added ones compare alike
        from mardi_importer.zbmath.ZBMathUpdate import claim_scalar
        return sorted(
            claim_scalar(c)
            for c in self.item.claims.get(LOCAL_PIDS["wdt:P577"]) if not c.removed
        )

    def test_year_change_writes_the_year(self):
        self.item.claims.add(LOCAL_PIDS["wdt:P577"],
                             FakeClaim("+2023-00-00T00:00:00Z", datatype="time"))
        self._run(self._diff_row(publication_year="2023", publication_year_new="2024"))
        self.assertEqual(self._dates(), ["+2024-00-00T00:00:00Z"])

    def test_year_change_replaces_a_datestamp_the_importer_wrote(self):
        """An item not yet corrected carries the datestamp, not the year."""
        self.item.claims.add(LOCAL_PIDS["wdt:P577"],
                             FakeClaim("+2024-08-21T00:00:00Z", datatype="time"))
        self._run(self._diff_row(
            creation_date="2024-08-21T14:32:25Z",
            publication_year="2023", publication_year_new="2024",
        ))
        self.assertEqual(self._dates(), ["+2024-00-00T00:00:00Z"])

    def test_year_change_replaces_the_year_one_placeholder(self):
        self.item.claims.add(LOCAL_PIDS["wdt:P577"],
                             FakeClaim("+0001-01-01T00:00:00Z", datatype="time"))
        self._run(self._diff_row(
            creation_date="0001-01-01T00:00:00Z",
            publication_year="2001", publication_year_new="2002",
        ))
        self.assertEqual(self._dates(), ["+2002-00-00T00:00:00Z"])

    def test_year_change_keeps_a_date_added_by_hand(self):
        self.item.claims.add(LOCAL_PIDS["wdt:P577"],
                             FakeClaim("+2023-00-00T00:00:00Z", datatype="time"))
        self.item.claims.add(LOCAL_PIDS["wdt:P577"],
                             FakeClaim("+2023-05-17T00:00:00Z", datatype="time"))
        self._run(self._diff_row(publication_year="2023", publication_year_new="2024"))
        self.assertEqual(self._dates(),
                         ["+2023-05-17T00:00:00Z", "+2024-00-00T00:00:00Z"])

    def test_creation_date_change_alone_does_not_touch_the_item(self):
        """The datestamp no longer goes anywhere on a publication item."""
        self.item.claims.add(LOCAL_PIDS["wdt:P577"],
                             FakeClaim("+2023-00-00T00:00:00Z", datatype="time"))
        stats = self._run(self._diff_row(
            publication_year="2023",
            creation_date="2024-01-01T00:00:00Z",
            creation_date_new="2025-06-01T00:00:00Z",
        ))
        self.assertEqual(stats["items_written"], 0)
        self.assertEqual(self.item.written, 0)
        self.assertEqual(self._dates(), ["+2023-00-00T00:00:00Z"])

    def test_unusable_new_year_leaves_the_date_alone(self):
        self.item.claims.add(LOCAL_PIDS["wdt:P577"],
                             FakeClaim("+2023-00-00T00:00:00Z", datatype="time"))
        self._run(self._diff_row(publication_year="2023",
                                 publication_year_new="in press"))
        self.assertEqual(self._dates(), ["+2023-00-00T00:00:00Z"])

    def _capture_authors(self):
        """Record which (id, name) pairs the updater tries to resolve."""
        calls = []

        def resolve(aid, name, create):
            calls.append((aid, name, create))
            return f"QA{aid}"

        self.up.resolve_author = resolve
        return calls

    def test_name_placeholder_keeps_the_names_aligned_with_ids(self):
        """process_data writes "None" for a nameless author so the two columns
        stay lined up; dropping it would give Doe's name to bob's ID."""
        calls = self._capture_authors()
        self._run(self._diff_row(
            author="Smith, J;None;Doe, A", author_ids="smith;bob;doe",
            author_ids_new="smith;bob;doe;new",
            author_new="Smith, J;None;Doe, A;New, N",
        ))
        created = {aid: name for aid, name, create in calls if create}
        self.assertEqual(created["doe"], "A Doe")
        self.assertEqual(created["bob"], "", "bob has no name, and must not get Doe's")

    def test_id_placeholder_skips_that_author_only(self):
        """An author without a zbMATH code cannot be resolved, but must not
        shift the next author's name onto the wrong ID either."""
        calls = self._capture_authors()
        self._run(self._diff_row(
            author="A, A;B, B;C, C", author_ids="a;None;c",
            author_new="A, A;B, B;C, C;D, D", author_ids_new="a;None;c;d",
        ))
        created = {aid: name for aid, name, create in calls if create}
        self.assertEqual(created, {"a": "A A", "c": "C C", "d": "D D"})

    def test_misaligned_new_author_columns_stop_the_run(self):
        self._capture_authors()
        with self.assertRaises(ZBMathUpdateError) as ctx:
            self._run(self._diff_row(
                author="A, A", author_ids="a",
                author_new="A, A;B, B", author_ids_new="a;b;c",
            ))
        self.assertIn("cannot be matched", str(ctx.exception))

    def test_dropped_names_keep_the_authors(self):
        """IDs decide who the authors are; zbMATH losing the names must not
        remove the author claims."""
        self._capture_authors()
        self.item.claims.add(LOCAL_PIDS["wdt:P50"], FakeClaim("QAa", datatype="wikibase-item"))
        self._run(self._diff_row(
            author="A, A", author_ids="a",
            author_new=diffmod.DELETED_SENTINEL,
        ))
        kept = [c for c in self.item.claims.get(LOCAL_PIDS["wdt:P50"]) if not c.removed]
        self.assertEqual(len(kept), 1)

    def test_people_who_lose_their_codes_are_not_removed(self):
        """Matches the original script's ``if authors:`` guard."""
        self._capture_authors()
        self.item.claims.add(LOCAL_PIDS["wdt:P50"], FakeClaim("QAa", datatype="wikibase-item"))
        self.item.claims.add(LOCAL_PIDS["wdt:P50"], FakeClaim("QAb", datatype="wikibase-item"))
        self._run(self._diff_row(
            author="A, A;B, B", author_ids="a;b",
            author_ids_new="None;None",
        ))
        kept = [c for c in self.item.claims.get(LOCAL_PIDS["wdt:P50"]) if not c.removed]
        self.assertEqual(len(kept), 2)

    def test_removing_everyone_is_still_a_removal(self):
        self._capture_authors()
        self.item.claims.add(LOCAL_PIDS["wdt:P50"], FakeClaim("QAa", datatype="wikibase-item"))
        self._run(self._diff_row(
            author="A, A", author_ids="a",
            author_new=diffmod.DELETED_SENTINEL,
            author_ids_new=diffmod.DELETED_SENTINEL,
        ))
        kept = [c for c in self.item.claims.get(LOCAL_PIDS["wdt:P50"]) if not c.removed]
        self.assertEqual(kept, [])

    def test_doi_fill_treats_the_licence_placeholder_as_no_title(self):
        """That placeholder is exactly when the importer asks Crossref."""
        self.crossref.return_value = "Title from Crossref"
        self._run(self._diff_row(
            document_title=diffmod.CONFLICT_STRING + ".",
            doi="10.1/old", doi_new="10.1/new",
        ))
        self.crossref.assert_any_call(doi="10.1/new", key="document_title")
        self.item.labels.set.assert_any_call(language="en", value="Title from Crossref")

    def test_unparseable_new_links_keep_the_old_ones(self):
        self.item.claims.add(LOCAL_PIDS["wdt:P953"], FakeClaim("http://a.example/x"))
        self._run(self._diff_row(
            links="http://a.example/x", links_new="not a link",
        ))
        kept = [c for c in self.item.claims.get(LOCAL_PIDS["wdt:P953"]) if not c.removed]
        self.assertEqual(len(kept), 1)

    def test_only_an_arxiv_link_left_removes_the_others(self):
        """A real change: zbMATH now lists just the arXiv link."""
        self.item.claims.add(LOCAL_PIDS["wdt:P953"], FakeClaim("http://a.example/x"))
        self._run(self._diff_row(
            links="http://a.example/x",
            links_new="https://arxiv.org/abs/2401.00001",
        ))
        kept = [c for c in self.item.claims.get(LOCAL_PIDS["wdt:P953"]) if not c.removed]
        self.assertEqual(kept, [])

    def test_missing_item_raises_with_the_de_number(self):
        self.up.api.search_entity_by_value.side_effect = lambda prop, value: []
        with self.assertRaises(ZBMathUpdateError) as ctx:
            self._run(self._diff_row(doi="a", doi_new="b"))
        self.assertIn("1", str(ctx.exception))
        self.assertIn("no publication item", str(ctx.exception))

    def test_arxiv_rows_are_skipped_in_non_arxiv_mode(self):
        stats = self._run(self._diff_row(
            zbl_id="arXiv:2401.00001",
            document_title="Old", document_title_new="New",
        ))
        self.assertEqual(stats["rows_seen"], 0)
        self.assertEqual(self.item.written, 0)

    def test_row_with_no_relevant_change_is_not_written(self):
        stats = self._run(self._diff_row(
            language="de", references="1;2", references_new="1;2;3",
        ))
        self.assertEqual(stats["items_written"], 0)
        self.assertEqual(self.item.written, 0, "references have their own pass")

    def _reference_diff(self, d, zbl_id):
        path = os.path.join(d, "diff.csv")
        with open(path, "w", encoding="utf-8") as f:
            f.write("de_number\tzbl_id\treferences\tzbl_id_new\treferences_new\n")
            f.write("1\t" + zbl_id + "\t100\t\t100;200\n")
        return path

    def test_references_on_an_arxiv_row_use_the_arxiv_item(self):
        """An arXiv record's references belong on its arXiv item, which is
        found by arXiv ID rather than by de_number."""
        seen = []

        def search(prop, value):
            seen.append((prop, value))
            return ["Q900"] if prop == "P21" else []

        self.up.api.search_entity_by_value.side_effect = search
        self.up.api.batch_search_by_value.return_value = {
            "100": ["Q101"], "200": ["Q202"],
        }
        # the reference the old dump already carried is on the item
        self.item.claims.add("P223", FakeClaim("Q101", datatype="wikibase-item"))
        with tempfile.TemporaryDirectory() as d:
            stats = self.up.apply_references(self._reference_diff(d, "arXiv:2401.00001"))
        self.assertEqual(stats["arxiv_roots"], 1)
        self.assertEqual(stats["rows_seen"], 1)
        self.assertIn(("P21", "2401.00001"), seen)
        self.assertNotIn(("P1451", "1"), seen)
        refs = [c.mainsnak.datavalue["value"]["id"] if isinstance(
                    c.mainsnak.datavalue["value"], dict)
                else c.mainsnak.datavalue["value"]
                for c in self.item.claims.get("P223") if not c.removed]
        self.assertEqual(refs, ["Q101", "Q202"], "kept the old one, added the new")

    def test_references_on_a_publication_row_use_the_de_number(self):
        seen = []

        def search(prop, value):
            seen.append((prop, value))
            return ["Q100"] if prop == "P1451" else []

        self.up.api.search_entity_by_value.side_effect = search
        self.up.api.batch_search_by_value.return_value = {
            "100": ["Q101"], "200": ["Q202"],
        }
        with tempfile.TemporaryDirectory() as d:
            stats = self.up.apply_references(self._reference_diff(d, "Zbl 1.1"))
        self.assertEqual(stats["arxiv_roots"], 0)
        self.assertIn(("P1451", "1"), seen)

    def test_references_raise_when_the_root_item_is_missing(self):
        self.up.api.search_entity_by_value.side_effect = lambda prop, value: []
        self.up.api.batch_search_by_value.return_value = {"100": ["Q101"]}
        for zbl_id in ("Zbl 1.1", "arXiv:2401.00001"):
            with self.subTest(zbl_id=zbl_id), tempfile.TemporaryDirectory() as d:
                with self.assertRaises(ZBMathUpdateError):
                    self.up.apply_references(self._reference_diff(d, zbl_id))

    def test_de_number_lookup_uses_the_resolved_property(self):
        self._run(self._diff_row(doi="a", doi_new="10.1/b"))
        self.assertIn(("P1451", "1"), self.searches)


class BackoffTest(unittest.TestCase):
    """Portal calls go through the import flow's ``misc._with_backoff``."""

    def setUp(self):
        self.up = _updater()
        patcher = unittest.mock.patch(
            "mardi_importer.zbmath.misc.time.sleep", lambda s: self.sleeps.append(s)
        )
        self.sleeps = []
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_transient_lookup_failure_is_retried_with_growing_delays(self):
        calls = {"n": 0}

        def flaky(prop, value):
            calls["n"] += 1
            if calls["n"] < 3:
                raise ConnectionError("sparql down")
            return ["Q7"]

        self.up.api.search_entity_by_value.side_effect = flaky
        self.assertEqual(self.up.find_publication("1"), "Q7")
        self.assertEqual(self.sleeps, [120, 240])

    def test_item_fetch_is_retried(self):
        item = FakeItem()
        fails = {"get": 1}

        def get(entity_id):
            if fails["get"]:
                fails["get"] -= 1
                raise ConnectionError("timeout")
            return item

        self.up.api.item.get.side_effect = get
        self.assertIs(self.up.get_item("Q1"), item)
        self.assertEqual(self.sleeps, [120])

    def test_gives_up_after_the_import_flows_time_budget(self):
        self.up.api.search_entity_by_value.side_effect = ConnectionError("down")
        with self.assertRaises(ConnectionError):
            self.up.find_publication("1")
        self.assertEqual(self.sleeps[:6], [120, 240, 480, 960, 1920, 3600])
        self.assertTrue(all(s <= 3600 for s in self.sleeps))
        self.assertGreaterEqual(sum(self.sleeps), 10 * 60 * 60)

    def test_missing_item_is_not_retried(self):
        self.up.api.search_entity_by_value.side_effect = lambda prop, value: []
        with self.assertRaises(ZBMathUpdateError):
            self.up.find_publication("1")
        self.assertEqual(self.sleeps, [])

    def test_no_datatype_cache(self):
        self.assertFalse(hasattr(self.up, "datatype"))
        self.assertFalse(hasattr(self.up, "_datatype_cache"))


class DiffFormatTest(unittest.TestCase):
    """Values the processed dumps really contain must survive the diff."""

    def _diff(self, old_rows, new_rows):
        d = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, d)
        old, new = os.path.join(d, "old.csv"), os.path.join(d, "new.csv")
        _write_dump(old, old_rows)
        _write_dump(new, new_rows)
        out, n = diffmod.build_diff(
            old, new, diffmod.hash_dump(old), diffmod.hash_dump(new),
            os.path.join(d, "diff.csv"),
        )
        return _read_diff(out), n

    def test_double_quotes_are_written_and_read_back_unchanged(self):
        title = 'On the "Hauptvermutung" of "combinatorial" topology'
        rows, n = self._diff(
            [_row(de_number="1", document_title=title, doi="10.1/a")],
            [_row(de_number="1", document_title='A "new" title', doi="10.1/a")],
        )
        self.assertEqual(n, 1)
        self.assertEqual(rows[0]["document_title"], title)
        self.assertEqual(rows[0]["document_title_new"], 'A "new" title')

    def test_float_formatted_year_is_not_a_change(self):
        rows, n = self._diff(
            [_row(de_number="1", publication_year="2024.0")],
            [_row(de_number="1", publication_year="2024")],
        )
        self.assertEqual(n, 0)

    def test_float_formatted_year_hashes_like_the_integer(self):
        d = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, d)
        a, b = os.path.join(d, "a.csv"), os.path.join(d, "b.csv")
        _write_dump(a, [_row(de_number="1", publication_year="2024.0")])
        _write_dump(b, [_row(de_number="1", publication_year="2024")])
        self.assertEqual(
            diffmod.read_hashes(diffmod.hash_dump(a)),
            diffmod.read_hashes(diffmod.hash_dump(b)),
        )

    def test_a_real_year_change_is_written_as_an_integer(self):
        rows, _ = self._diff(
            [_row(de_number="1", publication_year="2023")],
            [_row(de_number="1", publication_year="2024.0")],
        )
        self.assertEqual(rows[0]["publication_year_new"], "2024")

class DeletionReportTest(unittest.TestCase):
    """Counting how many records would lose a field, to catch API failures."""

    def _report(self, old_rows, new_rows):
        d = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, d)
        old, new = os.path.join(d, "old.csv"), os.path.join(d, "new.csv")
        _write_dump(old, old_rows)
        _write_dump(new, new_rows)
        out, _ = diffmod.build_diff(
            old, new, diffmod.hash_dump(old), diffmod.hash_dump(new),
            os.path.join(d, "diff.csv"),
        )
        return diffmod.deletion_report(old, out)

    def test_counts_deletions_against_records_that_had_a_value(self):
        report = self._report(
            [_row(de_number=str(i), keywords="a;b") for i in range(1, 5)]
            + [_row(de_number="5")],
            [_row(de_number=str(i), keywords="None") for i in range(1, 4)]
            + [_row(de_number="4", keywords="a;b"), _row(de_number="5")],
        )
        self.assertEqual(report, {"keywords": (3, 4)})

    def test_placeholder_lists_do_not_count_as_values(self):
        report = self._report(
            [_row(de_number="1", references="123"),
             _row(de_number="2", references="None;None")],
            [_row(de_number="1", references="None"),
             _row(de_number="2", references="None")],
        )
        self.assertEqual(report, {"references": (1, 1)})

    def test_fields_the_update_never_writes_are_ignored(self):
        report = self._report(
            [_row(de_number="1", creation_date="2020-01-01T00:00:00Z", language="en")],
            [_row(de_number="1", creation_date="None", language="None")],
        )
        self.assertEqual(report, {})

    def test_mass_deletions_needs_both_share_and_count(self):
        report = {"keywords": (900, 1000), "serial": (5, 6), "doi": (1500, 100000)}
        self.assertEqual(
            diffmod.mass_deletions(report, max_share=0.5, min_count=10),
            {"keywords": (900, 1000)},
            "serial: too few; doi: too small a share",
        )
        self.assertEqual(diffmod.mass_deletions(report, max_share=0.5, min_count=1000), {})

class YearCoercionTest(unittest.TestCase):
    def test_process_data_years_become_integer_strings(self):
        from mardi_importer.zbmath.misc import year_to_str

        self.assertEqual(year_to_str(2024), "2024")
        self.assertEqual(year_to_str(2024.0), "2024")
        self.assertEqual(year_to_str(float("nan")), "None")
        self.assertEqual(year_to_str(None), "None")
        self.assertEqual(year_to_str("2024.0"), "2024")
        self.assertEqual(year_to_str("2024"), "2024")


class PassStatsTest(unittest.TestCase):
    def test_each_pass_reports_only_its_own_rows(self):
        up = _updater()
        up.api.item.get.return_value = FakeItem()
        up.api.search_entity_by_value.side_effect = lambda prop, value: ["Q1"]
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "diff.csv")
            with open(path, "w", encoding="utf-8") as f:
                f.write("de_number\tdoi\tzbl_id\tdoi_new\tzbl_id_new\n")
                f.write("1\ta\tZbl 1.1\tb\t\n")
            with unittest.mock.patch(
                "mardi_importer.zbmath.ZBMathUpdate.get_info_from_doi",
                return_value=None,
            ):
                first = dict(up.apply(path, mode="non_arxiv"))
                second = up.apply(path, mode="arxiv")
        self.assertEqual(first["rows_seen"], 1)
        self.assertEqual(second["rows_seen"], 0)
        self.assertEqual(second["claims_added"], 0)


AUTHOR_STRING_PID = "P43"


class AuthorNameStringTest(unittest.TestCase):
    """Authors without a zbMATH code live on the item as name strings."""

    # Same fake Wikibase as RowApplicationTest, without running its tests twice.
    setUp = RowApplicationTest.setUp
    _search = RowApplicationTest._search
    _run = RowApplicationTest._run
    _diff_row = staticmethod(RowApplicationTest._diff_row)

    def _values(self, prop):
        out = []
        for c in self.item.claims.get(prop):
            if c.removed:
                continue
            v = c.mainsnak.datavalue["value"]
            out.append(v["id"] if isinstance(v, dict) else v)
        return sorted(out)

    def test_renamed_author_without_code_replaces_the_name_string(self):
        self.item.claims.add(AUTHOR_STRING_PID, FakeClaim("Anna Alt"))
        self._run(self._diff_row(
            author="Alt, Anna;Doe, Jo", author_ids="None;doe.jo",
            author_new="Neu, Anna;Doe, Jo",
        ))
        self.assertEqual(self._values(AUTHOR_STRING_PID), ["Anna Neu"])

    def test_new_author_without_code_is_added_as_a_name_string(self):
        self._run(self._diff_row(
            author="Doe, Jo", author_ids="doe.jo",
            author_new="Doe, Jo;Third, Tim", author_ids_new="doe.jo;None",
        ))
        self.assertEqual(self._values(AUTHOR_STRING_PID), ["Tim Third"])

    def test_gaining_a_code_moves_the_name_string_to_a_full_author(self):
        self.item.claims.add(AUTHOR_STRING_PID, FakeClaim("Anna Alt"))
        self._run(self._diff_row(
            author="Alt, Anna;Doe, Jo", author_ids="None;doe.jo",
            author_ids_new="alt.anna;doe.jo",
        ))
        self.assertEqual(self._values(AUTHOR_STRING_PID), [])
        self.assertIn("QAalt.anna", self._values(LOCAL_PIDS["wdt:P50"]))

    def test_losing_a_code_moves_the_author_to_a_name_string(self):
        self.item.claims.add(
            LOCAL_PIDS["wdt:P50"], FakeClaim("QAalt.anna", datatype="wikibase-item")
        )
        self._run(self._diff_row(
            author="Alt, Anna;Doe, Jo", author_ids="alt.anna;doe.jo",
            author_ids_new="None;doe.jo",
        ))
        self.assertEqual(self._values(AUTHOR_STRING_PID), ["Anna Alt"])
        self.assertNotIn("QAalt.anna", self._values(LOCAL_PIDS["wdt:P50"]))

    def test_hand_added_name_string_is_kept(self):
        self.item.claims.add(AUTHOR_STRING_PID, FakeClaim("Added By Hand"))
        self.item.claims.add(AUTHOR_STRING_PID, FakeClaim("Anna Alt"))
        self._run(self._diff_row(
            author="Alt, Anna", author_ids="None",
            author_new="Neu, Anna",
        ))
        self.assertEqual(
            self._values(AUTHOR_STRING_PID), ["Added By Hand", "Anna Neu"]
        )

    def test_everyone_losing_their_codes_leaves_the_authors_alone(self):
        self.item.claims.add(
            LOCAL_PIDS["wdt:P50"], FakeClaim("QAdoe.jo", datatype="wikibase-item")
        )
        self._run(self._diff_row(
            author="Doe, Jo", author_ids="doe.jo", author_ids_new="None",
        ))
        self.assertEqual(self._values(LOCAL_PIDS["wdt:P50"]), ["QAdoe.jo"])
        self.assertEqual(self._values(AUTHOR_STRING_PID), [])

    def test_reviewers_without_a_code_are_not_written_as_name_strings(self):
        self._run(self._diff_row(
            review_sign="Old, Rev", reviewer_id="old.rev",
            review_sign_new="New, Rev", reviewer_id_new="None",
        ))
        self.assertEqual(self._values(AUTHOR_STRING_PID), [])


class PersonLabelTest(unittest.TestCase):
    def setUp(self):
        self.up = _updater()
        self.up.resolve_author = lambda aid, name, create: f"Q{aid}"

    def test_renamed_reviewer_gets_the_label_the_importer_would_give(self):
        changes = self.up._person_values(
            {
                "de_number": "1",
                "review_sign": "Smith, J. (Berlin)",
                "review_sign_new": "Smith, John (Berlin)",
                "reviewer_id": "smith.john", "reviewer_id_new": "",
            },
            "review_sign", "reviewer_id", reviewer_label,
        )
        self.assertEqual(changes.relabel, [("Qsmith.john", "John Smith")])

    def test_reviewer_affiliation_change_alone_is_not_a_rename(self):
        changes = self.up._person_values(
            {
                "de_number": "1",
                "review_sign": "Smith, John (Berlin)",
                "review_sign_new": "Smith, John / Potsdam",
                "reviewer_id": "smith.john", "reviewer_id_new": "",
            },
            "review_sign", "reviewer_id", reviewer_label,
        )
        self.assertEqual(changes.relabel, [])

    def test_renamed_author_label(self):
        changes = self.up._person_values(
            {
                "de_number": "1",
                "author": "Smith, J.", "author_new": "Smith, John",
                "author_ids": "smith.john", "author_ids_new": "",
            },
            "author", "author_ids", author_label,
        )
        self.assertEqual(changes.relabel, [("Qsmith.john", "John Smith")])

    def test_relabel_sets_the_label_as_given(self):
        item = MagicMock()
        item.labels.values = {"en": types.SimpleNamespace(value="J. Smith")}
        self.up.api.item.get.return_value = item
        self.up.relabel_author("Q1", "John Smith")
        item.labels.set.assert_called_once_with(language="en", value="John Smith")


class MissingInPortalTest(unittest.TestCase):
    """A record without an item is set aside; the pass carries on."""

    def setUp(self):
        patcher = unittest.mock.patch(
            "mardi_importer.zbmath.ZBMathUpdate.get_info_from_doi", return_value=None
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.up = _updater()
        self.items = {}
        self.up.api.item.get.side_effect = (
            lambda entity_id: self.items.setdefault(entity_id, FakeItem())
        )
        # de_number 2 is not in the portal
        self.up.api.search_entity_by_value.side_effect = (
            lambda prop, value: [] if value == "2" else [f"Q{value}"]
        )
        self.dir = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, self.dir)
        self.diff = os.path.join(self.dir, "diff.csv")
        with open(self.diff, "w", encoding="utf-8") as f:
            f.write("de_number\tdoi\treferences\tdoi_new\treferences_new\n")
            for de in ("1", "2", "3"):
                f.write(f"{de}\ta\t100\tb\t100;200\n")
        self.missing = os.path.join(self.dir, "missing.csv")

    def test_the_rows_after_a_missing_one_are_still_applied(self):
        progress = []
        stats = self.up.apply(
            self.diff, mode="non_arxiv", missing_path=self.missing,
            progress_callback=progress.append,
        )
        self.assertEqual(progress, ["1", "2", "3"])
        self.assertEqual(stats["missing_in_portal"], 1)
        self.assertEqual(stats["items_written"], 2)
        self.assertEqual(self.items["Q3"].written, 1)

    def test_missing_rows_are_kept_in_diff_format(self):
        self.up.apply(self.diff, mode="non_arxiv", missing_path=self.missing)
        rows = _read_diff(self.missing)
        self.assertEqual([r["de_number"] for r in rows], ["2"])
        self.assertEqual(rows[0]["doi_new"], "b")

    def test_a_resumed_pass_does_not_list_a_row_twice(self):
        self.up.apply(self.diff, mode="non_arxiv", missing_path=self.missing)
        self.up.apply(
            self.diff, mode="non_arxiv", missing_path=self.missing,
            resume_after_de="1",
        )
        self.assertEqual(len(_read_diff(self.missing)), 1)

    def test_references_pass_sets_aside_missing_roots_too(self):
        self.up.api.batch_search_by_value.return_value = {
            "100": ["Q100"], "200": ["Q200"],
        }
        stats = self.up.apply_references(self.diff, missing_path=self.missing)
        self.assertEqual(stats["missing_in_portal"], 1)
        self.assertEqual(stats["items_written"], 2)
        self.assertEqual([r["de_number"] for r in _read_diff(self.missing)], ["2"])


class SampleRunTest(unittest.TestCase):
    """``max_rows`` applies a few records and lets the next run continue."""

    def setUp(self):
        patcher = unittest.mock.patch(
            "mardi_importer.zbmath.ZBMathUpdate.get_info_from_doi", return_value=None
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.up = _updater()
        self.items = {}
        self.up.api.item.get.side_effect = (
            lambda entity_id: self.items.setdefault(entity_id, FakeItem())
        )
        self.up.api.search_entity_by_value.side_effect = (
            lambda prop, value: [f"Q{value}"]
        )
        self.up.api.batch_search_by_value.return_value = {
            "100": ["Q100"], "200": ["Q200"],
        }
        self.dir = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, self.dir)
        self.diff = os.path.join(self.dir, "diff.csv")
        with open(self.diff, "w", encoding="utf-8") as f:
            f.write("de_number\tdoi\tzbl_id\treferences\t"
                    "doi_new\tzbl_id_new\treferences_new\n")
            for de in ("1", "2", "3", "4", "5"):
                zbl = "arXiv:2401.0000" + de if de == "2" else "Zbl " + de
                f.write(f"{de}\ta\t{zbl}\t100\tb\t\t100;200\n")

    def _written(self):
        return sorted(qid for qid, item in self.items.items() if item.written)

    def test_a_pass_stops_after_max_rows_of_its_kind(self):
        progress = []
        stats = self.up.apply(self.diff, mode="non_arxiv", max_rows=2,
                              progress_callback=progress.append)
        # de_number 2 is an arXiv record, so it neither counts nor is written
        self.assertEqual(self._written(), ["Q1", "Q3"])
        self.assertEqual(stats["rows_seen"], 2)
        self.assertTrue(stats["stopped_early"])
        self.assertEqual(progress[-1], "3")

    def test_the_next_run_continues_after_the_sample(self):
        progress = []
        self.up.apply(self.diff, mode="non_arxiv", max_rows=2,
                      progress_callback=progress.append)
        stats = self.up.apply(self.diff, mode="non_arxiv",
                              resume_after_de=progress[-1])
        self.assertFalse(stats["stopped_early"])
        self.assertEqual(stats["rows_seen"], 2)
        self.assertEqual(self._written(), ["Q1", "Q3", "Q4", "Q5"])
        self.assertEqual(self.items["Q1"].written, 1, "nothing applied twice")

    def test_a_limit_that_covers_everything_is_not_stopped_early(self):
        stats = self.up.apply(self.diff, mode="non_arxiv", max_rows=4)
        self.assertFalse(stats["stopped_early"])

    def test_zero_writes_nothing(self):
        stats = self.up.apply(self.diff, mode="non_arxiv", max_rows=0)
        self.assertTrue(stats["stopped_early"])
        self.assertEqual(self._written(), [])

    def test_references_pass_honours_the_limit(self):
        progress = []
        stats = self.up.apply_references(self.diff, max_rows=1,
                                         progress_callback=progress.append)
        self.assertTrue(stats["stopped_early"])
        self.assertEqual(stats["items_written"], 1)
        self.assertEqual(progress, ["1"])


if __name__ == "__main__":
    unittest.main()
