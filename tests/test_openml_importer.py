"""Offline tests for the OpenML importer.

Covers turning OpenML API responses into records, and the rules for what
the importer adds to and removes from an item. The OpenML descriptions are
shortened from the real API responses for datasets 61 (iris) and 3.
"""

import json
import os
import sys
import tempfile
import types
import unittest
from unittest import mock
from unittest.mock import MagicMock

# Installs the Prefect stub and the stand-ins for mardi_importer and zbmath.
from tests import test_zbmath_update  # noqa: F401

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")

if "mardi_importer.base" not in sys.modules:
    base = types.ModuleType("mardi_importer.base")

    class ADataSource:
        def __init__(self, user, password):
            self.logger = MagicMock()

    base.ADataSource = ADataSource
    sys.modules["mardi_importer.base"] = base
if "mardi_importer.openml" not in sys.modules:
    package = types.ModuleType("mardi_importer.openml")
    package.__path__ = [os.path.join(SRC, "mardi_importer", "openml")]
    sys.modules["mardi_importer.openml"] = package

from mardi_importer.openml import records as R  # noqa: E402
from mardi_importer.openml.OpenMLSource import OpenMLSource, read_records  # noqa: E402
from mardi_importer.openml.OpenMLUpdate import OpenMLUpdater, deletion_report  # noqa: E402
from mardi_portal.flows.checkpoint import Checkpoint  # noqa: E402

IRIS = {
    "id": "61", "name": "iris", "version": "1",
    "description": "**Author**: R.A. Fisher\n**Source**: UCI\n\n4. Relevant\tInformation",
    "format": "ARFF", "creator": "R.A. Fisher", "collection_date": "1936",
    "upload_date": "2014-04-06T23:23:39", "licence": "Public",
    "url": "https://openml.org/data/v1/download/61/iris.arff",
    "default_target_attribute": "class",
    "tag": ["Botany", "Machine Learning", "study_1", "uci"],
    "original_data_url": "https://archive.ics.uci.edu/ml/datasets/Iris",
    "paper_url": "http://digital.library.adelaide.edu.au/dspace/handle/2440/15227",
    "md5_checksum": "ad484452702105cbf3d30f8deaba39a9",
}
IRIS_QUALITIES = [
    {"name": "NumberOfInstances", "value": "150.0"},
    {"name": "NumberOfClasses", "value": "3.0"},
    {"name": "MajorityClassSize", "value": "50.0"},
]


class RecordTests(unittest.TestCase):
    def test_record_from_api(self):
        record = R.record_from_api(IRIS, IRIS_QUALITIES)
        self.assertEqual(record["description"], "**Author**: R.A. Fisher **Source**: UCI 4. Relevant Information")
        self.assertEqual(record["creators"], ["R.A. Fisher"])
        self.assertEqual(record["contributors"], [])
        self.assertEqual(record["tags"], ["Machine Learning"])
        self.assertEqual(record["upload_date"], "2014-04-06")
        self.assertEqual(record["qualities"], {"NumberOfInstances": "150", "NumberOfClasses": "3"})

    def test_single_values_instead_of_lists(self):
        record = R.record_from_api(dict(IRIS, tag="Mathematics", creator=["A", "B"]),
                                   {"name": "NumberOfFeatures", "value": "5.0"})
        self.assertEqual(record["tags"], ["Mathematics"])
        self.assertEqual(record["creators"], ["A", "B"])
        self.assertEqual(record["qualities"], {"NumberOfFeatures": "5"})

    def test_paper(self):
        self.assertEqual(R.paper("https://dl.acm.org/doi/abs/10.5555/32231"), "doi:10.5555/32231")
        self.assertEqual(R.paper("https://link.springer.com/article/10.1007/s10994-012-5320-9"),
                         "doi:10.1007/S10994-012-5320-9")
        self.assertEqual(R.paper("https://arxiv.org/abs/1708.03731v2"), "arxiv:1708.03731")
        self.assertEqual(R.paper("https://arxiv.org/pdf/1708.03731.pdf"), "arxiv:1708.03731")
        self.assertIsNone(R.paper("http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.33.5554"))
        self.assertEqual(R.paper("https://link.springer.com/article/10.1007%252FBF00114162"),
                         "doi:10.1007/BF00114162")
        self.assertIsNone(R.paper("Fisher, R.A. (1936). The use of multiple measurements."))

    def test_statements(self):
        fields = {(f.prop, f.qualifier): f for f in R.statements(R.record_from_api(IRIS, IRIS_QUALITIES))}
        self.assertEqual(fields[("MaRDI profile type", None)].values, [R.PROFILE])
        self.assertEqual(fields[("upload date", None)].values, ["+2014-04-06T00:00:00Z"])
        self.assertEqual(fields[("wdt:P275", None)].values, [])          # "Public" is not mapped
        self.assertEqual(fields[("wdt:P2701", None)].values, ["wd:Q4489412"])
        self.assertEqual(fields[("number of instances", None)].values, ["150"])
        self.assertEqual(fields[("number of features", None)].values, [])
        self.assertEqual(fields[("wdt:P2860", None)].values, [])
        self.assertEqual(fields[("wdt:P2093", ("wdt:P3831", R.CREATOR))].values, ["R.A. Fisher"])

    def test_parquet_download(self):
        parquet = "https://data.openml.org/datasets/0000/0061/dataset_61.pq"
        record = R.record_from_api(dict(IRIS, parquet_url=parquet), IRIS_QUALITIES)
        fields = {(f.prop, f.qualifier): f for f in R.statements(record)}
        self.assertEqual(fields[("wdt:P2701", None)].values, ["wd:Q4489412", R.PARQUET])
        self.assertIn(parquet, fields[("wdt:P953", None)].values)
        self.assertEqual(R.openml_download(parquet), None)    # not mistaken for the ARFF link

    def test_escaped_quotes(self):
        self.assertEqual(R.clean_text(r"""King-Rook\'s side, \"won\"."""), """King-Rook's side, "won".""")

    def test_plus_sign_only_ignored_on_numbers(self):
        s, item = source(), Item()
        target = local_property("default target attribute")
        item.claims.add(Claim(target, "+class"))
        OpenMLUpdater(s).apply(item, record(default_target_attribute="class"),
                               record(default_target_attribute="+class"))
        self.assertEqual(item.claims.values(target), ["class"])

    def test_text_key_ignores_old_codes(self):
        self.assertEqual(R.text_key("a\\Nb\\T c"), R.text_key("a b c"))


# ── fakes for the portal ─────────────────────────────────────────────────

DATATYPES = {"P1": "wikibase-item", "P2": "string", "P3": "quantity", "P4": "time", "P5": "wikibase-item"}


def local_property(label):
    """Every property gets an ID; a few chosen ones get the datatypes above."""
    if label not in PROPERTIES:
        PROPERTIES[label] = f"P{100 + len(PROPERTIES)}"
    return PROPERTIES[label]


PROPERTIES = {"MaRDI profile type": "P1", "wdt:P2093": "P2", "number of instances": "P3",
              "upload date": "P4", "wdt:P3831": "P5"}


class Claim:
    def __init__(self, prop, value, qualifiers=None):
        self.prop_nr, self.removed = prop, False
        datatype = DATATYPES.get(prop, "string")
        payload = {"value": {"id": value}} if datatype == "wikibase-item" else \
            {"value": {"amount": value}} if datatype == "quantity" else \
            {"value": {"time": value}} if datatype == "time" else {"value": value}
        self.mainsnak = types.SimpleNamespace(datavalue=payload, datatype=datatype)
        self.datavalue = payload
        self.qualifiers = Claims()
        for q in qualifiers or []:
            self.qualifiers.add(q)

    def remove(self):
        self.removed = True


class Claims:
    def __init__(self):
        self.by_prop = {}

    def get(self, prop):
        return self.by_prop.get(prop, [])

    def add(self, claim):
        self.by_prop.setdefault(claim.prop_nr, []).append(claim)

    def values(self, prop):
        return [c.mainsnak.datavalue["value"] for c in self.get(prop) if not c.removed]


class Terms:
    def __init__(self):
        self.values = {}

    def get(self, language):
        value = self.values.get(language)
        return types.SimpleNamespace(value=value) if value else None

    def set(self, language, value):
        self.values[language] = value


class Item:
    def __init__(self):
        self.claims, self.labels, self.descriptions = Claims(), Terms(), Terms()

    def add_claims(self, claims, action=None):
        for claim in claims:
            self.claims.add(claim)


def source():
    s = OpenMLSource.__new__(OpenMLSource)
    s.logger, s._ids = MagicMock(), {}
    s.api = MagicMock()

    def lookup(label, kind):
        if kind == "property":
            return [local_property(label)]
        if label.startswith("wd:"):
            return "L" + label[3:]
        return {"MaRDI dataset profile": ["Q10"], "Dataset": ["Q11"]}.get(label, ["Q99"])

    s.api.get_local_id_by_label.side_effect = lookup
    s.api.get_claim.side_effect = lambda prop, value, qualifiers=None: Claim(prop, value, qualifiers)
    s.api.search_entity_by_value.return_value = []
    return s


def record(**changes):
    r = R.record_from_api(IRIS, IRIS_QUALITIES)
    r.update(changes)
    return r


class ApplyTests(unittest.TestCase):
    def test_new_item(self):
        s, item = source(), Item()
        self.assertTrue(OpenMLUpdater(s).apply(item, record(), None))
        self.assertEqual(item.labels.values, {"en": "iris"})
        self.assertEqual(item.descriptions.values, {"en": "OpenML dataset with id 61"})
        self.assertEqual(item.claims.values("P1"), [{"id": "Q10"}])
        self.assertEqual(item.claims.values("P2"), ["R.A. Fisher"])
        self.assertEqual(item.claims.get("P2")[0].qualifiers.get("P5")[0].datavalue["value"]["id"], "LQ59275219")
        self.assertEqual(item.claims.values("P4"), [{"time": "+2014-04-06T00:00:00Z"}])
        # nothing changes when the same record is applied again
        self.assertFalse(OpenMLUpdater(s).apply(item, record(), record()))

    def test_only_values_from_the_old_download_are_removed(self):
        s, item = source(), Item()
        OpenMLUpdater(s).apply(item, record(), None)
        item.claims.add(Claim("P2", "Added By Hand"))
        OpenMLUpdater(s).apply(item, record(creators=["New Name"]), record())
        self.assertEqual(sorted(item.claims.values("P2")), ["Added By Hand", "New Name"])

    def test_creators_and_contributors_are_kept_apart(self):
        s, item = source(), Item()
        OpenMLUpdater(s).apply(item, record(contributors=["R.A. Fisher"]), None)
        self.assertEqual(item.claims.values("P2"), ["R.A. Fisher", "R.A. Fisher"])
        OpenMLUpdater(s).apply(item, record(contributors=[]), record(contributors=["R.A. Fisher"]))
        self.assertEqual(item.claims.values("P2"), ["R.A. Fisher"])
        role = item.claims.get("P2")[0].qualifiers.get("P5")[0].datavalue["value"]["id"]
        self.assertEqual(role, "LQ59275219")

    def test_items_from_the_old_importer(self):
        s, item = source(), Item()
        item.claims.add(Claim("P1", "Q11"))                                  # profile "Dataset"
        item.claims.add(Claim("P3", "+150"))                                 # quantity as stored
        description = local_property("description")
        item.claims.add(Claim(description, "**Author**: R.A. Fisher\\N**Source**: UCI\\N\\N4. Relevant\\TInformation"))
        item.labels.set("en", "iris")
        OpenMLUpdater(s).apply(item, record(), None)
        self.assertEqual(item.claims.values("P1"), [{"id": "Q10"}])
        self.assertEqual(item.claims.values("P3"), [{"amount": "+150"}])
        self.assertEqual(item.claims.values(description), [record()["description"]])

    def test_old_download_links_are_replaced(self):
        s, item = source(), Item()
        url = local_property("wdt:P953")
        item.claims.add(Claim(url, "https://api.openml.org/data/v1/download/61/iris.arff"))   # old host
        item.claims.add(Claim(url, "https://api.openml.org/data/v1/download/99/iris.arff"))   # old file
        item.claims.add(Claim(url, "https://example.org/added-by-hand"))
        OpenMLUpdater(s).apply(item, record(url="https://openml.org/data/v1/download/61/iris.arff"), None)
        self.assertEqual(sorted(item.claims.values(url)), sorted([
            "https://openml.org/data/v1/download/61/iris.arff",
            record()["original_data_url"],
            "https://example.org/added-by-hand",
        ]))

    def test_openml_download(self):
        self.assertEqual(R.openml_download("https://api.openml.org/data/v1/download/21411235/iris.arff"), "21411235")
        self.assertEqual(R.openml_download("https://www.openml.org/data/v1/download/61"), "61")
        self.assertIsNone(R.openml_download("https://archive.ics.uci.edu/ml/datasets/Iris"))

    def test_label_edited_by_hand_stays(self):
        s, item = source(), Item()
        OpenMLUpdater(s).apply(item, record(), None)
        item.labels.set("en", "Iris (Fisher)")
        OpenMLUpdater(s).apply(item, record(name="iris2"), record())
        self.assertEqual(item.labels.values["en"], "Iris (Fisher)")


def write_download(path, records):
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


class PushAndUpdateTests(unittest.TestCase):
    def test_push_creates_only_missing_datasets_and_resumes(self):
        s = source()
        s.find_items = lambda ids: {"3": "Q3"}
        s.api.item.new.side_effect = Item
        Item.write = lambda self, summary=None: types.SimpleNamespace(id="Q" + self.labels.values["en"])
        done = []
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "openml.jsonl")
            write_download(path, [record(id="61"), record(id="3", name="kr"), record(id="7", name="seven")])
            stats = s.push(path, progress_callback=lambda last, retry: done.append(last), max_rows=1)
            self.assertEqual((stats["created"], stats["already_in_portal"], stats["stopped_early"]), (1, 1, True))
            self.assertEqual(done, ["3", "7"])
            stats = s.push(path, resume_after_id=done[-1], progress_callback=lambda last, retry: done.append(last))
            self.assertEqual(stats["created"], 1)
            self.assertEqual(done, ["3", "7", "61"])
        del Item.write

    def test_failed_dataset_is_retried(self):
        s = source()
        s.find_items = lambda ids: {}
        s.api.item.new.side_effect = Item

        def write(self, summary=None):
            if self.labels.values["en"] == "kr":
                raise RuntimeError("portal error")
            return types.SimpleNamespace(id="Q" + self.labels.values["en"])
        Item.write = write
        done = []
        no_retry = mock.patch("mardi_importer.openml.OpenMLDataset._with_backoff", lambda f, *a, **k: f())
        with no_retry, tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "openml.jsonl")
            write_download(path, [record(id="1", name="one"), record(id="3", name="kr"), record(id="7", name="seven")])
            stats = s.push(path, progress_callback=lambda last, retry: done.append((last, retry)))
            self.assertEqual((stats["created"], stats["failed"]), (2, 1))
            # dataset 7 is not redone later; dataset 3 is kept to retry
            self.assertEqual(done, [("1", []), ("3", ["3"]), ("7", ["3"])])
            Item.write = lambda self, summary=None: types.SimpleNamespace(id="Q" + self.labels.values["en"])
            made = []
            s.api.item.new.side_effect = lambda: made.append(1) or Item()
            done.clear()
            stats = s.push(path, resume_after_id="7", retry_ids=("3",),
                           progress_callback=lambda last, retry: done.append((last, retry)))
            self.assertEqual((stats["created"], stats["failed"], len(made)), (1, 0, 1))
            self.assertEqual(done, [("7", [])])     # only 3 was tried again
        del Item.write

    def test_failed_update_is_retried(self):
        s = source()
        s.find_items = lambda ids: {"1": "Q1", "3": "Q3", "7": "Q7"}
        s.api.item.get.side_effect = lambda entity_id: Item()
        Item.write = lambda self, summary=None: (_ for _ in ()).throw(RuntimeError("x")) \
            if self.labels.values.get("en") == "kr" else self
        done = []
        no_retry = mock.patch("mardi_importer.openml.OpenMLUpdate._with_backoff", lambda f, *a, **k: f())
        with no_retry, tempfile.TemporaryDirectory() as tmp:
            new = os.path.join(tmp, "new.jsonl")
            write_download(new, [record(id="1", name="one"), record(id="3", name="kr"), record(id="7", name="seven")])
            stats = s.update_from_downloads(None, new, progress_callback=lambda last, retry: done.append((last, retry)))
            self.assertEqual((stats["updated"], stats["failed"]), (2, 1))
            self.assertEqual(done, [("1", []), ("3", ["3"]), ("7", ["3"])])
            Item.write = lambda self, summary=None: self
            done.clear()
            stats = s.update_from_downloads(None, new, resume_after_id="7", retry_ids=("3",),
                                            progress_callback=lambda last, retry: done.append((last, retry)))
            self.assertEqual((stats["changed"], stats["failed"]), (1, 0))
            self.assertEqual(done, [("7", [])])
        del Item.write

    def test_update_only_touches_changed_datasets(self):
        s = source()
        s.find_items = lambda ids: {"61": "Q61"}
        items = {}
        s.api.item.get.side_effect = lambda entity_id: items.setdefault(entity_id, Item())
        Item.write = lambda self, summary=None: self
        with tempfile.TemporaryDirectory() as tmp:
            old, new = os.path.join(tmp, "old.jsonl"), os.path.join(tmp, "new.jsonl")
            same = record(id="3", name="kr-vs-kp")
            write_download(old, [record(), same, record(id="5", name="gone")])
            write_download(new, [record(version="2"), same, record(id="7", name="added")])
            stats = s.update_from_downloads(old, new)
            self.assertEqual((stats["changed"], stats["updated"]), (1, 1))
            # a rerun after the last dataset done has nothing left
            self.assertEqual(s.update_from_downloads(old, new, resume_after_id="61")["changed"], 0)
            # without a previous download every dataset counts as changed
            stats = s.update_from_downloads(None, new, missing_path=os.path.join(tmp, "missing.txt"))
            self.assertEqual((stats["changed"], stats["missing_in_portal"]), (3, 2))
            with open(os.path.join(tmp, "missing.txt")) as f:
                self.assertEqual(f.read(), "3\n7\n")
        del Item.write

    def test_deletion_report(self):
        old = {"61": record(), "3": record(id="3")}
        new = {"61": record(md5_checksum=None), "3": record(id="3", md5_checksum=None)}
        self.assertEqual(deletion_report(old, new)["wdt:P4092"], (2, 2))
        self.assertEqual(deletion_report(old, new)["wdt:P11238"], (0, 2))


class CheckpointTests(unittest.TestCase):
    def test_resume_and_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cp.json")
            cp = Checkpoint(path)
            self.assertFalse(cp.start("run-1"))
            cp.mark("download_openml", {"download_path": "x.jsonl"})
            cp.save_progress("push_openml", {"last_id": "61"})
            again = Checkpoint(path)
            self.assertTrue(again.start("run-2"))
            self.assertTrue(again.done("download_openml"))
            self.assertEqual(again.outputs["download_path"], "x.jsonl")
            self.assertEqual(again.progress("push_openml"), {"last_id": "61"})
            again.mark("push_openml")
            self.assertIsNone(again.progress("push_openml"))
            again.archive()
            self.assertFalse(os.path.exists(path))
            self.assertFalse(Checkpoint(path).start("run-3"))


class DownloadTests(unittest.TestCase):
    def test_interrupted_download_resumes(self):
        s = source()
        calls = []

        def get(path):
            calls.append(path)
            if path.startswith("data/list"):
                return None if not path.endswith("/0") else {"data": {"dataset": [
                    {"did": 61, "quality": IRIS_QUALITIES}, {"did": 3, "quality": []}]}}
            if path == "data/61" and calls.count("data/61") == 1:
                raise ConnectionError("interrupted")
            return {"data_set_description": dict(IRIS, id=path.split("/")[1])}

        s._get = get
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "openml.jsonl")
            with self.assertRaises(ConnectionError):
                s.pull(path)
            self.assertFalse(os.path.exists(path))
            self.assertEqual(s.pull(path), path)
            self.assertEqual([r["id"] for r in read_records(path)], ["3", "61"])
            self.assertEqual(calls.count("data/3"), 1)

    def test_binary_features_fetched_once_then_reused(self):
        s = source()
        calls = []

        def get(path):
            calls.append(path)
            if path.startswith("data/list"):
                return None if not path.endswith("/0") else {"data": {"dataset": [
                    {"did": 61, "quality": IRIS_QUALITIES}, {"did": 3, "quality": []}]}}
            if path.startswith("data/qualities/"):
                return {"data_qualities": {"quality": [
                    {"name": "NumberOfBinaryFeatures", "value": "2.0"},
                    {"name": "NumberOfClasses", "value": "3.0"}]}}
            return {"data_set_description": dict(IRIS, id=path.split("/")[1])}

        s._get = get
        with tempfile.TemporaryDirectory() as tmp:
            first = s.pull(os.path.join(tmp, "openml_2026-01-01.jsonl"))
            self.assertEqual([r["qualities"]["NumberOfBinaryFeatures"] for r in read_records(first)], ["2", "2"])
            self.assertEqual(sorted(c for c in calls if "qualities" in c), ["data/qualities/3", "data/qualities/61"])
            calls.clear()
            second = s.pull(os.path.join(tmp, "openml_2026-02-01.jsonl"))
            self.assertEqual([r["qualities"]["NumberOfBinaryFeatures"] for r in read_records(second)], ["2", "2"])
            self.assertEqual([c for c in calls if "qualities" in c], [])

    def test_half_written_line_is_dropped(self):
        from mardi_importer.openml.OpenMLSource import drop_partial_line
        with tempfile.TemporaryDirectory() as tmp:
            part = os.path.join(tmp, "openml.jsonl.part")
            with open(part, "w") as f:
                f.write('{"id": "2"}\n{"id": "3"}\n{"id": "6", "na')
            drop_partial_line(part)
            self.assertEqual([r["id"] for r in read_records(part)], ["2", "3"])
            drop_partial_line(part)                 # a complete file stays as it is
            self.assertEqual([r["id"] for r in read_records(part)], ["2", "3"])

    def test_binary_count_missing(self):
        s = source()
        s._get = lambda path: None
        self.assertIsNone(s.binary_count("61"))


if __name__ == "__main__":
    unittest.main()
