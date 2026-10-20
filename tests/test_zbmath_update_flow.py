"""Offline tests for the ``zbmath-update`` Prefect flow itself.

The zbMATH source is replaced by a fake that writes small dumps and records
what the update passes were asked to do; hashing, diffing and the deleted list
run for real. Covers checkpointing and resumption, the baseline rotation, and
how records that are not in the portal are reported.
"""

import fnmatch
import glob
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

# Installs the Prefect stub and the lightweight mardi_importer stand-ins.
from tests import test_zbmath_update as helpers

import mardi_portal.flows.zbmath_update as F  # noqa: E402

_row = helpers._row
HEADER = helpers.HEADER


def _write_dump(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(HEADER) + "\n")
        for r in rows:
            f.write("\t".join(r[c] for c in HEADER) + "\n")


BASELINE = [
    _row(de_number="1", doi="10.1/a"),
    _row(de_number="2", doi="10.1/old"),
    _row(de_number="3", doi="10.1/gone"),
]
CURRENT = [
    _row(de_number="1", doi="10.1/a"),
    _row(de_number="2", doi="10.1/changed"),
    _row(de_number="4", doi="10.1/new"),
]


class FakeSource:
    def __init__(self, work):
        self.out_dir = work + "/"
        self.raw_dump_path = None
        self.processed_dump_path = None
        self.calls = []
        self.crash_in = None
        self.missing_in = set()
        self.runs = 0
        self.applied = []

    last_durable_offset = None

    def write_data_dump(self, start_after=0, output_path=None, progress_callback=None,
                        resume_offset=None):
        self.runs += 1
        self.raw_dump_path = output_path or os.path.join(
            self.out_dir, f"raw_zbmath_data_dump{self.runs}.txt"
        )
        with open(self.raw_dump_path, "w") as f:
            f.write("raw\n")

    def process_data(self, resume_after_de=None, progress_callback=None,
                     resume_offset=None):
        self.processed_dump_path = self.processed_dump_path or os.path.join(
            self.out_dir, f"zbmath_data_dump2026101{self.runs}-000000.csv"
        )
        _write_dump(self.processed_dump_path, CURRENT)

    #: Every pass pretends to have these records to apply.
    PASS_ROWS = ["10", "20", "30"]

    def _pass(self, kind, resume_after_de, progress_callback, missing_path, max_rows):
        self.calls.append((kind, resume_after_de, missing_path, max_rows))
        if kind in self.missing_in:
            with open(missing_path, "w") as f:
                f.write("de_number\tdoi\tdoi_new\n2\t10.1/old\t10.1/changed\n")
        if self.crash_in == kind:
            self.crash_in = None
            progress_callback("2")
            raise RuntimeError("portal down (simulated)")
        rows = self.PASS_ROWS
        if resume_after_de in rows:
            rows = rows[rows.index(resume_after_de) + 1:]
        todo = rows if max_rows is None else rows[:max_rows]
        for de in todo:
            self.applied.append((kind, de))
            progress_callback(de)
        return {"rows_seen": len(todo), "stopped_early": len(todo) < len(rows)}

    def update_from_diff(self, diff_path, mode, logger, resume_after_de,
                         progress_callback, missing_path, max_rows=None):
        return self._pass(mode, resume_after_de, progress_callback, missing_path, max_rows)

    def update_references_from_diff(self, diff_path, logger, resume_after_de,
                                    progress_callback, missing_path, max_rows=None):
        return self._pass("references", resume_after_de, progress_callback,
                          missing_path, max_rows)


class UpdateFlowTest(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.work)
        self.source = FakeSource(self.work)
        checkpoint = os.path.join(self.work, "update_checkpoint.json")
        for patcher in (
            mock.patch.object(F, "CHECKPOINT_DIR", self.work),
            mock.patch.object(F, "DATA_DIR", self.work),
            mock.patch.object(F, "CHECKPOINT_FILE", checkpoint),
            mock.patch.object(F, "_source", lambda log: self.source),
            mock.patch.object(F.shutil, "copyfile", lambda a, b: None),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.checkpoint = checkpoint

    def _seed(self):
        _write_dump(os.path.join(self.work, "zbmath_data_dump20260101-000000.csv"), BASELINE)

    def _only(self, pattern):
        files = glob.glob(os.path.join(self.work, pattern))
        self.assertEqual(len(files), 1, files)
        return files[0]

    def test_empty_volume_asks_for_a_full_processed_dump(self):
        with self.assertRaises(RuntimeError) as ctx:
            F.zbmath_update_flow()
        message = str(ctx.exception)
        self.assertIn("copy a full processed zbMATH dump", message)
        self.assertNotIn("import flow", message)

    def test_full_run(self):
        self._seed()
        F.zbmath_update_flow()
        self.assertFalse(os.path.exists(self.checkpoint), "checkpoint archived")
        diff = helpers._read_diff(self._only("differences_*.csv"))
        self.assertEqual([r["de_number"] for r in diff], ["2"])
        with open(self._only("deleted_*.tsv")) as f:
            self.assertEqual(f.read().split(), ["de_number", "3"])
        self.assertEqual([c[0] for c in self.source.calls],
                         ["non_arxiv", "arxiv", "references"])

    def test_each_pass_gets_its_own_missing_file(self):
        self._seed()
        F.zbmath_update_flow()
        paths = [c[2] for c in self.source.calls]
        self.assertEqual(len(set(paths)), 3)
        for path in paths:
            self.assertTrue(os.path.basename(path).startswith("missing_in_portal_"))
            self.assertFalse(
                fnmatch.fnmatch(os.path.basename(path), F.PROCESSED_PATTERN),
                "must never be mistaken for a baseline dump",
            )

    def test_interrupted_pass_resumes_where_it_stopped(self):
        self._seed()
        self.source.crash_in = "arxiv"
        with self.assertRaises(RuntimeError):
            F.zbmath_update_flow()
        self.source.calls.clear()
        F.zbmath_update_flow()
        self.assertEqual(
            [(c[0], c[1]) for c in self.source.calls],
            [("arxiv", "2"), ("references", None)],
        )

    def test_sample_run_stops_and_the_next_run_continues(self):
        self._seed()
        result = F.zbmath_update_flow(max_records_per_pass=1)
        self.assertIn("Stopped after at most 1 record(s) per pass", result.message)
        self.assertEqual(self.source.applied,
                         [("non_arxiv", "10"), ("arxiv", "10"), ("references", "10")])
        self.assertTrue(os.path.exists(self.checkpoint), "checkpoint kept")
        self.assertTrue(os.path.exists(self._only("differences_*.csv")))
        self.assertTrue(os.path.exists(self._only("deleted_*.tsv")))

        # a second sample continues after the first, without re-downloading
        self.source.applied.clear()
        F.zbmath_update_flow(max_records_per_pass=1)
        self.assertEqual(self.source.runs, 1, "no second download")
        self.assertEqual(self.source.applied,
                         [("non_arxiv", "20"), ("arxiv", "20"), ("references", "20")])

        # and a normal run does the rest and finishes
        self.source.applied.clear()
        result = F.zbmath_update_flow()
        self.assertIsNone(result)
        self.assertEqual(self.source.applied,
                         [("non_arxiv", "30"), ("arxiv", "30"), ("references", "30")])
        self.assertFalse(os.path.exists(self.checkpoint), "checkpoint archived")

    def test_zero_stops_after_the_diff(self):
        self._seed()
        result = F.zbmath_update_flow(max_records_per_pass=0)
        self.assertIn("not finished yet", result.message)
        self.assertEqual(self.source.applied, [])
        self.assertTrue(os.path.exists(self._only("differences_*.csv")))
        self.assertTrue(os.path.exists(self.checkpoint))

    def test_dump_steps_save_the_durable_offset_and_resume_with_it(self):
        seen = []

        class Interrupted(FakeSource):
            def __init__(self, work):
                super().__init__(work)
                self.first = {"process": True, "download": True}

            def process_data(self, resume_after_de=None, progress_callback=None,
                             resume_offset=None):
                seen.append(("process", resume_after_de, resume_offset))
                self.processed_dump_path = os.path.join(self.out_dir, "p.csv")
                if self.first["process"]:
                    self.first["process"] = False
                    self.last_durable_offset = 123
                    progress_callback("5")
                    raise RuntimeError("killed (simulated)")

            def write_data_dump(self, start_after=0, output_path=None,
                                progress_callback=None, resume_offset=None):
                seen.append(("download", start_after, resume_offset))
                self.raw_dump_path = output_path or os.path.join(self.out_dir, "r.txt")
                if self.first["download"]:
                    self.first["download"] = False
                    self.last_durable_offset = 456
                    progress_callback(300)
                    raise RuntimeError("killed (simulated)")

        self.source = Interrupted(self.work)
        for step in (F.convert_raw_to_processed, F.download_raw_dump):
            args = ("raw.txt",) if step is F.convert_raw_to_processed else ()
            with self.assertRaises(RuntimeError):
                step(*args)
            step(*args)
        self.assertEqual(seen, [
            ("process", None, None), ("process", "5", 123),
            ("download", 0, None), ("download", 300, 456),
        ])

    def test_mass_deletions_stop_the_run_before_anything_is_written(self):
        self._seed()
        emptied = [
            _row(de_number="1", doi="None"),
            _row(de_number="2", doi="None"),
            _row(de_number="4", doi="10.1/new"),
        ]
        with mock.patch.object(sys.modules[__name__], "CURRENT", emptied):
            with self.assertRaises(RuntimeError) as ctx:
                F.zbmath_update_flow(deletion_alarm_min=1)
        message = str(ctx.exception)
        self.assertIn("Nothing was written to the portal", message)
        self.assertIn("doi: 2 of 3", message)
        self.assertEqual(self.source.calls, [], "no update pass ran")
        self.assertTrue(os.path.exists(self.checkpoint), "the run can be resumed")

        # after checking: let the deletions through on the next run
        F.zbmath_update_flow(deletion_alarm_min=1, allow_mass_deletions=True)
        self.assertEqual([c[0] for c in self.source.calls],
                         ["non_arxiv", "arxiv", "references"])

    def test_a_diff_cleaned_by_hand_passes_the_check_on_the_next_run(self):
        self._seed()
        emptied = [
            _row(de_number="1", doi="None"),
            _row(de_number="2", doi="None"),
            _row(de_number="4", doi="10.1/new"),
        ]
        with mock.patch.object(sys.modules[__name__], "CURRENT", emptied):
            with self.assertRaises(RuntimeError):
                F.zbmath_update_flow(deletion_alarm_min=1)
        diff = self._only("differences_*.csv")
        with open(diff) as f:
            content = f.read().replace(F.DELETED_SENTINEL, "")
        with open(diff, "w") as f:
            f.write(content)
        F.zbmath_update_flow(deletion_alarm_min=1)
        self.assertEqual([c[0] for c in self.source.calls],
                         ["non_arxiv", "arxiv", "references"])
    
    def test_negative_limit_is_rejected(self):
        with self.assertRaises(ValueError):
            F.zbmath_update_flow(max_records_per_pass=-1)

    def test_records_missing_in_portal_fail_the_run_after_all_steps(self):
        self._seed()
        self.source.missing_in = {"non_arxiv"}
        with self.assertRaises(RuntimeError) as ctx:
            F.zbmath_update_flow()
        self.assertIn("1 in missing_in_portal_non_arxiv_", str(ctx.exception))
        # every step still ran, and the next run is not stuck on this one
        self.assertEqual([c[0] for c in self.source.calls],
                         ["non_arxiv", "arxiv", "references"])
        self.assertTrue(os.path.exists(self._only("deleted_*.tsv")))
        self.assertFalse(os.path.exists(self.checkpoint), "checkpoint archived")

    def test_next_run_compares_against_the_last_new_dump(self):
        self._seed()
        F.zbmath_update_flow()
        first_new = self.source.processed_dump_path
        self.source.processed_dump_path = None
        self.source.calls.clear()
        F.zbmath_update_flow()
        archives = sorted(glob.glob(self.checkpoint + ".done.*"))
        self.assertTrue(archives)
        diffs = glob.glob(os.path.join(self.work, "differences_*.csv"))
        stem = os.path.basename(first_new)[: -len(".csv")]
        self.assertTrue(any(os.path.basename(d).startswith(f"differences_{stem}_TO_")
                            for d in diffs), diffs)


if __name__ == "__main__":
    unittest.main()
