"""Resuming the zbMATH download and conversion after an interruption.

``ZBMathSource.write_data_dump`` and ``process_data`` report a resume position
only once the rows before it are on disk, together with the file size at that
point; a resume truncates the file back to that size. These tests simulate
what a killed pod leaves behind (rows written after the last checkpoint, half a
line) and check that the resumed file equals an uninterrupted one.

The source is loaded in a subprocess with its package stubbed out, so the test
needs neither an API login nor the other test modules' stubs.
"""

import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

HEADERS = ['biographic_references', 'contributors', 'database', 'datestamp',
           'document_type', 'editorial_contributions', 'id', 'identifier',
           'keywords', 'language', 'license', 'links', 'msc', 'references',
           'source', 'states', 'title', 'year', 'zbmath_url']


def _record(i):
    return {
        'biographic_references': [],
        'contributors': {'authors': [{'name': 'Doe, J.', 'codes': ['doe.j']}]},
        'database': 'Zbl', 'datestamp': '2024-01-01T00:00:00Z',
        'document_type': 'j', 'editorial_contributions': [], 'id': i,
        'identifier': f'Zbl {i}', 'keywords': ['k'],
        'language': {'languages': ['English']}, 'license': [],
        'links': [{'type': 'doi', 'identifier': f'10.1/{i}'}],
        'msc': [{'code': '11A01'}], 'references': [],
        'source': {'source': 'J. 1', 'series': []}, 'states': [],
        'title': {'title': f'Title "{i}"'}, 'year': 2020.0 if i % 2 else '2020',
        'zbmath_url': 'u',
    }


RECORDS = [_record(i) for i in range(1, 8)]

LOADER = textwrap.dedent('''
    import json, os, sys, types, importlib
    REPO, WORK, SCENARIO = sys.argv[1], sys.argv[2], sys.argv[3]
    sys.path.insert(0, os.path.join(REPO, "src"))
    pkg = types.ModuleType("mardi_importer")
    pkg.__path__ = [os.path.join(REPO, "src", "mardi_importer")]
    class _Importer:
        @classmethod
        def get_api(cls, name):
            return None
    pkg.Importer = _Importer
    sys.modules["mardi_importer"] = pkg
    base = types.ModuleType("mardi_importer.base")
    base.ADataSource = object
    sys.modules["mardi_importer.base"] = base
    zb = types.ModuleType("mardi_importer.zbmath")
    zb.__path__ = [os.path.join(REPO, "src", "mardi_importer", "zbmath")]
    sys.modules["mardi_importer.zbmath"] = zb
    ZS = importlib.import_module("mardi_importer.zbmath.ZBMathSource").ZBMathSource
    src = ZS.__new__(ZS)
    src.tags = ["author", "author_ids", "document_title", "source",
                "classifications", "language", "links", "keywords", "doi",
                "publication_year", "serial", "zbl_id", "references",
                "review_text", "review_sign", "reviewer_id", "license"]
    src.out_dir = WORK + "/"
    args = json.load(open(os.path.join(WORK, "args.json")))
    calls = []
    def cb(position):
        calls.append([position, src.last_durable_offset,
                      os.path.getsize(args["out"])])
    try:
        if SCENARIO == "process":
            src.raw_dump_path = args["raw"]
            src.processed_dump_path = args["out"]
            src.process_data(resume_after_de=args.get("resume"),
                             resume_offset=args.get("offset"),
                             progress_callback=cb, sync_every=2)
        else:
            import requests
            pages = json.load(open(args["pages"]))
            class R:
                def __init__(self, body, code=200):
                    self.status_code, self._b = code, body
                def json(self):
                    return self._b
            def fake_get(url, params=None, timeout=None):
                batch = [r for r in pages if r["id"] > int(params["start_after"])][:3]
                if not batch:
                    return R({"status": {"internal_code":
                              "successful access, but no result"}}, 404)
                return R({"result": batch, "status": {"last_id": batch[-1]["id"]}})
            requests.get = fake_get
            src.write_data_dump(start_after=args.get("resume") or 0,
                                output_path=args["out"], progress_callback=cb,
                                resume_offset=args.get("offset"))
        print(json.dumps({"calls": calls}))
    except Exception as e:
        print(json.dumps({"error": f"{type(e).__name__}: {e}"}))
''')


class DumpResumeTest(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, self.work)
        self.raw = os.path.join(self.work, "raw.txt")
        with open(self.raw, "w") as f:
            f.write("\t".join(HEADERS) + "\n")
            for r in RECORDS:
                f.write("\t".join(str(r[h]) for h in HEADERS) + "\n")
        self.pages = os.path.join(self.work, "pages.json")
        with open(self.pages, "w") as f:
            json.dump(RECORDS, f)

    def _run(self, scenario, out, **args):
        args.update(out=out, raw=self.raw, pages=self.pages)
        with open(os.path.join(self.work, "args.json"), "w") as f:
            json.dump(args, f)
        proc = subprocess.run(
            [sys.executable, "-I", "-W", "ignore", "-c", LOADER,
             REPO_ROOT, self.work, scenario],
            capture_output=True, text=True, timeout=120,
        )
        lines = [l for l in proc.stdout.splitlines() if l.startswith("{")]
        self.assertTrue(lines, proc.stderr[-2000:])
        return json.loads(lines[-1])

    def _reference(self, scenario):
        out = os.path.join(self.work, f"reference_{scenario}")
        result = self._run(scenario, out)
        self.assertNotIn("error", result)
        with open(out) as f:
            return result["calls"], f.read()

    # ── conversion ──────────────────────────────────────────────────────

    def test_progress_is_only_reported_for_rows_on_disk(self):
        calls, _ = self._reference("process")
        self.assertEqual([c[0] for c in calls], ["2", "4", "6", "7"])
        for _, offset, size in calls:
            self.assertEqual(offset, size, "offset must be what is on disk")

    def test_years_are_written_as_integers(self):
        _, content = self._reference("process")
        header, *rows = content.splitlines()
        col = header.split("\t").index("publication_year")
        self.assertEqual({r.split("\t")[col] for r in rows}, {"2020"})

    def test_resume_cuts_what_was_written_after_the_checkpoint(self):
        calls, expected = self._reference("process")
        position, offset, _ = calls[1]  # checkpoint after de_number 4
        out = os.path.join(self.work, "out.csv")
        with open(out, "w") as f:
            f.write(expected[:offset])
            # what a killed run can leave: more rows, then half of one
            f.write(expected[offset:offset + 120])
        result = self._run("process", out, resume=position, offset=offset)
        self.assertNotIn("error", result)
        with open(out) as f:
            self.assertEqual(f.read(), expected)

    def test_a_file_shorter_than_the_checkpoint_is_refused(self):
        calls, expected = self._reference("process")
        position, offset, _ = calls[1]
        out = os.path.join(self.work, "out.csv")
        with open(out, "w") as f:
            f.write(expected[:offset - 10])
        result = self._run("process", out, resume=position, offset=offset)
        self.assertIn("checkpoint says", result.get("error", ""))

    def test_a_missing_file_is_refused_instead_of_started_anew(self):
        calls, _ = self._reference("process")
        position, offset, _ = calls[1]
        out = os.path.join(self.work, "gone.csv")
        result = self._run("process", out, resume=position, offset=offset)
        self.assertIn("does not exist", result.get("error", ""))
        self.assertFalse(os.path.exists(out))
        calls, _ = self._reference("download")
        result = self._run("download", out, resume=calls[0][0], offset=calls[0][1])
        self.assertIn("does not exist", result.get("error", ""))

    def test_old_checkpoints_without_an_offset_still_resume(self):
        calls, expected = self._reference("process")
        position, offset, _ = calls[1]
        out = os.path.join(self.work, "out.csv")
        with open(out, "w") as f:
            f.write(expected[:offset])
        result = self._run("process", out, resume=position)
        self.assertNotIn("error", result)
        with open(out) as f:
            self.assertEqual(f.read(), expected)

    # ── download ────────────────────────────────────────────────────────

    def test_download_resume_does_not_duplicate_a_batch(self):
        calls, expected = self._reference("download")
        self.assertEqual([c[0] for c in calls], [3, 6, 7])
        last_id, offset, _ = calls[0]
        out = os.path.join(self.work, "raw_out.txt")
        with open(out, "w") as f:
            # the next batch reached the file, its checkpoint did not
            f.write(expected[:calls[1][1]])
        result = self._run("download", out, resume=last_id, offset=offset)
        self.assertNotIn("error", result)
        with open(out) as f:
            content = f.read()
        self.assertEqual(content, expected)
        ids = [l.split("\t")[6] for l in content.splitlines()[1:]]
        self.assertEqual(len(ids), len(set(ids)))


if __name__ == "__main__":
    unittest.main()
