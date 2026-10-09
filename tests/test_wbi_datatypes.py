"""contentmath statements are read; a failed CRAN package does not stop the run."""

import os
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(REPO_ROOT, "src")
sys.path.insert(0, SRC)

CHECK = r'''
import mardi_importer
from wikibaseintegrator.models import Claims
snak = lambda p, t, v: {"mainsnak": {"snaktype": "value", "property": p, "datatype": t,
                                     "datavalue": {"value": v, "type": "string"}},
                        "type": "statement", "id": p + "$x", "rank": "normal"}
claims = Claims().from_json({"P14": [snak("P14", "contentmath", r"\sin x")],
                             "P1455": [snak("P1455", "mathml", "<math/>")]})
print(type(claims.get("P14")[0]).__name__, type(claims.get("P1455")[0]).__name__)
'''


class TestDatatypes(unittest.TestCase):
    def test_contentmath_and_mathml_statements_are_read(self):
        # fresh interpreter: other test modules stub WikibaseIntegrator
        out = subprocess.run([sys.executable, "-c", CHECK], capture_output=True, text=True,
                             env={**os.environ, "PYTHONPATH": SRC})
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        self.assertEqual(out.stdout.split(), ["ContentMath", "MathML"])


class Table(list):

    def iterrows(self):
        return enumerate(self)


class TestPush(unittest.TestCase):
    def test_a_failed_package_does_not_stop_the_run(self):
        from mardi_importer.cran.CRANSource import CRANSource
        source = CRANSource.__new__(CRANSource)
        source.packages = Table({"Date": "2026-10-01", "Package": p, "Title": p}
                                for p in ("aedseo", "broken", "zoo"))
        done = []

        def new_package(date, label, title):
            if label == "broken":
                raise IndexError("list index out of range")
            pkg = Mock()
            pkg.exists.return_value = True
            pkg.is_updated.return_value = False
            pkg.update.side_effect = lambda: done.append(label)
            return pkg

        source.new_package = new_package
        source.sync_archive_status = Mock()
        with patch("mardi_importer.cran.CRANSource.time.sleep"):
            failed = source.push()
        self.assertEqual(done, ["aedseo", "zoo"])
        self.assertEqual(failed, ["broken"])
        source.sync_archive_status.assert_called_once()


if __name__ == "__main__":
    unittest.main()
