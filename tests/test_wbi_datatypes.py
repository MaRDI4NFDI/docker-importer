"""contentmath statements are read; a failed CRAN package does not stop the run."""

import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "src"))

import mardi_importer  # noqa: E402,F401
from mardi_importer.wbi_datatypes import ContentMath  # noqa: E402
from wikibaseintegrator import datatypes  # noqa: E402


def snak(p, t, v):
    return {"mainsnak": {"snaktype": "value", "property": p, "datatype": t,
                         "datavalue": {"value": v, "type": "string"}},
            "type": "statement", "id": p + "$x", "rank": "normal"}


class TestDatatypes(unittest.TestCase):
    def test_contentmath_defined(self):
        self.assertEqual(ContentMath.DTYPE, "contentmath")

    @unittest.skipUnless(hasattr(datatypes, "BaseDataType"), "WikibaseIntegrator is stubbed by another test module")
    def test_contentmath_and_mathml_statements_are_read(self):
        from wikibaseintegrator.models import Claims
        claims = Claims().from_json({"P14": [snak("P14", "contentmath", r"\sin x")],
                                     "P1455": [snak("P1455", "mathml", "<math/>")]})
        self.assertEqual([type(claims.get(p)[0]).__name__ for p in ("P14", "P1455")], ["ContentMath", "MathML"])


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
