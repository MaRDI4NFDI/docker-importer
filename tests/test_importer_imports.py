import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _reset_mardi_importer_modules() -> None:
    for name in list(sys.modules):
        if name == "mardi_importer" or name.startswith("mardi_importer."):
            sys.modules.pop(name, None)


class TestImporterImports(unittest.TestCase):
    """Tests for importer resolution in repo-checkout mode.

    Ensures the top-level shim resolves Importer and that subpackages
    remain importable without installing the package.

    Args:
        unittest.TestCase: Base class for unittest test cases.
    """
    def setUp(self) -> None:
        self._added_path = False
        if str(REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT))
            self._added_path = True
        _reset_mardi_importer_modules()

    def tearDown(self) -> None:
        _reset_mardi_importer_modules()
        if self._added_path:
            sys.path.remove(str(REPO_ROOT))

    def test_importer_reexport_repo_checkout(self) -> None:
        # Verify lazy shim re-exports Importer and registry is populated.
        from mardi_importer import Importer

        self.assertIn("arxiv", Importer._sources)

    def test_importer_import_subpackage(self) -> None:
        # Ensure subpackage imports work via shimmed package path.
        from mardi_importer.wikidata import WikidataImporter

        self.assertTrue(hasattr(WikidataImporter, "__name__"))


if __name__ == "__main__":
    unittest.main()
