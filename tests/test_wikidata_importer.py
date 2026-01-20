import logging
import unittest
from unittest.mock import patch


from mardi_importer.wikidata import WikidataImporter


class TestWikidataImporterImportEntities(unittest.TestCase):
    """Tests for WikidataImporter.import_entities.

    These tests focus on the fast path where entities are already cached
    (has_all_claims=True), so no network calls, DB writes, or entity
    materialization should occur.

    Args:
        unittest.TestCase: Base class for unittest test cases.
    """
    def test_import_entities_returns_local_id_when_cached(self) -> None:
        # Reset singleton state to avoid cross-test leakage.
        WikidataImporter._instance = None
        WikidataImporter._initialized = False

        # Bypass heavy initialization (MardiClient, DB connections).
        with patch.object(WikidataImporter, "__init__", return_value=None):
            wdi = WikidataImporter()

        # Minimal logger required by import_entities.
        wdi.log = logging.getLogger("test")

        # Stub query to simulate cached entity state.
        def fake_query(kind, wikidata_id):
            if kind == "has_all_claims":
                return True
            if kind == "local_id":
                return "QLOCAL1"
            raise AssertionError(f"Unexpected query: {kind} {wikidata_id}")

        wdi.query = fake_query

        # Cached path should return local id directly.
        result = wdi.import_entities("Q42")
        self.assertEqual(result, "QLOCAL1")


if __name__ == "__main__":
    unittest.main()
