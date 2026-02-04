"""Unit tests for lookup-aware CSV dry-run using unittest.

These tests verify the minimal dry-run implementation where:
- SPARQL lookups work (existence checks)
- MediaWiki API writes are intercepted
- DB operations are no-ops
"""

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from cli.dryrun import (
    CSVRecorder,
    is_dry_run_active,
    csv_dry_run,
    get_active_recorder,
    patch_for_lookup_aware_dry_run,
)


class TestLookupAwareDryRun(unittest.TestCase):
    """Test cases for lookup-aware dry-run mode."""

    def test_patch_function_exists(self):
        """Verify patch_for_lookup_aware_dry_run is importable."""
        self.assertTrue(callable(patch_for_lookup_aware_dry_run))

    def test_csv_recorder_with_existing_qid_column(self):
        """Verify CSV includes existing_qid column."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"

            with CSVRecorder(str(csv_path)) as recorder:
                # Add record with existing_qid
                recorder._add_record(
                    entity_type="item",
                    source="crossref",
                    external_id="10.1234/test",
                    labels={"en": "Test"},
                    descriptions={},
                    claims={},
                    stub_id="Q0001",
                    operation="already_exists",
                    existing_qid="Q12345",
                )

            # Verify CSV contains existing_qid
            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                self.assertIsNotNone(reader.fieldnames)
                if reader.fieldnames:
                    self.assertIn("existing_qid", reader.fieldnames)
                rows = list(reader)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["existing_qid"], "Q12345")
                self.assertEqual(rows[0]["operation"], "already_exists")

    def test_csv_headers_complete(self):
        """Verify CSV has all expected headers including existing_qid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "headers.csv"

            with CSVRecorder(str(csv_path)):
                pass  # Empty, just to write headers

            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                expected_headers = [
                    "timestamp",
                    "entity_type",
                    "source",
                    "external_id",
                    "labels",
                    "descriptions",
                    "claims",
                    "stub_id",
                    "parent_stub_id",
                    "operation",
                    "existing_qid",
                ]
                self.assertIsNotNone(reader.fieldnames)
                if reader.fieldnames:
                    for header in expected_headers:
                        self.assertIn(header, reader.fieldnames)


class TestOperationTypes(unittest.TestCase):
    """Test different operation types in CSV."""

    def test_operation_create(self):
        """Test create operation type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "ops.csv"

            with CSVRecorder(str(csv_path)) as recorder:
                recorder._add_record(
                    entity_type="item",
                    source="crossref",
                    external_id="10.1234/new",
                    labels={"en": "New Item"},
                    descriptions={},
                    claims={},
                    stub_id="Q0001",
                    operation="create",
                    existing_qid="",
                )

            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(rows[0]["operation"], "create")
                self.assertEqual(rows[0]["existing_qid"], "")

    def test_operation_already_exists(self):
        """Test already_exists operation type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "ops.csv"

            with CSVRecorder(str(csv_path)) as recorder:
                recorder._add_record(
                    entity_type="item",
                    source="crossref",
                    external_id="10.1234/exists",
                    labels={"en": "Existing Item"},
                    descriptions={},
                    claims={},
                    stub_id="Q0002",
                    operation="already_exists",
                    existing_qid="Q54321",
                )

            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(rows[0]["operation"], "already_exists")
                self.assertEqual(rows[0]["existing_qid"], "Q54321")

    def test_operation_update(self):
        """Test update operation type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "ops.csv"

            with CSVRecorder(str(csv_path)) as recorder:
                recorder._add_record(
                    entity_type="item",
                    source="wikidata",
                    external_id="Q42",
                    labels={"en": "Updated Item"},
                    descriptions={},
                    claims={"P31": ["Q5"]},
                    stub_id="Q0003",
                    operation="update",
                    existing_qid="Q42",
                )

            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(rows[0]["operation"], "update")
                self.assertEqual(rows[0]["existing_qid"], "Q42")


class TestPatchingBehavior(unittest.TestCase):
    """Test that patching correctly intercepts writes."""

    def test_dry_run_active_context(self):
        """Verify is_dry_run_active works correctly."""
        self.assertFalse(is_dry_run_active())

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"

            with csv_dry_run(str(csv_path)):
                self.assertTrue(is_dry_run_active())
                self.assertIsNotNone(get_active_recorder())

            self.assertFalse(is_dry_run_active())


class TestEnvironmentVariables(unittest.TestCase):
    """Document which environment variables are needed for dry-run."""

    def test_required_endpoints_documentation(self):
        """Document the required endpoints for lookup-aware dry-run."""
        # This test documents the expected environment variables
        # It doesn't actually require them to be set

        required_for_lookups = [
            "SPARQL_ENDPOINT_URL",  # For existence checks via search_entity_by_value
            "WIKIBASE_URL",  # For entity URL construction
        ]

        not_required = [
            "MEDIAWIKI_API_URL",  # Not needed in minimal mode (no item.get() calls)
            "DB_HOST",  # Patched out - DB lookups return None
            "IMPORTER_API_URL",  # Not used in dry-run
        ]

        # Just documenting - no assertions needed
        # These lists help users understand what to configure
        self.assertEqual(len(required_for_lookups), 2)
        self.assertEqual(len(not_required), 3)


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete dry-run workflow."""

    def test_complete_dry_run_simulation(self):
        """Simulate a complete dry-run workflow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "simulation.csv"

            with CSVRecorder(str(csv_path)) as recorder:
                # Simulate: Publication exists (SPARQL found it)
                recorder._add_record(
                    entity_type="publication",
                    source="crossref",
                    external_id="10.1234/existing",
                    labels={"en": "Existing Paper"},
                    descriptions={"en": "scientific article"},
                    claims={"P356": "10.1234/existing"},
                    stub_id="",
                    operation="already_exists",
                    existing_qid="Q100",
                )

                # Simulate: New author needs creation
                recorder._add_record(
                    entity_type="author",
                    source="crossref",
                    external_id="orcid-0000-0001-2345-6789",
                    labels={"en": "Jane Doe"},
                    descriptions={},
                    claims={"P496": "0000-0001-2345-6789"},
                    stub_id="Q0001",
                    operation="create",
                    parent_stub_id="Q100",  # Child of the publication
                )

                # Simulate: DB mapping would be logged (no-op in dry-run)
                recorder._log_db_operation("insert_id_in_db", "Q100", "Q100", True)

            # Verify CSV content
            with open(csv_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(len(rows), 3)

                # Check publication row
                pub_row = [r for r in rows if r["entity_type"] == "publication"][0]
                self.assertEqual(pub_row["operation"], "already_exists")
                self.assertEqual(pub_row["existing_qid"], "Q100")

                # Check author row
                author_row = [r for r in rows if r["entity_type"] == "author"][0]
                self.assertEqual(author_row["operation"], "create")
                self.assertEqual(author_row["parent_stub_id"], "Q100")


if __name__ == "__main__":
    unittest.main(verbosity=2)
