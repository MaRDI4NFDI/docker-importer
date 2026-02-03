"""Pre-patching module for lookup-aware CSV dry-run.

Applies patches BEFORE any importer classes are imported,
allowing SPARQL lookups to work while capturing writes to CSV.
"""

import contextvars
from typing import Any, Optional
from unittest.mock import MagicMock

# Context variable to track dry-run state
_dry_run_active: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "dry_run_active", default=False
)


def is_dry_run_active() -> bool:
    """Check if dry-run mode is active.

    Returns:
        True if dry-run patches have been applied.
    """
    return _dry_run_active.get()


def patch_for_lookup_aware_dry_run() -> dict:
    """Apply patches for lookup-aware dry-run mode.

    This function must be called BEFORE importing any services
    or importers. It patches:

    Patched (write operations):
    - MardiItem.write() -> logs to CSV, returns stub ID
    - MardiProperty.write() -> logs to CSV, returns stub ID
    - WikidataImporter.insert_id_in_db() -> no-op
    - WikidataImporter.update_has_all_claims() -> no-op
    - WikidataImporter.query() -> returns None (no DB lookups)

    Unpatched (read operations work normally):
    - MardiClient.__init__ (creates client with SPARQL endpoint)
    - MardiItem.exists() -> SPARQL query (works)
    - MardiItem.get() -> API read (works but not called in minimal mode)
    - search_entity_by_value() -> SPARQL query (works)

    Returns:
        Dictionary of original methods for reference (not used in CLI mode)
    """
    _dry_run_active.set(True)

    try:
        from mardiclient import MardiItem, MardiProperty
        from mardi_importer.wikidata.WikidataImporter import WikidataImporter
        from mardi_importer.dryrun.recorder import get_active_recorder
    except ImportError as e:
        # If we can't import, we can't patch
        print(f"Warning: Could not import classes for patching: {e}")
        return {}

    _originals = {}

    # 1. Patch MardiItem.write() - intercept writes, return stub IDs
    _originals["MardiItem.write"] = MardiItem.write

    def patched_item_write(self_item, *args, **kwargs):
        recorder = get_active_recorder()
        if recorder is None:
            # Not in CSVRecorder context, use original
            return _originals["MardiItem.write"](self_item, *args, **kwargs)

        # Check if this is a new item or update via SPARQL
        # In minimal mode, we rely on the item's internal state
        # The stub ID will be used regardless
        stub_id = recorder._generate_stub_id("item")

        # Log to CSV (this is the key interception)
        recorder._intercept_item_write(self_item, *args, **kwargs)

        # Return mock result with stub ID
        mock_result = MagicMock()
        mock_result.id = stub_id
        return mock_result

    MardiItem.write = patched_item_write

    # 2. Patch MardiProperty.write() - intercept property writes
    _originals["MardiProperty.write"] = MardiProperty.write

    def patched_prop_write(self_prop, *args, **kwargs):
        recorder = get_active_recorder()
        if recorder is None:
            return _originals["MardiProperty.write"](self_prop, *args, **kwargs)

        stub_id = recorder._generate_stub_id("property")
        recorder._intercept_property_write(self_prop, *args, **kwargs)

        mock_result = MagicMock()
        mock_result.id = stub_id
        return mock_result

    MardiProperty.write = patched_prop_write

    # 3. Patch WikidataImporter.insert_id_in_db() - no-op in dry-run
    _originals["WikidataImporter.insert_id_in_db"] = WikidataImporter.insert_id_in_db

    def patched_insert_id(self, wikidata_id, local_id, has_all_claims):
        recorder = get_active_recorder()
        if recorder:
            # Log the would-be DB operation
            recorder._log_db_operation(
                "insert_id_in_db", wikidata_id, local_id, has_all_claims
            )
        # No actual DB write - this is the key no-op

    WikidataImporter.insert_id_in_db = patched_insert_id

    # 4. Patch WikidataImporter.update_has_all_claims() - no-op in dry-run
    _originals["WikidataImporter.update_has_all_claims"] = (
        WikidataImporter.update_has_all_claims
    )

    def patched_update_claims(self, wikidata_id):
        recorder = get_active_recorder()
        if recorder:
            recorder._log_db_operation("update_has_all_claims", wikidata_id, None, True)
        # No actual DB update

    WikidataImporter.update_has_all_claims = patched_update_claims

    # 5. Patch WikidataImporter.query() - always return None (no DB lookups)
    _originals["WikidataImporter.query"] = WikidataImporter.query

    def patched_query(self, column, wikidata_id):
        recorder = get_active_recorder()
        if recorder:
            # Log that we checked, but return None (treat as not found)
            recorder._log_db_operation("query", wikidata_id, None, False)
        return None  # Always treat as new item (no existing mapping)

    WikidataImporter.query = patched_query

    return _originals
