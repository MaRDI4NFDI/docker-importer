"""CSV Dry-run module for CLI.

Captures all would-be writes to Wikibase and database without
actually performing them, logging them to a CSV file instead.

This module provides:
- CSVRecorder: Context manager that intercepts writes and logs to CSV
- patch_for_lookup_aware_dry_run: Pre-patching for SPARQL lookups with write interception
- run_import_with_optional_dryrun: Helper to run imports with optional dry-run mode
"""

import argparse
import contextvars
import csv
import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

# =============================================================================
# Context Variables
# =============================================================================

_active_recorder: contextvars.ContextVar[Optional["CSVRecorder"]] = (
    contextvars.ContextVar("csv_recorder", default=None)
)

_dry_run_active: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "dry_run_active", default=False
)


# =============================================================================
# Public API Functions
# =============================================================================


def is_dry_run_active() -> bool:
    """Check if a CSV dry-run is currently active.

    Returns:
        True if a CSVRecorder context is active, False otherwise.
    """
    return _active_recorder.get() is not None


def get_active_recorder() -> Optional["CSVRecorder"]:
    """Get the currently active CSVRecorder instance.

    Returns:
        The active CSVRecorder or None if not in dry-run mode.
    """
    return _active_recorder.get()


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
    - search_entity_by_value() -> SPARQL query (works)

    Returns:
        Dictionary of original methods for reference (not used in CLI mode)
    """
    _dry_run_active.set(True)

    try:
        from mardiclient import MardiItem, MardiProperty
        from mardi_importer.wikidata.WikidataImporter import WikidataImporter
    except ImportError as e:
        print(f"Warning: Could not import classes for patching: {e}")
        return {}

    _originals = {}

    # 1. Patch MardiItem.write()
    _originals["MardiItem.write"] = MardiItem.write

    def patched_item_write(self_item, *args, **kwargs):
        recorder = get_active_recorder()
        if recorder is None:
            return _originals["MardiItem.write"](self_item, *args, **kwargs)
        stub_id = recorder._generate_stub_id("item")
        recorder._intercept_item_write(self_item, *args, **kwargs)
        mock_result = MagicMock()
        mock_result.id = stub_id
        return mock_result

    MardiItem.write = patched_item_write

    # 2. Patch MardiProperty.write()
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

    # 3. Patch WikidataImporter.insert_id_in_db()
    _originals["WikidataImporter.insert_id_in_db"] = WikidataImporter.insert_id_in_db

    def patched_insert_id(self, wikidata_id, local_id, has_all_claims):
        recorder = get_active_recorder()
        if recorder:
            recorder._log_db_operation(
                "insert_id_in_db", wikidata_id, local_id, has_all_claims
            )

    WikidataImporter.insert_id_in_db = patched_insert_id

    # 4. Patch WikidataImporter.update_has_all_claims()
    _originals["WikidataImporter.update_has_all_claims"] = (
        WikidataImporter.update_has_all_claims
    )

    def patched_update_claims(self, wikidata_id):
        recorder = get_active_recorder()
        if recorder:
            recorder._log_db_operation("update_has_all_claims", wikidata_id, None, True)

    WikidataImporter.update_has_all_claims = patched_update_claims

    # 5. Patch WikidataImporter.query() - always return None
    _originals["WikidataImporter.query"] = WikidataImporter.query

    def patched_query(self, column, wikidata_id):
        recorder = get_active_recorder()
        if recorder:
            recorder._log_db_operation("query", wikidata_id, None, False)
        return None

    WikidataImporter.query = patched_query

    # 6. Patch WikidataImporter.__init__ to skip DB setup in dry-run
    _originals["WikidataImporter.__init__"] = WikidataImporter.__init__

    def patched_init(self, *args, **kwargs):
        # Check if we're in dry-run mode before doing anything
        if _dry_run_active.get():
            # Minimal initialization without DB connections
            self.languages = kwargs.get("languages", ["en", "de", "mul"])
            self.api = None  # Will be set later if needed
            self.engine = None
            self.engine_simplified = None
            self.setup_complete = False
            return
        # Normal initialization
        return _originals["WikidataImporter.__init__"](self, *args, **kwargs)

    WikidataImporter.__init__ = patched_init

    # 7. Patch WikidataImporter.setup() to be no-op in dry-run
    if hasattr(WikidataImporter, "setup"):
        _originals["WikidataImporter.setup"] = WikidataImporter.setup

        def patched_setup(self):
            if _dry_run_active.get():
                # No-op - don't create DB connections
                self.setup_complete = True
                return
            return _originals["WikidataImporter.setup"](self)

        WikidataImporter.setup = patched_setup

    return _originals


def run_import_with_optional_dryrun(
    import_func: Callable, args: argparse.Namespace, **import_kwargs
) -> Tuple[Dict[str, Any], int]:
    """Run an import function, with optional CSV dry-run mode.

    This helper centralizes the dry-run logic:
    - Checks args.csv_only flag
    - Applies patches if in dry-run mode (before imports)
    - Handles CSVRecorder context
    - Returns consistent payload format with CSV metadata

    Args:
        import_func: The already-imported sync function (e.g., import_doi_sync).
                     Must be imported INSIDE the CLI function (not at module level)
                     to ensure patches are applied first in dry-run mode.
        args: CLI arguments (must have csv_only and optionally csv_path attributes)
        **import_kwargs: Arguments to pass to import_func

    Returns:
        Tuple of (payload dict, exit_code)
    """
    if not getattr(args, "csv_only", False):
        # Normal mode - just run the import
        payload, all_ok = import_func(**import_kwargs)
        return payload, 0 if all_ok else 1

    # Dry-run mode
    patch_for_lookup_aware_dry_run()

    csv_path = getattr(args, "csv_path", None)
    with CSVRecorder(csv_path) as recorder:
        payload, all_ok = import_func(**import_kwargs)
        payload["csv_dryrun"] = {
            "csv_path": str(recorder.csv_path),
            "records_captured": recorder.record_count,
            "mode": "lookup-aware (SPARQL reads, no writes)",
        }
        return payload, 0 if all_ok else 1


@contextmanager
def csv_dry_run(csv_path: Optional[str] = None):
    """Context manager for CSV dry-run mode.

    Args:
        csv_path: Path to the CSV file. If None, a default is generated.

    Yields:
        CSVRecorder instance

    Example:
        with csv_dry_run('/tmp/test.csv') as recorder:
            import_wikidata_sync(['Q42'])
            print(f"Captured {recorder.record_count} records")
    """
    recorder = CSVRecorder(csv_path)
    try:
        yield recorder.__enter__()
    finally:
        recorder.__exit__(None, None, None)


# =============================================================================
# CSVRecorder Class
# =============================================================================


class CSVRecorder:
    """Context manager that intercepts Wikibase writes and logs them to CSV.

    When active, this recorder:
    1. Monkey-patches MardiItem.write() and MardiProperty.write() to log instead of write
    2. Monkey-patches WikidataImporter DB methods (insert_id_in_db, update_has_all_claims) to no-op
    3. Returns stub IDs instead of real IDs
    4. Captures all entity data (labels, descriptions, claims) to CSV

    Example:
        with CSVRecorder('/tmp/dryrun.csv') as recorder:
            # All writes are intercepted and logged to CSV
            result = import_doi_sync(['10.1234/example'])
            # No actual writes to Wikibase or DB occurred
    """

    CSV_HEADERS = [
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

    def __init__(self, csv_path: Optional[str] = None):
        """Initialize the CSV recorder."""
        if csv_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = f"mardi-dryrun-{timestamp}.csv"

        self.csv_path = Path(csv_path)
        self.records: List[Dict[str, Any]] = []
        self._item_counter = 0
        self._prop_counter = 0
        self._token = None
        self._original_methods: Dict[str, Any] = {}

    def __enter__(self) -> "CSVRecorder":
        """Enter the context and activate recording."""
        self._token = _active_recorder.set(self)
        self._patch_methods()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context, restore methods, and write CSV."""
        self._restore_methods()
        if self._token:
            _active_recorder.reset(self._token)
        self._write_csv()
        return False

    def _patch_methods(self) -> None:
        """Monkey-patch write methods to intercept calls."""
        try:
            from mardiclient import MardiItem, MardiProperty
            from mardi_importer.wikidata.WikidataImporter import WikidataImporter

            self._original_methods["MardiItem.write"] = MardiItem.write
            self._original_methods["MardiProperty.write"] = MardiProperty.write
            self._original_methods["WikidataImporter.insert_id_in_db"] = (
                WikidataImporter.insert_id_in_db
            )
            self._original_methods["WikidataImporter.update_has_all_claims"] = (
                WikidataImporter.update_has_all_claims
            )

            # Patches are set up in patch_for_lookup_aware_dry_run()
            # This method just stores originals for restoration

        except ImportError:
            pass

    def _restore_methods(self) -> None:
        """Restore original methods."""
        try:
            from mardiclient import MardiItem, MardiProperty
            from mardi_importer.wikidata.WikidataImporter import WikidataImporter

            if "MardiItem.write" in self._original_methods:
                MardiItem.write = self._original_methods["MardiItem.write"]
            if "MardiProperty.write" in self._original_methods:
                MardiProperty.write = self._original_methods["MardiProperty.write"]
            if "WikidataImporter.insert_id_in_db" in self._original_methods:
                WikidataImporter.insert_id_in_db = self._original_methods[
                    "WikidataImporter.insert_id_in_db"
                ]
            if "WikidataImporter.update_has_all_claims" in self._original_methods:
                WikidataImporter.update_has_all_claims = self._original_methods[
                    "WikidataImporter.update_has_all_claims"
                ]
        except ImportError:
            pass

    def _generate_stub_id(self, entity_type: str = "item") -> str:
        """Generate a sequential stub ID."""
        if entity_type == "property":
            self._prop_counter += 1
            return f"P_STUB_{self._prop_counter}"
        else:
            self._item_counter += 1
            return f"Q_STUB_{self._item_counter}"

    def _intercept_item_write(self, item, *args, **kwargs) -> MagicMock:
        """Intercept MardiItem.write() call and log to CSV."""
        stub_id = self._generate_stub_id("item")
        entity_data = self._extract_entity_data(item)
        source, external_id = self._detect_source_and_id(item, entity_data)

        self._add_record(
            entity_type=entity_data.get("type", "item"),
            source=source,
            external_id=external_id,
            labels=entity_data.get("labels", {}),
            descriptions=entity_data.get("descriptions", {}),
            claims=entity_data.get("claims", {}),
            stub_id=stub_id,
            operation="create" if kwargs.get("as_new") else "update",
        )

        mock_result = MagicMock()
        mock_result.id = stub_id
        return mock_result

    def _intercept_property_write(self, prop, *args, **kwargs) -> MagicMock:
        """Intercept MardiProperty.write() call and log to CSV."""
        stub_id = self._generate_stub_id("property")
        entity_data = self._extract_entity_data(prop)

        self._add_record(
            entity_type="property",
            source="wikidata",
            external_id=entity_data.get("external_id", ""),
            labels=entity_data.get("labels", {}),
            descriptions=entity_data.get("descriptions", {}),
            claims={},
            stub_id=stub_id,
            operation="create" if kwargs.get("as_new") else "update",
        )

        mock_result = MagicMock()
        mock_result.id = stub_id
        return mock_result

    def _extract_entity_data(self, entity) -> Dict[str, Any]:
        """Extract labels, descriptions, and claims from an entity."""
        data = {
            "type": getattr(entity, "type", "item"),
            "labels": {},
            "descriptions": {},
            "claims": {},
            "external_id": "",
        }

        if hasattr(entity, "labels") and entity.labels:
            for lang in getattr(entity.labels, "languages", []):
                try:
                    value = entity.labels.get(lang)
                    if value:
                        data["labels"][lang] = str(value)
                except:
                    pass

        if hasattr(entity, "descriptions") and entity.descriptions:
            for lang in getattr(entity.descriptions, "languages", []):
                try:
                    value = entity.descriptions.get(lang)
                    if value:
                        data["descriptions"][lang] = str(value)
                except:
                    pass

        if hasattr(entity, "get_json"):
            try:
                claims_json = entity.get_json()
                if claims_json:
                    data["claims"] = claims_json
            except:
                pass
        elif hasattr(entity, "claims") and entity.claims:
            try:
                data["claims"] = (
                    entity.claims.get_json()
                    if hasattr(entity.claims, "get_json")
                    else str(entity.claims)
                )
            except:
                pass

        return data

    def _detect_source_and_id(self, item, entity_data: Dict) -> tuple:
        """Detect the source type and external ID from the item/claims."""
        source = "unknown"
        external_id = ""
        claims = entity_data.get("claims", {})

        if "P356" in str(claims) or "wdt:P356" in str(claims):
            source = "crossref"
            external_id = self._extract_claim_value(claims, "P356")
        elif "P818" in str(claims) or "wdt:P818" in str(claims):
            source = "arxiv"
            external_id = self._extract_claim_value(claims, "P818")
        elif "P4901" in str(claims) or "wdt:P4901" in str(claims):
            source = "zenodo"
            external_id = self._extract_claim_value(claims, "P4901")
        elif "P5565" in str(claims) or "wdt:P5565" in str(claims):
            source = "cran"
            external_id = self._extract_claim_value(claims, "P5565")
        elif "P12" in str(claims) or "wdt:P12" in str(claims):
            source = "wikidata"
            external_id = self._extract_claim_value(claims, "P12")

        if source == "unknown" and entity_data.get("labels"):
            label = str(entity_data["labels"].get("en", "")).lower()
            if (
                "r package" in label
                or label.startswith("dplyr")
                or label.startswith("ggplot")
            ):
                source = "cran"
            elif "scientific article" in label or "journal" in label:
                source = "crossref"

        return source, external_id

    def _extract_claim_value(self, claims: Dict, property_id: str) -> str:
        """Extract a value from claims for a specific property."""
        try:
            if isinstance(claims, dict):
                for key in [property_id, f"P{property_id}", f"wdt:P{property_id}"]:
                    if key in claims:
                        value = claims[key]
                        if isinstance(value, list) and len(value) > 0:
                            value = value[0]
                        if isinstance(value, dict):
                            return value.get("value", str(value))
                        return str(value)
        except:
            pass
        return ""

    def _add_record(
        self,
        entity_type: str,
        source: str,
        external_id: str,
        labels: Dict,
        descriptions: Dict,
        claims: Dict,
        stub_id: str,
        operation: str = "create",
        parent_stub_id: str = "",
        existing_qid: str = "",
    ) -> None:
        """Add a record to the internal list."""
        record = {
            "timestamp": datetime.now().isoformat(),
            "entity_type": entity_type,
            "source": source,
            "external_id": external_id,
            "labels": json.dumps(labels),
            "descriptions": json.dumps(descriptions),
            "claims": json.dumps(claims) if isinstance(claims, dict) else str(claims),
            "stub_id": stub_id,
            "parent_stub_id": parent_stub_id,
            "operation": operation,
            "existing_qid": existing_qid,
        }
        self.records.append(record)

    def _log_db_operation(
        self,
        operation: str,
        wikidata_id: str,
        local_id: Optional[str],
        has_all_claims: bool,
    ) -> None:
        """Log a DB operation that was no-op'd."""
        record = {
            "timestamp": datetime.now().isoformat(),
            "entity_type": "db_mapping",
            "source": "wikidata",
            "external_id": wikidata_id,
            "labels": "",
            "descriptions": "",
            "claims": json.dumps(
                {"local_id": local_id, "has_all_claims": has_all_claims}
            ),
            "stub_id": "",
            "parent_stub_id": "",
            "operation": operation,
            "existing_qid": "",
        }
        self.records.append(record)

    def _write_csv(self) -> None:
        """Write all records to the CSV file."""
        if not self.records:
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
                writer.writeheader()
            return

        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
            writer.writeheader()
            writer.writerows(self.records)

    @property
    def record_count(self) -> int:
        """Return the number of records captured."""
        return len(self.records)
