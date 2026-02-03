"""CSV Recorder for dry-run mode.

Captures all would-be writes to Wikibase and database without
actually performing them, logging them to a CSV file instead.
"""

import csv
import contextvars
import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

# Context variable to track active CSV recorder
_active_recorder: contextvars.ContextVar[Optional["CSVRecorder"]] = (
    contextvars.ContextVar("csv_recorder", default=None)
)


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
        """Initialize the CSV recorder.

        Args:
            csv_path: Path to the CSV file. If None, a default path is generated.
        """
        if csv_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = f"mardi-dryrun-{timestamp}.csv"

        self.csv_path = Path(csv_path)
        self.records: List[Dict[str, Any]] = []
        self._item_counter = 0
        self._prop_counter = 0
        self._token = None

        # Store original methods for restoration
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

            # Store original methods
            self._original_methods["MardiItem.write"] = MardiItem.write
            self._original_methods["MardiProperty.write"] = MardiProperty.write
            self._original_methods["WikidataImporter.insert_id_in_db"] = (
                WikidataImporter.insert_id_in_db
            )
            self._original_methods["WikidataImporter.update_has_all_claims"] = (
                WikidataImporter.update_has_all_claims
            )

            # Patch MardiItem.write
            original_item_write = MardiItem.write

            def patched_item_write(self_item, *args, **kwargs):
                recorder = get_active_recorder()
                if recorder is None:
                    return original_item_write(self_item, *args, **kwargs)
                return recorder._intercept_item_write(self_item, *args, **kwargs)

            MardiItem.write = patched_item_write

            # Patch MardiProperty.write
            original_prop_write = MardiProperty.write

            def patched_prop_write(self_prop, *args, **kwargs):
                recorder = get_active_recorder()
                if recorder is None:
                    return original_prop_write(self_prop, *args, **kwargs)
                return recorder._intercept_property_write(self_prop, *args, **kwargs)

            MardiProperty.write = patched_prop_write

            # Patch WikidataImporter.insert_id_in_db
            original_insert = WikidataImporter.insert_id_in_db

            def patched_insert(self_importer, wikidata_id, local_id, has_all_claims):
                recorder = get_active_recorder()
                if recorder is None:
                    return original_insert(
                        self_importer, wikidata_id, local_id, has_all_claims
                    )
                # No-op in dry-run mode, just log
                recorder._log_db_operation(
                    "insert_id_in_db", wikidata_id, local_id, has_all_claims
                )

            WikidataImporter.insert_id_in_db = patched_insert

            # Patch WikidataImporter.update_has_all_claims
            original_update = WikidataImporter.update_has_all_claims

            def patched_update(self_importer, wikidata_id):
                recorder = get_active_recorder()
                if recorder is None:
                    return original_update(self_importer, wikidata_id)
                # No-op in dry-run mode, just log
                recorder._log_db_operation(
                    "update_has_all_claims", wikidata_id, None, True
                )

            WikidataImporter.update_has_all_claims = patched_update

        except ImportError as e:
            # If we can't import the classes, we can't patch them
            # This might happen in certain testing scenarios
            print(f"Warning: Could not patch methods for CSV dry-run: {e}")

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
        """Generate a sequential stub ID.

        Args:
            entity_type: 'item' or 'property'

        Returns:
            Stub ID like 'Q_STUB_1' or 'P_STUB_1'
        """
        if entity_type == "property":
            self._prop_counter += 1
            return f"P_STUB_{self._prop_counter}"
        else:
            self._item_counter += 1
            return f"Q_STUB_{self._item_counter}"

    def _intercept_item_write(self, item, *args, **kwargs) -> MagicMock:
        """Intercept MardiItem.write() call and log to CSV.

        Args:
            item: The MardiItem instance being written

        Returns:
            MagicMock with stub id to simulate successful write
        """
        stub_id = self._generate_stub_id("item")

        # Extract entity data
        entity_data = self._extract_entity_data(item)

        # Determine source and external ID from context
        source, external_id = self._detect_source_and_id(item, entity_data)

        # Log the record
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

        # Return a mock object with the stub id
        mock_result = MagicMock()
        mock_result.id = stub_id
        return mock_result

    def _intercept_property_write(self, prop, *args, **kwargs) -> MagicMock:
        """Intercept MardiProperty.write() call and log to CSV.

        Args:
            prop: The MardiProperty instance being written

        Returns:
            MagicMock with stub id to simulate successful write
        """
        stub_id = self._generate_stub_id("property")

        # Extract entity data
        entity_data = self._extract_entity_data(prop)

        # Log the record
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

        # Return a mock object with the stub id
        mock_result = MagicMock()
        mock_result.id = stub_id
        return mock_result

    def _extract_entity_data(self, entity) -> Dict[str, Any]:
        """Extract labels, descriptions, and claims from an entity.

        Args:
            entity: MardiItem or MardiProperty instance

        Returns:
            Dictionary with entity data
        """
        data = {
            "type": getattr(entity, "type", "item"),
            "labels": {},
            "descriptions": {},
            "claims": {},
            "external_id": "",
        }

        # Try to get labels
        if hasattr(entity, "labels") and entity.labels:
            for lang in getattr(entity.labels, "languages", []):
                try:
                    value = entity.labels.get(lang)
                    if value:
                        data["labels"][lang] = str(value)
                except:
                    pass

        # Try to get descriptions
        if hasattr(entity, "descriptions") and entity.descriptions:
            for lang in getattr(entity.descriptions, "languages", []):
                try:
                    value = entity.descriptions.get(lang)
                    if value:
                        data["descriptions"][lang] = str(value)
                except:
                    pass

        # Try to get claims using get_json() or similar
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
        """Detect the source type and external ID from the item/claims.

        Args:
            item: The MardiItem being written
            entity_data: Extracted entity data

        Returns:
            Tuple of (source, external_id)
        """
        source = "unknown"
        external_id = ""

        claims = entity_data.get("claims", {})

        # Check for DOI (Crossref)
        if "P356" in str(claims) or "wdt:P356" in str(claims):
            source = "crossref"
            # Try to extract DOI from claims
            external_id = self._extract_claim_value(claims, "P356")
        # Check for arXiv ID
        elif "P818" in str(claims) or "wdt:P818" in str(claims):
            source = "arxiv"
            external_id = self._extract_claim_value(claims, "P818")
        # Check for Zenodo
        elif "P4901" in str(claims) or "wdt:P4901" in str(claims):
            source = "zenodo"
            external_id = self._extract_claim_value(claims, "P4901")
        # Check for CRAN
        elif "P5565" in str(claims) or "wdt:P5565" in str(claims):
            source = "cran"
            external_id = self._extract_claim_value(claims, "P5565")
        # Check for Wikidata QID reference
        elif "P12" in str(claims) or "wdt:P12" in str(claims):  # Wikidata QID property
            source = "wikidata"
            external_id = self._extract_claim_value(claims, "P12")

        # Try to get source from label patterns
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
        """Extract a value from claims for a specific property.

        Args:
            claims: Claims dictionary
            property_id: Property ID without 'P' prefix or with 'wdt:P'

        Returns:
            The claim value or empty string
        """
        try:
            # Handle different claim formats
            if isinstance(claims, dict):
                # Try direct property access
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
        """Add a record to the internal list.

        Args:
            entity_type: Type of entity (item, property, author, etc.)
            source: Source system (wikidata, crossref, arxiv, etc.)
            external_id: Original external identifier
            labels: Dictionary of language -> label
            descriptions: Dictionary of language -> description
            claims: Claims dictionary
            stub_id: Generated stub ID
            operation: 'create', 'already_exists', or 'update'
            parent_stub_id: For nested entities, the parent's stub ID
            existing_qid: The existing QID if entity already exists (empty if new)
        """
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
        """Log a DB operation that was no-op'd.

        Args:
            operation: Name of the DB operation
            wikidata_id: The Wikidata ID
            local_id: The local ID (if applicable)
            has_all_claims: The has_all_claims flag
        """
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
        }
        self.records.append(record)

    def _write_csv(self) -> None:
        """Write all records to the CSV file."""
        if not self.records:
            # Write empty file with headers
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
