"""CSV Dry-run package for intercepting Wikibase writes.

This package provides a write-intercept layer that, when enabled,
replaces underlying write operations (Mardi item/property writes
and Wikidata DB-mapping helpers) with a recorder that logs payloads
and returns stub IDs, while no-oping DB updates.
"""

from .recorder import CSVRecorder, is_dry_run_active, csv_dry_run, get_active_recorder
from .patcher import patch_for_lookup_aware_dry_run
from .helpers import run_import_with_optional_dryrun

__all__ = [
    "CSVRecorder",
    "is_dry_run_active",
    "csv_dry_run",
    "get_active_recorder",
    "patch_for_lookup_aware_dry_run",
    "run_import_with_optional_dryrun",
]
