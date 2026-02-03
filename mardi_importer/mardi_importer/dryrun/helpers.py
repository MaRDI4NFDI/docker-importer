"""Helper functions for CSV dry-run mode.

This module provides utility functions to simplify the integration
of CSV dry-run mode into CLI commands.
"""

from typing import Any, Callable, Dict, Tuple
import argparse


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

    Example:
        def cmd_import_doi(args):
            dois = normalize_list(args.dois)
            if not dois:
                return error_response("missing doi")

            from services.import_service import import_doi_sync
            from mardi_importer.dryrun.helpers import run_import_with_optional_dryrun

            payload, exit_code = run_import_with_optional_dryrun(
                import_func=import_doi_sync,
                args=args,
                dois=dois
            )
            print(json.dumps(payload))
            return exit_code
    """
    if not getattr(args, "csv_only", False):
        # Normal mode - just run the import
        payload, all_ok = import_func(**import_kwargs)
        return payload, 0 if all_ok else 1

    # Dry-run mode
    from mardi_importer.dryrun.patcher import patch_for_lookup_aware_dry_run
    from mardi_importer.dryrun.recorder import CSVRecorder

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
