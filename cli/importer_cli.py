import argparse
import json
import logging
import os
import sys
from pathlib import Path

import requests

from services.import_service import (
    DEFAULT_WORKFLOW_NAME,
    build_health_payload,
    get_workflow_result,
    get_workflow_status,
    import_doi_sync,
    import_wikidata_sync,
    normalize_list,
    trigger_doi_async,
    trigger_wikidata_async,
)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger()

def print_mardi_logo():
    # Enable ANSI support on Windows (if needed)
    if os.name == "nt":
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)

    # ANSI colors
    ORANGE = "\033[38;5;208m"
    RESET = "\033[0m"

    mardi_nfo = r"""
██▄  ▄██  ▄▄▄  █████▄  ████▄  ██   ██  ██   ███  ██ ██████ ████▄  ██ 
██ ▀▀ ██ ██▀██ ██▄▄██▄ ██  ██ ██   ▀█████   ██ ▀▄██ ██▄▄   ██  ██ ██ 
██    ██ ██▀██ ██   ██ ████▀  ██       ██   ██   ██ ██     ████▀  ██
"""

    print(ORANGE + mardi_nfo + RESET)

def _load_secrets() -> None:
    """Load environment variables from the local CLI secrets file.

    Reads `cli/secrets.txt` if present. Lines must be in `KEY=VALUE` format.
    Blank lines and comments are ignored.
    """
    secrets_path = Path(__file__).resolve().parent / "secrets.txt"
    if not secrets_path.exists():
        return
    for line in secrets_path.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip()


_load_secrets()

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-mardi.zib.de/api")
PREFECT_API_AUTH_STRING = os.getenv("PREFECT_API_AUTH_STRING")  # "user:pass"


def cmd_health(_args: argparse.Namespace) -> int:
    """Print a health payload for the CLI service.

    Args:
        _args: Parsed CLI arguments (unused).

    Returns:
        Process exit code.
    """
    payload = build_health_payload()
    print(json.dumps(payload))
    return 0


def cmd_import_wikidata_async(args: argparse.Namespace) -> int:
    """Trigger a Prefect Wikidata import in the background.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Process exit code.
    """
    qids = normalize_list(args.qids)
    if not qids:
        print(json.dumps({"error": "missing qids"}))
        return 2

    log.info("Triggering Prefect workflow '%s'", DEFAULT_WORKFLOW_NAME)

    try:
        payload = trigger_wikidata_async(qids, workflow_name=DEFAULT_WORKFLOW_NAME)
        print(json.dumps(payload))
        return 0
    except Exception as exc:
        log.error("Failed to trigger Prefect flow: %s", exc)
        print(
            json.dumps(
                {"error": "Could not start background job", "details": str(exc)}
            )
        )
        return 1


def cmd_import_workflow_status(args: argparse.Namespace) -> int:
    """Fetch Prefect flow run status and optional result data.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Process exit code.
    """
    flow_run_id = args.id
    if not flow_run_id:
        print(json.dumps({"error": "missing flow run id (parameter: id)"}))
        return 2

    try:
        result = get_workflow_status(
            PREFECT_API_URL,
            PREFECT_API_AUTH_STRING,
            flow_run_id,
        )
        print(json.dumps(result))
        return 0
    except requests.HTTPError as exc:
        log.error("Prefect returned HTTP error: %s", exc, exc_info=True)
        print(json.dumps({"error": "could not fetch flow run", "details": str(exc)}))
        return 1
    except Exception as exc:
        log.error("Failed to query Prefect flow run: %s", exc, exc_info=True)
        print(json.dumps({"error": "prefect api error", "details": str(exc)}))
        return 1


def cmd_import_workflow_result(args: argparse.Namespace) -> int:
    """Fetch Prefect artifact data for a flow run.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Process exit code.
    """
    flow_run_id = args.id
    if not flow_run_id:
        print(json.dumps({"error": "missing flow run id (parameter: id)"}))
        return 2

    key_prefix = args.key_prefix or "mardi-importer-result-"

    try:
        payload, status_code = get_workflow_result(
            PREFECT_API_URL,
            PREFECT_API_AUTH_STRING,
            flow_run_id,
            key_prefix=key_prefix,
        )
        print(json.dumps(payload))
        if status_code == 404:
            return 1
        return 0
    except requests.HTTPError as exc:
        log.error("Prefect returned HTTP error: %s", exc, exc_info=True)
        print(json.dumps({"error": "prefect api error", "details": str(exc)}))
        return 1
    except Exception as exc:
        log.error("Failed to query Prefect artifact: %s", exc, exc_info=True)
        print(json.dumps({"error": "prefect api error", "details": str(exc)}))
        return 1


def cmd_import_wikidata(args: argparse.Namespace) -> int:
    """Import Wikidata entities synchronously by QID.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Process exit code.
    """
    qids = normalize_list(args.qids)
    if not qids:
        print(json.dumps({"error": "missing qids"}))
        return 2

    payload, all_ok = import_wikidata_sync(qids)
    print(json.dumps(payload))
    return 0 if all_ok else 1


def cmd_import_doi_async(args: argparse.Namespace) -> int:
    """Trigger a Prefect DOI import in the background.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Process exit code.
    """
    dois = normalize_list(args.dois)
    if not dois:
        print(json.dumps({"error": "missing dois"}))
        return 2

    log.info("Triggering Prefect workflow '%s'", DEFAULT_WORKFLOW_NAME)

    try:
        payload = trigger_doi_async(dois, workflow_name=DEFAULT_WORKFLOW_NAME)
        print(json.dumps(payload))
        return 0
    except Exception as exc:
        log.error("Failed to trigger Prefect flow: %s", exc)
        print(
            json.dumps(
                {"error": "Could not start background job", "details": str(exc)}
            )
        )
        return 1


def cmd_import_doi(args: argparse.Namespace) -> int:
    """Import DOI entries synchronously.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Process exit code.
    """
    dois = normalize_list(args.dois)
    if not dois:
        print(json.dumps({"error": "missing doi"}))
        return 2

    payload, all_ok = import_doi_sync(dois)
    print(json.dumps(payload))
    return 0 if all_ok else 1


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns:
        Configured argument parser.
    """
    parser = argparse.ArgumentParser(
        description="CLI for docker-importer workflows and imports."
    )
    subparsers = parser.add_subparsers(dest="command")

    sub = subparsers.add_parser("health", help="Report service health.")
    sub.set_defaults(func=cmd_health)

    sub = subparsers.add_parser(
        "import-wikidata-async", help="Trigger Prefect Wikidata import."
    )
    sub.add_argument("--qids", nargs="*", help="QIDs list or comma-separated.")
    sub.set_defaults(func=cmd_import_wikidata_async)

    sub = subparsers.add_parser(
        "import-workflow-status", help="Get Prefect flow run status."
    )
    sub.add_argument("--id", required=True, help="Flow run id.")
    sub.set_defaults(func=cmd_import_workflow_status)

    sub = subparsers.add_parser(
        "import-workflow-result", help="Get Prefect artifact for a flow run."
    )
    sub.add_argument("--id", required=True, help="Flow run id.")
    sub.add_argument(
        "--key-prefix",
        help="Artifact key prefix (default: mardi-importer-result-).",
    )
    sub.set_defaults(func=cmd_import_workflow_result)

    sub = subparsers.add_parser(
        "import-wikidata", help="Import Wikidata entities synchronously."
    )
    sub.add_argument("--qids", nargs="*", help="QIDs list or comma-separated.")
    sub.set_defaults(func=cmd_import_wikidata)

    sub = subparsers.add_parser(
        "import-doi-async", help="Trigger Prefect DOI import."
    )
    sub.add_argument("--dois", nargs="*", help="DOIs list or comma-separated.")
    sub.set_defaults(func=cmd_import_doi_async)

    sub = subparsers.add_parser("import-doi", help="Import DOI synchronously.")
    sub.add_argument("--dois", nargs="*", help="DOIs list or comma-separated.")
    sub.set_defaults(func=cmd_import_doi)

    return parser


def main() -> int:
    """Run the CLI entrypoint.

    Returns:
        Process exit code.
    """

    print_mardi_logo()
    log.info("Starting importer CLI")

    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        print(
            "This is the MaRDI importer CLI. To do a health check, execute: "
            "python -m cli.importer_cli health"
        )
        parser.print_help()
        sys.stderr.write(
            "importer_cli.py: error: the following arguments are required: command\n"
        )
        return 2
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
