import argparse
import base64
import json
import logging
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote

import requests
from prefect.deployments import run_deployment

from mardi_importer import Importer
from mardi_importer.wikidata import WikidataImporter


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def _load_secrets() -> None:
    secrets_path = Path(__file__).resolve().parent / "secrets.txt"
    if not secrets_path.exists():
        return
    for line in secrets_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ[key.strip()] = value.strip()


_load_secrets()

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-mardi.zib.de/api")
PREFECT_API_AUTH_STRING = os.getenv("PREFECT_API_AUTH_STRING")  # "user:pass"


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in re.split(r"[,\s]+", value) if v.strip()]
    return [str(value).strip()]


def _prefect_headers():
    headers = {"Content-Type": "application/json"}
    if PREFECT_API_AUTH_STRING:
        token = base64.b64encode(PREFECT_API_AUTH_STRING.encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    return headers


def _prefect_get(path: str, payload: dict | None = None, timeout: int = 30):
    url = f"{PREFECT_API_URL}{path}"
    if payload is None:
        response = requests.get(url, headers=_prefect_headers(), timeout=timeout)
    else:
        response = requests.post(
            url,
            headers=_prefect_headers(),
            json=payload,
            timeout=timeout,
        )
    response.raise_for_status()
    return response.json()


def cmd_health(_args: argparse.Namespace) -> int:
    payload = {"status": "healthy", "service": "docker-importer"}
    print(json.dumps(payload))
    return 0


def cmd_import_wikidata_async(args: argparse.Namespace) -> int:
    qids = as_list(args.qids)
    if not qids:
        print(json.dumps({"error": "missing qids"}))
        return 2

    workflow_name = "mardi-importer/prefect-mardi-importer"
    log.info("Triggering Prefect workflow '%s'", workflow_name)

    try:
        flow_run = run_deployment(
            name=workflow_name,
            parameters={"action": "import/wikidata", "qids": qids},
            timeout=0,
        )
        payload = {
            "status": "accepted",
            "message": "Wikidata import process started in background",
            "deployment_id": flow_run.deployment_id,
            "id": flow_run.id,
            "flow_id": flow_run.flow_id,
            "qids_queued": qids,
        }
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
    flow_run_id = args.id
    if not flow_run_id:
        print(json.dumps({"error": "missing flow run id (parameter: id)"}))
        return 2

    try:
        url = f"{PREFECT_API_URL}/flow_runs/{flow_run_id}"
        response = requests.get(url, headers=_prefect_headers(), timeout=30)
        response.raise_for_status()
        data = response.json()
        state = data.get("state", {}) or {}

        result = {
            "id": flow_run_id,
            "state": state.get("type"),
            "state_name": state.get("name"),
            "timestamp": state.get("timestamp"),
        }

        if state.get("type") == "COMPLETED":
            result_url = f"{PREFECT_API_URL}/flow_runs/{flow_run_id}/result"
            result_response = requests.get(
                result_url, headers=_prefect_headers(), timeout=10
            )
            if result_response.status_code == 200:
                result["result"] = result_response.json()

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
    flow_run_id = args.id
    if not flow_run_id:
        print(json.dumps({"error": "missing flow run id (parameter: id)"}))
        return 2

    key_prefix = args.key_prefix or "mardi-importer-result-"
    artifact_key = f"{key_prefix}{flow_run_id}"

    fr = _prefect_get(f"/flow_runs/{flow_run_id}", timeout=30)
    state = (fr.get("state") or {}).get("type")

    if state != "COMPLETED":
        print(
            json.dumps(
                {
                    "id": flow_run_id,
                    "state": state,
                    "message": "flow run not completed yet",
                }
            )
        )
        return 0

    key_enc = quote(artifact_key, safe="")
    try:
        artifact = _prefect_get(f"/artifacts/{key_enc}/latest", timeout=30)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            print(
                json.dumps(
                    {
                        "id": flow_run_id,
                        "state": state,
                        "error": "artifact not found",
                        "expected_key": artifact_key,
                    }
                )
            )
            return 1
        raise

    print(
        json.dumps(
            {
                "id": flow_run_id,
                "state": state,
                "artifact_id": artifact.get("id"),
                "key": artifact.get("key"),
                "created": artifact.get("created"),
                "data": artifact.get("data"),
            }
        )
    )
    return 0


def cmd_import_wikidata(args: argparse.Namespace) -> int:
    qids = as_list(args.qids)
    if not qids:
        print(json.dumps({"error": "missing qids"}))
        return 2

    wdi = WikidataImporter()
    results = {}
    all_ok = True

    for qid in qids:
        try:
            imported_q = wdi.import_entities(qid)
            if not imported_q:
                log.info("No import for wikidata qid %s", qid)
                status = "not_imported"
                ok = False
            else:
                log.info("Import for wikidata qid %s: %s", qid, imported_q)
                status = "success"
                ok = True
            results[qid] = {"qid": imported_q, "status": status}
            if not ok:
                all_ok = False
        except Exception as exc:
            log.error("importing wikidata failed: %s", exc, exc_info=True)
            results[qid] = {"qid": None, "status": "error", "error": str(exc)}
            all_ok = False

    print(
        json.dumps(
            {
                "qids": qids,
                "count": len(qids),
                "results": results,
                "all_imported": all_ok,
            }
        )
    )
    return 0 if all_ok else 1


def cmd_import_doi_async(args: argparse.Namespace) -> int:
    dois = as_list(args.dois)
    if not dois:
        print(json.dumps({"error": "missing dois"}))
        return 2
    dois = [doi.upper() for doi in dois]

    workflow_name = "mardi-importer/prefect-mardi-importer"
    log.info("Triggering Prefect workflow '%s'", workflow_name)

    try:
        flow_run = run_deployment(
            name=workflow_name,
            parameters={"action": "import/doi", "dois": dois},
            timeout=0,
        )
        payload = {
            "status": "accepted",
            "message": "DOI import process started in background",
            "deployment_id": flow_run.deployment_id,
            "id": flow_run.id,
            "flow_id": flow_run.flow_id,
            "dois_queued": dois,
        }
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
    dois = as_list(args.dois)
    if not dois:
        print(json.dumps({"error": "missing doi"}))
        return 2
    dois = [doi.upper() for doi in dois]

    results = {}
    all_ok = True
    arxiv = Importer.create_source("arxiv")
    zenodo = Importer.create_source("zenodo")
    crossref = Importer.create_source("crossref")

    for doi in dois:
        log.info("Importing for doi %s", doi)
        try:
            if "ARXIV" in doi:
                arxiv_id = doi.split("ARXIV.")[-1]
                publication = arxiv.new_publication(arxiv_id)
                log.info("arxiv recognized")
            elif "ZENODO" in doi:
                zenodo_id = doi.split(".")[-1]
                publication = zenodo.new_resource(zenodo_id)
                log.info("zenodo recognized")
            else:
                publication = crossref.new_publication(doi)
                log.info("crossref recognized")
            result = publication.create()
            if result:
                log.info("Imported item %s for doi %s.", result, doi)
                results[doi] = {"qid": result, "status": "success"}
            else:
                log.info("doi %s was not found, not imported.", doi)
                results[doi] = {
                    "qid": None,
                    "status": "not_found",
                    "error": "DOI was not found.",
                }
                all_ok = False
        except Exception as exc:
            log.error("importing doi failed: %s", exc, exc_info=True)
            results[doi] = {"qid": None, "status": "error", "error": str(exc)}
            all_ok = False

    print(
        json.dumps(
            {
                "dois": dois,
                "count": len(dois),
                "results": results,
                "all_imported": all_ok,
            }
        )
    )
    return 0 if all_ok else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="CLI for docker-importer workflows and imports."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

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
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
