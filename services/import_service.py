import base64
import logging
import re
from typing import Any
from urllib.parse import quote

import requests
from prefect.deployments import run_deployment

from mardi_importer import Importer
from mardi_importer.wikidata import WikidataImporter


DEFAULT_WORKFLOW_NAME = "mardi-importer/prefect-mardi-importer"

log = logging.getLogger(__name__)


def normalize_list(value: Any) -> list[str]:
    """Normalize a value into a list of non-empty strings.

    Args:
        value: Incoming value from CLI args or JSON payloads.

    Returns:
        List of stripped, non-empty strings.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in re.split(r"[,\s]+", value) if v.strip()]
    return [str(value).strip()]


def build_health_payload(service_name: str = "docker-importer") -> dict:
    """Build a health payload for the service.

    Args:
        service_name: Name of the service to include in the payload.

    Returns:
        Health payload dictionary.
    """
    return {"status": "healthy", "service": service_name}


def build_prefect_headers(prefect_api_auth_string: str | None) -> dict:
    """Build headers for Prefect API requests.

    Args:
        prefect_api_auth_string: Optional "user:pass" string for Basic Auth.

    Returns:
        Dictionary of HTTP headers.
    """
    headers = {"Content-Type": "application/json"}
    if prefect_api_auth_string:
        token = base64.b64encode(prefect_api_auth_string.encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    return headers


def prefect_request(
    prefect_api_url: str,
    prefect_api_auth_string: str | None,
    path: str,
    payload: dict | None = None,
    timeout: int = 30,
) -> dict:
    """Call the Prefect API via GET or POST depending on payload.

    Args:
        prefect_api_url: Base Prefect API URL.
        prefect_api_auth_string: Optional "user:pass" string for Basic Auth.
        path: API path, including leading slash.
        payload: JSON payload for POST requests; uses GET when omitted.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response.
    """
    url = f"{prefect_api_url}{path}"
    headers = build_prefect_headers(prefect_api_auth_string)
    if payload is None:
        response = requests.get(url, headers=headers, timeout=timeout)
    else:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=timeout,
        )
    response.raise_for_status()
    return response.json()


def get_workflow_status(
    prefect_api_url: str,
    prefect_api_auth_string: str | None,
    flow_run_id: str,
) -> dict:
    """Fetch Prefect flow run status and optional result.

    Args:
        prefect_api_url: Base Prefect API URL.
        prefect_api_auth_string: Optional "user:pass" string for Basic Auth.
        flow_run_id: Prefect flow run ID.

    Returns:
        Flow run status payload.
    """
    url = f"{prefect_api_url}/flow_runs/{flow_run_id}"
    headers = build_prefect_headers(prefect_api_auth_string)
    response = requests.get(url, headers=headers, timeout=30)
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
        result_url = f"{prefect_api_url}/flow_runs/{flow_run_id}/result"
        result_response = requests.get(
            result_url, headers=headers, timeout=10
        )
        if result_response.status_code == 200:
            result["result"] = result_response.json()

    return result


def get_workflow_result(
    prefect_api_url: str,
    prefect_api_auth_string: str | None,
    flow_run_id: str,
    key_prefix: str = "mardi-importer-result-",
) -> tuple[dict, int]:
    """Fetch the Prefect artifact for a completed flow run.

    Args:
        prefect_api_url: Base Prefect API URL.
        prefect_api_auth_string: Optional "user:pass" string for Basic Auth.
        flow_run_id: Prefect flow run ID.
        key_prefix: Artifact key prefix.

    Returns:
        Tuple of payload and HTTP-like status code.
    """
    artifact_key = f"{key_prefix}{flow_run_id}"
    flow_run = prefect_request(
        prefect_api_url,
        prefect_api_auth_string,
        f"/flow_runs/{flow_run_id}",
        timeout=30,
    )
    state = (flow_run.get("state") or {}).get("type")

    if state != "COMPLETED":
        return (
            {
                "id": flow_run_id,
                "state": state,
                "message": "flow run not completed yet",
            },
            202,
        )

    key_enc = quote(artifact_key, safe="")
    try:
        artifact = prefect_request(
            prefect_api_url,
            prefect_api_auth_string,
            f"/artifacts/{key_enc}/latest",
            timeout=30,
        )
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return (
                {
                    "id": flow_run_id,
                    "state": state,
                    "error": "artifact not found",
                    "expected_key": artifact_key,
                },
                404,
            )
        raise

    return (
        {
            "id": flow_run_id,
            "state": state,
            "artifact_id": artifact.get("id"),
            "key": artifact.get("key"),
            "created": artifact.get("created"),
            "data": artifact.get("data"),
        },
        200,
    )


def trigger_wikidata_async(
    qids: list[str],
    workflow_name: str = DEFAULT_WORKFLOW_NAME,
) -> dict:
    """Trigger a Prefect Wikidata import workflow.

    Args:
        qids: List of Wikidata QIDs to import.
        workflow_name: Prefect deployment name.

    Returns:
        Payload with flow run metadata.
    """
    flow_run = run_deployment(
        name=workflow_name,
        parameters={"action": "import/wikidata", "qids": qids},
        timeout=0,
    )
    return {
        "status": "accepted",
        "message": "Wikidata import process started in background",
        "deployment_id": flow_run.deployment_id,
        "id": flow_run.id,
        "flow_id": flow_run.flow_id,
        "qids_queued": qids,
    }


def trigger_doi_async(
    dois: list[str],
    workflow_name: str = DEFAULT_WORKFLOW_NAME,
) -> dict:
    """Trigger a Prefect DOI import workflow.

    Args:
        dois: List of DOIs to import.
        workflow_name: Prefect deployment name.

    Returns:
        Payload with flow run metadata.
    """
    doi_list = [doi.upper() for doi in dois]
    flow_run = run_deployment(
        name=workflow_name,
        parameters={"action": "import/doi", "dois": doi_list},
        timeout=0,
    )
    return {
        "status": "accepted",
        "message": "DOI import process started in background",
        "deployment_id": flow_run.deployment_id,
        "id": flow_run.id,
        "flow_id": flow_run.flow_id,
        "dois_queued": doi_list,
    }


def import_wikidata_sync(qids: list[str]) -> tuple[dict, bool]:
    """Import Wikidata entities synchronously.

    Args:
        qids: List of Wikidata QIDs.

    Returns:
        Tuple of payload and overall success flag.
    """
    wdi = WikidataImporter()
    results: dict[str, dict] = {}
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

    payload = {
        "qids": qids,
        "count": len(qids),
        "results": results,
        "all_imported": all_ok,
    }
    return payload, all_ok


def import_doi_sync(dois: list[str]) -> tuple[dict, bool]:
    """Import publications by DOI from supported sources.

    Args:
        dois: List of DOIs to import.

    Returns:
        Tuple of payload and overall success flag.
    """
    doi_list = [doi.upper() for doi in dois]
    results: dict[str, dict] = {}
    all_ok = True
    arxiv = Importer.create_source("arxiv")
    zenodo = Importer.create_source("zenodo")
    crossref = Importer.create_source("crossref")

    for doi in doi_list:
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

    payload = {
        "dois": doi_list,
        "count": len(doi_list),
        "results": results,
        "all_imported": all_ok,
    }
    return payload, all_ok
