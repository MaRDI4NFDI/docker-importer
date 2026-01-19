import base64
import os
from urllib.parse import quote

import requests
from flask import Flask, request, jsonify
from mardi_importer.wikidata import WikidataImporter
from prefect import get_client
from prefect.deployments import run_deployment

from mardi_importer import Importer
import re

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-mardi.zib.de/api")
PREFECT_API_AUTH_STRING = os.getenv("PREFECT_API_AUTH_STRING")  # "user:pass"

app = Flask(__name__)

def as_list(value):
    """Normalize request values into a list of stripped strings.

    Args:
        value: Incoming value from JSON or query params.

    Returns:
        List of non-empty, stripped strings.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in re.split(r'[,\s]+', value) if v.strip()]
    return [str(value).strip()]

@app.get("/health")
def health():
    """Return a basic health status payload.

    Returns:
        Flask response tuple with service status.
    """
    return jsonify({
        "status": "healthy",
        "service": "docker-importer"
    }), 200

@app.post("/import/wikidata_async")
def import_wikidata_async():
    """Trigger a background Wikidata import flow via Prefect.

    Expects JSON with a ``qids`` field, which may be a list or a string of
    comma/space-separated QIDs.

    Returns:
        Flask response tuple with flow metadata and HTTP status.
    """

    log.info("Called 'import_wikidata_async'.")

    data = request.get_json(silent=True) or {}
    qids = as_list(data.get("qids"))
    if not qids:
        log.error("missing QIDs")
        return jsonify(error="missing qids"), 400

    log.info(f"QIDs: {qids}")

    try:
        # Trigger the flow asynchronously on the Prefect Server
        # 'timeout=0' tells Prefect not to wait for the result
        workflow_name = "mardi-importer/prefect-mardi-importer"

        log.info(f"Triggering Prefect workflow '{workflow_name}'")

        flow_run = run_deployment(
            name=workflow_name,
            parameters={"action": "import/wikidata", "qids": qids},
            timeout=0,
        )

        log.info(f"Workflow triggered. ID: {flow_run.id}. Deployment ID: {flow_run.deployment_id}. Flow ID: {flow_run.flow_id}")

        return jsonify({
            "status": "accepted",
            "message": "Wikidata import process started in background",
            "deployment_id": flow_run.deployment_id,
            "id": flow_run.id,
            "flow_id": flow_run.flow_id,
            "qids_queued": qids
        }), 202

    except Exception as e:
        log.error("Failed to trigger Prefect flow: %s", e)
        return jsonify(error="Could not start background job", details=str(e)), 500

def _prefect_headers():
    """Build Prefect API headers, including Basic Auth when configured.

    Returns:
        Dict of headers for Prefect API requests.
    """
    headers = {"Content-Type": "application/json"}
    if PREFECT_API_AUTH_STRING:
        token = base64.b64encode(PREFECT_API_AUTH_STRING.encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    return headers

def _prefect_get(path: str, payload: dict | None = None, timeout: int = 30):
    """Call the Prefect API via GET or POST depending on payload.

    Uses Basic Auth via ``PREFECT_API_AUTH_STRING`` when present.

    Args:
        path: Prefect API path, including leading slash.
        payload: JSON payload for POST requests; uses GET when omitted.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response from Prefect.
    """
    url = f"{PREFECT_API_URL}{path}"
    if payload is None:
        r = requests.get(url, headers=_prefect_headers(), timeout=timeout)
    else:
        r = requests.post(url, headers=_prefect_headers(), json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


@app.get("/import/workflow_status")
def import_workflow_status():
    """Return Prefect flow run status and optional result data.

    Query params:
        id: Prefect flow run id (required).

    Returns:
        Flask response tuple with flow status details.
    """
    log.info("Called 'import_workflow_status'.")

    flow_run_id = request.args.get("id")
    if not flow_run_id:
        return jsonify(error="missing flow run id (parameter: id)"), 400

    try:
        url = f"{PREFECT_API_URL}/flow_runs/{flow_run_id}"
        r = requests.get(url, headers=_prefect_headers(), timeout=30)
        r.raise_for_status()

        data = r.json()
        state = data.get("state", {}) or {}

        result = {
            "id": flow_run_id,
            "state": state.get("type"),       # e.g. SCHEDULED, RUNNING, COMPLETED, FAILED
            "state_name": state.get("name"),
            "timestamp": state.get("timestamp"),
        }

        # Get result if job is finished
        if state.get("type") == "COMPLETED":
            result_url = f"{PREFECT_API_URL}/flow_runs/{flow_run_id}/result"
            rr = requests.get(result_url, headers=_prefect_headers(), timeout=10)
            if rr.status_code == 200:
                result["result"] = rr.json()

        return jsonify(result), 200

    except requests.HTTPError as e:
        log.error("Prefect returned HTTP error: %s", e, exc_info=True)
        return jsonify(
            error="could not fetch flow run",
            details=str(e),
        ), 404

    except Exception as e:
        log.error("Failed to query Prefect flow run: %s", e, exc_info=True)
        return jsonify(
            error="prefect api error",
            details=str(e),
        ), 500


@app.get("/import/workflow_result")
def import_workflow_result():
    """Get the stored Prefect artifact for a completed flow run.

    Query params:
        id: Prefect flow run id (required).
        key_prefix: Artifact key prefix (optional, default:
            ``mardi-importer-result-``).

    Returns:
        Flask response tuple with artifact data or status details.
    """
    flow_run_id = request.args.get("id")
    if not flow_run_id:
        return jsonify(error="missing flow run id (parameter: id)"), 400

    key_prefix = request.args.get("key_prefix", "mardi-importer-result-")
    artifact_key = f"{key_prefix}{flow_run_id}"

    # 1) Check flow run state
    fr = _prefect_get(f"/flow_runs/{flow_run_id}", timeout=30)
    state = (fr.get("state") or {}).get("type")

    if state != "COMPLETED":
        return jsonify(
            id=flow_run_id,
            state=state,
            message="flow run not completed yet",
        ), 202

    # 2) Fetch artifact by key (latest)
    key_enc = quote(artifact_key, safe="")
    try:
        a = _prefect_get(f"/artifacts/{key_enc}/latest", timeout=30)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return jsonify(
                id=flow_run_id,
                state=state,
                error="artifact not found",
                expected_key=artifact_key,
            ), 404
        raise

    return jsonify(
        id=flow_run_id,
        state=state,
        artifact_id=a.get("id"),
        key=a.get("key"),
        created=a.get("created"),
        data=a.get("data"),
    ), 200

@app.post("/import/wikidata")
def import_wikidata():
    """Import Wikidata entities synchronously by QID.

    Expects JSON with a ``qids`` field, which may be a list or a string of
    comma/space-separated QIDs.

    Returns:
        Flask response tuple with per-QID import results.
    """
    data = request.get_json(silent=True) or {}
    qids = as_list(data.get("qids"))
    if not qids:
        return jsonify(error="missing qids"), 400
    wdi = WikidataImporter()
    results = {}
    all_ok = True
    for q in qids:
        try:
            imported_q = wdi.import_entities(q)
            if not imported_q:
                log.info(f"No import for wikidata qid {q}")
                status = "not_imported"
                ok = False
            else:
                log.info(f"Import for wikidata qid {q}: {imported_q}")
                status = "success"
                ok = True
            results[q] = {"qid": imported_q,"status": status,}
            if not ok: all_ok = False
        except Exception as e:
            log.error("importing wikidata failed: %s", e, exc_info=True)
            results[q] = {"qid": None,"status": "error","error": str(e),}
            all_ok = False

    return jsonify({
        "qids": qids,
        "count": len(qids),
        "results": results,
        "all_imported": all_ok
    }), 200


@app.post("/import/doi")
def import_doi():
    """Import publications by DOI from supported sources.

    Expects JSON with a ``dois`` field, which may be a list or a string of
    comma/space-separated DOIs. Routes to arXiv, Zenodo, or Crossref based on
    DOI patterns.

    Returns:
        Flask response tuple with per-DOI import results.
    """
    data = request.get_json(silent=True) or {}
    dois = as_list(data.get("dois"))
    if not dois:
        return jsonify(error="missing doi"), 400
    dois = [x.upper() for x in dois]
    results = {}
    all_ok = True
    arxiv = Importer.create_source('arxiv')
    zenodo = Importer.create_source('zenodo')
    crossref = Importer.create_source('crossref')

    for doi in dois:
        log.info(f"Importing for doi {doi}")
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
                log.info(f"Imported item {result} for doi {doi}.")
                results[doi] = {"qid": result,"status": "success"}
            else:
                log.info(f"doi {doi} was not found, not imported.")
                results[doi] = {"qid": None,"status": "not_found", "error": "DOI was not found."}
                all_ok = False
        except Exception as e: 
            log.error("importing doi failed: %s", e, exc_info=True)
            results[doi] = {"qid": None,"status": "error", "error": str(e)}
            all_ok = False
    return jsonify({
        "dois": dois,
        "count": len(dois),
        "results": results,
        "all_imported": all_ok
    }), 200

if __name__ == "__main__":
    app.run(host = "0.0.0.0", port=8000)
