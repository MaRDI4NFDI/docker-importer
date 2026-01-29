import logging
import os

import requests
from flask import Flask, jsonify, request

from services.import_service import (
    DEFAULT_WORKFLOW_NAME,
    build_health_payload,
    get_workflow_result,
    get_workflow_status,
    import_cran_sync,
    import_doi_sync,
    import_wikidata_sync,
    normalize_list,
    trigger_doi_async,
    trigger_wikidata_async,
)


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-mardi.zib.de/api")
PREFECT_API_AUTH_STRING = os.getenv("PREFECT_API_AUTH_STRING")  # "user:pass"

app = Flask(__name__)


@app.get("/health")
def health():
    """Return a basic health status payload.

    Returns:
        Flask response tuple with service status.
    """
    return jsonify(build_health_payload()), 200


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
    qids = normalize_list(data.get("qids"))
    if not qids:
        log.error("missing QIDs")
        return jsonify(error="missing qids"), 400

    log.info("QIDs: %s", qids)

    try:
        log.info("Triggering Prefect workflow '%s'", DEFAULT_WORKFLOW_NAME)
        payload = trigger_wikidata_async(qids, workflow_name=DEFAULT_WORKFLOW_NAME)
        log.info(
            "Workflow triggered. ID: %s. Deployment ID: %s. Flow ID: %s",
            payload.get("id"),
            payload.get("deployment_id"),
            payload.get("flow_id"),
        )
        return jsonify(payload), 202
    except Exception as exc:
        log.error("Failed to trigger Prefect flow: %s", exc)
        return jsonify(error="Could not start background job", details=str(exc)), 500


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
        result = get_workflow_status(
            PREFECT_API_URL,
            PREFECT_API_AUTH_STRING,
            flow_run_id,
        )
        return jsonify(result), 200
    except requests.HTTPError as exc:
        log.error("Prefect returned HTTP error: %s", exc, exc_info=True)
        return jsonify(
            error="could not fetch flow run",
            details=str(exc),
        ), 404
    except Exception as exc:
        log.error("Failed to query Prefect flow run: %s", exc, exc_info=True)
        return jsonify(
            error="prefect api error",
            details=str(exc),
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

    try:
        payload, status_code = get_workflow_result(
            PREFECT_API_URL,
            PREFECT_API_AUTH_STRING,
            flow_run_id,
            key_prefix=key_prefix,
        )
        return jsonify(payload), status_code
    except requests.HTTPError as exc:
        log.error("Prefect returned HTTP error: %s", exc, exc_info=True)
        return jsonify(
            error="prefect api error",
            details=str(exc),
        ), 500
    except Exception as exc:
        log.error("Failed to query Prefect artifact: %s", exc, exc_info=True)
        return jsonify(
            error="prefect api error",
            details=str(exc),
        ), 500


@app.post("/import/wikidata")
def import_wikidata():
    """Import Wikidata entities synchronously by QID.

    Expects JSON with a ``qids`` field, which may be a list or a string of
    comma/space-separated QIDs.

    Returns:
        Flask response tuple with per-QID import results.
    """
    data = request.get_json(silent=True) or {}
    qids = normalize_list(data.get("qids"))
    if not qids:
        return jsonify(error="missing qids"), 400

    payload, _all_ok = import_wikidata_sync(qids)
    return jsonify(payload), 200


@app.post("/import/doi_async")
def import_doi_async():
    """Trigger a background DOI import flow via Prefect.

    Expects JSON with a ``dois`` field, which may be a list or a string of
    comma/space-separated DOIs.

    Returns:
        Flask response tuple with flow metadata and HTTP status.
    """
    log.info("Called 'import_doi_async'.")

    data = request.get_json(silent=True) or {}
    dois = normalize_list(data.get("dois"))
    if not dois:
        log.error("missing DOIs")
        return jsonify(error="missing dois"), 400

    log.info("DOIs: %s", dois)

    try:
        log.info("Triggering Prefect workflow '%s'", DEFAULT_WORKFLOW_NAME)
        payload = trigger_doi_async(dois, workflow_name=DEFAULT_WORKFLOW_NAME)
        log.info(
            "Workflow triggered. ID: %s. Deployment ID: %s. Flow ID: %s",
            payload.get("id"),
            payload.get("deployment_id"),
            payload.get("flow_id"),
        )
        return jsonify(payload), 202
    except Exception as exc:
        log.error("Failed to trigger Prefect flow: %s", exc)
        return jsonify(error="Could not start background job", details=str(exc)), 500


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
    dois = normalize_list(data.get("dois"))
    if not dois:
        return jsonify(error="missing doi"), 400

    payload, _all_ok = import_doi_sync(dois)
    return jsonify(payload), 200


@app.post("/import/cran")
def import_cran():
    """Import CRAN packages synchronously.

    Expects JSON with a ``packages`` field, which may be a list or a string of
    comma/space-separated package names.

    Returns:
        Flask response tuple with per-package import results.
    """
    data = request.get_json(silent=True) or {}
    packages = normalize_list(data.get("packages"))
    if not packages:
        return jsonify(error="missing packages"), 400

    payload, _all_ok = import_cran_sync(packages)
    return jsonify(payload), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
