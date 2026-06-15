import logging
import os
import time

import requests
from flask import Flask, jsonify, request

from services.import_service import (
    DEFAULT_WORKFLOW_NAME,
    build_health_payload,
    create_item_sync,
    create_typed_item_sync,
    get_workflow_result,
    get_workflow_runs_last_n_hours,
    get_workflow_status,
    import_cran_sync,
    import_doi_sync,
    import_wikidata_sync,
    normalize_list,
    trigger_doi_async,
    trigger_wikidata_async,
    trigger_update_wikidata_async,
    update_item_sync,
)
from services.item_schemas import KNOWN_TYPES
from services.version import get_version
from mardi_importer.wikidata import WikidataImporter


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
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


@app.get("/version")
def version():
    """Return the service version from the VERSION file."""
    return jsonify({"version": get_version()}), 200


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


@app.get("/import/workflow_runs")
def import_workflow_runs():
    """Return Prefect flow runs from the last 24 hours.

    Returns:
        Flask response tuple with a list of flow run details.
    """
    log.info("Called 'import_workflow_runs'.")

    try:
        result = get_workflow_runs_last_n_hours(
            PREFECT_API_URL,
            PREFECT_API_AUTH_STRING,
            72
        )
        return jsonify(result), 200
    except requests.HTTPError as exc:
        log.error("Prefect returned HTTP error: %s", exc, exc_info=True)
        return jsonify(
            error="could not fetch flow runs",
            details=str(exc),
        ), 404
    except Exception as exc:
        log.error("Failed to query Prefect flow runs: %s", exc, exc_info=True)
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

@app.post("/update/wikidata")
def update_wikidata_async():
    """Update person profile from wikidata; this is async and happens in Prefect

    Expects JSON with a ``qids`` field, , which may be a list or a string of
    comma/space-separated Wikidata QIDs. 

    Returns:
        Response, either the QID that was updated or an empty list
    """
    log.info("Called 'update_wikidata' (POST). PID: %s, Time: %s", os.getpid(), time.time())
    data = request.get_json(silent=True) or {}
    qids = normalize_list(data.get("qids"))
    if not qids:
        return jsonify(error="missing qids"), 400
    log.info("QID: %s", qids)
    try:
        log.info("Triggering Prefect workflow '%s'", DEFAULT_WORKFLOW_NAME)
        payload = trigger_update_wikidata_async(qids, workflow_name=DEFAULT_WORKFLOW_NAME)
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


@app.post("/create/item")
def create_item():
    """Create a Wikibase item from a JSON description.

    Accepts two formats:

    **Raw format** — supply explicit MaRDI KG property and item IDs::

        {
            "label": "My item",
            "description": "An optional description",
            "claims": {"<MaRDI-PID>": "<MaRDI-QID>"},
            "username": "User@BotName",
            "password": "<bot-password>"
        }

    **Typed format** — supply a schema type and human-readable fields::

        {
            "type": "WORKFLOW",
            "fields": {"name": "My workflow", "problem_statement": "Solve X"},
            "username": "User@BotName",
            "password": "<bot-password>"
        }

    Known types: WORKFLOW.

    Returns:
        Flask response tuple with the created item QID and status.
    """
    data = request.get_json(silent=True) or {}

    username = data.get("username")
    password = data.get("password")
    if not isinstance(username, str) or not username.strip():
        return jsonify(error="'username' is required"), 401
    if not isinstance(password, str) or not password.strip():
        return jsonify(error="'password' is required"), 401

    if "type" in data:
        type_name = data.get("type")
        if not isinstance(type_name, str) or type_name not in KNOWN_TYPES:
            return jsonify(error=f"Unknown type '{type_name}'. Known types: {sorted(KNOWN_TYPES)}"), 400
        fields = data.get("fields", {})
        if not isinstance(fields, dict):
            return jsonify(error="'fields' must be a JSON object"), 400
        payload, ok = create_typed_item_sync(type_name, fields, username=username, password=password)
        if ok:
            return jsonify(payload), 200
        if "errors" in payload:
            return jsonify(payload), 400
        return jsonify(payload), 500

    label = data.get("label")
    if not label:
        return jsonify(error="missing label"), 400
    description = data.get("description")
    claims = data.get("claims", {})
    if not isinstance(claims, dict):
        return jsonify(error="'claims' must be a JSON object"), 400
    payload, ok = create_item_sync(label, description, claims, username=username, password=password)
    return jsonify(payload), 200 if ok else 500


@app.post("/update/item")
def update_item():
    """Update an existing Wikibase item's label, description, or claims.

    Accepts a JSON body with the following fields:

    - ``qid`` (required): QID of the item to update.
    - ``username`` (required): Wiki bot username (``User@BotName`` format).
    - ``password`` (required): Wiki bot password.
    - ``label``: New English label.
    - ``description``: New English description.
    - ``claims``: Mapping of property IDs to value or list of values.
    - ``do_override``: If true, existing claim values are replaced. If false
      (default) and a property already has values, the request is refused
      with HTTP 409 and the existing values are returned.

    Returns:
        Flask response tuple with update status.
    """
    data = request.get_json(silent=True) or {}

    username = data.get("username")
    password = data.get("password")
    if not isinstance(username, str) or not username.strip():
        return jsonify(error="'username' is required"), 401
    if not isinstance(password, str) or not password.strip():
        return jsonify(error="'password' is required"), 401

    qid = data.get("qid")
    if not isinstance(qid, str) or not qid:
        return jsonify(error="'qid' must be a non-empty string"), 400

    label = data.get("label")
    if label is not None and (not isinstance(label, str) or not label):
        return jsonify(error="'label' must be a non-empty string"), 400
    description = data.get("description")
    if description is not None and (not isinstance(description, str) or not description):
        return jsonify(error="'description' must be a non-empty string"), 400
    claims = data.get("claims", {})
    if claims is None:
        claims = {}
    if not isinstance(claims, dict):
        return jsonify(error="'claims' must be a JSON object"), 400

    raw_override = data.get("do_override", False)
    if isinstance(raw_override, bool):
        do_override = raw_override
    elif isinstance(raw_override, str):
        do_override = raw_override.lower() == "true"
    elif isinstance(raw_override, int):
        do_override = raw_override == 1
    else:
        do_override = False

    if label is None and description is None and not claims:
        return jsonify(error="at least one of label, description, or claims must be provided"), 400

    payload, ok = update_item_sync(
        qid,
        label=label,
        description=description,
        claims=claims,
        do_override=do_override,
        username=username,
        password=password,
    )
    if not ok:
        if payload.get("status") == "conflict":
            return jsonify(payload), 409
        if payload.get("status") == "not_found":
            return jsonify(payload), 404
        return jsonify(payload), 500
    return jsonify(payload), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
