from flask import Flask, request, jsonify
from mardi_importer.wikidata import WikidataImporter
from prefect.deployments import run_deployment

from mardi_importer import Importer
import re

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)

def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in re.split(r'[,\s]+', value) if v.strip()]
    return [str(value).strip()]

@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "mardi-importer-api"
    }), 200

@app.get("/import/wikidata_async")
def import_wikidata_async():

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

@app.post("/import/wikidata")
def import_wikidata():
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
