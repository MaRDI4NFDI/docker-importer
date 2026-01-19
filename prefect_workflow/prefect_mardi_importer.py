import os
from typing import List, Dict, Any, Optional

from prefect import flow, task, get_run_logger
from mardi_importer.wikidata import WikidataImporter
from prefect.artifacts import Artifact
from prefect.blocks.system import Secret
from prefect.context import get_run_context


@task(retries=1, retry_delay_seconds=30)
def import_wikidata_batch(qids: List[str]) -> Dict[str, Any]:
    log = get_run_logger()

    # Set needed env variables for Wikidata importer
    os.environ["DB_PASS"] = Secret.load("wikidata-importer-db-password").get()
    os.environ["DB_USER"] = "importer-user"
    os.environ["DB_NAME"] = "wikidata-importer"
    os.environ["DB_HOST"] = "mariadb-primary"

    os.environ["WIKIDATA_USER"] = "Wikidata-Importer"
    os.environ["WIKIDATA_PASS"] = Secret.load("wikidata-importer-wiki-password").get()
    os.environ["MEDIAWIKI_API_URL"] = "http://wikibase-apache/w/api.php"
    os.environ["WIKIBASE_URL"] = "http://wikibase-apache"
    os.environ["SPARQL_ENDPOINT_URL"] = "http://wdqs:9999/bigdata/namespace/wdq/sparql"
    os.environ["IMPORTER_API_URL"] = "http://importer-api"

    wdi = WikidataImporter()
    results: Dict[str, Any] = {}
    all_ok = True

    log.info("Starting batch import for Wikidata items: %s", ", ".join(qids))

    for q in qids:
        try:
            imported_q = wdi.import_entities(q)

            if not imported_q:
                log.info("No import for wikidata qid %s", q)
                status = "not_imported"
                ok = False
            else:
                log.info("Import for wikidata qid %s: %s", q, imported_q)
                status = "success"
                ok = True

            results[q] = {
                "qid": imported_q,
                "status": status,
            }

            if not ok:
                all_ok = False

        except Exception as e:
            log.error("importing wikidata failed: %s", e, exc_info=True)
            results[q] = {
                "qid": None,
                "status": "error",
                "error": str(e),
            }
            all_ok = False

    return {
        "qids": qids,
        "count": len(qids),
        "results": results,
        "all_imported": all_ok,
    }


@flow(name="mardi-importer")
def prefect_mardi_importer_flow(action: str, qids: Optional[List[str]] = None) -> Dict[str, Any]:
    log = get_run_logger()
    qids = qids or []

    log.info("Flow triggered with action=%s qids_count=%d", action, len(qids))

    if action != "import/wikidata":
        raise ValueError(f"Unsupported action: {action}")
    if not qids:
        raise ValueError("missing qids")

    result = import_wikidata_batch(qids)

    ctx = get_run_context()
    flow_run_id = str(ctx.flow_run.id)

    # Store structured JSON in the artifact "data" field
    artifact = Artifact(
        type="json",
        key=f"mardi-importer-result-{flow_run_id}",  # unique per run
        description=f"Wikidata import result for flow_run_id={flow_run_id}",
        data=result,
    ).create()

    # Return pointers for later programmatic retrieval
    return {
        **result,
        "artifact_id": str(artifact.id),
        "artifact_key": artifact.key,
        "flow_run_id": flow_run_id,
    }

