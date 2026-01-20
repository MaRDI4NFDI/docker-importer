import os
from typing import List, Dict, Any, Optional

from prefect import flow, task, get_run_logger
from mardi_importer.wikidata import WikidataImporter
from mardi_importer.importer import Importer
from prefect.artifacts import Artifact
from prefect.blocks.system import Secret
from prefect.context import get_run_context


@task(retries=1, retry_delay_seconds=30)
def import_doi_batch(dois: List[str]) -> Dict[str, Any]:
    # Set needed env variables for Wikidata importer
    os.environ["DB_PASS"] = Secret.load("wikidata-importer-db-password").get()
    os.environ["DB_USER"] = "importer-user"
    os.environ["DB_NAME"] = "wikidata-importer"
    os.environ["DB_HOST"] = "mariadb-primary"

    os.environ["ARXIV_USER"] = "arXiv-Importer"
    os.environ["ARXIV_PASS"] = Secret.load("importer-arxiv-password").get()
    os.environ["ZENODO_USER"] = "Zenodo-Importer"
    os.environ["ZENODO_PASS"] = Secret.load("importer-zenodo-password").get()
    os.environ["CROSSREF_USER"] = "Crossref-Importer"
    os.environ["CROSSREF_PASS"] = Secret.load("importer-crossref-password").get()
    os.environ["WIKIDATA_USER"] = "Wikidata-Importer"
    os.environ["WIKIDATA_PASS"] = Secret.load("wikidata-importer-wiki-password").get()
    os.environ["MEDIAWIKI_API_URL"] = "http://wikibase-apache/w/api.php"
    os.environ["WIKIBASE_URL"] = "http://wikibase-apache"
    os.environ["SPARQL_ENDPOINT_URL"] = "http://wdqs:9999/bigdata/namespace/wdq/sparql"
    os.environ["IMPORTER_API_URL"] = "http://importer-api"

    log = get_run_logger()
    log.info("Starting batch import for DOIs: %s", ", ".join(dois))

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

    result = {
        "dois": dois,
        "count": len(dois),
        "results": results,
        "all_imported": all_ok,
    }

    return result


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
def prefect_mardi_importer_flow(
    action: str,
    qids: Optional[List[str]] = None,
    dois: Optional[List[str]] = None,
) -> Dict[str, Any]:

    log = get_run_logger()
    qids = qids or []
    dois = dois or []

    log.info("Flow triggered with action=%s qids_count=%d dois_count=%d", action, len(qids), len(dois))

    ctx = get_run_context()
    flow_run_id = str(ctx.flow_run.id)

    if action == "import/wikidata":
        if not qids:
            raise ValueError("missing qids")
        result = import_wikidata_batch(qids)

    elif action == "import/doi":
        if not dois:
            raise ValueError("missing dois")

        result = import_doi_batch(dois)

    else:
        raise ValueError(f"Unsupported action: {action}")

    # Store structured JSON in the artifact "data" field (unique per run)
    artifact = Artifact(
        type="json",
        key=f"mardi-importer-result-{flow_run_id}",
        description=f"Importer result for action={action} flow_run_id={flow_run_id}",
        data={
            "action": action,
            **result,
        },
    ).create()

    # Return pointers for later programmatic retrieval
    return {
        "action": action,
        **result,
        "artifact_id": str(artifact.id),
        "artifact_key": artifact.key,
        "flow_run_id": flow_run_id,
    }

