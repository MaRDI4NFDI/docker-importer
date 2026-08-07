import os
import re
from enum import Enum
from typing import List, Dict, Any, Optional

from prefect import flow, task, get_run_logger
from mardi_importer.wikidata import WikidataImporter

from mardi_importer.mardi_importer import Importer
from prefect.artifacts import Artifact
from prefect.blocks.system import Secret
from prefect.context import get_run_context

from services.version import get_version


class ImportAction(str, Enum):
    """Supported import actions.

    Inherits from ``str`` so that members compare equal to their values and
    serialise as plain strings over the Prefect API. Prefect builds the
    deployment's parameter schema from this, so unknown actions are rejected
    when the flow run is created rather than after a pod has started.
    """

    IMPORT_WIKIDATA = "import/wikidata"
    UPDATE_WIKIDATA = "update/wikidata"
    IMPORT_DOI = "import/doi"


@task(retries=1, retry_delay_seconds=30)
def import_doi_batch(dois: List[str]) -> Dict[str, Any]:

    log = get_run_logger()
    log.info("Starting batch import for DOIs: %s", ", ".join(dois))

    results = {}
    all_ok = True
    log.debug("Registered sources: %s", ", ".join(Importer._sources.keys()))

    # The following "create_source" calls trigger the respective source setups,
    # which imports required Wikidata entities into the local Wikibase.
    # See e.g. ArxivSource.py -> setup()
    # Note: This can take a while.

    log.debug("Creating source handler arxiv")
    arxiv = Importer.create_source("arxiv")

    log.debug("Creating source handler zenode")
    zenodo = Importer.create_source("zenodo")

    log.debug("Creating source handler crossref")
    crossref = Importer.create_source("crossref")

    log.debug("Creating source handlers done")

    for doi in dois:
        log.info(f"Importing for doi {doi}")
        try:
            doi_upper = doi.upper()
            if "ARXIV" in doi_upper:
                log.debug("trying to import from arxiv")
                match = re.search(r"(?i)arxiv[.:](.+)", doi)
                if not match:
                    raise ValueError(f"Unsupported arXiv DOI format: {doi}")
                arxiv_id = match.group(1).strip()
                publication = arxiv.new_publication(arxiv_id)
                log.info("arxiv recognized")
            elif "ZENODO" in doi_upper:
                log.debug("trying to import from zenodo")
                zenodo_id = doi.split(".")[-1]
                publication = zenodo.new_resource(zenodo_id)
                log.info("zenodo recognized")
            else:
                log.warning(
                    f"did not recognize 'ARXIV' or 'ZENODO' in doi {doi_upper}, trying crossref"
                )
                publication = crossref.new_publication(doi)
                log.info("crossref recognized")

            # Try to actually create the wiki item
            result = publication.create()

            if result:
                log.info(f"Imported item {result} for doi {doi}.")
                results[doi] = {"qid": result, "status": "success"}
            else:
                log.info(f"doi {doi} was not found, not imported.")
                results[doi] = {
                    "qid": None,
                    "status": "not_found",
                    "error": "DOI was not found.",
                }
                all_ok = False
        except Exception as e:
            log.error("importing doi failed: %s", e, exc_info=True)
            results[doi] = {"qid": None, "status": "error", "error": str(e)}
            all_ok = False

    result = {
        "dois": dois,
        "count": len(dois),
        "results": results,
        "all_imported": all_ok,
    }

    return result

@task(retries=1, retry_delay_seconds=30)
def update_wikidata_batch(qids: List[str]) -> Dict[str, Any]:
    log = get_run_logger()

    wdi = WikidataImporter()
    results: Dict[str, Any] = {}
    all_ok = True

    log.info("Starting batch update for Wikidata items: %s", ", ".join(qids))

    for q in qids:
        try:
            updated_q = wdi.update_entities(q, timeout=300)

            if not updated_q:
                log.info("No update for wikidata qid %s", q)
                status = "not_updated"
                ok = False
            else:
                log.info("Update for wikidata qid %s: %s", q, updated_q)
                status = "success"
                ok = True

            results[q] = {
                "qid": updated_q,
                "status": status,
            }

            if not ok:
                all_ok = False

        except Exception as e:
            log.error("Updating wikidata failed: %s", e, exc_info=True)
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

@task(retries=1, retry_delay_seconds=30)
def import_wikidata_batch(qids: List[str]) -> Dict[str, Any]:
    log = get_run_logger()

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
def mardi_importer_flow(
    action: ImportAction,
    qids: Optional[List[str]] = None,
    dois: Optional[List[str]] = None,
) -> Dict[str, Any]:
    log = get_run_logger()
    qids: List[str] = qids or []
    dois: List[str] = dois or []

    log.info("version: %s", get_version())
    action = ImportAction(action)

    log.info(
        "Flow triggered with action=%s qids_count=%d dois_count=%d",
        action.value,
        len(qids),
        len(dois),
    )

    ctx = get_run_context()
    flow_run_id = str(ctx.flow_run.id)

    if action is ImportAction.IMPORT_WIKIDATA:
        if not qids:
            raise ValueError("missing qids")
        result = import_wikidata_batch(qids)
    elif action is ImportAction.UPDATE_WIKIDATA:
        if not qids:
            raise ValueError("missing qids")
        result = update_wikidata_batch(qids)
    elif action is ImportAction.IMPORT_DOI:
        if not dois:
            raise ValueError("missing dois")

        result = import_doi_batch(dois)

    else:
        raise ValueError(f"Unsupported action: {action}")

    # Store structured JSON in the artifact "data" field (unique per run)
    artifact = Artifact(
        type="json",
        key=f"mardi-importer-result-{flow_run_id}",
        description=f"Importer result for action={action.value} flow_run_id={flow_run_id}",
        data={
            "action": action.value,
            **result,
        },
    ).create()  # type: ignore[call-arg]  # false positive: async_dispatch ParamSpec wrapper

    response_payload = {
        "action": action.value,
        **result,
        "artifact_id": str(artifact.id),
        "artifact_key": artifact.key,
        "flow_run_id": flow_run_id,
    }

    # Mark flow failed if not all items have been imported
    if not result.get("all_imported", True):
        log.error("Batch processing contained errors. Marking flow as failed.")
        raise RuntimeError(f"Partial import failure detected. See artifact: {artifact.key}")

    # Return pointers for later programmatic retrieval
    return response_payload
