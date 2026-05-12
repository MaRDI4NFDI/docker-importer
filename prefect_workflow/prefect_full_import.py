"""
Prefect workflow for full source imports with checkpoint/resume.

Design:
  - A JSON checkpoint file on a persistent volume tracks which steps
    have completed.  If the flow is interrupted (error, pod eviction, user
    cancellation) the next run reads the checkpoint and skips finished steps.
  - A Prefect concurrency_limit on the deployment plus the checkpoint
    file prevent overlapping runs from colliding.

Volume contract:
  The flow expects a writable directory at CHECKPOINT_DIR (default
  /mnt/workflow-data).  In Kubernetes this is a PVC mounted into the
  job pod via job_variables defined in prefect_deploy_full_import.py.

Adding new steps:
  1. Write a normal @task function.
  2. Append its name to STEP_ORDER.
  3. Call it inside full_import_flow guarded by should_run(…).
  4. After the call, write mark_done(…).
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Directory on the persistent volume where checkpoint + scratch data live.
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/mnt/workflow-data")
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, "checkpoint.json")

# Ordered list of step names.  The flow iterates over this list;
# steps that are already marked "done" in the checkpoint are skipped.
# To add a new step, give it a unique name and append it here.
STEP_ORDER: List[str] = [
    "zbmath_setup",
    "zbmath_pull",
    "zbmath_push",
]


# ---------------------------------------------------------------------------
# Checkpoint helpers  (pure functions, no Prefect magic)
# ---------------------------------------------------------------------------

def _read_checkpoint() -> Dict[str, Any]:
    """Load the checkpoint from disk, or return an empty state."""
    if not os.path.exists(CHECKPOINT_FILE):
        return {"completed_steps": [], "started_at": None, "last_updated": None}
    with open(CHECKPOINT_FILE, "r") as fh:
        return json.load(fh)


def _write_checkpoint(state: Dict[str, Any]) -> None:
    """Atomically write the checkpoint (write-then-rename)."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(state, fh, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)          # atomic on POSIX


def should_run(step_name: str, state: Dict[str, Any]) -> bool:
    """Return True if *step_name* has NOT been completed yet."""
    return step_name not in state.get("completed_steps", [])


def mark_done(step_name: str, state: Dict[str, Any]) -> Dict[str, Any]:
    """Record *step_name* as finished and persist the checkpoint."""
    state.setdefault("completed_steps", []).append(step_name)
    state["last_updated"] = datetime.now(timezone.utc).isoformat()
    _write_checkpoint(state)
    return state


def reset_checkpoint() -> Dict[str, Any]:
    """Start a fresh checkpoint (called when ALL steps are done)."""
    state = {
        "completed_steps": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    _write_checkpoint(state)
    return state


# ---------------------------------------------------------------------------
# Environment bootstrap (reusable across tasks)
# ---------------------------------------------------------------------------

def _load_env() -> None:
    """
    Populate environment variables needed by mardi_importer.

    In K8s the variables come from Secrets / ConfigMaps injected via
    ``envFrom`` (see the Helm chart).  This function is a safety net for
    Prefect-managed job pods that might not inherit them automatically.
    It only sets a variable if it is not already present.
    """
    from prefect.blocks.system import Secret

    defaults = {
        "IMPORTER_DB_USER": "importer-user",
        "DB_NAME":          "wikidata-importer",
        "DB_HOST":          "mariadb-primary",
        "MEDIAWIKI_API_URL":     "http://wikibase/w/api.php",
        "WIKIBASE_URL":          "http://wikibase",
        "SPARQL_ENDPOINT_URL":   "http://wdqs:9999/bigdata/namespace/wdq/sparql",
        "IMPORTER_API_URL":      "http://importer-api",
    }
    for k, v in defaults.items():
        os.environ.setdefault(k, v)

    # Secrets stored in Prefect Blocks – load once.
    secret_map = {
        "IMPORTER_DB_PASS": "wikidata-importer-db-password",
        "ZBMATH_USER":      "importer-zbmath-user",
        "ZBMATH_PASS":      "importer-zbmath-password",
        "WIKIDATA_USER":    "importer-wikidata-user",
        "WIKIDATA_PASS":    "wikidata-importer-wiki-password",
    }
    for env_var, block_name in secret_map.items():
        if env_var not in os.environ:
            try:
                os.environ[env_var] = Secret.load(block_name).get()
            except Exception:
                pass  # will be picked up from K8s envFrom instead


# ---------------------------------------------------------------------------
# Tasks – one per checkpoint step
# ---------------------------------------------------------------------------

@task(name="zbmath_setup", retries=1, retry_delay_seconds=60)
def zbmath_setup_task() -> str:
    log = get_run_logger()
    _load_env()

    import configparser
    config = configparser.ConfigParser()
    config.read("/config/import_config.config")
    config["DEFAULT"]["output_directory"] = "/mnt/workflow-data/zbmath/"
    with open("/config/import_config.config", "w") as f:
        config.write(f)
    log.info("Updated config to use persistent volume for output.")

    from mardi_importer import Importer
    log.info("Creating zbmath source (triggers setup if first run) …")
    Importer.create_source("zbmath")
    log.info("zbmath setup complete.")
    return "zbmath_setup"


@task(name="zbmath_pull", retries=1, retry_delay_seconds=60)
def zbmath_pull_task() -> str:
    log = get_run_logger()
    _load_env()

    from mardi_importer import Importer
    source = Importer.create_source("zbmath")
    log.info("Pulling zbmath data …")
    source.pull()
    log.info("zbmath pull complete.")
    return "zbmath_pull"


@task(name="zbmath_push", retries=2, retry_delay_seconds=120)
def zbmath_push_task() -> str:
    log = get_run_logger()
    _load_env()

    from mardi_importer import Importer
    source = Importer.create_source("zbmath")
    log.info("Pushing zbmath data …")
    source.push()
    log.info("zbmath push complete.")
    return "zbmath_push"


@task(name="zbmath_get_last_id", retries=1, retry_delay_seconds=60)
def zbmath_get_last_id_task() -> str:
    """Find the last zbmath DE number from the previous dump so the
    next pull can continue from that point instead of re-fetching
    everything."""
    log = get_run_logger()
    _load_env()
 
    os.makedirs(ZBMATH_DATA_DIR, exist_ok=True)
 
    # TODO: Scan ZBMATH_DATA_DIR for the most recent raw dump file,
    #       read its last line / last DE number, and write it to
    #       a well-known location (e.g. ZBMATH_DATA_DIR/last_id.txt)
    #       or into the checkpoint state so zbmath_pull can pick it up.
    #
    # Example:
    #   dump_files = sorted(Path(ZBMATH_DATA_DIR).glob("raw_zbmath_data_dump*.txt"))
    #   if dump_files:
    #       last_line = dump_files[-1].read_text().strip().split("\n")[-1]
    #       last_id = last_line.split("\t")[0]  # DE number is first column
    #       (Path(ZBMATH_DATA_DIR) / "last_id.txt").write_text(last_id)
    #       log.info("Last ID from previous dump: %s", last_id)
    #   else:
    #       log.info("No previous dump found — will pull from scratch.")
 
    log.info("zbmath_get_last_id complete.")
    return "zbmath_get_last_id"
 
 
@task(name="zbmath_split_arxiv", retries=1, retry_delay_seconds=60)
def zbmath_split_arxiv_task() -> str:
    """Split the processed dump into two files: one containing arXiv
    publications and one containing everything else."""
    log = get_run_logger()
    _load_env()
 
    # TODO: Read the processed dump CSV from ZBMATH_DATA_DIR,
    #       partition rows into arxiv vs non-arxiv based on the
    #       zbl_id prefix or presence of an arxiv link, and write
    #       two separate files.
    #
    # Example:
    #   processed = Path(ZBMATH_DATA_DIR) / "latest_processed.csv"
    #   arxiv_out = Path(ZBMATH_DATA_DIR) / "arxiv_dump.csv"
    #   non_arxiv_out = Path(ZBMATH_DATA_DIR) / "non_arxiv_dump.csv"
    #
    #   with open(processed) as infile:
    #       header = infile.readline()
    #       with open(arxiv_out, "w") as af, open(non_arxiv_out, "w") as nf:
    #           af.write(header)
    #           nf.write(header)
    #           for line in infile:
    #               if is_arxiv(line):  # e.g. check zbl_id contains "arXiv:"
    #                   af.write(line)
    #               else:
    #                   nf.write(line)
    #
    #   log.info("Split into %s and %s", arxiv_out, non_arxiv_out)
 
    log.info("zbmath_split_arxiv complete.")
    return "zbmath_split_arxiv"
 
 
@task(name="zbmath_check_arxiv_existing", retries=1, retry_delay_seconds=60)
def zbmath_check_arxiv_existing_task() -> str:
    """Check which entries from the arXiv dump already exist in the
    local Wikibase, so we only push new / changed ones."""
    log = get_run_logger()
    _load_env()
 
    # TODO: Read the arxiv dump from ZBMATH_DATA_DIR, query the
    #       local Wikibase / SPARQL endpoint for each arxiv ID,
    #       and produce a filtered file with only the new entries.
    #
    # Example:
    #   arxiv_dump = Path(ZBMATH_DATA_DIR) / "arxiv_dump.csv"
    #   new_arxiv = Path(ZBMATH_DATA_DIR) / "arxiv_new.csv"
    #
    #   from mardi_importer import Importer
    #   source = Importer.create_source("zbmath")
    #
    #   with open(arxiv_dump) as infile, open(new_arxiv, "w") as outfile:
    #       header = infile.readline()
    #       outfile.write(header)
    #       for line in infile:
    #           arxiv_id = extract_arxiv_id(line)
    #           if not source.arxiv_exists(arxiv_id):
    #               outfile.write(line)
    #
    #   log.info("Filtered arxiv dump to new entries only.")
 
    log.info("zbmath_check_arxiv_existing complete.")
    return "zbmath_check_arxiv_existing"
 
 
@task(name="zbmath_reference_run", retries=2, retry_delay_seconds=120)
def zbmath_reference_run_task() -> str:
    """Run a reference-linking pass over the imported publications,
    resolving zbmath reference IDs to local Wikibase QIDs."""
    log = get_run_logger()
    _load_env()
 
    # TODO: Iterate over the pushed publications and resolve their
    #       reference lists (the "references" column contains
    #       semicolon-separated DE numbers) into links between
    #       local Wikibase items.
    #
    # Example:
    #   from mardi_importer import Importer
    #   source = Importer.create_source("zbmath")
    #   processed = Path(ZBMATH_DATA_DIR) / "latest_processed.csv"
    #
    #   for publication in read_publications(processed):
    #       for ref_de_number in publication["references"].split(";"):
    #           ref_qid = source.api.search_entity_by_value("P1451", ref_de_number)
    #           if ref_qid:
    #               # add "cites work" claim linking publication → ref_qid
    #               pass
    #
    #   log.info("Reference linking complete.")
 
    log.info("zbmath_reference_run complete.")
    return "zbmath_reference_run"



# ---------------------------------------------------------------------------
# Mapping: step name  →  task callable
# ---------------------------------------------------------------------------
# Keep this dict in sync with STEP_ORDER.

STEP_TASKS = {
    "zbmath_setup": zbmath_setup_task,
    "zbmath_pull":  zbmath_pull_task,
    "zbmath_push":  zbmath_push_task,
    # "my_custom_script": my_custom_script_task,
}


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="full-import",
    log_prints=True,
    # Timeout the entire flow after 24 h – adjust to your needs.
    timeout_seconds=86_400,
)
def full_import_flow() -> Dict[str, Any]:
    """
    Run every step in STEP_ORDER, skipping those already recorded as done
    in the checkpoint file on the persistent volume.

    When all steps finish, the checkpoint is reset so the next scheduled
    run starts from scratch.
    """
    log = get_run_logger()
    ctx = get_run_context()
    flow_run_id = str(ctx.flow_run.id)

    # ── 1. Load checkpoint ──────────────────────────────────────────────
    state = _read_checkpoint()
    completed = state.get("completed_steps", [])
    log.info(
        "Checkpoint loaded.  Already completed: %s",
        completed if completed else "(none – fresh run)",
    )

    if not state.get("started_at"):
        state["started_at"] = datetime.now(timezone.utc).isoformat()
        _write_checkpoint(state)

    # ── 2. Walk through steps ───────────────────────────────────────────
    results: Dict[str, str] = {}

    for step_name in STEP_ORDER:
        if not should_run(step_name, state):
            log.info("⏭  Skipping '%s' (already done)", step_name)
            results[step_name] = "skipped"
            continue

        task_fn = STEP_TASKS.get(step_name)
        if task_fn is None:
            raise ValueError(
                f"Step '{step_name}' is listed in STEP_ORDER but has no "
                f"entry in STEP_TASKS.  Add the mapping."
            )

        log.info("▶  Running step '%s' …", step_name)
        task_fn()                         # exceptions propagate → flow fails
        state = mark_done(step_name, state)
        log.info("✔  Step '%s' done and checkpointed.", step_name)
        results[step_name] = "completed"

    # ── 3. All done → reset for next scheduled run ──────────────────────
    log.info("All steps finished.  Resetting checkpoint for next run.")
    reset_checkpoint()

    return {
        "flow_run_id": flow_run_id,
        "steps": results,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
