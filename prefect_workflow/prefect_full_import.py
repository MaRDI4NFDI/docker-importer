import json
import os
from datetime import datetime, timezone
 
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
 
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/mnt/workflow-data")
TEST_FILE = os.path.join(CHECKPOINT_DIR, "test_persistence.json")
 
 
@task(name="write_test")
def write() -> str:
    log = get_run_logger()
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
 
    data = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "message": "Hello from the persistent volume!",
    }
 
    tmp = TEST_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, TEST_FILE)
 
    log.info("Wrote test file to %s: %s", TEST_FILE, data)
    return "write_test"
 
 
@task(name="read_test")
def read() -> str:
    log = get_run_logger()
 
    if not os.path.exists(TEST_FILE):
        log.warning("Test file does not exist at %s — nothing to read.", TEST_FILE)
        return "read_test"
 
    with open(TEST_FILE, "r") as f:
        data = json.load(f)
 
    log.info("Read test file from %s: %s", TEST_FILE, data)
    log.info("File was written at: %s", data.get("written_at", "unknown"))
    return "read_test"
 
 
@flow(
    name="full-import",
    log_prints=True,
)
def full_import_flow():
    """
    Run 1: read() finds nothing, write() creates the file.
    Run 2: read() finds the file from run 1 → volume persists.
    """
    log = get_run_logger()
    ctx = get_run_context()
    log.info("Flow run: %s", ctx.flow_run.id)
 
    log.info("--- Attempting to read (should fail on first run) ---")
    read()
 
    log.info("--- Writing test file ---")
    write()
 
    log.info("--- Reading back (should always work) ---")
    read()
 
    log.info("Done. Run this flow again — if read() finds the file "
             "at the start, the volume persists between runs.")
