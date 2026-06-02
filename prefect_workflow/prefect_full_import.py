import glob
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Optional

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

from mardi_importer.zbmath.ZBMathSource import ZBMathSource
from mardi_importer.zbmath.misc import split_file, deduplicate_arxiv_file, run_references as run_references_impl
from prefect.blocks.system import Secret



# ── Configuration ────────────────────────────────────────────────────────────

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/mnt/workflow-data")
DATA_DIR = os.getenv("DATA_DIR", CHECKPOINT_DIR)
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, "full_import_checkpoint.json")
TEST_FILE = os.path.join(CHECKPOINT_DIR, "test_persistence.json")

# Pattern for the processed non-arxiv dump files
WO_ARXIV_PATTERN = "wo_arxiv_zbmath_data_dump*.csv"

# Steps in order — used for checkpoint tracking
STEPS = [
    "download_raw_dump",
    "convert_raw_to_processed",
    "split_arxiv_non_arxiv",
    "deduplicate_arxiv",
    "push_zbmath_non_arxiv",
    "push_zbmath_arxiv",
    "run_references_non_arxiv",
    "run_references_arxiv",
    "verify_files",
]


# ── Checkpoint helpers ───────────────────────────────────────────────────────

def _load_checkpoint() -> dict:
    """Load the checkpoint file, returning an empty dict if it doesn't exist."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}


def _save_checkpoint(data: dict) -> None:
    """Atomically write the checkpoint file."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)


def _step_done(checkpoint: dict, step: str) -> bool:
    return checkpoint.get("completed_steps", {}).get(step, False)


def _mark_step(checkpoint: dict, step: str, result: dict | None = None) -> dict:
    """Mark a step as completed and optionally store its outputs."""
    checkpoint.setdefault("completed_steps", {})[step] = True
    if result:
        checkpoint.setdefault("step_outputs", {}).update(result)
    # Clear intra-step progress now that the step is fully done
    checkpoint.get("step_progress", {}).pop(step, None)
    checkpoint["last_updated"] = datetime.now(timezone.utc).isoformat()
    _save_checkpoint(checkpoint)
    return checkpoint

def _save_progress(step: str, data: dict) -> None:
    """Save intra-step progress (e.g. last processed ID)."""
    checkpoint = _load_checkpoint()
    checkpoint.setdefault("step_progress", {})[step] = data
    _save_checkpoint(checkpoint)


def _load_progress(step: str) -> dict | None:
    """Load intra-step progress, or None if no progress saved."""
    checkpoint = _load_checkpoint()
    return checkpoint.get("step_progress", {}).get(step)


# ── Test tasks ──────────────────────────────────────────

@task(name="write_test")
def write_test() -> str:
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
def read_test() -> str:
    log = get_run_logger()

    if not os.path.exists(TEST_FILE):
        log.warning("Test file does not exist at %s — nothing to read.", TEST_FILE)
        return "read_test"

    with open(TEST_FILE, "r") as f:
        data = json.load(f)

    log.info("Read test file from %s: %s", TEST_FILE, data)
    log.info("File was written at: %s", data.get("written_at", "unknown"))
    return "read_test"


# ── Real workflow tasks ──────────────────────────────────────────────────────

@task(name="check_existing_dumps")
def check_existing_dumps() -> Optional[str]:
    """Look for existing wo_arxiv_zbmath_data_dump*.csv files.

    If any are found, read the newest one and return the last de_number.
    Otherwise return None.
    """
    log = get_run_logger()
    pattern = os.path.join(DATA_DIR, WO_ARXIV_PATTERN)
    files = sorted(glob.glob(pattern))

    if not files:
        log.info("No existing dump files found matching %s", pattern)
        return None

    newest = files[-1]
    log.info("Found %d existing dump file(s), newest: %s", len(files), newest)

    # Grab the de_number (first column) from the last line
    last_line = subprocess.check_output(["tail", "-1", newest], text=True).strip()
    last_de = last_line.split("\t")[0] if last_line else None

    if last_de:
        log.info("Last de_number from %s: %s", os.path.basename(newest), last_de)
    else:
        log.warning("File %s appears empty (no data rows)", newest)

    return last_de


@task(name="download_raw_dump", retries=2, retry_delay_seconds=60)
def download_raw_dump(start_after: Optional[str] = None) -> str:
    """Download the raw zbMath data dump via ZBMathSource.write_data_dump.

    Returns the path to the raw dump file.
    """

    log = get_run_logger()

    user = "zbMATH-Importer"
    password = Secret.load("importer-zbmath-password").get()
    source = ZBMathSource(user=user, password=password)
    source.out_dir = DATA_DIR + "/"

    progress = _load_progress("download_raw_dump")
    if progress:
        resume_after = progress["last_id"]
        output_path = progress["raw_dump_path"]
        log.info("Resuming download from last_id=%s into %s", resume_after, output_path)
    else:
        resume_after = int(start_after) if start_after else 0
        output_path = None

    def on_progress(last_id):
        _save_progress("download_raw_dump", {
            "last_id": last_id,
            "raw_dump_path": source.raw_dump_path,
        })

    source.write_data_dump(
        start_after=resume_after,
        output_path=output_path,
        progress_callback=on_progress,
    )

    log.info("Raw dump written to %s", source.raw_dump_path)
    return source.raw_dump_path



@task(name="convert_raw_to_processed")
def convert_raw_to_processed(raw_dump_path: str) -> str:
    """Convert a raw zbMath dump to the processed CSV via ZBMathSource.process_data.

    Returns the path to the processed dump file.
    """

    log = get_run_logger()

    user = "zbMATH-Importer"
    password = Secret.load("importer-zbmath-password").get()
    source = ZBMathSource(user=user, password=password)
    source.out_dir = DATA_DIR + "/"

    progress = _load_progress("convert_raw_to_processed")
    if progress:
        resume_after_de = progress["last_de"]
        source.processed_dump_path = progress["processed_dump_path"]
        log.info("Resuming conversion after de_number=%s", resume_after_de)
    else:
        resume_after_de = None
        source.processed_dump_path = None

    source.raw_dump_path = raw_dump_path

    def on_progress(last_de):
        _save_progress("convert_raw_to_processed", {
            "last_de": last_de,
            "processed_dump_path": source.processed_dump_path,
        })

    source.process_data(
        resume_after_de=resume_after_de,
        progress_callback=on_progress,
    )

    log.info("Processed dump written to %s", source.processed_dump_path)
    return source.processed_dump_path


@task(name="split_arxiv_non_arxiv")
def split_arxiv_non_arxiv(processed_dump_path: str) -> dict:
    """Split the processed dump into arxiv and non-arxiv files.

    TODO: Replace with real implementation.

    Returns dict with keys 'arxiv_path' and 'non_arxiv_path'.
    """
    log = get_run_logger()
    log.info("Splitting %s into arxiv / non-arxiv", processed_dump_path)

    non_arxiv_path, arxiv_path = split_file(processed_dump_path)

    log.info("Split complete: arxiv=%s, non_arxiv=%s", arxiv_path, non_arxiv_path)
    return {"arxiv_path": arxiv_path, "non_arxiv_path": non_arxiv_path}


@task(name="deduplicate_arxiv")
def deduplicate_arxiv(new_arxiv_path: str) -> str:
    """Deduplicate the new arxiv file against the most recent old one on the volume.
 
    If no previous arxiv file exists, returns the new file unchanged.
 
    Returns the path to the deduplicated arxiv file.
    """
    log = get_run_logger()
 
    # Find the newest existing arxiv file (excluding the one we just created)
    pattern = os.path.join(DATA_DIR, "only_arxiv_zbmath_data_dump*.csv")
    old_files = sorted(f for f in glob.glob(pattern) if f != new_arxiv_path)
 
    if not old_files:
        log.info("No previous arxiv file found — skipping deduplication")
        return new_arxiv_path
 
    old_arxiv_path = old_files[-1]
    log.info("Deduplicating %s against %s", new_arxiv_path, old_arxiv_path)

    deduped_path = deduplicate_arxiv_file(old_arxiv_path, new_arxiv_path)
    return deduped_path



@task(name="push_zbmath", retries=1, retry_delay_seconds=120)
def push_zbmath(dump_path: str, label: str = "") -> str:
    """Push a processed dump file to the MaRDI Wikibase via ZBMathSource.

    Args:
        dump_path: Path to the processed CSV (arxiv or non-arxiv).
        label: Human-readable label for logging (e.g. 'non-arxiv', 'arxiv').

    Returns the dump_path on success.
    """
    log = get_run_logger()
    log.info("Pushing zbMath data (%s) from %s", label, dump_path)


    user = "zbMATH-Importer"
    password = Secret.load("importer-zbmath-password").get()

    source = ZBMathSource(user=user, password=password)
    source.processed_dump_path = dump_path
    step_key = f"push_zbmath_{label}"
    progress = _load_progress(step_key)
    resume_after_de = progress["last_de"] if progress else None

    if resume_after_de:
        log.info("Resuming push (%s) after de_number=%s", label, resume_after_de)

    def on_progress(last_de):
        _save_progress(step_key, {"last_de": last_de})

    source.push(
        resume_after_de=resume_after_de,
        progress_callback=on_progress,
    )

    log.info("Push complete (%s): %s", label, dump_path)
    return dump_path



@task(name="run_references")
def run_references(dump_path: str, label: str = "") -> str:
    log = get_run_logger()
    log.info("Running reference pass (%s) for %s", label, dump_path)

    user = "zbMATH-Importer"
    password = Secret.load("importer-zbmath-password").get()
    source = ZBMathSource(user=user, password=password)

    step_key = f"run_references_{label}"
    progress = _load_progress(step_key)
    resume_after_de = progress["last_de"] if progress else None

    if resume_after_de:
        log.info("Resuming references (%s) after de_number=%s", label, resume_after_de)

    def on_progress(last_de):
        _save_progress(step_key, {"last_de": last_de})

    run_references_impl(
        dump_path, source.api, log,
        resume_after_de=resume_after_de,
        progress_callback=on_progress,
    )

    log.info("Reference run complete (%s)", label)
    return "references_done"


@task(name="verify_files")
def verify_files(expected_paths: list[str]) -> bool:
    """Check that all expected output files exist.

    Args:
        expected_paths: List of file paths that should be present.

    Returns True if all files exist, raises otherwise.
    """
    log = get_run_logger()
    missing = [p for p in expected_paths if not os.path.exists(p)]

    if missing:
        for m in missing:
            log.error("MISSING: %s", m)
        raise FileNotFoundError(
            f"{len(missing)} expected file(s) not found: {missing}"
        )

    for p in expected_paths:
        size = os.path.getsize(p)
        log.info("OK: %s (%d bytes)", p, size)

    log.info("All %d expected files verified", len(expected_paths))
    return True


# ── Test flow (preserved) ───────────────────────────────────────────────────

@flow(name="full-import-test", log_prints=True)
def full_import_test_flow():
    """
    Run 1: read_test() finds nothing, write_test() creates the file.
    Run 2: read_test() finds the file from run 1 → volume persists.
    """
    log = get_run_logger()
    ctx = get_run_context()
    log.info("Flow run: %s", ctx.flow_run.id)

    log.info("--- Attempting to read (should fail on first run) ---")
    read_test()

    log.info("--- Writing test file ---")
    write_test()

    log.info("--- Reading back (should always work) ---")
    read_test()

    log.info("Done. Run this flow again — if read_test() finds the file "
             "at the start, the volume persists between runs.")


# ── Main import flow ─────────────────────────────────────────────────────────

@flow(name="full-import", log_prints=True)
def full_import_flow():
    """Full zbMath import pipeline with checkpoint-based resumption.

    Steps:
      1. Check for existing dump files → get last de_number
      2. Download raw dump (incremental if prior data exists)
      3. Convert raw dump to processed CSV
      4. Split into arxiv / non-arxiv files
      5. Deduplicate the arxiv file
      6. Push non-arxiv data to Wikibase
      7. Push arxiv data to Wikibase
      8. Run reference linking for non-arxiv
      9. Run reference linking for arxiv data
      10. Verify all output files

    If the flow is interrupted, re-running it will skip already-completed
    steps based on the checkpoint file at CHECKPOINT_DIR.
    """
    os.environ["WIKIDATA_PASS"]        = Secret.load("wikidata-importer-wiki-password").get()
    os.environ["IMPORTER_DB_PASSWORD"] = Secret.load("wikidata-importer-db-password").get()
    log = get_run_logger()
    ctx = get_run_context()
    log.info("Full import flow run: %s", ctx.flow_run.id)

    checkpoint = _load_checkpoint()
    outputs = checkpoint.get("step_outputs", {})

    # If there is no active run recorded, start a fresh checkpoint
    if not checkpoint.get("flow_run_id"):
        checkpoint = {
            "flow_run_id": str(ctx.flow_run.id),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_steps": {},
            "step_outputs": {},
        }
        _save_checkpoint(checkpoint)
        outputs = {}
    else:
        log.info(
            "Resuming from checkpoint (originally started %s, steps done: %s)",
            checkpoint.get("started_at"),
            [s for s in STEPS if _step_done(checkpoint, s)],
        )

    # ── Step 0: Check existing dumps (always runs, not checkpointed) ─────
    last_de = check_existing_dumps()
    log.info("Last de_number from existing files: %s", last_de)

    # ── Step 1: Download raw dump ────────────────────────────────────────
    if _step_done(checkpoint, "download_raw_dump"):
        raw_path = outputs["raw_dump_path"]
        log.info("Skipping download_raw_dump (already done): %s", raw_path)
    else:
        raw_path = download_raw_dump(start_after=last_de)
        checkpoint = _mark_step(
            checkpoint, "download_raw_dump", {"raw_dump_path": raw_path}
        )

    # ── Step 2: Convert raw → processed ──────────────────────────────────
    if _step_done(checkpoint, "convert_raw_to_processed"):
        processed_path = outputs["processed_dump_path"]
        log.info("Skipping convert_raw_to_processed (already done): %s", processed_path)
    else:
        processed_path = convert_raw_to_processed(raw_path)
        checkpoint = _mark_step(
            checkpoint, "convert_raw_to_processed",
            {"processed_dump_path": processed_path},
        )

    # ── Step 3: Split arxiv / non-arxiv ──────────────────────────────────
    if _step_done(checkpoint, "split_arxiv_non_arxiv"):
        arxiv_path = outputs["arxiv_path"]
        non_arxiv_path = outputs["non_arxiv_path"]
        log.info("Skipping split (already done): arxiv=%s, non_arxiv=%s",
                 arxiv_path, non_arxiv_path)
    else:
        split_result = split_arxiv_non_arxiv(processed_path)
        arxiv_path = split_result["arxiv_path"]
        non_arxiv_path = split_result["non_arxiv_path"]
        checkpoint = _mark_step(
            checkpoint, "split_arxiv_non_arxiv",
            {"arxiv_path": arxiv_path, "non_arxiv_path": non_arxiv_path},
        )

    # ── Step 4: Deduplicate arxiv ────────────────────────────────────────
    if _step_done(checkpoint, "deduplicate_arxiv"):
        deduped_arxiv_path = outputs["deduped_arxiv_path"]
        log.info("Skipping deduplicate_arxiv (already done): %s", deduped_arxiv_path)
    else:
        deduped_arxiv_path = deduplicate_arxiv(arxiv_path)
        checkpoint = _mark_step(
            checkpoint, "deduplicate_arxiv",
            {"deduped_arxiv_path": deduped_arxiv_path},
        )

    # ── Step 5: Push non-arxiv ───────────────────────────────────────────
    if _step_done(checkpoint, "push_zbmath_non_arxiv"):
        log.info("Skipping push_zbmath non-arxiv (already done)")
    else:
        push_zbmath(non_arxiv_path, label="non_arxiv")
        checkpoint = _mark_step(checkpoint, "push_zbmath_non_arxiv")

    # ── Step 6: Push arxiv ───────────────────────────────────────────────
    if _step_done(checkpoint, "push_zbmath_arxiv"):
        log.info("Skipping push_zbmath arxiv (already done)")
    else:
        push_zbmath(deduped_arxiv_path, label="arxiv")
        checkpoint = _mark_step(checkpoint, "push_zbmath_arxiv")

    # ── Step 7: Reference run for non-arxiv ────────────────────────────────────────────
    if _step_done(checkpoint, "run_references_non_arxiv"):
        log.info("Skipping run_references for non-arxiv (already done)")
    else:
        run_references(non_arxiv_path, label="non_arxiv")
        checkpoint = _mark_step(checkpoint, "run_references_non_arxiv")

    # ── Step 8: Reference run for arxiv ────────────────────────────────────────────
    if _step_done(checkpoint, "run_references_arxiv"):
        log.info("Skipping run_references for arxiv (already done)")
    else:
        run_references(deduped_arxiv_path, label="arxiv")
        checkpoint = _mark_step(checkpoint, "run_references_arxiv")

    # ── Step 9: Verify files ─────────────────────────────────────────────
    if _step_done(checkpoint, "verify_files"):
        log.info("Skipping verify_files (already done)")
    else:
        expected = [
            raw_path,
            processed_path,
            arxiv_path,
            non_arxiv_path,
            deduped_arxiv_path,
        ]
        verify_files(expected)
        checkpoint = _mark_step(checkpoint, "verify_files")

    # ── Done — clear checkpoint so the next run starts fresh ─────────────
    checkpoint["finished_at"] = datetime.now(timezone.utc).isoformat()
    _save_checkpoint(checkpoint)

    # Rename checkpoint to archive so next run starts clean
    archive = CHECKPOINT_FILE + f".done.{time.strftime('%Y%m%d-%H%M%S')}"
    os.rename(CHECKPOINT_FILE, archive)
    log.info("Flow complete. Checkpoint archived to %s", archive)