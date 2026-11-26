"""Prefect flow that imports new OpenML datasets into the MaRDI portal.

Steps, checkpointed like ``zbmath_import``:

1. ``download_openml``  — every active OpenML dataset to ``openml_<date>.jsonl``
2. ``push_openml``      — an item for every dataset the portal does not have

Datasets that already have an item are left to ``openml_update``. A rerun
after an interruption skips the steps done, continues an unfinished download,
and continues the push after the last dataset it handled.
"""

import glob
import os
from datetime import date
from typing import Optional

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from prefect.states import Completed

from mardi_importer import Importer
from mardi_portal.flows.checkpoint import Checkpoint

DATA_DIR = os.getenv("OPENML_DATA_DIR", "/mnt/openml-data")
CHECKPOINT_FILE = os.path.join(DATA_DIR, "openml_import_checkpoint.json")
STEPS = ["download_openml", "push_openml"]


def download_path(data_dir: str) -> str:
    """The download to continue (an unfinished one), else today's."""
    unfinished = sorted(glob.glob(os.path.join(data_dir, "openml_*.jsonl.part")))
    if unfinished:
        return unfinished[-1][:-len(".part")]
    return os.path.join(data_dir, f"openml_{date.today().isoformat()}.jsonl")


@task(name="download_openml", retries=1, retry_delay_seconds=300)
def download_openml(data_dir: str) -> str:
    """Download every active OpenML dataset; returns the download file."""
    os.makedirs(data_dir, exist_ok=True)
    return Importer.create_source("openml").pull(download_path(data_dir))


@task(name="push_openml", retries=1, retry_delay_seconds=120)
def push_openml(path: str, max_rows: Optional[int]) -> dict:
    """Create items for the new datasets in ``path``."""
    log = get_run_logger()
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    progress = checkpoint.progress("push_openml")
    resume = progress["last_id"] if progress else None
    if resume:
        log.info("Resuming push_openml after OpenML dataset %s", resume)
    stats = Importer.create_source("openml").push(
        path,
        resume_after_id=resume,
        progress_callback=lambda did: checkpoint.save_progress("push_openml", {"last_id": did}),
        max_rows=max_rows,
    )
    log.info("push_openml finished: %s", stats)
    return stats


@flow(name="openml-import", log_prints=True)
def openml_import_flow(max_records: Optional[int] = None):
    """Import OpenML datasets that are not in the portal yet.

    Args:
        max_records: create at most this many items, then end the run and
            keep the checkpoint, so the result can be checked before the next
            run continues. ``None`` (the default) does everything.
    """
    log = get_run_logger()
    log.info("OpenML import flow run: %s", get_run_context().flow_run.id)
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    if checkpoint.start(str(get_run_context().flow_run.id)):
        log.info("Resuming from checkpoint (started %s, steps done: %s)",
                 checkpoint.data.get("started_at"), [s for s in STEPS if checkpoint.done(s)])

    # ── Step 1: download ─────────────────────────────────────────────────
    if checkpoint.done("download_openml"):
        path = checkpoint.outputs["download_path"]
        log.info("Skipping download_openml (already done): %s", path)
    else:
        path = download_openml(DATA_DIR)
        checkpoint.mark("download_openml", {"download_path": path})

    # ── Step 2: push ─────────────────────────────────────────────────────
    stats = {"failed": 0}
    if checkpoint.done("push_openml"):
        log.info("Skipping push_openml (already done)")
    else:
        stats = push_openml(path, max_records)
        checkpoint = Checkpoint(CHECKPOINT_FILE)     # the task saved its progress
        if stats["stopped_early"]:
            message = (f"Stopped after {max_records} new item(s). Check them, then run again "
                       "with the same max_records for the next batch or without it for the rest.")
            log.warning(message)
            return Completed(message=message)
        checkpoint.mark("push_openml")

    # ── Done: archive the checkpoint so the next run starts fresh ────────
    archive = checkpoint.archive()
    log.info("OpenML import complete; checkpoint archived to %s", archive)
    if stats["failed"]:
        raise RuntimeError(f"{stats['failed']} dataset(s) could not be created; see the log")
    return stats
