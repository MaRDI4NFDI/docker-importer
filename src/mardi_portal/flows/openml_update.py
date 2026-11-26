"""Prefect flow that updates existing OpenML dataset items in the MaRDI portal.

Where ``openml_import`` adds datasets the portal does not have, this flow
takes a fresh download, compares it with the previous download kept on its
volume, and applies the differences. Steps, checkpointed like ``zbmath_update``:

1. ``find_previous_download`` — newest complete download already on the volume
2. ``download_openml``        — a fresh download from the OpenML API
3. ``check_deletions``        — stop if a field would be emptied for most
                                datasets that had it
4. ``update_items``           — apply the differences to the items
5. ``list_gone_datasets``     — datasets that vanished from OpenML

Datasets that are new in the download are left to the import flow. Datasets
that vanished are listed in a file and never deleted from the portal. The
first run, with no previous download, checks every dataset and only replaces
values the earlier OpenML importer wrote in an outdated form.
"""

import glob
import os
from typing import Optional

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from prefect.states import Completed

from mardi_importer import Importer
from mardi_importer.openml.OpenMLSource import read_records
from mardi_importer.openml.OpenMLUpdate import deletion_report, read_ids
from mardi_portal.flows.checkpoint import Checkpoint
from mardi_portal.flows.openml_import import download_path

DATA_DIR = os.getenv("OPENML_UPDATE_DATA_DIR", "/mnt/openml-update-data")
CHECKPOINT_FILE = os.path.join(DATA_DIR, "openml_update_checkpoint.json")
STEPS = ["find_previous_download", "download_openml", "check_deletions",
         "update_items", "list_gone_datasets"]


def _stem(path: str) -> str:
    return os.path.basename(path)[:-len(".jsonl")]


@task(name="find_previous_download")
def find_previous_download() -> Optional[str]:
    """The newest complete download on the volume, or ``None``."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, "openml_*.jsonl")))
    return files[-1] if files else None


@task(name="download_openml", retries=1, retry_delay_seconds=300)
def download_openml(previous: Optional[str]) -> str:
    """A fresh download, never the previous one again."""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = download_path(DATA_DIR)
    if path == previous:
        raise RuntimeError(f"{path} is already the previous download; run again tomorrow")
    return Importer.create_source("openml").pull(path)


@task(name="check_deletions")
def check_deletions(old_path: Optional[str], new_path: str, max_share: float,
                    min_count: int, allow: bool) -> dict:
    """Refuse to go on if the new download would empty a field almost everywhere.

    That usually means the OpenML API stopped delivering the field, not that
    OpenML removed it from all those datasets.
    """
    log = get_run_logger()
    old = {r["id"]: r for r in read_records(old_path)}
    new = {r["id"]: r for r in read_records(new_path)}
    report = deletion_report(old, new)
    suspicious = {}
    for name, (lost, had) in sorted(report.items()):
        if lost:
            log.info("%s: %d of %d datasets with a value would lose it", name, lost, had)
        if lost >= min_count and lost > max_share * had:
            suspicious[name] = (lost, had)
    if suspicious:
        details = "; ".join(f"{n}: {lost} of {had}" for n, (lost, had) in suspicious.items())
        if allow:
            log.warning("Mass deletions let through (allow_mass_deletions): %s", details)
        else:
            raise RuntimeError(
                "Nothing was written to the portal. The new download would empty these "
                f"fields for most datasets that had them: {details}. Check the download; "
                "if the deletions are real, run again with allow_mass_deletions=True.")
    return {"deletion_report": {n: list(v) for n, v in report.items()}}


@task(name="update_items", retries=1, retry_delay_seconds=120)
def update_items(old_path: Optional[str], new_path: str, max_rows: Optional[int]) -> dict:
    """Apply the differences between the two downloads to the items."""
    log = get_run_logger()
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    progress = checkpoint.progress("update_items")
    resume = progress["last_id"] if progress else None
    if resume:
        log.info("Resuming update_items after OpenML dataset %s", resume)
    stats = Importer.create_source("openml").update_from_downloads(
        old_path, new_path,
        resume_after_id=resume,
        progress_callback=lambda did: checkpoint.save_progress("update_items", {"last_id": did}),
        missing_path=f"{new_path[:-len('.jsonl')]}_missing_in_portal.txt",
        max_rows=max_rows,
    )
    log.info("update_items finished: %s", stats)
    return stats


@task(name="list_gone_datasets")
def list_gone_datasets(old_path: Optional[str], new_path: str) -> dict:
    """Write the datasets that are no longer on OpenML to a file."""
    old = {r["id"] for r in read_records(old_path)}
    new = {r["id"] for r in read_records(new_path)}
    gone_path = f"{new_path[:-len('.jsonl')]}_gone_from_openml.txt"
    with open(gone_path, "w", encoding="utf-8") as f:
        f.writelines(f"{did}\n" for did in sorted(old - new, key=int))
    return {"gone_path": gone_path}


@flow(name="openml-update", log_prints=True)
def openml_update_flow(
    max_records_per_pass: Optional[int] = None,
    allow_mass_deletions: bool = False,
    deletion_alarm_share: float = 0.5,
    deletion_alarm_min: int = 100,
):
    """Update OpenML dataset items from a fresh download.

    Args:
        max_records_per_pass: check at most this many changed datasets, then
            end the run and keep the checkpoint, so the result can be checked
            before the next run continues. ``None`` (the default) does all.
        allow_mass_deletions: let ``check_deletions`` pass even if a field
            would be emptied for most datasets; only after checking by hand.
        deletion_alarm_share: share of the datasets with a value in a field
            above which emptying it counts as suspicious.
        deletion_alarm_min: a field needs at least this many deletions to be
            suspicious.
    """
    log = get_run_logger()
    log.info("OpenML update flow run: %s", get_run_context().flow_run.id)
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    if checkpoint.start(str(get_run_context().flow_run.id)):
        log.info("Resuming from checkpoint (started %s, steps done: %s)",
                 checkpoint.data.get("started_at"), [s for s in STEPS if checkpoint.done(s)])
    out = checkpoint.outputs

    # ── Step 1: the comparison baseline, pinned before the new download ──
    if checkpoint.done("find_previous_download"):
        previous = out["previous_path"]
    else:
        previous = find_previous_download()
        checkpoint.mark("find_previous_download", {"previous_path": previous})
    log.info("Comparison baseline: %s", previous or "none (first update: every dataset is checked)")

    # ── Step 2: fresh download ───────────────────────────────────────────
    if checkpoint.done("download_openml"):
        new_path = out["download_path"]
        log.info("Skipping download_openml (already done): %s", new_path)
    else:
        new_path = download_openml(previous)
        checkpoint.mark("download_openml", {"download_path": new_path})

    # ── Step 3: refuse mass deletions (not marked done when it fails) ────
    if not checkpoint.done("check_deletions"):
        result = check_deletions(previous, new_path, deletion_alarm_share,
                                 deletion_alarm_min, allow_mass_deletions)
        checkpoint.mark("check_deletions", result)

    # ── Step 4: update the items ─────────────────────────────────────────
    stats = {"failed": 0}
    if checkpoint.done("update_items"):
        log.info("Skipping update_items (already done)")
    else:
        stats = update_items(previous, new_path, max_records_per_pass)
        checkpoint = Checkpoint(CHECKPOINT_FILE)     # the task saved its progress
        if stats["stopped_early"]:
            message = (f"Stopped after {max_records_per_pass} dataset(s). Check the items "
                       "listed as 'updated' in the log, then run again with the same "
                       "max_records_per_pass for the next batch or without it for the rest.")
            log.warning(message)
            return Completed(message=message)
        checkpoint.mark("update_items")

    # ── Step 5: datasets gone from OpenML ────────────────────────────────
    if not checkpoint.done("list_gone_datasets"):
        checkpoint.mark("list_gone_datasets", list_gone_datasets(previous, new_path))

    # ── Done: archive the checkpoint; the new download is the next baseline
    archive = checkpoint.archive()
    log.info("OpenML update complete. %s is the next baseline; checkpoint archived to %s",
             new_path, archive)
    missing = read_ids(f"{new_path[:-len('.jsonl')]}_missing_in_portal.txt")
    if missing or stats["failed"]:
        raise RuntimeError(
            f"All steps finished, but {len(missing)} changed dataset(s) have no item in "
            f"the portal (listed in {_stem(new_path)}_missing_in_portal.txt) and "
            f"{stats['failed']} could not be updated; see the log.")
    return stats
