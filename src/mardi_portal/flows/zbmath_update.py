"""Prefect flow that updates existing zbMATH data in the MaRDI Wikibase.

Where ``zbmath_import`` adds records it has not seen before, this flow takes a
fresh full dump, compares it against the last full dump kept on its volume, and
applies the differences:

1. ``find_previous_dump``       — newest processed dump already on the volume
2. ``download_raw_dump``        — a fresh full dump from the zbMATH API
3. ``convert_raw_to_processed`` — raw dump -> processed CSV
4. ``hash_dumps``               — de_number/hash tables for both dumps
5. ``build_diff``               — per-field differences for changed records
6. ``update_non_arxiv``         — apply the diff to publication items
7. ``update_arxiv``             — apply the diff to arXiv items
8. ``update_references``        — merge changed reference lists
9. ``list_deleted_records``     — records that vanished from zbMATH
10. ``verify_files``            — every expected artefact is on the volume

Every step is checkpointed the same way ``zbmath_import`` checkpoints its
steps, and the long-running steps additionally record a de_number so an
interrupted run resumes mid-step instead of starting the step over.

Records that appeared since the previous dump are not handled here at all:
they get a de_number above the import flow's watermark, and that flow
downloads everything past its watermark, so it picks them up on its own run.
Records that disappeared are written to a list on the volume and never deleted
from the portal, because whether they really should go is a separate decision
taken later against that list.

A changed record whose item is not in the portal does not stop its step: the
row is set aside in a ``missing_in_portal_*`` file (diff format) and the step
carries on. Once every step is done and the checkpoint is archived, the run is
marked failed if any such file has rows, so it shows up without blocking the
records after it or the next scheduled run.

Checking a sample first
-----------------------
Run with ``max_records_per_pass=20`` and each update pass applies only the
next 20 records of its kind, then the run ends ("Completed", with a message
saying so) and keeps its checkpoint. Run again with the same value for the next
20, or without it to do the rest; nothing is downloaded or diffed again.
``max_records_per_pass=0`` stops right after the diff, before anything is
written to the portal.
"""

import glob
import json
import os
import shutil
import time
from datetime import datetime, timezone
from typing import Optional

from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from prefect.states import Completed

from mardi_importer import Importer
from mardi_importer.zbmath.diff import (
    build_deleted as build_deleted_impl,
    build_diff as build_diff_impl,
    hash_dump as hash_dump_impl,
    hash_path_for,
)
from mardi_importer.zbmath.ZBMathUpdate import count_rows


# ── Configuration ────────────────────────────────────────────────────────────

CHECKPOINT_DIR = os.getenv("UPDATE_CHECKPOINT_DIR", "/mnt/update-data")
DATA_DIR = os.getenv("UPDATE_DATA_DIR", CHECKPOINT_DIR)
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, "update_checkpoint.json")

#: Processed dumps this flow writes and compares.
PROCESSED_PATTERN = "zbmath_data_dump*.csv"

STEPS = [
    "find_previous_dump",
    "download_raw_dump",
    "convert_raw_to_processed",
    "hash_dumps",
    "build_diff",
    "update_non_arxiv",
    "update_arxiv",
    "update_references",
    "list_deleted_records",
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
    # Intra-step progress is written by the tasks straight to the file, so the
    # file is authoritative for it: taking the flow's in-memory copy would
    # throw away the position of a pass that is not finished yet (a sample run
    # leaves passes unfinished and then marks later steps done).
    checkpoint["step_progress"] = _load_checkpoint().get("step_progress", {})
    # Clear intra-step progress now that the step is fully done
    checkpoint["step_progress"].pop(step, None)
    checkpoint["last_updated"] = datetime.now(timezone.utc).isoformat()
    _save_checkpoint(checkpoint)
    return checkpoint


def _save_progress(step: str, data: dict) -> None:
    """Save intra-step progress (e.g. last processed de_number)."""
    checkpoint = _load_checkpoint()
    checkpoint.setdefault("step_progress", {})[step] = data
    _save_checkpoint(checkpoint)


def _load_progress(step: str) -> dict | None:
    """Load intra-step progress, or None if no progress saved."""
    checkpoint = _load_checkpoint()
    return checkpoint.get("step_progress", {}).get(step)


def _source(log):
    """Create the zbMATH source, which also registers the API for the
    author/journal/publication classes to pick up via ``Importer.get_api``."""
    source = Importer.create_source("zbmath")
    source.out_dir = DATA_DIR + "/"
    log.debug("zbmath source ready, writing to %s", source.out_dir)
    return source


# ── Tasks ────────────────────────────────────────────────────────────────────

@task(name="find_previous_dump")
def find_previous_dump(exclude: Optional[str] = None) -> Optional[str]:
    """Newest processed dump already on the volume, if any.

    Scans newest to oldest and returns the first file that has data rows, so a
    truncated or header-only leftover does not become the comparison baseline.
    """
    log = get_run_logger()
    pattern = os.path.join(DATA_DIR, PROCESSED_PATTERN)
    candidates = sorted(
        f for f in glob.glob(pattern)
        if f != exclude
    )
    if not candidates:
        log.info("No previous processed dump found matching %s", pattern)
        return None

    for path in reversed(candidates):
        with open(path, "r", encoding="utf-8") as f:
            f.readline()  # header
            has_rows = bool(f.readline().strip())
        if has_rows:
            log.info("Using %s as the comparison baseline", os.path.basename(path))
            return path
        log.warning("Skipping %s — no data rows", os.path.basename(path))

    log.warning("All %d previous dump(s) are header-only", len(candidates))
    return None


@task(name="download_raw_dump", retries=1, retry_delay_seconds=60)
def download_raw_dump() -> str:
    """Download a full raw zbMATH dump. Resumes from the last id on retry."""
    log = get_run_logger()
    source = _source(log)

    progress = _load_progress("download_raw_dump")
    if progress:
        resume_after = progress["last_id"]
        output_path = progress["raw_dump_path"]
        log.info("Resuming download from last_id=%s into %s", resume_after, output_path)
    else:
        resume_after = 0
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
    """Convert the raw dump to the processed CSV the comparison works on."""
    log = get_run_logger()
    source = _source(log)

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


@task(name="hash_dumps")
def hash_dumps(old_dump: str, new_dump: str) -> dict:
    """Build the de_number/hash tables the comparison is driven by.

    The old dump's table is reused when a previous run already wrote it.
    """
    log = get_run_logger()
    result = {}
    for label, path in (("old", old_dump), ("new", new_dump)):
        hash_path = hash_path_for(path)
        if label == "old" and os.path.exists(hash_path):
            log.info("Reusing existing hash file %s", os.path.basename(hash_path))
        else:
            log.info("Hashing %s", os.path.basename(path))
            hash_dump_impl(path, hash_path=hash_path)
        result[f"{label}_hash_path"] = hash_path
    return result


@task(name="build_diff")
def build_diff(
    old_dump: str, new_dump: str, old_hash: str, new_hash: str
) -> dict:
    """Write the per-field differences for records present in both dumps."""
    log = get_run_logger()
    stem = f"{_stem(old_dump)}_TO_{_stem(new_dump)}"
    output = os.path.join(DATA_DIR, f"differences_{stem}.csv")

    log.info("Comparing %s against %s", os.path.basename(old_dump),
             os.path.basename(new_dump))
    path, rows = build_diff_impl(
        old_data_file=old_dump,
        new_data_file=new_dump,
        old_hash_file=old_hash,
        new_hash_file=new_hash,
        output_file=output,
    )
    log.info("%d changed record(s) written to %s", rows, path)
    return {"diff_path": path, "diff_rows": rows}


@task(name="update_items", retries=1, retry_delay_seconds=120)
def update_items(diff_path: str, mode: str, max_rows: Optional[int] = None) -> dict:
    """Apply the diff to the portal for one kind of item.

    Args:
        diff_path: The diff file.
        mode: ``"non_arxiv"`` (publication items) or ``"arxiv"``.
        max_rows: Apply at most this many records, then stop (see the flow's
            ``max_records_per_pass``).
    """
    log = get_run_logger()
    if not _has_rows(diff_path):
        log.info("Diff file has no rows; nothing to update")
        return {"items_written": 0}

    source = _source(log)
    step_key = f"update_{mode}"
    progress = _load_progress(step_key)
    resume_after_de = progress["last_de"] if progress else None
    if resume_after_de:
        log.info("Resuming %s after de_number=%s", step_key, resume_after_de)

    def on_progress(last_de):
        _save_progress(step_key, {"last_de": last_de})

    missing_path = _missing_path(diff_path, mode)
    stats = source.update_from_diff(
        diff_path,
        mode=mode,
        logger=log,
        resume_after_de=resume_after_de,
        progress_callback=on_progress,
        missing_path=missing_path,
        max_rows=max_rows,
    )
    log.info("%s finished: %s", step_key, stats)
    _log_missing(log, step_key, missing_path)
    return stats


@task(name="update_references")
def update_references(diff_path: str, max_rows: Optional[int] = None) -> dict:
    """Merge changed reference lists onto the publication items."""
    log = get_run_logger()
    if not _has_rows(diff_path):
        log.info("Diff file has no rows; no references to update")
        return {"items_written": 0}

    source = _source(log)
    progress = _load_progress("update_references")
    resume_after_de = progress["last_de"] if progress else None
    if resume_after_de:
        log.info("Resuming reference update after de_number=%s", resume_after_de)

    def on_progress(last_de):
        _save_progress("update_references", {"last_de": last_de})

    missing_path = _missing_path(diff_path, "references")
    stats = source.update_references_from_diff(
        diff_path,
        logger=log,
        resume_after_de=resume_after_de,
        progress_callback=on_progress,
        missing_path=missing_path,
        max_rows=max_rows,
    )
    log.info("Reference update finished: %s", stats)
    _log_missing(log, "update_references", missing_path)
    return stats


@task(name="list_deleted_records")
def list_deleted_records(old_hash: str, new_hash: str, stem: str) -> dict:
    """Work out which de_numbers vanished from zbMATH, write them, report them.

    Nothing is deleted in the portal: removing an item cannot be undone and
    other items may reference it, so the list is left on the volume for a
    human to act on (``bot_scripts/delete_references.py`` is the existing tool
    for that).
    """
    log = get_run_logger()
    deleted_path = os.path.join(DATA_DIR, f"deleted_{stem}.tsv")
    deleted_path, n_deleted = build_deleted_impl(
        old_hash_file=old_hash,
        new_hash_file=new_hash,
        deleted_file=deleted_path,
    )

    if not n_deleted:
        log.info("No records disappeared from zbMATH")
    else:
        log.warning(
            "%d record(s) are no longer in zbMATH. Nothing was deleted in the "
            "portal; the de_numbers are listed in %s", n_deleted, deleted_path,
        )
    return {"deleted_path": deleted_path, "n_deleted": n_deleted}


@task(name="verify_files")
def verify_files(expected_paths: list[str]) -> bool:
    """Check that all expected output files exist."""
    log = get_run_logger()
    present = [p for p in expected_paths if p]
    missing = [p for p in present if not os.path.exists(p)]

    if missing:
        for m in missing:
            log.error("MISSING: %s", m)
        raise FileNotFoundError(
            f"{len(missing)} expected file(s) not found: {missing}"
        )

    for p in present:
        log.info("OK: %s (%d bytes)", p, os.path.getsize(p))

    log.info("All %d expected files verified", len(present))
    return True


# ── small helpers ────────────────────────────────────────────────────────────

def _stem(path: str) -> str:
    """Short identifier for a dump, used to name the derived files."""
    name = os.path.basename(path)
    for suffix in (".csv", ".txt", ".tsv"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name


#: The passes that can set rows aside, keyed by the name in the file name.
MISSING_KINDS = ("non_arxiv", "arxiv", "references")


def _missing_path(diff_path: str, kind: str) -> str:
    """Where a pass sets aside the rows whose item is not in the portal.

    Named after the diff, so the files of one comparison belong together and a
    resumed run appends to the same file.
    """
    name = os.path.basename(diff_path)
    if name.startswith("differences_"):
        name = name[len("differences_"):]
    return os.path.join(os.path.dirname(diff_path), f"missing_in_portal_{kind}_{name}")


def _log_missing(log, step: str, missing_path: str) -> None:
    n = count_rows(missing_path)
    if n:
        log.warning(
            "%s: %d changed record(s) have no item in the portal and were not "
            "applied; they are in %s", step, n, missing_path,
        )


def _has_rows(path: str) -> bool:
    if not path or not os.path.exists(path):
        return False
    with open(path, "r", encoding="utf-8") as f:
        f.readline()
        return bool(f.readline().strip())


# ── Main update flow ─────────────────────────────────────────────────────────

@flow(name="zbmath-update", log_prints=True)
def zbmath_update_flow(max_records_per_pass: Optional[int] = None):
    """Update existing zbMATH data from a fresh full dump.

    Re-running after an interruption skips completed steps based on the
    checkpoint file on the volume and resumes the long steps mid-file.

    Args:
        max_records_per_pass: Apply at most this many records in each of the
            three update passes, then end the run and keep the checkpoint, so
            the result can be checked before the next run continues after
            them. ``0`` stops after the diff. ``None`` (the default) does
            everything.
    """
    if max_records_per_pass is not None and max_records_per_pass < 0:
        raise ValueError("max_records_per_pass must be 0 or more")
    shutil.copyfile(
        "/config/import_config.config.template",
        "/config/import_config.config",
    )

    log = get_run_logger()
    ctx = get_run_context()
    log.info("Update flow run: %s", ctx.flow_run.id)

    checkpoint = _load_checkpoint()

    if not checkpoint.get("flow_run_id"):
        checkpoint = {
            "flow_run_id": str(ctx.flow_run.id),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_steps": {},
            "step_outputs": {},
        }
        _save_checkpoint(checkpoint)
    else:
        log.info(
            "Resuming from checkpoint (originally started %s, steps done: %s)",
            checkpoint.get("started_at"),
            [s for s in STEPS if _step_done(checkpoint, s)],
        )

    # Same dict object the _mark_step calls below update, so it stays current.
    outputs = checkpoint.setdefault("step_outputs", {})

    # ── Step 1: resolve the comparison baseline ────────────────────────────
    # Has to run before the fresh dump lands in the same directory, and is
    # pinned in the checkpoint so a resumed run keeps comparing against the
    # same file.
    if _step_done(checkpoint, "find_previous_dump"):
        previous_dump = outputs["previous_dump_path"]
        log.info("Skipping find_previous_dump (already done): %s", previous_dump)
    else:
        previous_dump = find_previous_dump(
            exclude=outputs.get("processed_dump_path")
        )
        if not previous_dump:
            raise RuntimeError(
                f"No previous processed dump in {DATA_DIR}. The update flow "
                "compares a fresh full dump against the previous full dump; "
                "copy a full processed zbMATH dump (a file matching "
                f"{PROCESSED_PATTERN}) onto this volume first."
            )
        checkpoint = _mark_step(
            checkpoint, "find_previous_dump",
            {"previous_dump_path": previous_dump},
        )
    log.info("Comparison baseline: %s", previous_dump)

    # ── Step 2: fresh full dump ────────────────────────────────────────────
    if _step_done(checkpoint, "download_raw_dump"):
        raw_path = outputs["raw_dump_path"]
        log.info("Skipping download_raw_dump (already done): %s", raw_path)
    else:
        log.info("Starting download of a fresh full dump")
        raw_path = download_raw_dump()
        checkpoint = _mark_step(
            checkpoint, "download_raw_dump", {"raw_dump_path": raw_path}
        )

    # ── Step 3: convert ────────────────────────────────────────────────────
    if _step_done(checkpoint, "convert_raw_to_processed"):
        new_dump = outputs["processed_dump_path"]
        log.info("Skipping convert_raw_to_processed (already done): %s", new_dump)
    else:
        log.info("Converting the raw dump to a processed dump")
        new_dump = convert_raw_to_processed(raw_path)
        checkpoint = _mark_step(
            checkpoint, "convert_raw_to_processed",
            {"processed_dump_path": new_dump},
        )

    # ── Step 4: hashes ─────────────────────────────────────────────────────
    if _step_done(checkpoint, "hash_dumps"):
        old_hash = outputs["old_hash_path"]
        new_hash = outputs["new_hash_path"]
        log.info("Skipping hash_dumps (already done)")
    else:
        log.info("Hashing both dumps")
        hashes = hash_dumps(previous_dump, new_dump)
        old_hash = hashes["old_hash_path"]
        new_hash = hashes["new_hash_path"]
        checkpoint = _mark_step(checkpoint, "hash_dumps", hashes)

    # ── Step 5: the diff ───────────────────────────────────────────────────
    if _step_done(checkpoint, "build_diff"):
        diff_path = outputs["diff_path"]
        log.info("Skipping build_diff (already done): %s", diff_path)
    else:
        log.info("Building the diff")
        result = build_diff(previous_dump, new_dump, old_hash, new_hash)
        diff_path = result["diff_path"]
        checkpoint = _mark_step(checkpoint, "build_diff", result)

    # ── Steps 6-8: the update passes ─────────────────────────────────────
    # With max_records_per_pass each pass stops after that many records; a
    # pass that stopped with records left is not marked done, so the next run
    # resumes it after the last record it applied.
    if max_records_per_pass is not None:
        log.info("Sample run: at most %d record(s) per update pass",
                 max_records_per_pass)
    unfinished = []

    def _run_pass(step, label, fn, *args, **kwargs):
        nonlocal checkpoint
        if _step_done(checkpoint, step):
            log.info("Skipping %s (already done)", step)
            return
        log.info(label)
        stats = fn(*args, max_rows=max_records_per_pass, **kwargs)
        if stats.get("stopped_early"):
            unfinished.append(step)
        else:
            checkpoint = _mark_step(checkpoint, step)

    # Step 6: publication items
    _run_pass("update_non_arxiv", "Applying the diff to publication items",
              update_items, diff_path, mode="non_arxiv")
    # Step 7: arXiv items
    _run_pass("update_arxiv", "Applying the diff to arXiv items",
              update_items, diff_path, mode="arxiv")
    # Step 8: references
    _run_pass("update_references", "Updating changed reference lists",
              update_references, diff_path)

    # ── Step 9: list vanished records ────────────────────────────────────
    if _step_done(checkpoint, "list_deleted_records"):
        deleted_path = outputs["deleted_path"]
        log.info("Skipping list_deleted_records (already done)")
    else:
        result = list_deleted_records(
            old_hash, new_hash, f"{_stem(previous_dump)}_TO_{_stem(new_dump)}"
        )
        deleted_path = result["deleted_path"]
        checkpoint = _mark_step(checkpoint, "list_deleted_records", result)

    if unfinished:
        # Sample run: keep the checkpoint so the next run continues the passes.
        missing = sum(
            count_rows(_missing_path(diff_path, kind)) for kind in MISSING_KINDS
        )
        message = (
            f"Stopped after at most {max_records_per_pass} record(s) per pass; "
            f"{', '.join(unfinished)} not finished yet. Check the items listed "
            f"as 'updated ...' in the logs against {os.path.basename(diff_path)} "
            "(and the deleted list), then run again with the same "
            "max_records_per_pass for the next batch or without it for the rest."
        )
        if missing:
            message += f" {missing} record(s) so far have no item in the portal."
        log.warning(message)
        return Completed(message=message)

    # ── Step 10: verify ────────────────────────────────────────────────────
    if _step_done(checkpoint, "verify_files"):
        log.info("Skipping verify_files (already done)")
    else:
        log.info("Verifying files")
        verify_files([
            raw_path,
            new_dump,
            old_hash,
            new_hash,
            diff_path,
            deleted_path,
        ])
        checkpoint = _mark_step(checkpoint, "verify_files")

    # ── Done — archive the checkpoint so the next run starts fresh ───────
    checkpoint["finished_at"] = datetime.now(timezone.utc).isoformat()
    _save_checkpoint(checkpoint)

    archive = CHECKPOINT_FILE + f".done.{time.strftime('%Y%m%d-%H%M%S')}"
    os.rename(CHECKPOINT_FILE, archive)
    log.info(
        "Update flow complete. %s is now the baseline for the next run. "
        "Checkpoint archived to %s", new_dump, archive,
    )

    # Every step has run and the checkpoint is archived, so the next run starts
    # fresh; only now is the run failed over records that could not be applied.
    missing = {
        kind: count_rows(_missing_path(diff_path, kind)) for kind in MISSING_KINDS
    }
    if any(missing.values()):
        details = ", ".join(
            f"{n} in {os.path.basename(_missing_path(diff_path, kind))}"
            for kind, n in missing.items() if n
        )
        raise RuntimeError(
            "All steps finished, but some changed records have no item in the "
            f"portal and were not applied: {details} (in {DATA_DIR})."
        )
