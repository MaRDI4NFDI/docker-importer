"""A step checkpoint for a resumable Prefect flow.

The same scheme as the zbMATH flows: a JSON file on the flow's volume records
which steps are done, the outputs they produced (file paths), and progress
inside a long step. A rerun after an interruption skips the steps done and
continues the long step where it stopped; a finished run archives the file,
so the next run starts fresh.
"""

import json
import os
import time
from datetime import datetime, timezone


class Checkpoint:
    """The checkpoint of one flow, kept in the JSON file ``path``."""

    def __init__(self, path: str):
        self.path = path
        self.data = {}
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                self.data = json.load(f)

    @property
    def outputs(self) -> dict:
        return self.data.setdefault("step_outputs", {})

    def start(self, run_id: str) -> bool:
        """Start a new checkpoint unless one is open; True if resuming."""
        if self.data.get("flow_run_id"):
            return True
        self.data = {"flow_run_id": run_id, "started_at": _now(),
                     "completed_steps": {}, "step_outputs": {}}
        self.save()
        return False

    def done(self, step: str) -> bool:
        return self.data.get("completed_steps", {}).get(step, False)

    def mark(self, step: str, outputs: dict | None = None) -> None:
        """Mark ``step`` done, store its outputs and drop its progress."""
        self.data.setdefault("completed_steps", {})[step] = True
        self.outputs.update(outputs or {})
        self.data.get("step_progress", {}).pop(step, None)
        self.save()

    def progress(self, step: str):
        """The progress saved inside ``step``, or ``None``."""
        return self.data.get("step_progress", {}).get(step)

    def save_progress(self, step: str, value) -> None:
        self.data.setdefault("step_progress", {})[step] = value
        self.save()

    def save(self) -> None:
        """Write the file atomically, so an interruption never leaves half of it."""
        self.data["last_updated"] = _now()
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def archive(self) -> str:
        """Close the checkpoint; the next run starts fresh. Returns the archive path."""
        self.data["finished_at"] = _now()
        self.save()
        archive = f"{self.path}.done.{time.strftime('%Y%m%d-%H%M%S')}"
        os.rename(self.path, archive)
        return archive


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
