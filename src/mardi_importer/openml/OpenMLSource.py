"""Import OpenML datasets into the MaRDI portal.

Like the zbMATH source, the OpenML source is driven by two Prefect flows
(``mardi_portal.flows.openml_import`` and ``openml_update``):

- :meth:`OpenMLSource.pull` downloads every active OpenML dataset as one
  record per line (see :mod:`.records`) to a download file. An interrupted
  download continues where it stopped.
- :meth:`OpenMLSource.push` creates an item (:class:`.OpenMLDataset`) for
  every dataset in a download that the portal does not have yet.
- :meth:`OpenMLSource.update_from_downloads` compares two downloads and
  updates the items of the datasets that changed (:class:`.OpenMLUpdater`).

Existing items are found by their OpenML dataset ID.
"""

from __future__ import annotations

import glob
import json
import os
from typing import Callable, Iterator, Optional

import requests

from mardi_importer.base import ADataSource
from mardi_importer.zbmath.misc import _with_backoff

from .OpenMLDataset import OpenMLDataset
from .OpenMLUpdate import OpenMLUpdater
from .records import record_from_api

API = "https://www.openml.org/api/v1/json"
PAGE_SIZE = 1000


def qid_number(qid: str) -> int:
    """``"Q123"`` -> 123, so that Q99 sorts before Q100."""
    return int(qid.lstrip("Q"))


def read_records(path: Optional[str]) -> Iterator[dict]:
    """The records of a download file, in file order."""
    if not path or not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def drop_partial_line(path: str) -> None:
    """Cut a half-written last line, left by a run stopped while writing it.

    Every record is written as one line ending in a newline, so a file that
    does not end in a newline ends in a fragment. The complete records stay.
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return
    with open(path, "rb+") as f:
        data = f.read()
        if data.endswith(b"\n"):
            return
        f.truncate(data.rfind(b"\n") + 1)


class Progress:
    """Where a run is, saved through a callback after every dataset.

    The last ID handled moves forward past failures, so datasets handled
    after a failure are not done twice. The failed IDs are kept in a retry
    list instead, which the next run handles first.
    """

    def __init__(self, last_id: Optional[str], retry_ids, callback):
        self.last_id = last_id
        self.pending = set(retry_ids)        # from earlier runs, not handled yet
        self.failed: set[str] = set()
        self.callback = callback

    def done(self, did: str, failed: bool) -> None:
        self.pending.discard(did)
        if failed:
            self.failed.add(did)
        if self.last_id is None or int(did) > int(self.last_id):
            self.last_id = did
        if self.callback:
            self.callback(self.last_id, sorted(self.pending | self.failed, key=int))


BINARY = "NumberOfBinaryFeatures"


def binary_counts(path: str) -> dict[str, str]:
    """NumberOfBinaryFeatures per dataset from the newest other download.

    A dataset's data never changes under its ID, so the count can be taken
    from an earlier download instead of asking OpenML again.
    """
    folder = os.path.dirname(os.path.abspath(path))
    files = sorted(f for f in glob.glob(os.path.join(folder, "openml_*.jsonl"))
                   if os.path.abspath(f) != os.path.abspath(path))
    if not files:
        return {}
    return {r["id"]: r["qualities"][BINARY] for r in read_records(files[-1])
            if BINARY in r.get("qualities", {})}


class OpenMLSource(ADataSource):
    """Downloads OpenML datasets and writes them to the portal."""

    def __init__(self, user: str, password: str):
        super().__init__(user, password)
        self.session = requests.Session()
        self._ids: dict[str, Optional[str]] = {}

    def setup(self):
        """Create the properties and items the dataset items use."""
        self.import_wikidata_entities("/wikidata_entities.txt")
        self.create_local_entities("/new_entities.json")

    # ── OpenML ───────────────────────────────────────────────────────────

    def _get(self, path: str) -> Optional[dict]:
        """GET an OpenML API path; ``None`` when OpenML has no result (412)."""
        def call():
            response = self.session.get(f"{API}/{path}", timeout=120)
            if response.status_code == 412:
                return None
            response.raise_for_status()
            return response.json()
        return _with_backoff(call, self.logger, f"OpenML {path}", base=10, cap=600, max_total=3600)

    def list_datasets(self) -> dict[str, list]:
        """Dataset ID -> its qualities, for every active dataset on OpenML."""
        datasets, offset = {}, 0
        while True:
            page = self._get(f"data/list/limit/{PAGE_SIZE}/offset/{offset}")
            if page is None:
                if offset == 0:
                    raise RuntimeError("OpenML listed no datasets")
                return datasets
            entries = page["data"]["dataset"]
            entries = entries if isinstance(entries, list) else [entries]
            for entry in entries:
                datasets[str(entry["did"])] = entry.get("quality", [])
            if len(entries) < PAGE_SIZE:
                return datasets
            offset += PAGE_SIZE

    def binary_count(self, did: str) -> Optional[str]:
        """NumberOfBinaryFeatures of one dataset, from ``/data/qualities``.

        The dataset list does not include it. ``None`` when OpenML has no
        value, for example while it is still computing the qualities.
        """
        response = self._get(f"data/qualities/{did}")
        qualities = ((response or {}).get("data_qualities") or {}).get("quality")
        for quality in (qualities if isinstance(qualities, list) else [qualities]):
            if isinstance(quality, dict) and quality.get("name") == BINARY:
                return quality.get("value")
        return None

    def pull(self, path: str) -> str:
        """Save every active OpenML dataset to ``path``, one record per line.

        The records are first written to ``path + ".part"``. A rerun after an
        interruption skips the datasets already in that file. The file is
        renamed to ``path`` once every dataset is in it.
        """
        if os.path.exists(path):
            return path
        part = path + ".part"
        drop_partial_line(part)
        have = {r["id"] for r in read_records(part)}
        datasets = self.list_datasets()
        todo = [did for did in sorted(datasets, key=int) if did not in have]
        self.logger.info("%d datasets on OpenML, %d to download", len(datasets), len(todo))
        known = binary_counts(path)
        self.logger.info("binary feature counts known from the previous download: %d", len(known))
        for n, did in enumerate(todo, 1):
            response = self._get(f"data/{did}")
            if response is None:
                self.logger.warning("OpenML dataset %s has no description; skipped", did)
                continue
            qualities = list(datasets[did])
            if not any(q.get("name") == BINARY for q in qualities):
                count = known.get(did) or self.binary_count(did)
                if count is not None:
                    qualities.append({"name": BINARY, "value": count})
            record = record_from_api(response["data_set_description"], qualities)
            with open(part, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
                f.flush()
                os.fsync(f.fileno())
            if n % 500 == 0:
                self.logger.info("downloaded %d of %d", n, len(todo))
        os.replace(part, path)
        return path

    # ── portal lookups ───────────────────────────────────────────────────

    def find_items(self, dataset_ids: list[str]) -> dict[str, str]:
        """OpenML dataset ID -> QID of its item, for the IDs the portal has."""
        found = {}
        for start in range(0, len(dataset_ids), 200):
            chunk = dataset_ids[start:start + 200]
            mapping = _with_backoff(
                lambda: self.api.batch_search_by_value("wdt:P11238", chunk),
                self.logger, "searching OpenML dataset IDs")
            for did, qids in mapping.items():
                if len(qids) > 1:
                    self.logger.warning("OpenML dataset %s has several items %s; using %s",
                                        did, sorted(qids, key=qid_number), min(qids, key=qid_number))
                found[str(did)] = min(qids, key=qid_number)
        return found

    def resolve(self, ref: str, create: bool = True) -> Optional[str]:
        """The portal ID for an item reference from :mod:`.records`.

        ``doi:`` and ``arxiv:`` references are created through the Crossref-
        or arXiv-Importer when the portal does not have them and ``create``
        is set. Returns ``None`` when the item cannot be found or made.
        """
        if not ref.startswith(("wd:", "label:", "doi:", "arxiv:")):
            return ref
        if ref in self._ids and (self._ids[ref] or not create):
            return self._ids[ref]
        kind, value = ref.split(":", 1)
        if kind == "wd":
            qid = self.api.get_local_id_by_label(ref, "item")
        elif kind == "label":
            qids = self.api.get_local_id_by_label(value, "item")
            qid = qids[0] if qids else None
        else:
            qid = self._publication(kind, value, create)
        self._ids[ref] = qid if qid and qid != "None" else None
        return self._ids[ref]

    def _publication(self, kind: str, value: str, create: bool) -> Optional[str]:
        from mardi_importer import Importer

        found = self.api.search_entity_by_value("wdt:P356" if kind == "doi" else "wdt:P818", value)
        if found or not create:
            return min(found, key=qid_number) if found else None
        try:
            source = Importer.create_source("crossref" if kind == "doi" else "arxiv")
            return source.new_publication(value).create()
        except Exception as e:     # one bad paper must not stop the run
            self.logger.warning("could not create publication %s:%s: %s", kind, value, e)
            return None

    # ── writing ──────────────────────────────────────────────────────────

    def push(
        self,
        path: str,
        resume_after_id: Optional[str] = None,
        progress_callback: Optional[Callable[[Optional[str], list], None]] = None,
        max_rows: Optional[int] = None,
        retry_ids: tuple = (),
    ) -> dict:
        """Create items for the datasets in ``path`` that the portal lacks.

        Datasets are handled in ID order; ones that already have an item are
        left to the update flow.

        Args:
            path: a complete download file.
            resume_after_id: skip datasets up to and including this ID...
            retry_ids: ...except these, which failed in an earlier run.
            progress_callback: called after each dataset with the last ID
                handled and the IDs still to retry (the failed ones, and
                retry IDs not reached yet). Save both and pass them back.
            max_rows: create at most this many items, then stop with
                ``stopped_early`` set in the returned stats.
        """
        records = sorted(read_records(path), key=lambda r: int(r["id"]))
        records = [r for r in records if r["id"] in retry_ids or resume_after_id is None
                   or int(r["id"]) > int(resume_after_id)]
        existing = self.find_items([r["id"] for r in records])
        stats = {"created": 0, "already_in_portal": 0, "failed": 0, "stopped_early": False}
        progress = Progress(resume_after_id, retry_ids, progress_callback)
        for record in records:
            if max_rows is not None and stats["created"] >= max_rows:
                stats["stopped_early"] = True
                break
            failed = False
            if record["id"] in existing:
                stats["already_in_portal"] += 1
            else:
                try:
                    OpenMLDataset(self, record).create()
                    stats["created"] += 1
                except Exception as e:
                    self.logger.error("OpenML dataset %s not created: %s", record["id"], e)
                    stats["failed"] += 1
                    failed = True
            progress.done(record["id"], failed)
        return stats

    def update_from_downloads(self, old_path: Optional[str], new_path: str, **kwargs) -> dict:
        """Update the items of datasets that changed between two downloads.

        ``old_path`` ``None`` means there is no previous download (see
        :mod:`.OpenMLUpdate`). Keyword arguments go to
        :meth:`OpenMLUpdater.update`.
        """
        old = {r["id"]: r for r in read_records(old_path)}
        new = {r["id"]: r for r in read_records(new_path)}
        return OpenMLUpdater(self).update(old, new, **kwargs)
