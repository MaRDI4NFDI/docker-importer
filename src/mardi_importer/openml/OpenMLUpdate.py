"""Bring existing OpenML dataset items in line with a newer download.

The update compares two downloads record by record. For each dataset whose
record changed, every value the new record has and the item lacks is added,
and a value is removed only if the old record had it and the new one does
not. A value someone added by hand was never in a download, so it stays.

Without an old download (the first update), nothing is removed except values
the earlier OpenML importer wrote in an outdated form: the profile item
"Dataset", and descriptions with literal ``\\N`` and ``\\T`` codes.
"""

from __future__ import annotations

import re
from typing import Callable, Optional

from wikibaseintegrator.wbi_enums import ActionIfExists

from mardi_importer.zbmath.misc import _with_backoff
from mardi_importer.zbmath.ZBMathUpdate import claim_scalar

from .OpenMLDataset import SUMMARY
from .records import Field, openml_download, statements, text_key


SIGNED = re.compile(r"^\+[0-9][0-9.:TZ-]*$")


def read_ids(path: Optional[str]) -> list[str]:
    """Dataset IDs listed one per line in a file."""
    try:
        with open(path, encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except (FileNotFoundError, TypeError):
        return []


def deletion_report(old: dict, new: dict) -> dict[str, tuple[int, int]]:
    """Per field: (records that would lose all values, records that had one).

    Only datasets in both downloads count; a dataset that vanished is not a
    deletion of its values.
    """
    report: dict[str, list[int]] = {}
    for did, old_record in old.items():
        if did not in new:
            continue
        for before, after in zip(statements(old_record), statements(new[did])):
            if not before.values:
                continue
            name = before.prop + (f" ({before.qualifier[1]})" if before.by_qualifier else "")
            counts = report.setdefault(name, [0, 0])
            counts[1] += 1
            counts[0] += not after.values
    return {name: tuple(counts) for name, counts in report.items()}


class OpenMLUpdater:
    """Adds and removes the statements of OpenML dataset items.

    Attributes:
        source: the :class:`OpenMLSource`, for its portal client, logger and
            the lookup of item references (:meth:`OpenMLSource.resolve`).
        stats: what the last :meth:`update` did.
    """

    def __init__(self, source):
        self.source = source
        self.api = source.api
        self.log = source.logger
        self.stats: dict = {}

    def _pid(self, prop: str) -> str:
        pid = self.api.get_local_id_by_label(prop, "property")
        return pid[0] if isinstance(pid, list) else pid

    def apply(self, item, new: dict, old: Optional[dict]) -> bool:
        """Bring ``item`` in line with record ``new``; True if it changed.

        Args:
            item: a new or fetched portal item.
            new: the dataset's record in the new download.
            old: its record in the previous download, or ``None``.
        """
        changed = False
        old_fields = statements(old) if old else [None] * len(statements(new))
        for field, old_field in zip(statements(new), old_fields):
            changed |= self.merge_claims(item, field, old_field.values if old_field else [])
        name = new.get("name")
        label = item.labels.get("en")
        label = label.value if label else None
        if name and name != label and (label is None or label == (old or {}).get("name")):
            item.labels.set(language="en", value=name)
            changed = True
        if not item.descriptions.get("en"):
            item.descriptions.set(language="en", value=f"OpenML dataset with id {new['id']}")
            changed = True
        return changed

    def merge_claims(self, item, field: Field, old_values: list) -> bool:
        """Add and remove the values of one field; True if the item changed."""
        resolve = self.source.resolve
        prop = self._pid(field.prop)
        new_values = [v for v in (resolve(v) for v in field.values) if v]
        old_values = [v for v in (resolve(v, create=False) for v in old_values) if v]
        replaced = {resolve(v, create=False) for v in field.replaces} - {None}
        qualifier = None
        if field.qualifier:
            qualifier = (self._pid(field.qualifier[0]), resolve(field.qualifier[1]))

        def key(value):
            value = str(value)
            if field.compare == "text":
                return text_key(value)
            if field.compare == "url" and openml_download(value):
                return f"openml-download:{openml_download(value)}"
            # quantities and times come back from the portal as "+150"
            return value[1:] if SIGNED.match(value) else value

        def mine(claim):
            if not field.by_qualifier:
                return True
            return any((q.datavalue or {}).get("value", {}).get("id") == qualifier[1]
                       for q in claim.qualifiers.get(qualifier[0]))

        new_raw = {str(v) for v in new_values}
        new_keys = {key(v) for v in new_values}
        # Only the importer writes OpenML download links, so once the new
        # record has one, any other one on the item is outdated: an older
        # host, or a file OpenML has since replaced.
        has_download = field.compare == "url" and any(openml_download(v) for v in new_values)
        old_keys = {key(v) for v in old_values}
        changed = False
        for claim in item.claims.get(prop):
            value = None if claim.removed else claim_scalar(claim)
            if value is None or not mine(claim):
                continue
            stale = key(value) in old_keys and key(value) not in new_keys
            # the same text in an older form, such as with \N codes
            reformatted = field.compare == "text" and key(value) in new_keys and value not in new_raw
            old_download = has_download and openml_download(value) and str(value) not in new_raw
            if stale or reformatted or old_download or value in replaced:
                claim.remove()
                changed = True

        present = {key(claim_scalar(c)) for c in item.claims.get(prop)
                   if not c.removed and mine(c) and claim_scalar(c) is not None}
        for value in new_values:
            if key(value) in present:
                continue
            kwargs = {}
            if qualifier:
                kwargs["qualifiers"] = [self.api.get_claim(qualifier[0], qualifier[1])]
            item.add_claims([self.api.get_claim(prop, value, **kwargs)], ActionIfExists.FORCE_APPEND)
            present.add(key(value))
            changed = True
        return changed

    def update(
        self,
        old: dict,
        new: dict,
        resume_after_id: Optional[str] = None,
        progress_callback: Optional[Callable[[Optional[str], list], None]] = None,
        missing_path: Optional[str] = None,
        max_rows: Optional[int] = None,
        retry_ids: tuple = (),
    ) -> dict:
        """Update the items of the datasets whose record changed.

        Datasets are handled in ID order. A dataset only in ``new`` is left to
        the import flow. With ``old`` empty every dataset counts as changed.

        Args:
            old: dataset ID -> record, from the previous download.
            new: dataset ID -> record, from the new download.
            resume_after_id: skip datasets up to and including this ID...
            retry_ids: ...except these, which failed in an earlier run.
            progress_callback: called after each dataset with the last ID
                handled and the IDs still to retry (see ``OpenMLSource.push``).
            missing_path: changed datasets without an item are appended here.
            max_rows: handle at most this many datasets, then stop with
                ``stopped_early`` set in the returned stats.
        """
        changed = [did for did in sorted(new, key=int)
                   if did in retry_ids
                   or ((not old or (did in old and old[did] != new[did]))
                       and (resume_after_id is None or int(did) > int(resume_after_id)))]
        items = self.source.find_items(changed)
        missing = set(read_ids(missing_path))
        stats = {"changed": len(changed), "updated": 0, "unchanged": 0,
                 "missing_in_portal": 0, "failed": 0, "stopped_early": False}
        from .OpenMLSource import Progress
        progress = Progress(resume_after_id, retry_ids, progress_callback)
        for n, did in enumerate(changed):
            if max_rows is not None and n >= max_rows:
                stats["stopped_early"] = True
                break
            failed = False
            if did not in items:
                if missing_path and did not in missing:
                    with open(missing_path, "a", encoding="utf-8") as f:
                        f.write(did + "\n")
                stats["missing_in_portal"] += 1
            else:
                try:
                    qid = items[did]
                    item = _with_backoff(lambda: self.api.item.get(entity_id=qid),
                                         self.log, f"fetching {qid}")
                    if self.apply(item, new[did], old.get(did)):
                        _with_backoff(lambda: item.write(summary=SUMMARY), self.log, f"writing {qid}")
                        self.log.info("updated %s (OpenML dataset %s)", qid, did)
                        stats["updated"] += 1
                    else:
                        stats["unchanged"] += 1
                except Exception as e:
                    self.log.error("OpenML dataset %s (%s) not updated: %s", did, items[did], e)
                    stats["failed"] += 1
                    failed = True
            progress.done(did, failed)
        self.stats = stats
        return stats
