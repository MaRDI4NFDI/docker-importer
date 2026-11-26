"""One OpenML dataset as an item in the portal."""

from __future__ import annotations

from typing import Optional

from mardi_importer.zbmath.misc import _with_backoff

SUMMARY = "OpenML importer"


class OpenMLDataset:
    """Creates the portal item for one OpenML dataset record.

    The statements come from :func:`mardi_importer.openml.records.statements`
    and are written by :class:`mardi_importer.openml.OpenMLUpdate.OpenMLUpdater`,
    the same code the update flow uses, so new and updated items look alike.

    Attributes:
        source: the :class:`OpenMLSource`, for its portal client and logger.
        record: the dataset's record from a download file.
        QID: the item's ID once it exists.
    """

    def __init__(self, source, record: dict, QID: Optional[str] = None):
        self.source = source
        self.record = record
        self.QID = QID

    def exists(self) -> Optional[str]:
        """The item's ID if the portal has an item with this OpenML dataset ID."""
        if not self.QID:
            self.QID = self.source.find_items([self.record["id"]]).get(self.record["id"])
        return self.QID

    def create(self) -> Optional[str]:
        """Create the item, unless it exists; returns its ID."""
        if self.exists():
            return self.QID
        from .OpenMLUpdate import OpenMLUpdater

        item = self.source.api.item.new()
        OpenMLUpdater(self.source).apply(item, self.record, None)
        written = _with_backoff(lambda: item.write(summary=SUMMARY), self.source.logger,
                                f"writing OpenML dataset {self.record['id']}")
        self.QID = written.id
        self.source.logger.info("OpenML dataset %s created as %s", self.record["id"], self.QID)
        return self.QID
