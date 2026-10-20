"""Zenodo source: which records the nightly import pulls."""

import importlib
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

module = importlib.import_module("mardi_importer.zenodo.ZenodoSource")


def page(ids, total):
    return {"hits": {"total": total, "hits": [{"id": i} for i in ids]}}


class TestPull(unittest.TestCase):
    def source(self):
        s = module.ZenodoSource.__new__(module.ZenodoSource)
        s.zenodo_ids, s.orcid_ids, s.resourceTypes, s.customQ = [], None, None, None
        s.communities = ["mathplus", "mardigmci", "mardi"]
        return s

    def test_since_filters_on_update_date(self):
        s = self.source()
        with patch.object(module.ZenodoSource, "get_with_retries",
                          side_effect=[page([1], 3), page([1, 2], 3), page([3], 3)]) as get:
            s.pull(since="2026-10-19")
        self.assertEqual(s.zenodo_ids, ["1", "2", "3"])
        self.assertEqual(get.call_args.kwargs["params"]["q"],
                         "updated:[2026-10-19 TO *] AND communities:(mathplus mardigmci mardi)")

    def test_stops_on_an_empty_page(self):
        s = self.source()
        with patch.object(module.ZenodoSource, "get_with_retries",
                          side_effect=[page([1], 5), page([1], 5), page([], 5)]):
            s.pull()
        self.assertEqual(s.zenodo_ids, ["1"])


if __name__ == "__main__":
    unittest.main()
