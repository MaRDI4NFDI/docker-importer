import os
import unittest
from typing import Dict


class TestImporterArxivSource(unittest.TestCase):
    """Tests for creating the arXiv source and publications via Importer.

    Uses a fake source to avoid network and real API usage while still
    exercising Importer.create_source and new_publication plumbing.

    Args:
        unittest.TestCase: Base class for unittest test cases.
    """

    def setUp(self) -> None:
        self._old_env = dict(os.environ)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_env)

    def test_importer_create_source_and_new_publication(self) -> None:
        from mardi_importer import Importer

        class FakeArxivSource:
            def __init__(self, user: str, password: str) -> None:
                self.user = user
                self.password = password
                self.api = object()

            def new_publication(self, arxiv_id: str) -> Dict[str, str]:
                return {"arxiv_id": arxiv_id, "user": self.user}

        old_sources = dict(Importer._sources)
        old_credentials = dict(Importer._credentials)
        old_apis = dict(Importer._apis)

        try:
            Importer._sources["arxiv"] = FakeArxivSource
            Importer._credentials["arxiv"] = ("ARXIV_USER", "ARXIV_PASS")
            Importer._apis.pop("arxiv", None)

            os.environ["ARXIV_USER"] = "test-user"
            os.environ["ARXIV_PASS"] = "test-pass"

            arxiv = Importer.create_source("arxiv")
            publication = arxiv.new_publication("1234.56789")

            self.assertEqual(publication["arxiv_id"], "1234.56789")
            self.assertEqual(publication["user"], "test-user")
        finally:
            Importer._sources = old_sources
            Importer._credentials = old_credentials
            Importer._apis = old_apis


if __name__ == "__main__":
    unittest.main()
