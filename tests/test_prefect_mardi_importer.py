import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from pathlib import Path

# Ensure project root is importable when Prefect isn't installed.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _install_prefect_stub() -> None:
    """Install a minimal Prefect stub in sys.modules for tests (always)."""
    fake_prefect = types.ModuleType("prefect")
    fake_states = types.ModuleType("prefect.states")
    fake_artifacts = types.ModuleType("prefect.artifacts")
    fake_blocks = types.ModuleType("prefect.blocks")
    fake_blocks_system = types.ModuleType("prefect.blocks.system")
    fake_context = types.ModuleType("prefect.context")

    def task(*_args, **_kwargs):
        def decorator(fn):
            return fn
        return decorator

    def flow(*_args, **_kwargs):
        def decorator(fn):
            return fn
        return decorator

    class State:
        pass

    class Failed(State):
        def __init__(self, message=None, data=None):
            self.message = message
            self.data = data

    class Artifact:
        def __init__(self, type, key, description, data):
            self.type = type
            self.key = key
            self.description = description
            self.data = data
            self.id = "artifact-id"

        def create(self):
            return self

    class Secret:
        @classmethod
        def load(cls, _name):
            return SimpleNamespace(get=lambda: "secret")

    def get_run_logger():
        logger = Mock()
        logger.info = Mock()
        logger.debug = Mock()
        logger.warning = Mock()
        logger.error = Mock()
        return logger

    def get_run_context():
        return SimpleNamespace(flow_run=SimpleNamespace(id="flow-run-id"))

    fake_prefect.task = task
    fake_prefect.flow = flow
    fake_prefect.get_run_logger = get_run_logger
    fake_prefect.State = State

    fake_prefect.states = fake_states
    fake_prefect.artifacts = fake_artifacts
    fake_prefect.blocks = fake_blocks
    fake_prefect.context = fake_context

    # Ensure dotted access works when real Prefect is installed elsewhere.
    fake_prefect.__path__ = []  # mark as package
    fake_blocks.__path__ = []
    fake_blocks_system.__path__ = []
    fake_states.__path__ = []
    fake_artifacts.__path__ = []
    fake_context.__path__ = []

    fake_states.Failed = Failed
    fake_artifacts.Artifact = Artifact
    fake_blocks.system = fake_blocks_system
    fake_blocks_system.Secret = Secret
    fake_context.get_run_context = get_run_context

    sys.modules["prefect"] = fake_prefect
    sys.modules["prefect.states"] = fake_states
    sys.modules["prefect.artifacts"] = fake_artifacts
    sys.modules["prefect.blocks"] = fake_blocks
    sys.modules["prefect.blocks.system"] = fake_blocks_system
    sys.modules["prefect.context"] = fake_context


def _install_mardi_importer_stub() -> None:
    """Install minimal stubs for the mardi_importer package tree."""
    fake_pkg = types.ModuleType("mardi_importer")
    fake_wikidata = types.ModuleType("mardi_importer.wikidata")
    fake_mardi_importer = types.ModuleType("mardi_importer.mardi_importer")

    class WikidataImporter:
        def import_entities(self, *_args, **_kwargs):
            raise NotImplementedError

    class Importer:
        _sources = {}

        @staticmethod
        def create_source(_name):
            raise NotImplementedError

    fake_wikidata.WikidataImporter = WikidataImporter
    fake_mardi_importer.Importer = Importer
    fake_pkg.wikidata = fake_wikidata
    fake_pkg.mardi_importer = fake_mardi_importer

    sys.modules["mardi_importer"] = fake_pkg
    sys.modules["mardi_importer.wikidata"] = fake_wikidata
    sys.modules["mardi_importer.mardi_importer"] = fake_mardi_importer


_install_prefect_stub()
_install_mardi_importer_stub()

from prefect_workflow import prefect_mardi_importer as pmi


class TestImportDoiBatch(unittest.TestCase):
    """Tests for DOI batch imports."""

    def test_import_doi_batch_handles_sources_and_failures(self) -> None:
        """Handle arXiv/Zenodo/Crossref paths and failures."""
        arxiv_publication = Mock()
        arxiv_publication.create.return_value = "Q1"

        zenodo_publication = Mock()
        zenodo_publication.create.return_value = None

        crossref_publication = Mock()
        crossref_publication.create.side_effect = RuntimeError("boom")

        arxiv_source = Mock()
        arxiv_source.new_publication.return_value = arxiv_publication
        zenodo_source = Mock()
        zenodo_source.new_resource.return_value = zenodo_publication
        crossref_source = Mock()
        crossref_source.new_publication.return_value = crossref_publication

        def create_source(name):
            if name == "arxiv":
                return arxiv_source
            if name == "zenodo":
                return zenodo_source
            if name == "crossref":
                return crossref_source
            raise AssertionError(f"Unexpected source {name}")

        with patch("prefect_workflow.prefect_mardi_importer.Secret.load") as secret_load, \
            patch("prefect_workflow.prefect_mardi_importer.Importer.create_source", side_effect=create_source):
            secret_load.return_value.get.return_value = "secret"

            result = pmi.import_doi_batch(
                ["arXiv:1234.5678", "10.5281/zenodo.12345", "10.1000/xyz"]
            )

        self.assertEqual(result["count"], 3)
        self.assertFalse(result["all_imported"])
        self.assertEqual(result["results"]["arXiv:1234.5678"]["status"], "success")
        self.assertEqual(result["results"]["10.5281/zenodo.12345"]["status"], "not_found")
        self.assertEqual(result["results"]["10.1000/xyz"]["status"], "error")

    def test_import_doi_batch_invalid_arxiv_format(self) -> None:
        """Return an error for unsupported arXiv DOI formats."""
        arxiv_source = Mock()
        arxiv_source.new_publication.side_effect = AssertionError("should not be called")

        with patch("prefect_workflow.prefect_mardi_importer.Secret.load") as secret_load, \
            patch("prefect_workflow.prefect_mardi_importer.Importer.create_source") as create_source:
            secret_load.return_value.get.return_value = "secret"
            create_source.return_value = arxiv_source

            result = pmi.import_doi_batch(["ARXIV1234"])

        self.assertFalse(result["all_imported"])
        self.assertEqual(result["results"]["ARXIV1234"]["status"], "error")
        self.assertIn("Unsupported arXiv DOI format", result["results"]["ARXIV1234"]["error"])

    def test_import_doi_batch_all_success(self) -> None:
        """Return all_imported=True when every source imports."""
        arxiv_publication = Mock()
        arxiv_publication.create.return_value = "Q1"

        zenodo_publication = Mock()
        zenodo_publication.create.return_value = "Q2"

        crossref_publication = Mock()
        crossref_publication.create.return_value = "Q3"

        arxiv_source = Mock()
        arxiv_source.new_publication.return_value = arxiv_publication
        zenodo_source = Mock()
        zenodo_source.new_resource.return_value = zenodo_publication
        crossref_source = Mock()
        crossref_source.new_publication.return_value = crossref_publication

        def create_source(name):
            if name == "arxiv":
                return arxiv_source
            if name == "zenodo":
                return zenodo_source
            if name == "crossref":
                return crossref_source
            raise AssertionError(f"Unexpected source {name}")

        with patch("prefect_workflow.prefect_mardi_importer.Secret.load") as secret_load, \
            patch("prefect_workflow.prefect_mardi_importer.Importer.create_source", side_effect=create_source):
            secret_load.return_value.get.return_value = "secret"

            result = pmi.import_doi_batch(
                ["arXiv:1234.5678", "10.5281/zenodo.12345", "10.1000/xyz"]
            )

        self.assertTrue(result["all_imported"])
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["results"]["arXiv:1234.5678"]["status"], "success")
        self.assertEqual(result["results"]["10.5281/zenodo.12345"]["status"], "success")
        self.assertEqual(result["results"]["10.1000/xyz"]["status"], "success")


class TestImportWikidataBatch(unittest.TestCase):
    """Tests for Wikidata batch imports."""

    def test_import_wikidata_batch_handles_success_and_errors(self) -> None:
        """Handle success, no import, and error results."""
        importer = Mock()
        importer.import_entities.side_effect = ["Q1", None, RuntimeError("boom")]

        with patch("prefect_workflow.prefect_mardi_importer.Secret.load") as secret_load, \
            patch("prefect_workflow.prefect_mardi_importer.WikidataImporter", return_value=importer):
            secret_load.return_value.get.return_value = "secret"

            result = pmi.import_wikidata_batch(["Q1", "Q2", "Q3"])

        self.assertEqual(result["count"], 3)
        self.assertFalse(result["all_imported"])
        self.assertEqual(result["results"]["Q1"]["status"], "success")
        self.assertEqual(result["results"]["Q2"]["status"], "not_imported")
        self.assertEqual(result["results"]["Q3"]["status"], "error")


class TestPrefectMardiImporterFlow(unittest.TestCase):
    """Tests for the Prefect flow wrapper."""

    def test_flow_import_wikidata_success(self) -> None:
        """Return payload when all items are imported."""
        result_payload = {
            "qids": ["Q1"],
            "count": 1,
            "results": {"Q1": {"qid": "Q1", "status": "success"}},
            "all_imported": True,
        }

        artifact = Mock()
        artifact.id = "artifact-id"
        artifact.key = "artifact-key"
        artifact.create.return_value = artifact

        with patch("prefect_workflow.prefect_mardi_importer.get_run_context") as get_ctx, \
            patch("prefect_workflow.prefect_mardi_importer.import_wikidata_batch", return_value=result_payload) as import_batch, \
            patch("prefect_workflow.prefect_mardi_importer.Artifact", return_value=artifact):
            get_ctx.return_value.flow_run.id = "flow-run-id"

            result = pmi.prefect_mardi_importer_flow("import/wikidata", qids=["Q1"])

        import_batch.assert_called_once_with(["Q1"])
        self.assertEqual(result["artifact_id"], "artifact-id")
        self.assertEqual(result["artifact_key"], "artifact-key")
        self.assertEqual(result["flow_run_id"], "flow-run-id")
        self.assertTrue(result["all_imported"])

    def test_flow_import_doi_partial_failure_returns_failed(self) -> None:
        """Return Failed state when not all items import."""
        result_payload = {
            "dois": ["10.1000/xyz"],
            "count": 1,
            "results": {"10.1000/xyz": {"qid": None, "status": "error", "error": "boom"}},
            "all_imported": False,
        }

        artifact = Mock()
        artifact.id = "artifact-id"
        artifact.key = "artifact-key"
        artifact.create.return_value = artifact

        with patch("prefect_workflow.prefect_mardi_importer.get_run_context") as get_ctx, \
            patch("prefect_workflow.prefect_mardi_importer.import_doi_batch", return_value=result_payload), \
            patch("prefect_workflow.prefect_mardi_importer.Artifact", return_value=artifact):
            get_ctx.return_value.flow_run.id = "flow-run-id"

            result = pmi.prefect_mardi_importer_flow("import/doi", dois=["10.1000/xyz"])

        self.assertIsInstance(result, pmi.Failed)
        self.assertEqual(result.data["artifact_id"], "artifact-id")
        self.assertFalse(result.data["all_imported"])

    def test_flow_missing_inputs(self) -> None:
        """Validate required inputs by action."""
        with self.assertRaises(ValueError):
            pmi.prefect_mardi_importer_flow("import/wikidata", qids=[])

        with self.assertRaises(ValueError):
            pmi.prefect_mardi_importer_flow("import/doi", dois=[])

    def test_flow_unsupported_action(self) -> None:
        """Raise for unsupported actions."""
        with self.assertRaises(ValueError):
            pmi.prefect_mardi_importer_flow("import/unknown", qids=["Q1"])

    def test_flow_import_doi_success(self) -> None:
        """Return payload when DOI import succeeds."""
        result_payload = {
            "dois": ["10.1000/xyz"],
            "count": 1,
            "results": {"10.1000/xyz": {"qid": "Q42", "status": "success"}},
            "all_imported": True,
        }

        artifact = Mock()
        artifact.id = "artifact-id"
        artifact.key = "artifact-key"
        artifact.create.return_value = artifact

        with patch("prefect_workflow.prefect_mardi_importer.get_run_context") as get_ctx, \
            patch("prefect_workflow.prefect_mardi_importer.import_doi_batch", return_value=result_payload) as import_batch, \
            patch("prefect_workflow.prefect_mardi_importer.Artifact", return_value=artifact):
            get_ctx.return_value.flow_run.id = "flow-run-id"

            result = pmi.prefect_mardi_importer_flow("import/doi", dois=["10.1000/xyz"])

        import_batch.assert_called_once_with(["10.1000/xyz"])
        self.assertEqual(result["artifact_id"], "artifact-id")
        self.assertEqual(result["artifact_key"], "artifact-key")
        self.assertEqual(result["flow_run_id"], "flow-run-id")
        self.assertTrue(result["all_imported"])

    def test_flow_import_wikidata_partial_failure_returns_failed(self) -> None:
        """Return Failed state when Wikidata batch not fully imported."""
        result_payload = {
            "qids": ["Q1", "Q2"],
            "count": 2,
            "results": {
                "Q1": {"qid": "Q1", "status": "success"},
                "Q2": {"qid": None, "status": "not_imported"},
            },
            "all_imported": False,
        }

        artifact = Mock()
        artifact.id = "artifact-id"
        artifact.key = "artifact-key"
        artifact.create.return_value = artifact

        with patch("prefect_workflow.prefect_mardi_importer.get_run_context") as get_ctx, \
            patch("prefect_workflow.prefect_mardi_importer.import_wikidata_batch", return_value=result_payload), \
            patch("prefect_workflow.prefect_mardi_importer.Artifact", return_value=artifact):
            get_ctx.return_value.flow_run.id = "flow-run-id"

            result = pmi.prefect_mardi_importer_flow("import/wikidata", qids=["Q1", "Q2"])

        self.assertIsInstance(result, pmi.Failed)
        self.assertEqual(result.data["artifact_id"], "artifact-id")
        self.assertEqual(result.data["flow_run_id"], "flow-run-id")
        self.assertFalse(result.data["all_imported"])
