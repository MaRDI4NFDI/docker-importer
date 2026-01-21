import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests


def _install_prefect_stub() -> None:
    """Install a minimal Prefect stub in sys.modules for tests.

    This prevents import errors when Prefect is not installed.

    Returns:
        None.
    """
    if "prefect" in sys.modules:
        return

    fake_prefect = types.ModuleType("prefect")
    fake_deployments = types.ModuleType("prefect.deployments")

    def run_deployment(*_args, **_kwargs):
        """Placeholder run_deployment for test imports.

        Raises:
            RuntimeError: Always raised if called without a test patch.
        """
        raise RuntimeError("run_deployment stub was called without patching")

    fake_deployments.run_deployment = run_deployment
    fake_prefect.deployments = fake_deployments

    sys.modules["prefect"] = fake_prefect
    sys.modules["prefect.deployments"] = fake_deployments


_install_prefect_stub()

from services import import_service


class TestImportService(unittest.TestCase):
    """Tests for shared service helpers used by CLI and Flask."""

    def test_normalize_list_none(self) -> None:
        """Return an empty list for None input."""
        self.assertEqual(import_service.normalize_list(None), [])

    def test_normalize_list_list(self) -> None:
        """Normalize list input by stripping and dropping empties."""
        value = [" a ", "", "b"]
        self.assertEqual(import_service.normalize_list(value), ["a", "b"])

    def test_normalize_list_string(self) -> None:
        """Split string input by commas and whitespace."""
        value = " a, b   c"
        self.assertEqual(import_service.normalize_list(value), ["a", "b", "c"])

    def test_normalize_list_other(self) -> None:
        """Convert non-list/string input to a single-item list."""
        self.assertEqual(import_service.normalize_list(123), ["123"])

    def test_build_health_payload_default(self) -> None:
        """Build the default health payload."""
        self.assertEqual(
            import_service.build_health_payload(),
            {"status": "healthy", "service": "docker-importer"},
        )

    def test_build_health_payload_custom(self) -> None:
        """Build the health payload with a custom service name."""
        self.assertEqual(
            import_service.build_health_payload("svc"),
            {"status": "healthy", "service": "svc"},
        )

    def test_build_prefect_headers_no_auth(self) -> None:
        """Create Prefect headers without auth."""
        self.assertEqual(
            import_service.build_prefect_headers(None),
            {"Content-Type": "application/json"},
        )

    def test_build_prefect_headers_with_auth(self) -> None:
        """Create Prefect headers with Basic auth."""
        headers = import_service.build_prefect_headers("user:pass")
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertTrue(headers["Authorization"].startswith("Basic "))

    def test_prefect_request_get(self) -> None:
        """Use GET when payload is not provided."""
        response = Mock()
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None

        with patch("services.import_service.requests.get", return_value=response):
            result = import_service.prefect_request(
                "http://prefect",
                None,
                "/path",
                payload=None,
                timeout=1,
            )

        self.assertEqual(result, {"ok": True})

    def test_prefect_request_post(self) -> None:
        """Use POST when payload is provided."""
        response = Mock()
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None

        with patch("services.import_service.requests.post", return_value=response):
            result = import_service.prefect_request(
                "http://prefect",
                None,
                "/path",
                payload={"a": 1},
                timeout=1,
            )

        self.assertEqual(result, {"ok": True})

    def test_get_workflow_status_completed(self) -> None:
        """Return flow run status and result when completed."""
        flow_response = Mock()
        flow_response.json.return_value = {
            "state": {
                "type": "COMPLETED",
                "name": "Completed",
                "timestamp": "2024-01-01T00:00:00Z",
            }
        }
        flow_response.raise_for_status.return_value = None

        result_response = Mock()
        result_response.status_code = 200
        result_response.json.return_value = {"value": 123}

        with patch(
            "services.import_service.requests.get",
            side_effect=[flow_response, result_response],
        ):
            result = import_service.get_workflow_status(
                "http://prefect",
                None,
                "flow-id",
            )

        self.assertEqual(result["id"], "flow-id")
        self.assertEqual(result["state"], "COMPLETED")
        self.assertEqual(result["result"], {"value": 123})

    def test_get_workflow_result_not_completed(self) -> None:
        """Return a 202 payload when the flow run is not completed."""
        with patch(
            "services.import_service.prefect_request",
            return_value={"state": {"type": "RUNNING"}},
        ):
            payload, status_code = import_service.get_workflow_result(
                "http://prefect",
                None,
                "flow-id",
            )

        self.assertEqual(status_code, 202)
        self.assertEqual(payload["state"], "RUNNING")

    def test_get_workflow_result_completed(self) -> None:
        """Return artifact payload when the flow run is completed."""
        with patch(
            "services.import_service.prefect_request",
            side_effect=[
                {"state": {"type": "COMPLETED"}},
                {"id": "a1", "key": "k", "created": "now", "data": {"x": 1}},
            ],
        ):
            payload, status_code = import_service.get_workflow_result(
                "http://prefect",
                None,
                "flow-id",
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["artifact_id"], "a1")

    def test_get_workflow_result_artifact_missing(self) -> None:
        """Return a 404 payload when artifact is missing."""
        error = requests.HTTPError("not found")
        error.response = types.SimpleNamespace(status_code=404)

        with patch(
            "services.import_service.prefect_request",
            side_effect=[{"state": {"type": "COMPLETED"}}, error],
        ):
            payload, status_code = import_service.get_workflow_result(
                "http://prefect",
                None,
                "flow-id",
            )

        self.assertEqual(status_code, 404)
        self.assertEqual(payload["error"], "artifact not found")

    def test_trigger_wikidata_async(self) -> None:
        """Return flow metadata after triggering a Wikidata workflow."""
        flow_run = SimpleNamespace(deployment_id="d1", id="r1", flow_id="f1")
        with patch("services.import_service.run_deployment", return_value=flow_run):
            payload = import_service.trigger_wikidata_async(["Q1"])

        self.assertEqual(payload["deployment_id"], "d1")
        self.assertEqual(payload["qids_queued"], ["Q1"])

    def test_trigger_doi_async(self) -> None:
        """Return flow metadata after triggering a DOI workflow."""
        flow_run = SimpleNamespace(deployment_id="d1", id="r1", flow_id="f1")
        with patch("services.import_service.run_deployment", return_value=flow_run):
            payload = import_service.trigger_doi_async(["10.1/ABC"])

        self.assertEqual(payload["deployment_id"], "d1")
        self.assertEqual(payload["dois_queued"], ["10.1/ABC"])

    def test_import_wikidata_sync(self) -> None:
        """Return per-QID status and overall success."""
        importer = Mock()
        importer.import_entities.side_effect = ["Q1", None, Exception("boom")]

        with patch("services.import_service.log.error"):
            with patch(
                "services.import_service.WikidataImporter", return_value=importer
            ):
                payload, all_ok = import_service.import_wikidata_sync(
                    ["Q1", "Q2", "Q3"]
                )

        self.assertFalse(all_ok)
        self.assertEqual(payload["results"]["Q1"]["status"], "success")
        self.assertEqual(payload["results"]["Q2"]["status"], "not_imported")
        self.assertEqual(payload["results"]["Q3"]["status"], "error")

    def test_import_doi_sync(self) -> None:
        """Return per-DOI results and overall success flag."""
        arxiv_source = Mock()
        zenodo_source = Mock()
        crossref_source = Mock()

        arxiv_publication = Mock()
        arxiv_publication.create.return_value = "Q1"
        arxiv_source.new_publication.return_value = arxiv_publication

        zenodo_publication = Mock()
        zenodo_publication.create.return_value = "Q2"
        zenodo_source.new_resource.return_value = zenodo_publication

        crossref_publication = Mock()
        crossref_publication.create.return_value = None
        crossref_source.new_publication.return_value = crossref_publication

        with patch(
            "services.import_service.Importer.create_source",
            side_effect=[arxiv_source, zenodo_source, crossref_source],
        ):
            payload, all_ok = import_service.import_doi_sync(
                ["10.1234/arxiv.0001", "10.5281/zenodo.123", "10.5555/xyz"]
            )

        self.assertFalse(all_ok)
        self.assertEqual(payload["results"]["10.1234/ARXIV.0001"]["status"], "success")
        self.assertEqual(payload["results"]["10.5281/ZENODO.123"]["status"], "success")
        self.assertEqual(payload["results"]["10.5555/XYZ"]["status"], "not_found")


if __name__ == "__main__":
    unittest.main()
