import sys
import types
import unittest
from unittest.mock import Mock, patch

from tests.prefect_stub import install_prefect_stub

install_prefect_stub(force=True)

# Mock Flask module to avoid dependency
def _install_flask_stub() -> None:
    """Install a minimal Flask stub for tests."""
    if "flask" in sys.modules:
        return

    def jsonify(data=None, **kwargs):
        if kwargs:
            return kwargs
        return data

    fake_request = Mock()

    class FakeFlask:
        def __init__(self, name):
            self.name = name

        def get(self, path):
            def decorator(func):
                return func
            return decorator

        def post(self, path):
            def decorator(func):
                return func
            return decorator

    fake_flask = types.ModuleType("flask")
    fake_flask.Flask = FakeFlask
    fake_flask.jsonify = jsonify
    fake_flask.request = fake_request

    sys.modules["flask"] = fake_flask

_install_flask_stub()

from flask_app.app import (
    health,
    import_wikidata_async,
    import_workflow_status,
    import_workflow_result,
    import_wikidata,
    import_doi_async,
    import_doi,
)
from services import import_service

# Get the fake request
fake_request = sys.modules["flask"].request


class TestFlaskApp(unittest.TestCase):
    """Tests for Flask web endpoints."""

    def setUp(self) -> None:
        # Ensure PREFECT_API_AUTH_STRING is None for tests
        import flask_app.app
        flask_app.app.PREFECT_API_AUTH_STRING = None

        # Suppress logging output during tests
        import logging
        self.logger = logging.getLogger("flask_app.app")
        self.original_level = self.logger.level
        self.logger.setLevel(logging.CRITICAL)

    def tearDown(self) -> None:
        # Restore original logging level
        self.logger.setLevel(self.original_level)

    def test_health_endpoint(self) -> None:
        """Test health endpoint returns healthy status."""
        response, status = health()
        self.assertEqual(status, 200)
        self.assertEqual(response["status"], "healthy")
        self.assertEqual(response["service"], "docker-importer")

    @patch("services.import_service.run_deployment")
    def test_import_wikidata_async_success(self, mock_run) -> None:
        """Test successful async Wikidata import trigger."""
        mock_run.return_value = Mock(deployment_id="dep1", id="run1", flow_id="flow1")

        fake_request.get_json.return_value = {"qids": ["Q1"]}
        response, status = import_wikidata_async()
        self.assertEqual(status, 202)
        self.assertEqual(response["id"], "run1")
        self.assertEqual(response["qids_queued"], ["Q1"])

    def test_import_wikidata_async_missing_qids(self) -> None:
        """Test async Wikidata import with missing QIDs."""
        fake_request.get_json.return_value = {}
        response, status = import_wikidata_async()
        self.assertEqual(status, 400)
        self.assertEqual(response["error"], "missing qids")

    @patch("services.import_service.run_deployment")
    def test_import_wikidata_async_trigger_error(self, mock_run) -> None:
        """Test async Wikidata import with trigger failure."""
        mock_run.side_effect = RuntimeError("Prefect error")

        fake_request.get_json.return_value = {"qids": ["Q1"]}
        response, status = import_wikidata_async()
        self.assertEqual(status, 500)
        self.assertEqual(response["error"], "Could not start background job")
        self.assertIn("Prefect error", response["details"])

    @patch('requests.get')
    def test_import_workflow_status_success(self, mock_get) -> None:
        """Test successful workflow status retrieval."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "state": {
                "type": "COMPLETED",
                "name": "Completed",
                "timestamp": "2024-01-01T00:00:00Z",
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fake_request.args.get.side_effect = None
        fake_request.args.get.return_value = "run1"
        response, status = import_workflow_status()
        self.assertEqual(status, 200)
        self.assertEqual(response["state"], "COMPLETED")

    def test_import_workflow_status_missing_id(self) -> None:
        """Test workflow status with missing ID."""
        fake_request.args.get.side_effect = None
        fake_request.args.get.return_value = None
        response, status = import_workflow_status()
        self.assertEqual(status, 400)
        self.assertEqual(response["error"], "missing flow run id (parameter: id)")

    @patch('requests.get')
    def test_import_workflow_status_http_error(self, mock_get) -> None:
        """Test workflow status with HTTP error."""
        from requests import HTTPError
        mock_get.side_effect = HTTPError("404 Not Found")

        fake_request.args.get.side_effect = None
        fake_request.args.get.return_value = "run1"
        response, status = import_workflow_status()
        self.assertEqual(status, 404)
        self.assertEqual(response["error"], "could not fetch flow run")

    @patch('requests.get')
    def test_import_workflow_result_success(self, mock_get) -> None:
        """Test successful workflow result retrieval."""
        mock_resp1 = Mock()
        mock_resp1.json.return_value = {"state": {"type": "COMPLETED"}}
        mock_resp1.raise_for_status.return_value = None
        mock_resp2 = Mock()
        mock_resp2.json.return_value = {"id": "a1", "key": "k", "created": "now", "data": {"x": 1}}
        mock_resp2.raise_for_status.return_value = None
        mock_get.side_effect = [mock_resp1, mock_resp2]

        fake_request.args.get.side_effect = lambda *args: "run1" if args[0] == "id" else "mardi-importer-result-"
        response, status = import_workflow_result()
        self.assertEqual(status, 200)
        self.assertEqual(response["artifact_id"], "a1")

    def test_import_workflow_result_missing_id(self) -> None:
        """Test workflow result with missing ID."""
        fake_request.args.get.side_effect = None
        fake_request.args.get.return_value = None
        response, status = import_workflow_result()
        self.assertEqual(status, 400)
        self.assertEqual(response["error"], "missing flow run id (parameter: id)")

    @patch('requests.get')
    def test_import_workflow_result_http_error(self, mock_get) -> None:
        """Test workflow result with HTTP error."""
        from requests import HTTPError
        mock_resp = Mock()
        mock_resp.json.return_value = {"state": {"type": "COMPLETED"}}
        mock_resp.raise_for_status.return_value = None
        mock_get.side_effect = [mock_resp, HTTPError("500 Internal Error")]

        fake_request.args.get.side_effect = None
        fake_request.args.get.return_value = "run1"
        response, status = import_workflow_result()
        self.assertEqual(status, 500)
        self.assertEqual(response["error"], "prefect api error")

    @patch("services.import_service.run_deployment")
    def test_import_doi_async_success(self, mock_run) -> None:
        """Test successful async DOI import trigger."""
        mock_run.return_value = Mock(deployment_id="dep2", id="run2", flow_id="flow2")

        fake_request.get_json.return_value = {"dois": ["10.123/abc"]}
        response, status = import_doi_async()
        self.assertEqual(status, 202)
        self.assertEqual(response["dois_queued"], ["10.123/ABC"])

    def test_import_doi_async_missing_dois(self) -> None:
        """Test async DOI import with missing DOIs."""
        fake_request.get_json.return_value = {}
        response, status = import_doi_async()
        self.assertEqual(status, 400)
        self.assertEqual(response["error"], "missing dois")


if __name__ == "__main__":
    unittest.main()
