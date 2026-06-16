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
    create_item,
    update_item,
    import_wikidata_async,
    import_workflow_status,
    import_workflow_result,
    import_wikidata,
    import_doi_async,
    import_doi,
    import_cran,
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

    @patch("prefect.deployments.run_deployment")
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

    @patch("prefect.deployments.run_deployment")
    def test_import_wikidata_async_trigger_error(self, mock_run) -> None:
        """Test async Wikidata import with trigger failure."""
        mock_run.side_effect = RuntimeError("Prefect error")

        fake_request.get_json.return_value = {"qids": ["Q1"]}
        response, status = import_wikidata_async()
        self.assertEqual(status, 500)
        self.assertEqual(response["error"], "Could not start background job")
        self.assertIn("Prefect error", response["details"])

    @patch("requests.get")
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

    @patch("requests.get")
    def test_import_workflow_status_http_error(self, mock_get) -> None:
        """Test workflow status with HTTP error."""
        from requests import HTTPError

        mock_get.side_effect = HTTPError("404 Not Found")

        fake_request.args.get.side_effect = None
        fake_request.args.get.return_value = "run1"
        response, status = import_workflow_status()
        self.assertEqual(status, 404)
        self.assertEqual(response["error"], "could not fetch flow run")

    @patch("requests.get")
    def test_import_workflow_result_success(self, mock_get) -> None:
        """Test successful workflow result retrieval."""
        mock_resp1 = Mock()
        mock_resp1.json.return_value = {"state": {"type": "COMPLETED"}}
        mock_resp1.raise_for_status.return_value = None
        mock_resp2 = Mock()
        mock_resp2.json.return_value = {
            "id": "a1",
            "key": "k",
            "created": "now",
            "data": {"x": 1},
        }
        mock_resp2.raise_for_status.return_value = None
        mock_get.side_effect = [mock_resp1, mock_resp2]

        fake_request.args.get.side_effect = (
            lambda *args: "run1" if args[0] == "id" else "mardi-importer-result-"
        )
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

    @patch("requests.get")
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

    @patch("prefect.deployments.run_deployment")
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

    def test_import_wikidata_missing_qids(self) -> None:
        """Test sync Wikidata import with missing QIDs."""
        fake_request.get_json.return_value = {}
        response, status = import_wikidata()
        self.assertEqual(status, 400)
        self.assertEqual(response["error"], "missing qids")

    def test_import_wikidata_success(self) -> None:
        """Test successful sync Wikidata import."""
        fake_request.get_json.return_value = {"qids": ["Q1"]}
        importer = Mock()
        importer.import_entities.return_value = "Q1"

        with patch(
            "services.import_service.WikidataImporter",
            return_value=importer,
        ):
            response, status = import_wikidata()

        self.assertEqual(status, 200)
        self.assertEqual(response["results"]["Q1"]["status"], "success")

    def test_import_doi_missing_dois(self) -> None:
        """Test sync DOI import with missing DOIs."""
        fake_request.get_json.return_value = {}
        response, status = import_doi()
        self.assertEqual(status, 400)
        self.assertEqual(response["error"], "missing doi")

    def test_import_doi_success(self) -> None:
        """Test successful sync DOI import."""
        fake_request.get_json.return_value = {"dois": ["10.1234/arxiv.0001"]}

        arxiv_source = Mock()
        zenodo_source = Mock()
        crossref_source = Mock()

        arxiv_publication = Mock()
        arxiv_publication.create.return_value = "Q1"
        arxiv_source.new_publication.return_value = arxiv_publication

        zenodo_publication = Mock()
        zenodo_publication.create.return_value = None
        zenodo_source.new_resource.return_value = zenodo_publication

        crossref_publication = Mock()
        crossref_publication.create.return_value = None
        crossref_source.new_publication.return_value = crossref_publication

        with patch(
            "services.import_service.Importer.create_source",
            side_effect=[arxiv_source, zenodo_source, crossref_source],
        ):
            response, status = import_doi()

        self.assertEqual(status, 200)
        self.assertEqual(response["results"]["10.1234/ARXIV.0001"]["status"], "success")

    def test_import_cran_missing_packages(self) -> None:
        """Test CRAN import with missing packages."""
        fake_request.get_json.return_value = {}
        response, status = import_cran()
        self.assertEqual(status, 400)
        self.assertEqual(response["error"], "missing packages")

    def test_import_cran_success(self) -> None:
        """Test successful CRAN import."""
        fake_request.get_json.return_value = {"packages": ["dplyr"]}

        import pandas as pd

        cran_source = Mock()
        cran_source.pull.return_value = pd.DataFrame(
            [{"Date": "2024-01-01", "Package": "dplyr", "Title": "Dplyr"}]
        )
        software = Mock()
        software.exists.return_value = True
        software.is_updated.return_value = True
        software.QID = "Q1"

        with patch(
            "services.import_service.Importer.create_source",
            return_value=cran_source,
        ):
            with patch("services.import_service.RPackage", return_value=software):
                response, status = import_cran()

        self.assertEqual(status, 200)
        self.assertEqual(response["results"]["dplyr"]["status"], "success")


    def test_create_item_missing_username(self) -> None:
        """Test create item without credentials returns 401."""
        fake_request.get_json.return_value = {"label": "Test item"}
        response, status = create_item()
        self.assertEqual(status, 401)
        self.assertIn("username", response["error"])

    def test_create_item_missing_password(self) -> None:
        """Test create item with username but no password returns 401."""
        fake_request.get_json.return_value = {"label": "Test item", "username": "testuser"}
        response, status = create_item()
        self.assertEqual(status, 401)
        self.assertIn("password", response["error"])

    def test_create_item_missing_label(self) -> None:
        """Test create item with credentials but missing label."""
        fake_request.get_json.return_value = {"username": "testuser", "password": "testpass"}
        response, status = create_item()
        self.assertEqual(status, 400)
        self.assertEqual(response["error"], "missing label")

    def test_create_item_success(self) -> None:
        """Test successful item creation."""
        fake_request.get_json.return_value = {
            "label": "Test item",
            "description": "A test",
            "claims": {"wdt:P31": "wd:Q5"},
            "username": "testuser",
            "password": "testpass",
        }
        mock_result = Mock()
        mock_result.id = "Q999"
        mock_item = Mock()
        mock_item.write.return_value = mock_result
        mock_api = Mock()
        mock_api.item.new.return_value = mock_item

        with patch("services.import_service.MardiClient", return_value=mock_api):
            response, status = create_item()

        self.assertEqual(status, 200)
        self.assertEqual(response["qid"], "Q999")
        self.assertEqual(response["status"], "success")
        mock_item.labels.set.assert_called_once_with(language="en", value="Test item")
        mock_item.descriptions.set.assert_called_once_with(language="en", value="A test")
        mock_item.add_claim.assert_called_once_with("wdt:P31", "wd:Q5")

    def test_create_item_write_failure(self) -> None:
        """Test item creation when write returns no ID."""
        fake_request.get_json.return_value = {
            "label": "Test item",
            "username": "testuser",
            "password": "testpass",
        }
        mock_result = Mock()
        mock_result.id = None
        mock_item = Mock()
        mock_item.write.return_value = mock_result
        mock_api = Mock()
        mock_api.item.new.return_value = mock_item

        with patch("services.import_service.MardiClient", return_value=mock_api):
            response, status = create_item()

        self.assertEqual(status, 500)
        self.assertEqual(response["status"], "error")


    def test_update_item_missing_username(self) -> None:
        """Test update item without credentials returns 401."""
        fake_request.get_json.return_value = {"qid": "Q1", "label": "x"}
        response, status = update_item()
        self.assertEqual(status, 401)
        self.assertIn("username", response["error"])

    def test_update_item_missing_password(self) -> None:
        """Test update item with username but no password returns 401."""
        fake_request.get_json.return_value = {"qid": "Q1", "label": "x", "username": "testuser"}
        response, status = update_item()
        self.assertEqual(status, 401)
        self.assertIn("password", response["error"])

    def test_update_item_missing_qid(self) -> None:
        fake_request.get_json.return_value = {"label": "x", "username": "testuser", "password": "testpass"}
        response, status = update_item()
        self.assertEqual(status, 400)
        self.assertIn("qid", response["error"])

    def test_update_item_non_string_qid(self) -> None:
        fake_request.get_json.return_value = {"qid": 123, "label": "x", "username": "testuser", "password": "testpass"}
        response, status = update_item()
        self.assertEqual(status, 400)
        self.assertIn("qid", response["error"])

    def test_update_item_invalid_label_type(self) -> None:
        fake_request.get_json.return_value = {"qid": "Q1", "label": 42, "username": "testuser", "password": "testpass"}
        response, status = update_item()
        self.assertEqual(status, 400)
        self.assertIn("label", response["error"])

    def test_update_item_invalid_description_type(self) -> None:
        fake_request.get_json.return_value = {"qid": "Q1", "description": ["bad"], "username": "testuser", "password": "testpass"}
        response, status = update_item()
        self.assertEqual(status, 400)
        self.assertIn("description", response["error"])

    def test_update_item_invalid_claims_type(self) -> None:
        fake_request.get_json.return_value = {"qid": "Q1", "claims": "bad", "username": "testuser", "password": "testpass"}
        response, status = update_item()
        self.assertEqual(status, 400)
        self.assertIn("claims", response["error"])

    def test_update_item_empty_payload(self) -> None:
        fake_request.get_json.return_value = {"qid": "Q1", "username": "testuser", "password": "testpass"}
        response, status = update_item()
        self.assertEqual(status, 400)
        self.assertIn("at least one", response["error"])

    def test_update_item_success(self) -> None:
        fake_request.get_json.return_value = {"qid": "Q1", "label": "New label", "username": "testuser", "password": "testpass"}
        with patch("flask_app.app.update_item_sync",
                   return_value=({"qid": "Q1", "status": "updated"}, True)):
            response, status = update_item()
        self.assertEqual(status, 200)
        self.assertEqual(response["status"], "updated")

    def test_update_item_conflict(self) -> None:
        fake_request.get_json.return_value = {"qid": "Q1", "claims": {"P16": "Q99"}, "username": "testuser", "password": "testpass"}
        conflict = {
            "qid": "Q1",
            "status": "conflict",
            "error": "P16 already has values",
            "existing_values": ["Q50"],
        }
        with patch("flask_app.app.update_item_sync", return_value=(conflict, False)):
            response, status = update_item()
        self.assertEqual(status, 409)
        self.assertEqual(response["status"], "conflict")

    def test_update_item_not_found(self) -> None:
        fake_request.get_json.return_value = {"qid": "Q999", "label": "x", "username": "testuser", "password": "testpass"}
        not_found = {"qid": "Q999", "status": "not_found", "error": "Item not found"}
        with patch("flask_app.app.update_item_sync", return_value=(not_found, False)):
            response, status = update_item()
        self.assertEqual(status, 404)
        self.assertEqual(response["status"], "not_found")

    def test_update_item_do_override_string_false(self) -> None:
        """String 'false' must not enable override mode."""
        fake_request.get_json.return_value = {
            "qid": "Q1", "claims": {"P16": "Q99"}, "do_override": "false",
            "username": "testuser", "password": "testpass",
        }
        conflict = {"qid": "Q1", "status": "conflict", "error": "x", "existing_values": []}
        with patch("flask_app.app.update_item_sync", return_value=(conflict, False)) as m:
            _, status = update_item()
        self.assertEqual(status, 409)
        self.assertFalse(m.call_args.kwargs["do_override"])


if __name__ == "__main__":
    unittest.main()
