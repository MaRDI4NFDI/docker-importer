Web API Endpoints
=============

Base URL
--------

Local (e.g. via Kubernetes port-forward)::

  http://localhost:3333

This assumes a port-forward like this::

  kubectl port-forward -n production svc/importer 3333:80

All endpoints return JSON unless noted otherwise.

Common request format
---------------------

For endpoints that accept JSON, send::

  Content-Type: application/json

In curl::

  -H "Content-Type: application/json"

REMARK: if you are having a proxy server, you might want to try::

  curl --noproxy "*" ...
  Example for the health endpoint: curl --noproxy "*" -sS http://localhost:3333/health

Endpoints
---------

GET /health
~~~~~~~~~~~

Health probe for liveness/readiness checks.

**Request**

Browser::

  http://localhost:3333/health

curl::

  curl -sS http://localhost:3333/health | jq .

**Response (200)**

.. code-block:: json

  {
    "status": "healthy",
    "service": "mardi-importer-api"
  }


GET /version
~~~~~~~~~~~~

Returns the current service version from the root ``VERSION`` file.

**Request**

Browser::

  http://localhost:3333/version

curl::

  curl -sS http://localhost:3333/version | jq .

**Response (200)**

.. code-block:: json

  {
    "version": "0.0.1"
  }


GET /import/workflow_status
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Returns the current Prefect state of a flow run.

**Query params**

- ``id`` (required): Prefect flow run id (UUID)

**Request**

curl::

  curl -sS "http://localhost:3333/import/workflow_status?id=16e267d3-61d0-4484-b8ad-1af9221a969c" | jq .

**Response (200)**

.. code-block:: json

  {
    "id": "16e267d3-61d0-4484-b8ad-1af9221a969c",
    "state": "RUNNING",
    "state_name": "Running",
    "timestamp": "2026-01-19T10:45:00.000Z"
  }

If the run is completed, the endpoint may also include ``result`` (depending on Prefect configuration/API behavior).

**Errors**

- 400 if ``id`` missing
- 404 if Prefect cannot find the flow run
- 500 on other Prefect/API errors


GET /import/workflow_result
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fetches the stored Prefect artifact that contains the final batch-import result.

This endpoint assumes your Prefect flow creates an artifact with key::

  mardi-importer-result-<FLOW_RUN_ID>

**Query params**

- ``id`` (required): Prefect flow run id (UUID)
- ``key_prefix`` (optional): artifact key prefix, default ``mardi-importer-result-``

**Request**

curl (flow run id only)::

  curl -sS "http://localhost:3333/import/workflow_result?id=16e267d3-61d0-4484-b8ad-1af9221a969c" | jq .

**Response**

- **202** if the flow run is not completed yet

.. code-block:: json

  {
    "id": "16e267d3-61d0-4484-b8ad-1af9221a969c",
    "state": "RUNNING",
    "message": "flow run not completed yet"
  }

- **200** when completed and artifact exists

.. code-block:: json

  {
    "id": "16e267d3-61d0-4484-b8ad-1af9221a969c",
    "state": "COMPLETED",
    "artifact_id": "bab18338-c069-4d29-9512-ba00e5df55c6",
    "key": "mardi-importer-result-16e267d3-61d0-4484-b8ad-1af9221a969c",
    "created": "2026-01-19T10:47:12.345Z",
    "data": {
      "qids": ["Q42","Q1"],
      "count": 2,
      "results": {
        "Q42": {"qid": "Q123", "status": "success"},
        "Q1": {"qid": null, "status": "error", "error": "..."}
      },
      "all_imported": false
    }
  }

- **404** if the flow run is completed but artifact is not found (key mismatch or artifact not created)

**Direct Prefect API debugging**

If you know the flow run id and use Basic Auth on Prefect::

  curl -u "admin:pass" \
    "http://prefect-mardi.zib.de/api/artifacts/mardi-importer-result-16e267d3-61d0-4484-b8ad-1af9221a969c/latest" | jq .


GET /import/workflow_runs
~~~~~~~~~~~~~~~~~~~~~~~~~

Returns information about Prefect flow runs from the last 24 hours (excluding ``SCHEDULED``).

**Request**

curl::

  curl -sS "http://localhost:3333/import/workflow_runs" | jq .

**Response (200)**

Response includes many additional Prefect fields not shown here.

.. code-block:: json

  [
    {
      "id": "c3faea1b-e118-461f-9a42-9111763097e7",
      "name": "important-grouse",
      "state_type": "COMPLETED",
      "start_time": "2026-02-02T11:00:01.766235Z",
      "parameters": {
        "qids": ["Q26708"],
        "action": "import/wikidata"
      }
    }
  ]


POST /import/wikidata
~~~~~~~~~~~~~~~~~~~~~

Runs the Wikidata import synchronously in the Flask process (no Prefect).

**Request**

curl::

  curl -sS -X POST "http://localhost:3333/import/wikidata" \
    -H "Content-Type: application/json" \
    -d '{"qids":["Q42","Q1"]}' | jq .

**Response (200)**

.. code-block:: json

  {
    "qids": ["Q42","Q1"],
    "count": 2,
    "results": {
      "Q42": {"qid": "Q123", "status": "success"},
      "Q1": {"qid": null, "status": "error", "error": "..."}
    },
    "all_imported": false
  }

**Errors**

- 400 if ``qids`` missing


POST /import/wikidata_async
~~~~~~~~~~~~~~~~~~~~~~~~~~

Triggers the Prefect deployment asynchronously (returns immediately with a Flow Run ID).

This endpoint accepts a JSON request body.

**Request**

curl::

  curl -sS -X POST "http://localhost:3333/import/wikidata_async" \
    -H "Content-Type: application/json" \
    -d '{"qids":["Q42","Q1"]}' | jq .

**Response (202)**

.. code-block:: json

  {
    "status": "accepted",
    "message": "Wikidata import process started in background",
    "deployment_id": "...",
    "id": "16e267d3-61d0-4484-b8ad-1af9221a969c",
    "flow_id": "...",
    "qids_queued": ["Q42","Q1"]
  }

**Errors**

- 400 if ``qids`` missing
- 500 if triggering the Prefect deployment fails


POST /import/doi
~~~~~~~~~~~~~~~~

Imports publications/resources given DOIs (or special pseudo-DOIs for arXiv/Zenodo per project logic).

**Request**

curl::

  curl -sS -X POST "http://localhost:3333/import/doi" \
    -H "Content-Type: application/json" \
    -d '{"dois":["10.1000/xyz123","ARXIV.2101.00001","ZENODO.1234567"]}' | jq .

**Response (200)**

.. code-block:: json

  {
    "dois": ["10.1000/XYZ123","ARXIV.2101.00001","ZENODO.1234567"],
    "count": 3,
    "results": {
      "10.1000/XYZ123": {"qid": "Q...", "status": "success"},
      "ARXIV.2101.00001": {"qid": "Q...", "status": "success"},
      "ZENODO.1234567": {"qid": null, "status": "not_found", "error": "DOI was not found."}
    },
    "all_imported": false
  }

**Errors**

- 400 if ``dois`` missing


POST /import/doi_async
~~~~~~~~~~~~~~~~~~~~~~~~~~

Triggers the Prefect deployment asynchronously (returns immediately with a Flow Run ID).

This endpoint accepts a JSON request body.

**Request**

curl::

  curl -sS -X POST "http://localhost:3333/import/doi_async" \
    -H "Content-Type: application/json" \
    -d '{"dois":["10.1000/XYZ123","ARXIV.2101.00001"]}' | jq .

**Response (202)**

.. code-block:: json

  {
    "status": "accepted",
    "message": "DOI import process started in background",
    "deployment_id": "...",
    "id": "16e267d3-61d0-4484-b8ad-1af9221a969c",
    "flow_id": "...",
    "dois_queued": ["10.1000/XYZ123","ARXIV.2101.00001"]
  }

**Errors**

- 400 if ``dois`` missing
- 500 if triggering the Prefect deployment fails
