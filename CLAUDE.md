# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

`docker-importer` is a Docker service that imports metadata from external sources (Wikidata, zbMATH, CRAN, arXiv, Crossref, Zenodo, polyDB, ORCID) into the [MaRDI knowledge graph](https://portal.mardi4nfdi.de) — a Wikibase instance. It exposes a Flask HTTP API, a CLI tool, and a Prefect workflow for background batch processing.

## Commands

### Build Docker image
```bash
docker build -t ghcr.io/mardi4nfdi/docker-importer:main .
```

### Install for local development
```bash
pip install -r tests/requirements_tests.txt  # minimal test deps
pip install -U -e mardi_importer/            # install mardi_importer package in editable mode
```

### Run tests
```bash
bash tests/run_tests.sh                      # discover and run all tests
python -m unittest tests/test_flask_app.py  # run a single test file
```

### Run the Flask app locally
```bash
gunicorn -w 2 --timeout 300 -b 0.0.0.0:8000 flask_app.app:app
# or for dev:
python flask_app/app.py
```

### Run the CLI
```bash
python -m cli.importer_cli --help
python -m cli.importer_cli health
python -m cli.importer_cli import-wikidata --qids Q42 Q43
python -m cli.importer_cli import-doi --dois 10.1234/example
python -m cli.importer_cli import-cran --packages dplyr ggplot2
```

## Architecture

### Core package: `mardi_importer/`
The `mardi_importer` Python package (installed via `setup.py`) is the heart of the system. Its modules:

- **`importer.py` / `Importer`** — a class-level registry. Each data source registers itself with `Importer.register(name, cls, USER_ENV, PASS_ENV)`. `Importer.create_source(name)` instantiates the source from environment variables and authenticates against the Wikibase instance.
- **`base/ADataSource.py`** — abstract base class for all sources. Sources are singletons; `setup()` runs once (tracked via `/tmp/mardi_importer/` marker files). Each source gets a `MardiClient` (from the `mardiclient` package) for writing to Wikibase, and optionally a `WikidataImporter` for pulling from Wikidata.
- **`wikidata/WikidataImporter.py`** — imports Wikidata entities by QID into the local Wikibase. Used both directly and as a dependency within source `setup()` calls.
- **Source modules** (`arxiv/`, `cran/`, `crossref/`, `polydb/`, `zbmath/`, `zenodo/`) — each implements `setup()`, `pull()`, and `push()` from `ADataSource`.

### Flask API: `flask_app/app.py`
HTTP endpoints served by gunicorn. All import logic is delegated to `services/import_service.py`. Two patterns:
- **Sync** (`POST /import/wikidata`, `/import/doi`, `/import/cran`): imports happen in-process and return results directly.
- **Async** (`POST /import/wikidata_async`, `/import/doi_async`): triggers a Prefect deployment and returns a flow run ID for polling.

### Services layer: `services/import_service.py`
Shared logic consumed by both `flask_app/app.py` and `cli/importer_cli.py`. Contains `import_wikidata_sync`, `import_doi_sync`, `import_cran_sync`, and functions to trigger/poll Prefect flows.

### Prefect workflow: `prefect_workflow/prefect_mardi_importer.py`
Defines the `mardi-importer` Prefect flow that runs batch imports. Secrets (passwords, DB credentials) are loaded from Prefect `Secret` blocks at task runtime. The flow is deployed as `mardi-importer/prefect-mardi-importer` and accepts `action`, `qids`, and `dois` parameters.

### CLI: `cli/importer_cli.py`
Thin wrapper over `services/import_service.py`. Credentials can be loaded from `cli/secrets.txt` (copy from `cli/secrets.example.txt`).

## Key environment variables

| Variable | Purpose |
|---|---|
| `MEDIAWIKI_API_URL` | Wikibase MediaWiki API (e.g. `http://wikibase/w/api.php`) |
| `WIKIBASE_URL` | Wikibase base URL |
| `SPARQL_ENDPOINT_URL` | SPARQL endpoint |
| `IMPORTER_API_URL` | Internal importer API URL |
| `{SOURCE}_USER` / `{SOURCE}_PASS` | Credentials per source (e.g. `ARXIV_USER`, `CRAN_PASS`) |
| `PREFECT_API_URL` | Prefect server API (default: `http://prefect-mardi.zib.de/api`) |
| `PREFECT_API_AUTH_STRING` | `user:pass` for Prefect basic auth |

## Adding a new data source

1. Create a new module under `mardi_importer/mardi_importer/<source>/`.
2. Implement a class extending `ADataSource` with `setup()`, `pull()`, and `push()`.
3. Register it in `mardi_importer/mardi_importer/__init__.py` via `Importer.register(...)`.
4. Add Flask endpoints in `flask_app/app.py` and service functions in `services/import_service.py` following the existing pattern.

## Git workflow

When pushing to any `MaRDI4NFDI` repository, all changes must go through a pull request — do not push directly to `main`. Commits must be GPG-signed.

**Exception:** `MaRDI4NFDI/mardi_doip_server` — direct pushes to `main` are allowed for this repo.

```bash
git checkout -b my-feature
# make changes
git commit -S -m "my commit message"
git push origin my-feature
# then open a PR on GitHub
```

`git commit -S` requires an interactive terminal for the GPG passphrase prompt — it cannot be run from a non-interactive context. If signing fails with `Inappropriate ioctl for device`, run the commit manually in your terminal:

```bash
export GPG_TTY=$(tty)
git commit -S -m "my commit message"
```

## External dependencies

- [`mardiclient`](https://github.com/MaRDI4NFDI/mardiclient) — client for writing to the MaRDI Wikibase
- [`WikibaseIntegrator`](https://github.com/LeMyst/WikibaseIntegrator) — both are installed from source in Docker (see `Dockerfile`)
- A custom `ContentMath` datatype is patched into `WikibaseIntegrator` at Docker build time (`config/contentmath.py`)
