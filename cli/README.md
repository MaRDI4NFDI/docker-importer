# docker-importer CLI

Small command-line wrapper around the same functionality exposed by
`flask_app/app.py`. All commands print JSON to stdout and use process
exit codes to indicate success (0) or failure (non-zero).

## Usage

```bash
python -m cli.importer_cli health
```

### Wikidata import (async via Prefect)

```bash
python -m cli.importer_cli import-wikidata-async --qids Q42 Q1
python -m cli.importer_cli import-wikidata-async --qids "Q42,Q1"
```

### Wikidata import (sync)

```bash
python -m cli.importer_cli import-wikidata --qids Q42 Q1
```

### DOI import (async via Prefect)

```bash
python -m cli.importer_cli import-doi-async --dois 10.48550/ARXIV.2101.00001
```

### DOI import (sync)

```bash
python -m cli.importer_cli import-doi --dois 10.48550/ARXIV.2101.00001
```

### CRAN import (sync)

```bash
python -m cli.importer_cli import-cran --packages dplyr ggplot2
python -m cli.importer_cli import-cran --packages "dplyr,ggplot2"
```

### CSV Dry-run mode (simulation without writes)

The `--csv-only` flag simulates an import without actually writing to Wikibase or the database. 
All would-be writes are captured to a CSV file instead.

Required environment variables:
```bash
export SPARQL_ENDPOINT_URL=http://staging-wdqs:9999/bigdata/namespace/wdq/sparql
export WIKIBASE_URL=http://staging-wikibase-apache
```

Examples:
```bash
# DOI import dry-run
python -m cli.importer_cli import-doi --dois 10.1234/example --csv-only
python -m cli.importer_cli import-doi --dois 10.1234/example --csv-only --csv-path /tmp/dryrun.csv

# Wikidata import dry-run
python -m cli.importer_cli import-wikidata --qids Q42 --csv-only

# CRAN import dry-run
python -m cli.importer_cli import-cran --packages dplyr --csv-only --csv-path /tmp/cran_dryrun.csv
```

The CSV output includes:
- `timestamp`: ISO8601 timestamp of each operation
- `entity_type`: item, property, author, etc.
- `source`: crossref, arxiv, zenodo, cran, wikidata
- `external_id`: Original identifier (DOI, arXiv ID, etc.)
- `labels`, `descriptions`, `claims`: Entity data as JSON
- `stub_id`: Generated ID (Q0001, P0001, etc.)
- `parent_stub_id`: For nested entities (e.g., author of publication)
- `operation`: create, already_exists, or update
- `existing_qid`: QID if entity already exists (SPARQL lookup)

### Prefect flow run status

```bash
python -m cli.importer_cli import-workflow-status --id <flow_run_id>
```

### Prefect artifact result

```bash
python -m cli.importer_cli import-workflow-result --id <flow_run_id>
python -m cli.importer_cli import-workflow-result --id <flow_run_id> --key-prefix custom-prefix-
```

## Secrets

Make sure to rename the provided `secrets.example.txt` file to `secrets.txt` and fill in the
required values.
