# docker-importer CLI

Small command-line wrapper around the same functionality exposed by
`flask_app/app.py`. All commands print JSON to stdout and use process
exit codes to indicate success (0) or failure (non-zero).

## Usage

```bash
python -m cli.main health
```

### Wikidata import (async via Prefect)

```bash
python -m cli.main import-wikidata-async --qids Q42 Q1
python -m cli.main import-wikidata-async --qids "Q42,Q1"
```

### Wikidata import (sync)

```bash
python -m cli.main import-wikidata --qids Q42 Q1
```

### DOI import (async via Prefect)

```bash
python -m cli.main import-doi-async --dois 10.48550/ARXIV.2101.00001
```

### DOI import (sync)

```bash
python -m cli.main import-doi --dois 10.48550/ARXIV.2101.00001
```

### Prefect flow run status

```bash
python -m cli.main import-workflow-status --id <flow_run_id>
```

### Prefect artifact result

```bash
python -m cli.main import-workflow-result --id <flow_run_id>
python -m cli.main import-workflow-result --id <flow_run_id> --key-prefix custom-prefix-
```

## Environment

- `PREFECT_API_URL` (default: `http://prefect-mardi.zib.de/api`)
- `PREFECT_API_AUTH_STRING` (optional, `user:pass` for Basic Auth)
