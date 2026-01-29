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
