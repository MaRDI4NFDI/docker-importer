# docker-importer CLI

Small command-line wrapper around the same functionality exposed by
`mardi_portal/api/app.py`. All commands print JSON to stdout and use process
exit codes to indicate success (0) or failure (non-zero).

## Usage

Install the project first (`pip install -e .` from the repository root); this
provides the `mardi-importer` console script. The examples below use
`python -m mardi_portal.cli.importer_cli`, which is equivalent.

```bash
mardi-importer health
```

### Wikidata import (async via Prefect)

```bash
python -m mardi_portal.cli.importer_cli import-wikidata-async --qids Q42 Q1
python -m mardi_portal.cli.importer_cli import-wikidata-async --qids "Q42,Q1"
```

### Wikidata import (sync)

```bash
python -m mardi_portal.cli.importer_cli import-wikidata --qids Q42 Q1
```

Restrict or widen the imported label/description/alias languages with
`--languages` (comma-separated). `mul` is always included unless you pass
`all`. When omitted, the default is `en,de,mul`.

```bash
python -m mardi_portal.cli.importer_cli import-wikidata --qids Q42 --languages en,de,fr   # -> en,de,fr,mul
python -m mardi_portal.cli.importer_cli import-wikidata --qids Q42 --languages all        # every language
```

### DOI import (async via Prefect)

```bash
python -m mardi_portal.cli.importer_cli import-doi-async --dois 10.48550/ARXIV.2101.00001
```

### DOI import (sync)

```bash
python -m mardi_portal.cli.importer_cli import-doi --dois 10.48550/ARXIV.2101.00001
```

### CRAN import (sync)

```bash
python -m mardi_portal.cli.importer_cli import-cran --packages dplyr ggplot2
python -m mardi_portal.cli.importer_cli import-cran --packages "dplyr,ggplot2"
```

### Prefect flow run status

```bash
python -m mardi_portal.cli.importer_cli import-workflow-status --id <flow_run_id>
```

### Prefect artifact result

```bash
python -m mardi_portal.cli.importer_cli import-workflow-result --id <flow_run_id>
python -m mardi_portal.cli.importer_cli import-workflow-result --id <flow_run_id> --key-prefix custom-prefix-
```

## Secrets

Make sure to rename the provided `secrets.example.txt` file to `secrets.txt` and fill in the
required values.
