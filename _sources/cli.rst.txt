Command-Line Interface (CLI) Guide
==================================

The CLI wraps common importer and Prefect actions. It is intended for manual
invocation and quick checks.

Usage
-----

Run the CLI module directly::

  python -m cli.importer_cli --help

Version
-------

The CLI exposes the current release version (from the root ``VERSION`` file)::

  python -m cli.importer_cli --version

Commands
--------

Health check::

  python -m cli.importer_cli health

Trigger Prefect flows asynchronously::

  python -m cli.importer_cli import-wikidata-async --qids Q42 Q1
  python -m cli.importer_cli import-doi-async --dois 10.1000/XYZ123

Check Prefect flow status or results::

  python -m cli.importer_cli import-workflow-status --id <flow-run-id>
  python -m cli.importer_cli import-workflow-result --id <flow-run-id>
  python -m cli.importer_cli import-workflow-runs

Run synchronous imports::

  python -m cli.importer_cli import-wikidata --qids Q42 Q1
  python -m cli.importer_cli import-doi --dois 10.1000/XYZ123
  python -m cli.importer_cli import-cran --packages dplyr ggplot2

  The ``import-wikidata`` command accepts an optional ``--languages`` flag
  (comma-separated language codes, or ``all``) to control which label,
  description and alias languages are imported. ``mul`` is always included
  unless ``all`` is given; the default when omitted is ``en,de,mul``. The flag
  applies only to this synchronous command, not to ``import-wikidata-async``::

  python -m cli.importer_cli import-wikidata --qids Q42 --languages en,de,fr
  python -m cli.importer_cli import-wikidata --qids Q42 --languages all

Create a knowledge graph item::

  # Typed format — schema fills predefined claims automatically
  python -m cli.importer_cli create-item \
      --type WORKFLOW \
      --fields '{"name": "My workflow", "problem_statement": "Solve X"}'

  # Raw format — supply label and explicit property/item IDs
  python -m cli.importer_cli create-item \
      --label "My item" \
      --claims '{"<MaRDI-PID>": "<MaRDI-QID>"}'
