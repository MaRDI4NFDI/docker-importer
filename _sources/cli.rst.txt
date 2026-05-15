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
