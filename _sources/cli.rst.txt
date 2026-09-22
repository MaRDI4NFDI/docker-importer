Command-Line Interface (CLI) Guide
==================================

The CLI wraps common importer and Prefect actions. It is intended for manual
invocation and quick checks.

Usage
-----

Run the console script installed with the package::

  mardi-importer --help

Equivalently, run the module directly::

  python -m mardi_portal.cli.importer_cli --help

Version
-------

The CLI exposes the current release version (derived from the git tag by
setuptools-scm)::

  mardi-importer --version

Commands
--------

Health check::

  mardi-importer health

Trigger Prefect flows asynchronously::

  mardi-importer import-wikidata-async --qids Q42 Q1
  mardi-importer import-doi-async --dois 10.1000/XYZ123

Check Prefect flow status or results::

  mardi-importer import-workflow-status --id <flow-run-id>
  mardi-importer import-workflow-result --id <flow-run-id>
  mardi-importer import-workflow-runs

Run synchronous imports::

  mardi-importer import-wikidata --qids Q42 Q1
  mardi-importer import-doi --dois 10.1000/XYZ123
  mardi-importer import-cran --packages dplyr ggplot2

  The ``import-wikidata`` command accepts an optional ``--languages`` flag
  (comma-separated language codes, or ``all``) to control which label,
  description and alias languages are imported. ``mul`` is always included
  unless ``all`` is given; the default when omitted is ``en,de,mul``. The flag
  applies only to this synchronous command, not to ``import-wikidata-async``::

  mardi-importer import-wikidata --qids Q42 --languages en,de,fr
  mardi-importer import-wikidata --qids Q42 --languages all

Create a knowledge graph item::

  # Typed format — schema fills predefined claims automatically
  mardi-importer create-item \
      --type WORKFLOW \
      --fields '{"name": "My workflow", "problem_statement": "Solve X"}'

  # Raw format — supply label and explicit property/item IDs
  mardi-importer create-item \
      --label "My item" \
      --claims '{"<MaRDI-PID>": "<MaRDI-QID>"}'
