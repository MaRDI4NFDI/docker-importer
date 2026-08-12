#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# repo_root for the `tests` package itself, src for the distribution's packages
# (mardi_importer, mardi_portal) so the suite runs without an editable install.
export PYTHONPATH="${repo_root}:${repo_root}/src${PYTHONPATH:+:${PYTHONPATH}}"

python -m unittest discover -s "${repo_root}/tests"
