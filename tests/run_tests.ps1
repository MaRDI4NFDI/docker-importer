$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = "$repoRoot" + ($(if ($env:PYTHONPATH) { ";" + $env:PYTHONPATH } else { "" }))

python -m unittest discover -s "$repoRoot\tests"
