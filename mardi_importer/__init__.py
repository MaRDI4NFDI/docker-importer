"""Top-level package shim for repo checkout usage.

Use lazy attribute access to avoid circular imports during package init.
"""

from importlib import import_module
from pathlib import Path
from typing import Any


_inner_pkg = Path(__file__).resolve().parent / "mardi_importer"
if _inner_pkg.is_dir():
    __path__.append(str(_inner_pkg))


def __getattr__(name: str) -> Any:
    if name == "Importer":
        return import_module("mardi_importer.mardi_importer").Importer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
