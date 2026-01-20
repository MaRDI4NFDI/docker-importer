"""Top-level package shim for repo checkout usage.

Use lazy attribute access to avoid circular imports during package init.
"""

from importlib import import_module
from typing import Any


def __getattr__(name: str) -> Any:
    if name == "Importer":
        return import_module("mardi_importer.mardi_importer").Importer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

