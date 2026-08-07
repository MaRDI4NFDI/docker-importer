"""Utility subpackage.

This package is imported by low-level modules during startup. Keep `__init__`
free of eager imports to avoid circular-import issues.

Historically, `mardi_importer.wikidata.WikidataImporter` imported logging helpers
from `mardi_importer.utils.logging_utils`, which forced this package to import
early. Importing
`Author` here would re-import `mardi_importer.wikidata` while it is still
initializing and can cause `from mardi_importer.wikidata import WikidataImporter`
to resolve to the *module* instead of the class (leading to
`TypeError: 'module' object is not callable`).
"""

from importlib import import_module
from typing import Any

__all__ = ["Author"]


def __getattr__(name: str) -> Any:
    """Lazily expose selected package attributes.

    Args:
        name: Attribute name requested from this package.

    Returns:
        The requested attribute.

    Raises:
        AttributeError: If the attribute is not provided by this package.
    """
    if name == "Author":
        return import_module("mardi_importer.utils.Author").Author
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
