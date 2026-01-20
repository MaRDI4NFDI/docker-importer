"""Top-level package shim for repo checkout usage.

This re-exports symbols from the actual package in mardi_importer/mardi_importer
so imports like `from mardi_importer import Importer` work without installation.
"""

from mardi_importer.mardi_importer import Importer  # noqa: F401

