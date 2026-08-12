"""Logger utilities for the importer.

This package is intentionally separate from `mardi_importer.utils` to avoid
import-time cycles. Several low-level modules (e.g. `WikidataImporter`) need a
logger very early during initialization, and importing `mardi_importer.utils`
must not trigger higher-level objects (like `Author`) to load.
"""

from .logging_utils import get_logger_safe

__all__ = ["get_logger_safe"]

