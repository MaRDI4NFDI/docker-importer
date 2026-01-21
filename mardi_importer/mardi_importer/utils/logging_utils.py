"""Backward-compatible wrapper for `mardi_importer.logger.logging_utils`.

Prefer importing `get_logger_safe` from `mardi_importer.logger` going forward.

This module remains to avoid breaking external imports.
"""

from mardi_importer.logger.logging_utils import get_logger_safe

__all__ = ["get_logger_safe"]
