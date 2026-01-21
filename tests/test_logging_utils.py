import logging
import sys
import types
import unittest
from unittest.mock import patch

from mardi_importer.utils.logging_utils import get_logger_safe


class TestGetLoggerSafe(unittest.TestCase):
    def test_get_logger_safe_without_prefect(self) -> None:
        with patch.dict(
            sys.modules,
            {"prefect": None, "prefect.logging": None, "prefect.exceptions": None},
        ):
            logger = get_logger_safe("no-prefect")

        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "no-prefect")

    def test_get_logger_safe_with_prefect_context(self) -> None:
        fake_logging = types.ModuleType("prefect.logging")
        fake_exceptions = types.ModuleType("prefect.exceptions")

        class MissingContextError(Exception):
            pass

        run_logger = logging.getLogger("prefect-run")

        def get_run_logger():
            return run_logger

        fake_logging.get_run_logger = get_run_logger
        fake_exceptions.MissingContextError = MissingContextError

        fake_prefect = types.ModuleType("prefect")
        fake_prefect.logging = fake_logging
        fake_prefect.exceptions = fake_exceptions

        with patch.dict(
            sys.modules,
            {
                "prefect": fake_prefect,
                "prefect.logging": fake_logging,
                "prefect.exceptions": fake_exceptions,
            },
        ):
            logger = get_logger_safe("with-prefect")

        self.assertIs(logger, run_logger)

    def test_get_logger_safe_prefect_missing_context(self) -> None:
        fake_logging = types.ModuleType("prefect.logging")
        fake_exceptions = types.ModuleType("prefect.exceptions")

        class MissingContextError(Exception):
            pass

        def get_run_logger():
            raise MissingContextError("no context")

        fake_logging.get_run_logger = get_run_logger
        fake_exceptions.MissingContextError = MissingContextError

        fake_prefect = types.ModuleType("prefect")
        fake_prefect.logging = fake_logging
        fake_prefect.exceptions = fake_exceptions

        with patch.dict(
            sys.modules,
            {
                "prefect": fake_prefect,
                "prefect.logging": fake_logging,
                "prefect.exceptions": fake_exceptions,
            },
        ):
            logger = get_logger_safe("prefect-fallback")

        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "prefect-fallback")


if __name__ == "__main__":
    unittest.main()
