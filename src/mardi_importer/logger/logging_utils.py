"""Logging helpers.

The functions here are designed to be safe to call both inside and outside a
Prefect runtime context.
"""

import logging


def get_logger_safe(name: str = __name__) -> logging.Logger:
    """Return a logger that works with or without Prefect context.

    If called inside a Prefect flow/task, this returns Prefect's run logger so
    logs are captured by the Prefect UI. Outside Prefect, it returns a standard
    library logger with the given name.

    Args:
        name: Logger name to use when Prefect context is unavailable.

    Returns:
        A `logging.Logger` instance.
    """
    try:
        from prefect.exceptions import MissingContextError
        from prefect.logging import get_run_logger

        try:
            return get_run_logger()
        except MissingContextError:
            return logging.getLogger(name)
    except ModuleNotFoundError:
        return logging.getLogger(name)

