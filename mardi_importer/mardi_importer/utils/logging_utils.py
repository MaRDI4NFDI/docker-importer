import logging


def get_logger_safe(name: str = __name__) -> logging.Logger:
    try:
        from prefect.logging import get_run_logger
        from prefect.exceptions import MissingContextError

        try:
            return get_run_logger()
        except MissingContextError:
            return logging.getLogger(name)
    except ModuleNotFoundError:
        return logging.getLogger(name)
