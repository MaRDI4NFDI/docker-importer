from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version


DISTRIBUTION = "mardi-importer"


def get_version(fallback: str = "unknown") -> str:
    """Return the version of the installed distribution.

    The value is baked into the distribution metadata at build time by
    setuptools-scm, which derives it from git tags. Returns ``fallback`` when
    the package is not installed, e.g. when running straight from a checkout.
    """
    try:
        return _dist_version(DISTRIBUTION)
    except PackageNotFoundError:
        return fallback
