from pathlib import Path


def get_version(fallback: str = "unknown", _file_path: str = __file__) -> str:
    """Return the app version from the root VERSION file."""
    version_path = Path(_file_path).resolve().parents[1] / "VERSION"
    try:
        return version_path.read_text().strip() or fallback
    except OSError:
        return fallback
