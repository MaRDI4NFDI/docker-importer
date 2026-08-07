import unittest
from importlib.metadata import PackageNotFoundError
from unittest.mock import patch

from mardi_portal.services.version import get_version


class TestGetVersion(unittest.TestCase):
    """Tests for reading the version from distribution metadata."""

    def test_returns_distribution_version(self) -> None:
        """Return the version recorded in the distribution metadata."""
        with patch(
            "mardi_portal.services.version._dist_version", return_value="1.2.3"
        ):
            self.assertEqual(get_version(), "1.2.3")

    def test_returns_fallback_when_not_installed(self) -> None:
        """Return the fallback when the distribution is not installed."""
        with patch(
            "mardi_portal.services.version._dist_version",
            side_effect=PackageNotFoundError,
        ):
            self.assertEqual(get_version(), "unknown")
            self.assertEqual(get_version(fallback="0.0.0"), "0.0.0")


if __name__ == "__main__":
    unittest.main()
