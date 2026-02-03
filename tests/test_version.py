import unittest
from pathlib import Path
import shutil
import os

from services.version import get_version


class TestGetVersion(unittest.TestCase):

    def setUp(self):
        """Create a temporary directory structure for each test."""
        # Use a unique temporary directory for each test method
        # This is a bit more manual than pytest's tmp_path fixture but works with unittest
        self.tmp_dir = Path(f"tmp_test_dir_{self._testMethodName}")
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        self.project_root = self.tmp_dir / "mock_project_root"
        self.services_dir = self.project_root / "services"
        self.services_dir.mkdir(parents=True, exist_ok=True)
        
        self.mock_version_py_path = self.services_dir / "version.py"
        self.mock_version_py_path.touch()

        self.version_file_path = self.project_root / "VERSION" # This is where get_version will look

    def tearDown(self):
        """Clean up the temporary directory after each test."""
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir)

    def test_get_version_reads_version_file(self):
        """Test that get_version reads the version from the VERSION file."""
        self.version_file_path.write_text("1.2.3")
        self.assertEqual(get_version(_file_path=str(self.mock_version_py_path)), "1.2.3")

    def test_get_version_returns_fallback_if_file_missing(self):
        """Test that get_version returns the fallback if VERSION file is missing."""
        # Do not create the VERSION file (it's implicitly missing after setUp)
        self.assertEqual(get_version(_file_path=str(self.mock_version_py_path)), "unknown")

    def test_get_version_returns_fallback_if_file_empty(self):
        """Test that get_version returns the fallback if VERSION file is empty."""
        self.version_file_path.write_text("")
        self.assertEqual(get_version(_file_path=str(self.mock_version_py_path)), "unknown")

    def test_get_version_with_custom_fallback(self):
        """Test that get_version uses a custom fallback value."""
        # Test file missing with custom fallback (it's implicitly missing after setUp)
        self.assertEqual(get_version(_file_path=str(self.mock_version_py_path), fallback="0.0.0"), "0.0.0")

        # Test file empty with custom fallback
        self.version_file_path.write_text("")
        self.assertEqual(get_version(_file_path=str(self.mock_version_py_path), fallback="0.0.0"), "0.0.0")

if __name__ == '__main__':
    unittest.main()