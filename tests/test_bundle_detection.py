"""Tests for bundle detection logic."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.bundle.detection import is_databricks_yml_present, should_enable_bundle_support


class TestBundleDetection:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_bundle_detection_with_databricks_yml_exists(self):
        """Should return True when databricks.yml exists and no CLI override."""
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")

        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_without_databricks_yml(self):
        """Should return False when databricks.yml doesn't exist."""
        assert not (self.project_root / "databricks.yml").exists()

        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_cli_no_bundle_override_true(self):
        """Should return False when --no-bundle flag is set, even if databricks.yml exists."""
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")

        result = should_enable_bundle_support(self.project_root, cli_no_bundle=True)
        assert result is False

    def test_bundle_detection_cli_no_bundle_false_with_databricks_yml(self):
        """Should use databricks.yml detection when --no-bundle is False."""
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")

        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_cli_no_bundle_false_without_databricks_yml(self):
        """Should return False when --no-bundle is False but no databricks.yml exists."""
        assert not (self.project_root / "databricks.yml").exists()

        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_with_empty_databricks_yml(self):
        """Should return True even if databricks.yml exists but is empty (existence check only)."""
        (self.project_root / "databricks.yml").write_text("")

        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_with_malformed_databricks_yml(self):
        """Should return True even if databricks.yml exists but has invalid YAML."""
        (self.project_root / "databricks.yml").write_text(
            "invalid: yaml: content:\n  - malformed"
        )

        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_default_cli_no_bundle_parameter(self):
        """Should use default False for cli_no_bundle parameter."""
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")

        result = should_enable_bundle_support(self.project_root)
        assert result is True

    def test_bundle_detection_with_nonexistent_project_root(self):
        """Should handle nonexistent project root gracefully."""
        nonexistent_path = self.temp_dir / "nonexistent_project"

        result = should_enable_bundle_support(nonexistent_path, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_with_permission_denied(self):
        """Should handle permission denied errors gracefully."""
        databricks_file = self.project_root / "databricks.yml"
        databricks_file.write_text("bundle:\n  name: test")

        with patch(
            "pathlib.Path.exists", side_effect=PermissionError("Permission denied")
        ):
            result = should_enable_bundle_support(
                self.project_root, cli_no_bundle=False
            )
            assert result is False

    def test_is_databricks_yml_present_when_exists(self):
        """Should return True when databricks.yml exists."""
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")

        result = is_databricks_yml_present(self.project_root)
        assert result is True

    def test_is_databricks_yml_present_when_not_exists(self):
        """Should return False when databricks.yml doesn't exist."""
        assert not (self.project_root / "databricks.yml").exists()

        result = is_databricks_yml_present(self.project_root)
        assert result is False

    def test_is_databricks_yml_present_with_directory_as_databricks_yml(self):
        """Should return False when databricks.yml is a directory instead of file."""
        (self.project_root / "databricks.yml").mkdir()

        result = is_databricks_yml_present(self.project_root)
        assert result is False

    # def test_bundle_detection_case_sensitive_filename(self):
    #     """Should be case-sensitive for databricks.yml filename."""
    #     # First check if file system is case-sensitive
    #     test_file_lower = self.project_root / "test_case.txt"
    #     test_file_upper = self.project_root / "TEST_CASE.txt"

    #     test_file_lower.write_text("lower")
    #     is_case_sensitive = not test_file_upper.exists()

    #     if not is_case_sensitive:
    #         pytest.skip("File system is case-insensitive, skipping case-sensitivity test")

    #     # Clean up test files
    #     test_file_lower.unlink()

    #     # Create file with different case
    #     (self.project_root / "Databricks.yml").write_text("bundle:\n  name: test")
    #     (self.project_root / "DATABRICKS.YML").write_text("bundle:\n  name: test")

    #     result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
    #     assert result is False

    def test_bundle_detection_with_yaml_extension(self):
        """Should not detect databricks.yaml (only .yml extension)."""
        (self.project_root / "databricks.yaml").write_text("bundle:\n  name: test")

        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_priority_order(self):
        """Should test the priority order: CLI override > databricks.yml existence."""
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")

        assert (
            should_enable_bundle_support(self.project_root, cli_no_bundle=True) is False
        )
        assert (
            should_enable_bundle_support(self.project_root, cli_no_bundle=False) is True
        )

    def test_bundle_detection_with_symbolic_link(self):
        """Should handle symbolic links to databricks.yml correctly."""
        actual_file = self.temp_dir / "actual_databricks.yml"
        actual_file.write_text("bundle:\n  name: test")

        link_file = self.project_root / "databricks.yml"
        try:
            link_file.symlink_to(actual_file)

            result = should_enable_bundle_support(
                self.project_root, cli_no_bundle=False
            )
            assert result is True
        except OSError:
            # Skip test if symlinks not supported on this platform
            pytest.skip("Symbolic links not supported on this platform")


class TestBundleDetectionEdgeCases:
    """Test edge cases and error conditions for bundle detection."""

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_bundle_detection_with_none_project_root(self):
        """Should handle None project root gracefully."""
        from lhp.errors import LHPConfigError

        with pytest.raises(LHPConfigError, match="project_root cannot be None"):
            should_enable_bundle_support(None, cli_no_bundle=False)

    def test_bundle_detection_with_string_project_root(self):
        """Should handle string project root by converting to Path."""
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")

        result = should_enable_bundle_support(
            str(self.project_root), cli_no_bundle=False
        )
        assert result is True

    def test_bundle_detection_with_relative_path(self):
        """Should handle relative paths correctly."""
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")

        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(self.temp_dir)
            relative_path = Path("test_project")

            result = should_enable_bundle_support(relative_path, cli_no_bundle=False)
            assert result is True
        finally:
            os.chdir(original_cwd)

    def test_bundle_detection_concurrent_access(self):
        """Should handle concurrent file access safely."""
        import threading
        import time

        results = []

        def check_bundle_detection():
            (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
            time.sleep(0.01)  # Small delay to simulate concurrent access
            result = should_enable_bundle_support(
                self.project_root, cli_no_bundle=False
            )
            results.append(result)

        threads = []
        for _ in range(5):
            thread = threading.Thread(target=check_bundle_detection)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert all(results)
        assert len(results) == 5
