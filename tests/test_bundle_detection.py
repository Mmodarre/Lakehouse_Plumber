"""
Tests for bundle detection logic.

Tests the core logic that determines when bundle support should be enabled
based on project structure and CLI flags.
"""

import pytest
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch

from lhp.utils.bundle_detection import should_enable_bundle_support, is_databricks_yml_present


class TestBundleDetection:
    """Test suite for bundle detection functionality."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_bundle_detection_with_databricks_yml_exists(self):
        """Should return True when databricks.yml exists and no CLI override."""
        # Create databricks.yml file
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_without_databricks_yml(self):
        """Should return False when databricks.yml doesn't exist."""
        # Ensure no databricks.yml exists
        assert not (self.project_root / "databricks.yml").exists()
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_cli_no_bundle_override_true(self):
        """Should return False when --no-bundle flag is set, even if databricks.yml exists."""
        # Create databricks.yml file
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=True)
        assert result is False

    def test_bundle_detection_cli_no_bundle_false_with_databricks_yml(self):
        """Should use databricks.yml detection when --no-bundle is False."""
        # Create databricks.yml file
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_cli_no_bundle_false_without_databricks_yml(self):
        """Should return False when --no-bundle is False but no databricks.yml exists."""
        # Ensure no databricks.yml exists
        assert not (self.project_root / "databricks.yml").exists()
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_with_empty_databricks_yml(self):
        """Should return True even if databricks.yml exists but is empty (existence check only)."""
        # Create empty databricks.yml file
        (self.project_root / "databricks.yml").write_text("")
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_with_malformed_databricks_yml(self):
        """Should return True even if databricks.yml exists but has invalid YAML."""
        # Create malformed databricks.yml file
        (self.project_root / "databricks.yml").write_text("invalid: yaml: content:\n  - malformed")
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_default_cli_no_bundle_parameter(self):
        """Should use default False for cli_no_bundle parameter."""
        # Create databricks.yml file
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
        
        # Call without cli_no_bundle parameter (should default to False)
        result = should_enable_bundle_support(self.project_root)
        assert result is True

    def test_bundle_detection_with_nonexistent_project_root(self):
        """Should handle nonexistent project root gracefully."""
        nonexistent_path = self.temp_dir / "nonexistent_project"
        
        result = should_enable_bundle_support(nonexistent_path, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_with_permission_denied(self):
        """Should handle permission denied errors gracefully."""
        # Create databricks.yml file
        databricks_file = self.project_root / "databricks.yml"
        databricks_file.write_text("bundle:\n  name: test")
        
        # Mock file access to raise PermissionError
        with patch('pathlib.Path.exists', side_effect=PermissionError("Permission denied")):
            result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
            assert result is False

    def test_is_databricks_yml_present_when_exists(self):
        """Should return True when databricks.yml exists."""
        # Create databricks.yml file
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
        
        result = is_databricks_yml_present(self.project_root)
        assert result is True

    def test_is_databricks_yml_present_when_not_exists(self):
        """Should return False when databricks.yml doesn't exist."""
        # Ensure no databricks.yml exists
        assert not (self.project_root / "databricks.yml").exists()
        
        result = is_databricks_yml_present(self.project_root)
        assert result is False

    def test_is_databricks_yml_present_with_directory_as_databricks_yml(self):
        """Should return False when databricks.yml is a directory instead of file."""
        # Create databricks.yml as directory
        (self.project_root / "databricks.yml").mkdir()
        
        result = is_databricks_yml_present(self.project_root)
        assert result is False

    def test_bundle_detection_case_sensitive_filename(self):
        """Should be case-sensitive for databricks.yml filename."""
        # First check if file system is case-sensitive
        test_file_lower = self.project_root / "test_case.txt"
        test_file_upper = self.project_root / "TEST_CASE.txt"
        
        test_file_lower.write_text("lower")
        is_case_sensitive = not test_file_upper.exists()
        
        if not is_case_sensitive:
            pytest.skip("File system is case-insensitive, skipping case-sensitivity test")
        
        # Clean up test files
        test_file_lower.unlink()
        
        # Create file with different case
        (self.project_root / "Databricks.yml").write_text("bundle:\n  name: test")
        (self.project_root / "DATABRICKS.YML").write_text("bundle:\n  name: test")
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_with_yaml_extension(self):
        """Should not detect databricks.yaml (only .yml extension)."""
        # Create databricks.yaml (different extension)
        (self.project_root / "databricks.yaml").write_text("bundle:\n  name: test")
        
        result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
        assert result is False

    def test_bundle_detection_priority_order(self):
        """Should test the priority order: CLI override > databricks.yml existence."""
        # Create databricks.yml file
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
        
        # Test CLI override takes precedence
        assert should_enable_bundle_support(self.project_root, cli_no_bundle=True) is False
        assert should_enable_bundle_support(self.project_root, cli_no_bundle=False) is True

    def test_bundle_detection_with_symbolic_link(self):
        """Should handle symbolic links to databricks.yml correctly."""
        # Create actual databricks.yml file
        actual_file = self.temp_dir / "actual_databricks.yml"
        actual_file.write_text("bundle:\n  name: test")
        
        # Create symbolic link
        link_file = self.project_root / "databricks.yml"
        try:
            link_file.symlink_to(actual_file)
            
            result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
            assert result is True
        except OSError:
            # Skip test if symlinks not supported on this platform
            pytest.skip("Symbolic links not supported on this platform")


class TestBundleDetectionEdgeCases:
    """Test edge cases and error conditions for bundle detection."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_bundle_detection_with_none_project_root(self):
        """Should handle None project root gracefully."""
        with pytest.raises(TypeError):
            should_enable_bundle_support(None, cli_no_bundle=False)

    def test_bundle_detection_with_string_project_root(self):
        """Should handle string project root by converting to Path."""
        # Create databricks.yml file
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
        
        # Pass string instead of Path object
        result = should_enable_bundle_support(str(self.project_root), cli_no_bundle=False)
        assert result is True

    def test_bundle_detection_with_relative_path(self):
        """Should handle relative paths correctly."""
        # Create databricks.yml file
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
        
        # Change to parent directory and use relative path
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
            # Create databricks.yml file
            (self.project_root / "databricks.yml").write_text("bundle:\n  name: test")
            time.sleep(0.01)  # Small delay to simulate concurrent access
            result = should_enable_bundle_support(self.project_root, cli_no_bundle=False)
            results.append(result)
        
        # Run multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=check_bundle_detection)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All results should be True
        assert all(results)
        assert len(results) == 5 