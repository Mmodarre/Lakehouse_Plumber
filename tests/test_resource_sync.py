"""
Tests for bundle resource file synchronization logic.

Tests the sync functionality that keeps bundle resource files
in sync with generated Python notebooks.
"""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.bundle.exceptions import BundleResourceError
from lhp.bundle.manager import BundleManager


class TestResourceSync:
    """Test suite for resource file synchronization."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "project"
        self.project_root.mkdir()
        self.generated_dir = self.project_root / "generated" / "dev"
        self.generated_dir.mkdir(parents=True)
        self.resources_dir = self.project_root / "resources" / "lhp"  # Root level now

        # Pipeline config with catalog/schema (required after refactor)
        config_file = self.project_root / "pipeline_config.yaml"
        config_file.write_text(
            "project_defaults:\n" "  catalog: test_catalog\n" "  schema: test_schema\n"
        )
        self.manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )
        # Set the resources_dir to the root level directory (new behavior)
        self.manager.resources_dir = self.resources_dir

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_sync_resources_empty_generated_directory(self):
        """Should handle empty generated directory gracefully."""
        # Create empty generated directory
        self.generated_dir.mkdir(exist_ok=True)

        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")

        # Verify no resource files were created
        assert (
            not self.resources_dir.exists()
            or len(list(self.resources_dir.glob("*.yml"))) == 0
        )

    def test_sync_resources_nonexistent_generated_directory(self):
        """Should raise BundleResourceError for nonexistent generated directory."""
        # Don't create generated directory
        nonexistent_dir = self.project_root / "nonexistent"

        # Run sync and expect error
        with pytest.raises(BundleResourceError) as exc_info:
            self.manager.sync_resources_with_generated_files(nonexistent_dir, "dev")

        # Should raise appropriate error
        assert "Output directory does not exist" in str(exc_info.value)

    def test_sync_resources_pipeline_with_no_python_files(self):
        """Should handle pipeline directories with no Python files."""
        # Create pipeline directory with non-Python files
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "readme.txt").write_text("Documentation")
        (pipeline_dir / "config.json").write_text("{}")

        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")

        # Should create resource file but with empty libraries
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        assert resource_file.exists()

        content = resource_file.read_text()
        # Should have basic structure but no notebook entries
        assert "raw_ingestion_pipeline" in content
        assert "libraries:" in content

    def test_sync_resources_permission_denied_resources_directory(self):
        """Should handle permission denied on resources directory."""
        # Create pipeline directory
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")

        # Create resources directory with restricted permissions
        self.resources_dir.mkdir(parents=True)
        self.resources_dir.chmod(0o444)  # Read-only

        try:
            with pytest.raises(BundleResourceError) as exc_info:
                self.manager.sync_resources_with_generated_files(
                    self.generated_dir, "dev"
                )

            assert "Permission denied" in str(
                exc_info.value
            ) or "Failed to create resource file" in str(exc_info.value)
        finally:
            # Restore permissions for cleanup
            self.resources_dir.chmod(0o755)

    def test_sync_resources_with_special_characters_in_filenames(self):
        """Should handle special characters in notebook filenames."""
        # Create pipeline directory with special character filenames
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer-data.py").write_text("# Customer data notebook")
        (pipeline_dir / "order_history.py").write_text("# Order history notebook")
        (pipeline_dir / "product.catalog.py").write_text("# Product catalog notebook")

        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")

        # Verify resource file was created with glob pattern
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        content = resource_file.read_text()

        # Should use glob pattern instead of individual notebook paths
        assert "- glob:" in content
        assert (
            "include: ${workspace.file_path}/generated/${bundle.target}/raw_ingestion/**"
            in content
        )
        assert (
            "root_path: ${workspace.file_path}/generated/${bundle.target}/raw_ingestion"
            in content
        )

    def test_sync_resources_logging_output(self):
        """Should produce appropriate logging output."""
        with patch.object(self.manager.logger, "info") as mock_info:
            # Create pipeline directory
            pipeline_dir = self.generated_dir / "raw_ingestion"
            pipeline_dir.mkdir()
            (pipeline_dir / "customer.py").write_text("# Customer notebook")

            # Run sync
            self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")

            # Verify logging calls
            info_calls = [call.args[0] for call in mock_info.call_args_list]

            # Should have sync start message
            assert any("Syncing bundle resources" in msg for msg in info_calls)

            # Should have creation message for new file
            assert any("Created new resource file" in msg for msg in info_calls)

    def test_sync_resources_reports_update_count(self):
        """Should report the number of updated resource files."""
        with patch.object(self.manager.logger, "info") as mock_info:
            # Create multiple pipeline directories
            raw_dir = self.generated_dir / "raw_ingestion"
            raw_dir.mkdir()
            (raw_dir / "customer.py").write_text("# Customer notebook")

            bronze_dir = self.generated_dir / "bronze_load"
            bronze_dir.mkdir()
            (bronze_dir / "orders.py").write_text("# Orders notebook")

            # Run sync
            self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")

            # Verify count reporting
            info_calls = [call.args[0] for call in mock_info.call_args_list]
            assert any("Wrote 2 bundle resource file(s)" in msg for msg in info_calls)
