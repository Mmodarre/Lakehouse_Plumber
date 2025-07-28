"""
Tests for bundle resource file synchronization logic.

Tests the sync functionality that keeps bundle resource files
in sync with generated Python notebooks.
"""

import pytest
import tempfile
import shutil
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, call

from lhp.bundle.manager import BundleManager
from lhp.bundle.exceptions import BundleResourceError
from lhp.bundle.yaml_processor import YAMLParsingError


class TestResourceSync:
    """Test suite for resource file synchronization."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "project"
        self.project_root.mkdir()
        self.generated_dir = self.project_root / "generated"
        self.generated_dir.mkdir()
        self.resources_dir = self.project_root / "resources" / "lhp"
        
        self.manager = BundleManager(self.project_root)

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
        assert not self.resources_dir.exists() or len(list(self.resources_dir.glob("*.yml"))) == 0

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
                self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
            
            assert "Permission denied" in str(exc_info.value) or "Failed to create resource file" in str(exc_info.value)
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
        assert "include: ../../generated/raw_ingestion/**" in content
        assert "root_path: ${workspace.file_path}/generated/raw_ingestion" in content

    def test_sync_resources_logging_output(self):
        """Should produce appropriate logging output."""
        with patch.object(self.manager.logger, 'info') as mock_info:
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
        with patch.object(self.manager.logger, 'info') as mock_info:
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
            assert any("Updated 2 bundle resource file(s)" in msg for msg in info_calls)

    def test_sync_preserves_user_dab_files(self):
        """Should preserve user-created DAB files that are not LHP-generated."""
        # Create user's custom DAB files in the resources directory (parent of lhp/)
        user_resources_dir = self.project_root / "resources"
        user_resources_dir.mkdir()
        
        # User DAB file without LHP header
        user_dab_file = user_resources_dir / "user_pipeline.yml"
        user_dab_file.write_text("""# User's custom DAB pipeline
resources:
  pipelines:
    user_custom_pipeline:
      name: user_custom_pipeline
      catalog: custom_catalog
      libraries:
        - jar: /path/to/custom.jar
        - pypi:
            package: pandas==1.5.0
""")
        
        # User DAB file that looks like LHP but isn't
        fake_lhp_file = user_resources_dir / "fake_lhp.pipeline.yml"
        fake_lhp_file.write_text("""# This mentions LakehousePlumber but is not generated by it
resources:
  pipelines:
    fake_pipeline:
      name: fake_pipeline
""")
        
        # Create LHP pipeline that will generate resource file
        pipeline_dir = self.generated_dir / "lhp_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "notebook.py").write_text("# LHP generated notebook")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        # Verify user DAB files are preserved
        assert user_dab_file.exists(), "User DAB file should be preserved"
        assert fake_lhp_file.exists(), "Fake LHP file should be preserved"
        
        # Verify user file content is unchanged
        user_content = user_dab_file.read_text()
        assert "user_custom_pipeline" in user_content
        assert "custom_catalog" in user_content

        # Verify LHP resource file was created in lhp subdirectory
        lhp_resource_file = self.resources_dir / "lhp_pipeline.pipeline.yml"
        assert lhp_resource_file.exists(), "LHP resource file should be created"

        # Verify LHP file has the proper header and glob pattern
        lhp_content = lhp_resource_file.read_text()
        assert "Generated by LakehousePlumber" in lhp_content
        assert "- glob:" in lhp_content
        assert "include: ../../generated/lhp_pipeline/**" in lhp_content

    def test_sync_manages_all_files_in_lhp_directory(self):
        """Should backup and manage all files in /resources/lhp/ directory regardless of headers."""
        # Create various YAML files in resources/lhp/ directory
        self.resources_dir.mkdir(parents=True)

        # Non-LHP file in LHP directory - should be backed up according to new rules
        user_file_in_lhp = self.resources_dir / "user_file.yml"
        user_file_in_lhp.write_text("""# User file without LHP header
some_config: value
""")

        # LHP file for pipeline that no longer exists - should be backed up
        lhp_file = self.resources_dir / "managed_pipeline.pipeline.yml"
        lhp_file.write_text("""# Generated by LakehousePlumber - Bundle Resource for managed_pipeline
resources:
  pipelines:
    managed_pipeline_pipeline:
      name: managed_pipeline_pipeline
      libraries:
        - notebook:
            path: ../../generated/managed_pipeline/old_notebook.py
""")

        # Create new pipeline structure (managed_pipeline no longer exists)
        new_pipeline_dir = self.generated_dir / "new_pipeline"
        new_pipeline_dir.mkdir()
        (new_pipeline_dir / "new_notebook.py").write_text("# New notebook")

        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")

        # Verify user file was backed up (not preserved)
        assert not user_file_in_lhp.exists(), "User file should be backed up, not preserved"
        user_backup_file = self.resources_dir / "user_file.yml.bkup"
        assert user_backup_file.exists(), "User file should be backed up to .bkup"
        backup_content = user_backup_file.read_text()
        assert "some_config: value" in backup_content

        # Verify LHP file was backed up (since managed_pipeline no longer exists)
        assert not lhp_file.exists(), "LHP file should be backed up for non-existent pipeline"
        lhp_backup_file = self.resources_dir / "managed_pipeline.pipeline.yml.bkup"
        assert lhp_backup_file.exists(), "LHP file should be backed up to .bkup"

        # Verify new LHP file was created
        new_lhp_file = self.resources_dir / "new_pipeline.pipeline.yml"
        assert new_lhp_file.exists(), "New LHP file should be created"

        new_content = new_lhp_file.read_text()
        assert "Generated by LakehousePlumber" in new_content
        assert "- glob:" in new_content
        assert "include: ../../generated/new_pipeline/**" in new_content

    def test_sync_backup_file_without_header_when_pipeline_exists(self):
        """Should backup file without LHP header and create new one when pipeline exists (Scenario C1)."""
        # Create pipeline directory with Python files
        pipeline_dir = self.generated_dir / "active_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "notebook1.py").write_text("# Notebook 1")
        (pipeline_dir / "notebook2.py").write_text("# Notebook 2")

        # Create resource file WITHOUT LHP header for the same pipeline
        self.resources_dir.mkdir(parents=True)
        resource_file = self.resources_dir / "active_pipeline.pipeline.yml"
        original_content = """# User's custom resource file without LHP header
resources:
  pipelines:
    custom_pipeline_name:
      name: custom_pipeline_name
      catalog: user_catalog
      schema: user_schema
      custom_setting: user_value
      libraries:
        - jar: /path/to/user.jar
"""
        resource_file.write_text(original_content)

        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")

        # Verify original file was backed up
        backup_file = self.resources_dir / "active_pipeline.pipeline.yml.bkup"
        assert backup_file.exists(), "Original file should be backed up to .bkup"
        backup_content = backup_file.read_text()
        assert "custom_pipeline_name" in backup_content
        assert "user_catalog" in backup_content
        assert "custom_setting: user_value" in backup_content

        # Verify new LHP file was created with correct header
        assert resource_file.exists(), "New LHP file should be created"
        new_content = resource_file.read_text()
        assert "Generated by LakehousePlumber" in new_content
        assert "active_pipeline_pipeline" in new_content  # Standard LHP naming

        # Verify new file contains glob pattern for all files
        assert "- glob:" in new_content
        assert "include: ../../generated/active_pipeline/**" in new_content
        
        # Verify old user customizations are NOT in new file (clean slate)
        assert "custom_pipeline_name" not in new_content
        assert "user_catalog" not in new_content
        assert "custom_setting" not in new_content

    def test_sync_handles_multiple_resource_files_for_same_pipeline(self):
        """Should backup all resource files for same pipeline and create one new file (Scenario E2)."""
        # Create pipeline directory with Python files
        pipeline_dir = self.generated_dir / "multi_file_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "notebook1.py").write_text("# Notebook 1")
        (pipeline_dir / "notebook2.py").write_text("# Notebook 2")

        # Create multiple resource files for the same pipeline
        self.resources_dir.mkdir(parents=True)

        # File 1: Standard .pipeline.yml format without header
        file1 = self.resources_dir / "multi_file_pipeline.pipeline.yml"
        file1.write_text("""# First file without LHP header
resources:
  pipelines:
    first_pipeline:
      name: first_pipeline
      catalog: catalog1
""")

        # File 2: .yml format without header (alternative naming)
        file2 = self.resources_dir / "multi_file_pipeline.yml"
        file2.write_text("""# Second file without LHP header
resources:
  pipelines:
    second_pipeline:
      name: second_pipeline
      catalog: catalog2
""")

        # File 3: Another .pipeline.yml with different prefix (edge case)
        file3 = self.resources_dir / "multi_file_pipeline_custom.pipeline.yml"
        file3.write_text("""# Third file without LHP header
resources:
  pipelines:
    third_pipeline:
      name: third_pipeline
      catalog: catalog3
""")

        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")

        # Verify all original files were backed up
        backup1 = self.resources_dir / "multi_file_pipeline.pipeline.yml.bkup"
        backup2 = self.resources_dir / "multi_file_pipeline.yml.bkup"
        backup3 = self.resources_dir / "multi_file_pipeline_custom.pipeline.yml.bkup"

        assert backup1.exists(), "First file should be backed up"
        assert backup2.exists(), "Second file should be backed up"
        assert backup3.exists(), "Third file should be backed up"

        # Verify backup contents are preserved
        assert "first_pipeline" in backup1.read_text()
        assert "second_pipeline" in backup2.read_text()
        assert "third_pipeline" in backup3.read_text()

        # Verify original files no longer exist
        assert not file2.exists(), "Original .yml file should be removed"
        assert not file3.exists(), "Original custom file should be removed"

        # Verify new LHP file was created (standard naming)
        new_file = self.resources_dir / "multi_file_pipeline.pipeline.yml"
        assert new_file.exists(), "New LHP file should be created"

        new_content = new_file.read_text()
        assert "Generated by LakehousePlumber" in new_content
        assert "multi_file_pipeline_pipeline" in new_content

        # Verify new file contains glob pattern for all files
        assert "- glob:" in new_content
        assert "include: ../../generated/multi_file_pipeline/**" in new_content
        
        # Verify old pipeline names are NOT in new file
        assert "first_pipeline" not in new_content
        assert "second_pipeline" not in new_content
        assert "third_pipeline" not in new_content 