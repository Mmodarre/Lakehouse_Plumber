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

    def test_sync_resources_with_new_pipeline(self):
        """Should create new resource file for new pipeline."""
        # Create a pipeline directory with Python files
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        (pipeline_dir / "orders.py").write_text("# Orders notebook")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        # Verify resource file was created
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        assert resource_file.exists()
        
        # Verify content
        content = resource_file.read_text()
        assert "raw_ingestion_pipeline" in content
        assert "../../generated/raw_ingestion/customer.py" in content
        assert "../../generated/raw_ingestion/orders.py" in content

    def test_sync_resources_with_existing_pipeline_no_changes(self):
        """Should not modify existing resource file if no changes needed."""
        # Create pipeline directory
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        
        # Create existing resource file
        self.resources_dir.mkdir(parents=True)
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        resource_file.write_text("""
resources:
  pipelines:
    raw_ingestion_pipeline:
      name: raw_ingestion_pipeline
      catalog: main
      libraries:
        - notebook:
            path: ../../generated/raw_ingestion/customer.py
""")
        
        original_mtime = resource_file.stat().st_mtime
        
        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        # Verify file was not modified
        new_mtime = resource_file.stat().st_mtime
        assert new_mtime == original_mtime

    def test_sync_resources_add_notebooks_to_existing_file(self):
        """Should add new notebooks to existing resource file."""
        # Create pipeline directory with files
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        (pipeline_dir / "orders.py").write_text("# Orders notebook")
        (pipeline_dir / "products.py").write_text("# Products notebook")
        
        # Create existing resource file with only one notebook
        self.resources_dir.mkdir(parents=True)
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        resource_file.write_text("""
resources:
  pipelines:
    raw_ingestion_pipeline:
      name: raw_ingestion_pipeline
      catalog: main
      schema: test_dev
      libraries:
        - notebook:
            path: ../generated/raw_ingestion/customer.py
      configuration:
        bundle.sourcePath: ${workspace.file_path}/generated
""")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        # Verify new notebooks were added
        updated_content = resource_file.read_text()
        assert "../../generated/raw_ingestion/customer.py" in updated_content
        assert "../../generated/raw_ingestion/orders.py" in updated_content
        assert "../../generated/raw_ingestion/products.py" in updated_content
        
        # Verify other content was preserved
        assert "test_dev" in updated_content
        assert "bundle.sourcePath" in updated_content

    def test_sync_resources_remove_notebooks_from_existing_file(self):
        """Should remove obsolete notebooks from existing resource file."""
        # Create pipeline directory with only one file
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        
        # Create existing resource file with multiple notebooks
        self.resources_dir.mkdir(parents=True)
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        resource_file.write_text("""
resources:
  pipelines:
    raw_ingestion_pipeline:
      name: raw_ingestion_pipeline
      catalog: main
      libraries:
        - notebook:
            path: ../generated/raw_ingestion/customer.py
        - notebook:
            path: ../generated/raw_ingestion/old_orders.py
        - notebook:
            path: ../generated/raw_ingestion/old_products.py
        - jar: /path/to/some.jar
""")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        # Verify obsolete notebooks were removed
        updated_content = resource_file.read_text()
        assert "../../generated/raw_ingestion/customer.py" in updated_content
        assert "old_orders.py" not in updated_content
        assert "old_products.py" not in updated_content
        
        # Verify non-notebook libraries were preserved
        assert "jar: /path/to/some.jar" in updated_content

    def test_sync_resources_mixed_add_and_remove(self):
        """Should handle both adding and removing notebooks in one operation."""
        # Create pipeline directory
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        (pipeline_dir / "new_orders.py").write_text("# New orders notebook")
        
        # Create existing resource file
        self.resources_dir.mkdir(parents=True)
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        resource_file.write_text("""
resources:
  pipelines:
    raw_ingestion_pipeline:
      libraries:
        - notebook:
            path: ../generated/raw_ingestion/customer.py
        - notebook:
            path: ../generated/raw_ingestion/old_products.py
""")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        # Verify changes
        updated_content = resource_file.read_text()
        assert "../../generated/raw_ingestion/customer.py" in updated_content
        assert "../../generated/raw_ingestion/new_orders.py" in updated_content
        assert "old_products.py" not in updated_content

    def test_sync_resources_multiple_pipelines(self):
        """Should handle multiple pipelines correctly."""
        # Create multiple pipeline directories
        raw_dir = self.generated_dir / "raw_ingestion"
        raw_dir.mkdir()
        (raw_dir / "customer.py").write_text("# Customer notebook")
        
        bronze_dir = self.generated_dir / "bronze_load"
        bronze_dir.mkdir()
        (bronze_dir / "customer_bronze.py").write_text("# Customer bronze notebook")
        (bronze_dir / "orders_bronze.py").write_text("# Orders bronze notebook")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        # Verify both resource files were created
        raw_resource = self.resources_dir / "raw_ingestion.pipeline.yml"
        bronze_resource = self.resources_dir / "bronze_load.pipeline.yml"
        
        assert raw_resource.exists()
        assert bronze_resource.exists()
        
        # Verify correct content
        raw_content = raw_resource.read_text()
        assert "raw_ingestion_pipeline" in raw_content
        assert "../../generated/raw_ingestion/customer.py" in raw_content
        
        bronze_content = bronze_resource.read_text()
        assert "bronze_load_pipeline" in bronze_content
        assert "../../generated/bronze_load/customer_bronze.py" in bronze_content
        assert "../../generated/bronze_load/orders_bronze.py" in bronze_content

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

    def test_sync_resources_invalid_yaml_in_existing_file(self):
        """Should raise appropriate error for invalid YAML in existing file."""
        # Create pipeline directory
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        
        # Create resource file with invalid YAML
        self.resources_dir.mkdir(parents=True)
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        resource_file.write_text("""
resources:
  pipelines:
    raw_ingestion_pipeline:
      invalid: yaml: structure:
        - malformed
""")
        
        # Run sync and expect error
        with pytest.raises(BundleResourceError) as exc_info:
            self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        assert "Unexpected error for pipeline" in str(exc_info.value)
        assert "raw_ingestion" in str(exc_info.value)

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

    def test_sync_resources_readonly_existing_resource_file(self):
        """Should handle read-only existing resource files."""
        # Create pipeline directory with new file
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        (pipeline_dir / "orders.py").write_text("# Orders notebook")
        
        # Create existing resource file with one notebook
        self.resources_dir.mkdir(parents=True)
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        resource_file.write_text("""
resources:
  pipelines:
    raw_ingestion_pipeline:
      libraries:
        - notebook:
            path: ../generated/raw_ingestion/customer.py
""")
        resource_file.chmod(0o444)  # Read-only
        
        try:
            with pytest.raises(BundleResourceError) as exc_info:
                self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
            
            assert "Unexpected error for pipeline" in str(exc_info.value)
        finally:
            # Restore permissions for cleanup
            resource_file.chmod(0o644)

    def test_sync_resources_preserves_user_customizations(self):
        """Should preserve user customizations in resource files."""
        # Create pipeline directory
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        
        # Create existing resource file with custom settings
        self.resources_dir.mkdir(parents=True)
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        resource_file.write_text("""
# User's custom comment
resources:
  pipelines:
    raw_ingestion_pipeline:
      name: "custom_pipeline_name"
      catalog: custom_catalog
      schema: custom_schema_${bundle.target}
      libraries:
        - notebook:
            path: ../generated/raw_ingestion/customer.py
        - jar: /path/to/custom.jar
        - pypi:
            package: pandas==1.5.0
      configuration:
        bundle.sourcePath: ${workspace.file_path}/generated
        custom.setting: user_value
        another.setting: 
          nested: value
""")
        
        # Add new file to pipeline
        (pipeline_dir / "orders.py").write_text("# Orders notebook")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(self.generated_dir, "dev")
        
        # Parse updated content
        updated_content = resource_file.read_text()
        data = yaml.safe_load(updated_content)
        
        # Verify new notebook was added
        assert "../../generated/raw_ingestion/orders.py" in updated_content
        
        # Verify user customizations were preserved
        pipeline_config = data['resources']['pipelines']['raw_ingestion_pipeline']
        assert pipeline_config['name'] == "custom_pipeline_name"
        assert pipeline_config['catalog'] == "custom_catalog"
        assert pipeline_config['schema'] == "custom_schema_${bundle.target}"
        
        # Check that custom libraries are preserved
        libraries = pipeline_config['libraries']
        jar_libs = [lib for lib in libraries if 'jar' in lib]
        pypi_libs = [lib for lib in libraries if 'pypi' in lib]
        assert len(jar_libs) == 1
        assert len(pypi_libs) == 1
        
        # Check custom configuration
        config = pipeline_config['configuration']
        assert config['custom.setting'] == "user_value"
        assert config['another.setting']['nested'] == "value"

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
        
        # Verify resource file was created with correct paths
        resource_file = self.resources_dir / "raw_ingestion.pipeline.yml"
        content = resource_file.read_text()
        
        assert "../../generated/raw_ingestion/customer-data.py" in content
        assert "../../generated/raw_ingestion/order_history.py" in content
        assert "../../generated/raw_ingestion/product.catalog.py" in content

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
        
        # Verify LHP file has the proper header
        lhp_content = lhp_resource_file.read_text()
        assert "Generated by LakehousePlumber" in lhp_content
        assert "../../generated/lhp_pipeline/notebook.py" in lhp_content

    def test_sync_only_manages_lhp_generated_files(self):
        """Should only manage files with LHP header, ignoring other YAML files."""
        # Create various YAML files in resources/lhp/ directory
        self.resources_dir.mkdir(parents=True)
        
        # Non-LHP file in LHP directory (shouldn't happen but test for robustness)
        user_file_in_lhp = self.resources_dir / "user_file.yml"
        user_file_in_lhp.write_text("""# User file without LHP header
some_config: value
""")
        
        # LHP file that should be managed
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
        
        # Verify user file is preserved (not deleted)
        assert user_file_in_lhp.exists(), "User file should be preserved"
        user_content = user_file_in_lhp.read_text()
        assert "some_config: value" in user_content
        
        # Verify LHP file was removed (since managed_pipeline no longer exists)
        assert not lhp_file.exists(), "LHP file should be removed for non-existent pipeline"
        
        # Verify new LHP file was created
        new_lhp_file = self.resources_dir / "new_pipeline.pipeline.yml"
        assert new_lhp_file.exists(), "New LHP file should be created"
        
        new_content = new_lhp_file.read_text()
        assert "Generated by LakehousePlumber" in new_content
        assert "../../generated/new_pipeline/new_notebook.py" in new_content 