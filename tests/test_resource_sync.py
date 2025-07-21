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
        self.resources_dir = self.project_root / "resources"
        
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
        assert "../generated/raw_ingestion/customer.py" in content
        assert "../generated/raw_ingestion/orders.py" in content

    def test_sync_resources_with_existing_pipeline_no_changes(self):
        """Should not modify existing resource file if no changes needed."""
        # Create pipeline directory
        pipeline_dir = self.generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        
        # Create existing resource file
        self.resources_dir.mkdir()
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
        self.resources_dir.mkdir()
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
        assert "../generated/raw_ingestion/customer.py" in updated_content
        assert "../generated/raw_ingestion/orders.py" in updated_content
        assert "../generated/raw_ingestion/products.py" in updated_content
        
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
        self.resources_dir.mkdir()
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
        assert "../generated/raw_ingestion/customer.py" in updated_content
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
        self.resources_dir.mkdir()
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
        assert "../generated/raw_ingestion/customer.py" in updated_content
        assert "../generated/raw_ingestion/new_orders.py" in updated_content
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
        assert "../generated/raw_ingestion/customer.py" in raw_content
        
        bronze_content = bronze_resource.read_text()
        assert "bronze_load_pipeline" in bronze_content
        assert "../generated/bronze_load/customer_bronze.py" in bronze_content
        assert "../generated/bronze_load/orders_bronze.py" in bronze_content

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
        self.resources_dir.mkdir()
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
        self.resources_dir.mkdir()
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
        self.resources_dir.mkdir()
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
        self.resources_dir.mkdir()
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
        assert "../generated/raw_ingestion/orders.py" in updated_content
        
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
        
        assert "../generated/raw_ingestion/customer-data.py" in content
        assert "../generated/raw_ingestion/order_history.py" in content
        assert "../generated/raw_ingestion/product.catalog.py" in content

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


class TestResourceSyncEdgeCases:
    """Test edge cases and error conditions for resource sync."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_sync_resources_with_unicode_filenames(self):
        """Should handle Unicode characters in filenames and paths."""
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline with Unicode filenames
        pipeline_dir = generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "客户数据.py").write_text("# Customer data in Chinese")
        (pipeline_dir / "заказы.py").write_text("# Orders in Russian")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(generated_dir, "dev")
        
        # Verify resource file was created correctly
        resource_file = self.project_root / "resources" / "raw_ingestion.pipeline.yml"
        content = resource_file.read_text(encoding='utf-8')
        
        assert "../generated/raw_ingestion/客户数据.py" in content
        assert "../generated/raw_ingestion/заказы.py" in content

    def test_sync_resources_concurrent_operations(self):
        """Should handle concurrent sync operations safely."""
        import threading
        import time
        
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline directory
        pipeline_dir = generated_dir / "raw_ingestion"
        pipeline_dir.mkdir()
        (pipeline_dir / "customer.py").write_text("# Customer notebook")
        
        errors = []
        
        def run_sync():
            try:
                time.sleep(0.01)  # Small delay to create race conditions
                self.manager.sync_resources_with_generated_files(generated_dir, "dev")
            except Exception as e:
                errors.append(e)
        
        # Run multiple sync operations concurrently
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=run_sync)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should complete without errors
        assert len(errors) == 0
        
        # Resource file should exist and be valid
        resource_file = self.project_root / "resources" / "raw_ingestion.pipeline.yml"
        assert resource_file.exists()
        content = resource_file.read_text()
        assert "../generated/raw_ingestion/customer.py" in content

    def test_sync_resources_with_deep_directory_structure(self):
        """Should only process direct subdirectories, not deeply nested ones."""
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create a direct pipeline directory (this should be processed)
        direct_pipeline = generated_dir / "direct_pipeline"
        direct_pipeline.mkdir()
        (direct_pipeline / "notebook.py").write_text("# Direct notebook")
        
        # Create deep directory structure (this should be ignored)
        deep_dir = generated_dir / "level1" / "level2" / "level3" / "nested_pipeline"
        deep_dir.mkdir(parents=True)
        (deep_dir / "nested_notebook.py").write_text("# Deep notebook")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(generated_dir, "dev")
        
        # Should create resource file for direct pipeline only
        direct_resource = self.project_root / "resources" / "direct_pipeline.pipeline.yml"
        nested_resource = self.project_root / "resources" / "nested_pipeline.pipeline.yml"
        
        assert direct_resource.exists()
        assert not nested_resource.exists()  # Deep directories should be ignored
        
        content = direct_resource.read_text()
        assert "../generated/direct_pipeline/notebook.py" in content

    def test_sync_resources_large_number_of_notebooks(self):
        """Should handle large numbers of notebooks efficiently."""
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline with many notebooks
        pipeline_dir = generated_dir / "large_pipeline"
        pipeline_dir.mkdir()
        
        # Create 100 notebook files
        for i in range(100):
            (pipeline_dir / f"notebook_{i:03d}.py").write_text(f"# Notebook {i}")
        
        # Run sync
        self.manager.sync_resources_with_generated_files(generated_dir, "dev")
        
        # Verify all notebooks are included
        resource_file = self.project_root / "resources" / "large_pipeline.pipeline.yml"
        content = resource_file.read_text()
        
        # Check that all notebooks are present
        for i in range(100):
            assert f"../generated/large_pipeline/notebook_{i:03d}.py" in content

    def test_sync_resources_error_recovery(self):
        """Should handle partial failures gracefully."""
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create multiple pipelines
        good_pipeline = generated_dir / "good_pipeline"
        good_pipeline.mkdir()
        (good_pipeline / "notebook.py").write_text("# Good notebook")
        
        bad_pipeline = generated_dir / "bad_pipeline"
        bad_pipeline.mkdir()
        (bad_pipeline / "notebook.py").write_text("# Bad notebook")
        
        # Create existing resource file with invalid YAML for bad_pipeline
        resources_dir = self.project_root / "resources"
        resources_dir.mkdir()
        bad_resource = resources_dir / "bad_pipeline.pipeline.yml"
        bad_resource.write_text("invalid: yaml: content:")
        
        # Sync should fail on the bad pipeline
        with pytest.raises(BundleResourceError) as exc_info:
            self.manager.sync_resources_with_generated_files(generated_dir, "dev")
        
        assert "bad_pipeline" in str(exc_info.value)
        
        # Good pipeline should not have been processed due to fail-fast behavior
        good_resource = resources_dir / "good_pipeline.pipeline.yml"
        assert not good_resource.exists() 