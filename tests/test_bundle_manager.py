"""
Tests for BundleManager core functionality.

Tests the core bundle management operations including initialization,
directory discovery, and resource file operations.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

from lhp.bundle.manager import BundleManager
from lhp.bundle.exceptions import BundleResourceError


class TestBundleManagerCore:
    """Test suite for BundleManager core functionality."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_bundle_manager_initialization(self):
        """Should initialize with correct project root and resources directory."""
        assert self.manager.project_root == self.project_root
        assert self.manager.resources_dir == self.project_root / "resources" / "lhp"

    def test_bundle_manager_creates_resources_directory(self):
        """Should create resources/lhp directory if it doesn't exist."""
        # Ensure resources/lhp directory doesn't exist initially
        assert not (self.project_root / "resources" / "lhp").exists()
        
        # Initialize manager and call method that ensures directory exists
        self.manager._ensure_resources_directory()
        
        # Resources/lhp directory should now exist
        assert (self.project_root / "resources" / "lhp").exists()
        assert (self.project_root / "resources" / "lhp").is_dir()
        # Parent resources directory should also exist
        assert (self.project_root / "resources").exists()
        assert (self.project_root / "resources").is_dir()

    def test_bundle_manager_resources_directory_already_exists(self):
        """Should handle existing resources/lhp directory gracefully."""
        # Create resources/lhp directory
        resources_lhp_dir = self.project_root / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True)
        
        # Should not raise error
        self.manager._ensure_resources_directory()
        
        # Directory should still exist
        assert (self.project_root / "resources" / "lhp").exists()
        assert (self.project_root / "resources").exists()

    def test_get_pipeline_directories_with_multiple_pipelines(self):
        """Should correctly identify pipeline directories in generated/."""
        # Create generated directory structure
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline directories
        (generated_dir / "raw_ingestions").mkdir()
        (generated_dir / "bronze_load").mkdir() 
        (generated_dir / "silver_load").mkdir()
        
        # Create some files to ignore
        (generated_dir / "readme.txt").write_text("not a directory")
        
        pipeline_dirs = self.manager._get_pipeline_directories(generated_dir)
        
        # Should return only directories
        assert len(pipeline_dirs) == 3
        
        pipeline_names = [d.name for d in pipeline_dirs]
        assert "raw_ingestions" in pipeline_names
        assert "bronze_load" in pipeline_names
        assert "silver_load" in pipeline_names

    def test_get_pipeline_directories_returns_sorted_order(self):
        """Should return pipeline directories in sorted order for deterministic processing."""
        # Create generated directory structure
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline directories in non-alphabetical order to test sorting
        (generated_dir / "pipeline_3").mkdir()
        (generated_dir / "pipeline_1").mkdir() 
        (generated_dir / "pipeline_2").mkdir()
        (generated_dir / "aaa_first").mkdir()
        (generated_dir / "zzz_last").mkdir()
        
        pipeline_dirs = self.manager._get_pipeline_directories(generated_dir)
        
        # Should return directories in sorted order
        assert len(pipeline_dirs) == 5
        
        pipeline_names = [d.name for d in pipeline_dirs]
        expected_order = ["aaa_first", "pipeline_1", "pipeline_2", "pipeline_3", "zzz_last"]
        assert pipeline_names == expected_order

    def test_get_pipeline_directories_empty_generated(self):
        """Should return empty list when generated directory is empty."""
        # Create empty generated directory
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        pipeline_dirs = self.manager._get_pipeline_directories(generated_dir)
        
        assert pipeline_dirs == []

    def test_get_pipeline_directories_nonexistent_generated(self):
        """Should raise BundleResourceError when generated directory doesn't exist."""
        nonexistent_dir = self.project_root / "nonexistent"
        
        with pytest.raises(BundleResourceError) as exc_info:
            self.manager._get_pipeline_directories(nonexistent_dir)
        
        assert "Output directory does not exist" in str(exc_info.value)







    def test_resource_file_path_generation(self):
        """Should generate correct resource file paths for pipelines."""
        # Test resource file path generation
        resource_path = self.manager._get_resource_file_path("raw_ingestions")
        
        expected_path = self.project_root / "resources" / "lhp" / "raw_ingestions.pipeline.yml"
        assert resource_path == expected_path

    def test_resource_file_path_generation_with_special_characters(self):
        """Should handle pipeline names with special characters."""
        # Test with pipeline name containing underscores and numbers
        resource_path = self.manager._get_resource_file_path("bronze_layer_v2")
        
        expected_path = self.project_root / "resources" / "lhp" / "bronze_layer_v2.pipeline.yml"
        assert resource_path == expected_path

    def test_bundle_manager_logging(self):
        """Should initialize logger correctly."""
        assert hasattr(self.manager, 'logger')
        assert self.manager.logger.name == 'lhp.bundle.manager'


class TestBundleManagerFileOperations:
    """Test file operations and edge cases for BundleManager."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_get_pipeline_directories_with_permission_error(self):
        """Should handle permission errors gracefully."""
        # Create generated directory
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline directory and restrict permissions
        pipeline_dir = generated_dir / "restricted_pipeline"
        pipeline_dir.mkdir()
        pipeline_dir.chmod(0o000)  # No permissions
        
        try:
            # Should not raise exception
            pipeline_dirs = self.manager._get_pipeline_directories(generated_dir)
            
            # Should return only accessible directories (implementation dependent)
            assert isinstance(pipeline_dirs, list)
            
        finally:
            # Restore permissions for cleanup
            pipeline_dir.chmod(0o755)



    def test_get_pipeline_directories_with_symbolic_links(self):
        """Should handle symbolic links appropriately."""
        # Create generated directory
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create real pipeline directory
        real_pipeline = generated_dir / "real_pipeline"
        real_pipeline.mkdir()
        
        # Create symbolic link to pipeline directory
        try:
            link_pipeline = generated_dir / "link_pipeline"
            link_pipeline.symlink_to(real_pipeline)
            
            pipeline_dirs = self.manager._get_pipeline_directories(generated_dir)
            
            # Should include both real directory and symlink (both are directories)
            assert len(pipeline_dirs) == 2
            
            pipeline_names = [d.name for d in pipeline_dirs]
            assert "real_pipeline" in pipeline_names
            assert "link_pipeline" in pipeline_names
            
        except OSError:
            # Skip test if symlinks not supported on this platform
            pytest.skip("Symbolic links not supported on this platform")

    def test_bundle_manager_with_readonly_project_root(self):
        """Should handle read-only project root by raising appropriate errors."""
        # Make project root read-only
        self.project_root.chmod(0o444)
        
        try:
            # Should not raise exception during initialization
            readonly_manager = BundleManager(self.project_root)
            assert readonly_manager.project_root == self.project_root
            
            # Operations that require directory access should fail with proper error
            with pytest.raises(BundleResourceError) as exc_info:
                readonly_manager._get_pipeline_directories(self.project_root / "nonexistent")
            
            assert "Permission denied" in str(exc_info.value)
            
        finally:
            # Restore permissions for cleanup
            self.project_root.chmod(0o755)

    def test_concurrent_bundle_manager_operations(self):
        """Should handle concurrent operations safely."""
        import threading
        import time
        
        # Create test data
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        pipeline_dir = generated_dir / "test_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "test.py").write_text("# test")
        
        results = []
        
        def get_directories():
            time.sleep(0.01)  # Small delay to increase chance of race condition
            dirs = self.manager._get_pipeline_directories(generated_dir)
            results.append(len(dirs))
        
        def test_template_rendering():
            time.sleep(0.01)
            content = self.manager._generate_resource_file_content("test_pipeline", generated_dir)
            results.append(len(content))
        
        # Run operations concurrently
        threads = []
        for _ in range(3):
            threads.append(threading.Thread(target=get_directories))
            threads.append(threading.Thread(target=test_template_rendering))
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All operations should complete successfully
        assert len(results) == 6
        # Directory count results should be 1
        assert results.count(1) >= 3  # At least 3 directory results
        # Template content length results should be much larger (string length)
        template_results = [r for r in results if r > 10]  # Template content lengths
        assert len(template_results) >= 3  # At least 3 template results

    def test_bundle_manager_large_number_of_files(self):
        """Should handle pipeline directories with many files efficiently."""
        # Create pipeline directory with many Python files
        pipeline_dir = self.project_root / "generated" / "large_pipeline"
        pipeline_dir.mkdir(parents=True)
        
        # Create 100 Python files
        for i in range(100):
            (pipeline_dir / f"file_{i:03d}.py").write_text(f"# file {i}")
        
        # Test template rendering instead of notebook path scanning (removed method)
        output_dir = self.project_root / "generated"
        content = self.manager._generate_resource_file_content("large_pipeline", output_dir)
        
        # Should generate template efficiently
        assert "large_pipeline" in content
        assert "- glob:" in content
        assert "include: ../../generated/large_pipeline/**" in content

    def test_bundle_manager_error_handling_initialization(self):
        """Should handle initialization errors appropriately."""
        # Test with None project root
        with pytest.raises(TypeError):
            BundleManager(None)
        
        # Test with non-Path object
        string_path = str(self.project_root)
        manager = BundleManager(string_path)
        assert manager.project_root == Path(string_path) 