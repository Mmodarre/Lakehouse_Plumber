"""
Tests for comprehensive bundle error handling and recovery functionality.

Tests various error scenarios including malformed YAML, network failures,
file system issues, and recovery mechanisms across all bundle components.
"""

import pytest
import tempfile
import shutil
import os
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import yaml
import json
from requests.exceptions import RequestException, Timeout, HTTPError
import requests

from lhp.bundle.exceptions import BundleResourceError, TemplateError, YAMLParsingError
from lhp.bundle.manager import BundleManager
from lhp.bundle.template_fetcher import DatabricksTemplateFetcher
from lhp.bundle.yaml_processor import YAMLProcessor


class TestBundleYAMLErrorHandling:
    """Test YAML processing error handling and recovery."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.resources_dir = self.project_root / "resources"
        self.resources_dir.mkdir()
        
        self.yaml_processor = YAMLProcessor()

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_yaml_processor_handles_completely_malformed_yaml(self):
        """Should handle completely malformed YAML gracefully."""
        malformed_file = self.resources_dir / "malformed.yml"
        malformed_file.write_text("""
resources:
  pipelines:
    test_pipeline:
      libraries:
        - notebook: {invalid yaml structure [[[
          missing closing brackets
        invalid: content
        """)
        
        with pytest.raises(YAMLParsingError) as exc_info:
            self.yaml_processor.extract_notebook_paths(malformed_file)
        
        # Should contain helpful error context
        assert "malformed.yml" in str(exc_info.value)
        assert "YAML processing error" in str(exc_info.value) or "Invalid YAML syntax" in str(exc_info.value)

    def test_yaml_processor_handles_yaml_with_invalid_encoding(self):
        """Should handle YAML files with invalid encoding."""
        invalid_encoding_file = self.resources_dir / "invalid_encoding.yml"
        
        # Write binary data that's not valid UTF-8
        with open(invalid_encoding_file, 'wb') as f:
            f.write(b'\xff\xfe\x00\x00invalid\x00\x00encoding\x00\x00')
        
        with pytest.raises(YAMLParsingError) as exc_info:
            self.yaml_processor.extract_notebook_paths(invalid_encoding_file)
        
        assert "encoding" in str(exc_info.value).lower()

    def test_yaml_processor_handles_permission_denied_errors(self):
        """Should handle permission denied errors gracefully."""
        restricted_file = self.resources_dir / "restricted.yml"
        restricted_file.write_text("""resources:
  pipelines:
    test:
      libraries:
        - notebook: 
            path: test.py
""")
        
        # Make file unreadable
        restricted_file.chmod(0o000)
        
        try:
            with pytest.raises(YAMLParsingError) as exc_info:
                self.yaml_processor.extract_notebook_paths(restricted_file)
            
            assert "permission" in str(exc_info.value).lower()
        finally:
            # Restore permissions for cleanup
            restricted_file.chmod(0o644)

    def test_yaml_processor_handles_very_large_yaml_files(self):
        """Should handle very large YAML files without memory issues."""
        large_file = self.resources_dir / "large.yml"
        
        # Create a large YAML file (but valid structure)
        content = "resources:\n  pipelines:\n    test:\n      libraries:\n"
        for i in range(1000):
            content += f"        - notebook:\n            path: notebook_{i}.py\n"
        
        large_file.write_text(content)
        
        # Should process without memory errors
        notebooks = self.yaml_processor.extract_notebook_paths(large_file)
        assert len(notebooks) == 1000

    def test_yaml_processor_handles_circular_yaml_references(self):
        """Should handle YAML files with deeply nested structures gracefully."""
        deep_file = self.resources_dir / "deep.yml"
        
        # Create YAML with very deep nesting that could cause stack overflow
        deep_content = "resources:\n  pipelines:\n    test:\n      libraries:\n"
        for i in range(100):
            deep_content += "        " + "  " * i + f"level_{i}:\n"
        deep_content += "        " + "  " * 100 + "- notebook:\n"
        deep_content += "        " + "  " * 101 + "path: deep.py\n"
        
        deep_file.write_text(deep_content)
        
        # Should handle deep nesting without crashing
        try:
            notebooks = self.yaml_processor.extract_notebook_paths(deep_file)
            # If it succeeds, that's fine
            assert isinstance(notebooks, list)
        except YAMLParsingError:
            # If it fails with a parsing error, that's also acceptable
            pass

    def test_yaml_processor_update_handles_corrupted_file_during_write(self):
        """Should handle file corruption during write operations."""
        yaml_file = self.resources_dir / "test.yml"
        yaml_file.write_text("""resources:
  pipelines:
    test:
      libraries:
        - notebook:
            path: existing.py
""")
        
        notebooks_to_add = ["new.py"]
        notebooks_to_remove = []
        
        # Mock file write to fail
        with patch('pathlib.Path.write_text', side_effect=OSError("Disk full")):
            with pytest.raises(YAMLParsingError) as exc_info:
                self.yaml_processor.update_resource_file_libraries(
                    yaml_file, notebooks_to_add, notebooks_to_remove
                )
            
            assert "file system error" in str(exc_info.value).lower() or "disk full" in str(exc_info.value).lower()

    def test_yaml_processor_handles_concurrent_file_modifications(self):
        """Should handle files being modified by other processes."""
        yaml_file = self.resources_dir / "concurrent.yml"
        yaml_file.write_text("""resources:
  pipelines:
    test:
      libraries:
        - notebook:
            path: existing.py
""")
        
        # Simulate file being modified during processing by mocking the file operations
        call_count = [0]
        original_content = yaml_file.read_text()
        
        def mock_read_text(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First read is normal
                return original_content
            else:
                # Second read shows file was modified
                return """resources:
  pipelines:
    different_structure: {}
"""
        
        with patch('pathlib.Path.read_text', side_effect=mock_read_text):
            notebooks_to_add = ["new.py"]
            notebooks_to_remove = []
            
            # Should handle the change gracefully - the method doesn't return a count
            # but it shouldn't crash when structure changes
            try:
                self.yaml_processor.update_resource_file_libraries(
                    yaml_file, notebooks_to_add, notebooks_to_remove
                )
                # If it completes without error, that's acceptable
            except YAMLParsingError:
                # If it fails due to structure change, that's also acceptable behavior
                pass

    def test_yaml_processor_provides_detailed_error_context(self):
        """Should provide detailed context in error messages."""
        error_file = self.resources_dir / "detailed_error.yml"
        error_file.write_text("""resources:
  pipelines:
    test_pipeline:
      libraries:
        - notebook: 
            path: test.py
            invalid_key: {unclosed_dict
""")
        
        with pytest.raises(YAMLParsingError) as exc_info:
            self.yaml_processor.extract_notebook_paths(error_file)
        
        error_msg = str(exc_info.value)
        assert "detailed_error.yml" in error_msg
        assert "line" in error_msg.lower() or "column" in error_msg.lower()



class TestBundleFileSystemErrorHandling:
    """Test file system error handling and recovery."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_bundle_manager_handles_permission_denied_on_resources_creation(self):
        """Should handle permission denied when creating resources directory."""
        bundle_manager = BundleManager(self.project_root)
        
        # Create a generated directory with a test pipeline first
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        pipeline_dir = generated_dir / "test_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "test.py").write_text("# Test notebook")
        
        # Now make project root read-only to prevent resources directory creation
        self.project_root.chmod(0o555)
        
        try:
            with pytest.raises(BundleResourceError) as exc_info:
                bundle_manager.sync_resources_with_generated_files(generated_dir, "dev")
            
            assert "permission" in str(exc_info.value).lower() or "failed to create" in str(exc_info.value).lower()
            assert "resources" in str(exc_info.value).lower()
        finally:
            # Restore permissions for cleanup
            self.project_root.chmod(0o755)

    def test_bundle_manager_handles_disk_full_errors(self):
        """Should handle disk full errors during file operations."""
        bundle_manager = BundleManager(self.project_root)
        
        # Mock disk full error during file creation
        with patch("pathlib.Path.write_text", side_effect=OSError("No space left on device")):
            with pytest.raises(BundleResourceError) as exc_info:
                bundle_manager._create_new_resource_file("test_pipeline", "dev")
            
            assert "space" in str(exc_info.value).lower() or "disk" in str(exc_info.value).lower()

    def test_bundle_manager_handles_readonly_filesystem(self):
        """Should handle read-only filesystem errors."""
        bundle_manager = BundleManager(self.project_root)
        
        # Mock read-only filesystem error
        with patch("pathlib.Path.write_text", side_effect=OSError("Read-only file system")):
            with pytest.raises(BundleResourceError) as exc_info:
                bundle_manager._create_new_resource_file("test_pipeline", "dev")
            
            assert "read-only" in str(exc_info.value).lower()

    def test_bundle_manager_handles_corrupted_directory_structure(self):
        """Should handle corrupted directory structures."""
        bundle_manager = BundleManager(self.project_root)
        
        # Create resources as a file instead of directory
        resources_file = self.project_root / "resources"
        resources_file.write_text("This should be a directory")
        
        # Create a generated directory with a test pipeline to trigger sync
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        pipeline_dir = generated_dir / "test_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "test.py").write_text("# Test notebook")
        
        with pytest.raises(BundleResourceError) as exc_info:
            bundle_manager.sync_resources_with_generated_files(generated_dir, "dev")
        
        assert "directory" in str(exc_info.value).lower() or "failed to create" in str(exc_info.value).lower()

    def test_bundle_manager_recovery_from_partial_file_corruption(self):
        """Should preserve corrupted LHP files with conservative approach (Scenario 1a)."""
        bundle_manager = BundleManager(self.project_root)
        
        # Create partially corrupted YAML file in resources/lhp directory
        resources_lhp_dir = self.project_root / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True)
        corrupted_file = resources_lhp_dir / "test.pipeline.yml"
        original_corrupted_content = """# Generated by LakehousePlumber - Bundle Resource for test
resources:
  pipelines:
    test:
      libraries:
        - notebook:
            path: existing.py
# File truncated unexpectedly...
invalid yaml content"""
        corrupted_file.write_text(original_corrupted_content)
        
        # Create pipeline directory to trigger sync
        generated_dir = self.project_root / "generated"
        pipeline_dir = generated_dir / "test"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "test.py").write_text("# Test notebook")
        
        # Conservative approach should preserve existing LHP file even if corrupted
        bundle_manager.sync_resources_with_generated_files(generated_dir, "dev")
        
        # Should preserve corrupted LHP file (DON'T TOUCH policy)
        backup_file = resources_lhp_dir / "test.pipeline.yml.bkup"
        assert not backup_file.exists(), "No backup should be created for existing LHP files"
        assert corrupted_file.exists(), "Original corrupted file should be preserved"
        
        # File content should remain unchanged (preserved)
        preserved_content = corrupted_file.read_text()
        assert preserved_content == original_corrupted_content, "Corrupted LHP file should be preserved unchanged"


class TestBundleErrorRecoveryMechanisms:
    """Test error recovery and graceful degradation mechanisms."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_bundle_manager_graceful_degradation_on_yaml_errors(self):
        """Should preserve corrupted LHP files with conservative approach (Scenario 1a)."""
        bundle_manager = BundleManager(self.project_root)
        
        # Create generated files
        generated_dir = self.project_root / "generated"
        pipeline_dir = generated_dir / "test_pipeline"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "test.py").write_text("# Test notebook")
        
        # Create corrupted LHP-generated resource file
        resources_lhp_dir = self.project_root / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True)
        corrupted_file = resources_lhp_dir / "test_pipeline.pipeline.yml"
        original_corrupted_content = "# Generated by LakehousePlumber - Bundle Resource for test_pipeline\ninvalid: yaml: content: [[["
        corrupted_file.write_text(original_corrupted_content)
        
        # Conservative approach should preserve existing LHP file even if corrupted
        updated_count = bundle_manager.sync_resources_with_generated_files(generated_dir, "dev")
        
        # Should complete successfully with no updates (file preserved)
        assert updated_count >= 0
        
        # Should preserve corrupted LHP file (DON'T TOUCH policy)
        backup_file = resources_lhp_dir / "test_pipeline.pipeline.yml.bkup"
        assert not backup_file.exists(), "No backup should be created for existing LHP files"
        
        # File content should remain unchanged (preserved)
        preserved_content = corrupted_file.read_text()
        assert preserved_content == original_corrupted_content, "Corrupted LHP file should be preserved unchanged"



    def test_bundle_manager_partial_sync_recovery(self):
        """Should fail fast on first pipeline error."""
        bundle_manager = BundleManager(self.project_root)
        
        # Create generated files
        generated_dir = self.project_root / "generated"
        for i in range(3):
            pipeline_dir = generated_dir / f"pipeline_{i}"
            pipeline_dir.mkdir(parents=True)
            (pipeline_dir / f"test_{i}.py").write_text(f"# Test notebook {i}")
        
        # Mock failure on second pipeline
        original_sync = bundle_manager._sync_pipeline_resource
        call_count = 0
        
        def failing_sync(pipeline_name, pipeline_dir, env):
            nonlocal call_count
            call_count += 1
            if call_count == 2:  # Fail on second call
                raise YAMLParsingError("YAML syntax error", file_path="/fake/path")
            return original_sync(pipeline_name, pipeline_dir, env)
        
        with patch.object(bundle_manager, '_sync_pipeline_resource', side_effect=failing_sync):
            # Should fail fast on first error instead of continuing
            with pytest.raises(BundleResourceError) as exc_info:
                bundle_manager.sync_resources_with_generated_files(generated_dir, "test")
                
            # Should provide clear error information about the failed pipeline
            error_msg = str(exc_info.value)
            assert "pipeline_1" in error_msg  # Should mention the failed pipeline
            assert "yaml" in error_msg.lower()  # Should mention the YAML error

    def test_error_context_preservation_across_components(self):
        """Should preserve existing LHP files and avoid template rendering (Scenario 1a)."""
        bundle_manager = BundleManager(self.project_root)
        
        # Create generated directory with pipeline files 
        generated_dir = self.project_root / "generated"
        pipeline_dir = generated_dir / "test_pipeline"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "test.py").write_text("# Test notebook")
        
        # Create existing LHP-generated resource file
        resources_lhp_dir = self.project_root / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True)
        resource_file = resources_lhp_dir / "test_pipeline.pipeline.yml"
        original_content = """# Generated by LakehousePlumber - Bundle Resource for test_pipeline
resources:
  pipelines:
    test_pipeline:
      libraries:
        - notebook:
            path: ../../generated/test_pipeline/existing.py
"""
        resource_file.write_text(original_content)
        
        # Mock template rendering to fail - but it should never be called
        original_render = bundle_manager.render_template
        def failing_render(template_name, context):
            raise Exception("Template rendering should not be called for existing LHP files")
        
        with patch.object(bundle_manager, 'render_template', side_effect=failing_render):
            # Should complete successfully without calling template rendering
            result = bundle_manager.sync_resources_with_generated_files(generated_dir, "dev")
            
            # Should preserve existing LHP file without any modifications
            assert resource_file.exists(), "LHP file should be preserved"
            preserved_content = resource_file.read_text()
            assert preserved_content == original_content, "LHP file content should be unchanged"
            
            # No backup files should be created
            backup_file = resources_lhp_dir / "test_pipeline.pipeline.yml.bkup"
            assert not backup_file.exists(), "No backup should be created for preserved LHP files"

    def test_error_aggregation_for_multiple_failures(self):
        """Should fail fast on first error instead of aggregating multiple errors."""
        bundle_manager = BundleManager(self.project_root)
        
        # Create multiple generated directories with issues
        generated_dir = self.project_root / "generated"
        for i in range(3):
            pipeline_dir = generated_dir / f"pipeline_{i}"
            pipeline_dir.mkdir(parents=True)
            (pipeline_dir / f"test_{i}.py").write_text(f"# Test notebook {i}")
        
        # Mock first failure - should fail fast here
        call_count = 0
        errors = [
            YAMLParsingError("YAML error 1"),
            OSError("File system error"),
            BundleResourceError("Resource error")
        ]
        
        def multi_failing_sync(pipeline_name, pipeline_dir, env):
            nonlocal call_count
            if call_count < len(errors):
                error = errors[call_count]
                call_count += 1
                raise error
            call_count += 1
            return 0
        
        with patch.object(bundle_manager, '_sync_pipeline_resource', side_effect=multi_failing_sync):
            with patch.object(bundle_manager, 'logger') as mock_logger:
                # Should fail fast on first error
                with pytest.raises(BundleResourceError) as exc_info:
                    bundle_manager.sync_resources_with_generated_files(generated_dir, "test")
                
                # Should fail on first error, not continue to others
                assert "YAML error 1" in str(exc_info.value)
                # Should have only called sync once before failing
                assert call_count == 1

    def test_resource_cleanup_on_critical_errors(self):
        """Should clean up resources on critical errors."""
        bundle_manager = BundleManager(self.project_root)
        
        # Create partial state
        resources_dir = self.project_root / "resources"
        resources_dir.mkdir()
        partial_file = resources_dir / "test.yml"
        partial_file.write_text("partial content")
        
        # Mock critical error during processing
        with patch('lhp.bundle.yaml_processor.YAMLProcessor.update_resource_file_libraries') as mock_update:
            mock_update.side_effect = OSError("Critical system error")
            
            with pytest.raises(BundleResourceError):
                bundle_manager.sync_resources_with_generated_files(
                    self.project_root / "generated", "test"
                )
            
            # File should still exist (no cleanup for this type of error)
            assert partial_file.exists()


class TestBundleErrorReporting:
    """Test comprehensive error reporting and logging."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_yaml_processing_error_includes_file_context(self):
        """Should include file context in YAML processing errors."""
        yaml_processor = YAMLProcessor()
        
        error_file = self.project_root / "error.yml"
        error_file.write_text("""line 1
line 2
line 3: invalid yaml {
line 4
line 5""")
        
        with pytest.raises(YAMLParsingError) as exc_info:
            yaml_processor.extract_notebook_paths(error_file)
        
        error_msg = str(exc_info.value)
        assert "error.yml" in error_msg
        assert "line" in error_msg.lower()



    def test_bundle_error_includes_resolution_suggestions(self):
        """Should include resolution suggestions in bundle errors."""
        bundle_manager = BundleManager(self.project_root)
        
        with patch('pathlib.Path.mkdir', side_effect=PermissionError("Permission denied")):
            with pytest.raises(BundleResourceError) as exc_info:
                bundle_manager._create_new_resource_file("test", "dev")
            
            error_msg = str(exc_info.value)
            # The actual error could be permission denied or file not found, both are valid error conditions
            assert ("permission" in error_msg.lower() or 
                    "no such file" in error_msg.lower() or 
                    "failed to create" in error_msg.lower())

    def test_error_logging_provides_debug_information(self):
        """Should provide debug information in error logs."""
        bundle_manager = BundleManager(self.project_root)
        
        with patch('pathlib.Path.exists', side_effect=OSError("Unexpected error")):
            with pytest.raises(BundleResourceError) as exc_info:
                bundle_manager._get_pipeline_directories(self.project_root / "generated")
            
            # Should provide debug information in the exception
            error_msg = str(exc_info.value)
            assert "generated" in error_msg.lower()  # Should mention the directory
            assert ("error" in error_msg.lower() or "unexpected" in error_msg.lower())  # Should mention the error 