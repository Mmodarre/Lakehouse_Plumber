"""
Tests for YAML parsing and manipulation functionality.

Tests the YAML processing capabilities for bundle resource files,
including parsing, validation, and incremental updates.
"""

import pytest
import tempfile
import shutil
import time
from pathlib import Path
import yaml
from unittest.mock import Mock, patch

from lhp.bundle.yaml_processor import YAMLProcessor, YAMLParsingError
from lhp.bundle.exceptions import BundleResourceError


class TestYAMLProcessor:
    """Test suite for YAML processing functionality."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.processor = YAMLProcessor()

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_yaml_processor_initialization(self):
        """Should initialize YAML processor correctly."""
        assert hasattr(self.processor, 'logger')
        assert self.processor.logger.name == 'lhp.bundle.yaml_processor'



















    def test_update_resource_file_libraries_add_notebooks(self):
        """Should correctly add new notebook entries while preserving other content."""
        original_yaml = """
resources:
  pipelines:
    test_pipeline:
      name: test_pipeline
      catalog: main
      schema: test_${bundle.target}
      libraries:
        - notebook:
            path: ../generated/test/existing1.py
        - notebook:
            path: ../generated/test/existing2.py
      configuration:
        bundle.sourcePath: ${workspace.file_path}/generated
        custom_setting: important_value
"""
        
        yaml_file = self.temp_dir / "update_test.yml"
        yaml_file.write_text(original_yaml)
        
        to_add = ["../generated/test/new1.py", "../generated/test/new2.py"]
        to_remove = []
        
        self.processor.update_resource_file_libraries(yaml_file, to_add, to_remove)
        
        # Verify the file was updated
        updated_paths = self.processor.extract_notebook_paths(yaml_file)
        
        expected_paths = [
            "../generated/test/existing1.py",
            "../generated/test/existing2.py",
            "../generated/test/new1.py",
            "../generated/test/new2.py"
        ]
        
        assert sorted(updated_paths) == sorted(expected_paths)
        
        # Verify other content is preserved
        updated_content = yaml_file.read_text()
        assert "custom_setting: important_value" in updated_content
        assert "catalog: main" in updated_content

    def test_update_resource_file_libraries_remove_notebooks(self):
        """Should correctly remove notebook entries while preserving other content."""
        original_yaml = """
resources:
  pipelines:
    test_pipeline:
      name: test_pipeline
      catalog: main
      libraries:
        - notebook:
            path: ../generated/test/keep1.py
        - notebook:
            path: ../generated/test/remove1.py
        - notebook:
            path: ../generated/test/keep2.py
        - notebook:
            path: ../generated/test/remove2.py
      configuration:
        bundle.sourcePath: ${workspace.file_path}/generated
"""
        
        yaml_file = self.temp_dir / "remove_test.yml"
        yaml_file.write_text(original_yaml)
        
        to_add = []
        to_remove = ["../generated/test/remove1.py", "../generated/test/remove2.py"]
        
        self.processor.update_resource_file_libraries(yaml_file, to_add, to_remove)
        
        # Verify the file was updated
        updated_paths = self.processor.extract_notebook_paths(yaml_file)
        
        expected_paths = [
            "../generated/test/keep1.py",
            "../generated/test/keep2.py"
        ]
        
        assert sorted(updated_paths) == sorted(expected_paths)

    def test_update_resource_file_libraries_add_and_remove(self):
        """Should handle both adding and removing notebooks in single operation."""
        original_yaml = """
resources:
  pipelines:
    test_pipeline:
      libraries:
        - notebook:
            path: ../generated/test/keep.py
        - notebook:
            path: ../generated/test/remove.py
        - jar: /path/to/preserve.jar
"""
        
        yaml_file = self.temp_dir / "add_remove_test.yml"
        yaml_file.write_text(original_yaml)
        
        to_add = ["../generated/test/new.py"]
        to_remove = ["../generated/test/remove.py"]
        
        self.processor.update_resource_file_libraries(yaml_file, to_add, to_remove)
        
        # Verify the file was updated
        updated_paths = self.processor.extract_notebook_paths(yaml_file)
        
        expected_paths = [
            "../generated/test/keep.py",
            "../generated/test/new.py"
        ]
        
        assert sorted(updated_paths) == sorted(expected_paths)
        
        # Verify non-notebook libraries are preserved
        updated_content = yaml_file.read_text()
        assert "jar: /path/to/preserve.jar" in updated_content

    def test_update_resource_file_libraries_preserve_formatting(self):
        """Should maintain reasonable YAML formatting after updates."""
        original_yaml = """# Important comment at top
resources:
  pipelines:
    test_pipeline:
      name: test_pipeline  # Pipeline name comment
      catalog: main
      libraries:
        - notebook:
            path: ../generated/test/existing.py
      # End of configuration
      configuration:
        bundle.sourcePath: ${workspace.file_path}/generated
"""
        
        yaml_file = self.temp_dir / "formatting_test.yml"
        yaml_file.write_text(original_yaml)
        
        to_add = ["../generated/test/new.py"]
        to_remove = []
        
        self.processor.update_resource_file_libraries(yaml_file, to_add, to_remove)
        
        updated_content = yaml_file.read_text()
        
        # Check that basic structure is maintained
        assert "resources:" in updated_content
        assert "pipelines:" in updated_content
        assert "libraries:" in updated_content
        assert "../generated/test/new.py" in updated_content
        assert "../generated/test/existing.py" in updated_content

    def test_extract_notebook_paths_file_not_found(self):
        """Should handle file not found errors gracefully."""
        nonexistent_file = self.temp_dir / "nonexistent.yml"
        
        with pytest.raises(YAMLParsingError) as exc_info:
            self.processor.extract_notebook_paths(nonexistent_file)
        
        assert "File not found" in str(exc_info.value)
        assert "nonexistent.yml" in str(exc_info.value)

    def test_extract_notebook_paths_permission_denied(self):
        """Should handle permission denied errors gracefully."""
        yaml_file = self.temp_dir / "restricted.yml"
        yaml_file.write_text("resources:\n  pipelines: {}")
        yaml_file.chmod(0o000)  # No permissions
        
        try:
            with pytest.raises(YAMLParsingError) as exc_info:
                self.processor.extract_notebook_paths(yaml_file)
            
            assert "Permission denied" in str(exc_info.value)
        finally:
            # Restore permissions for cleanup
            yaml_file.chmod(0o644)

    def test_update_resource_file_libraries_invalid_yaml(self):
        """Should handle attempts to update invalid YAML files."""
        invalid_yaml = """
resources:
  pipelines:
    test_pipeline:
      invalid: yaml: structure:
        - malformed
"""
        
        yaml_file = self.temp_dir / "invalid.yml"
        yaml_file.write_text(invalid_yaml)
        
        with pytest.raises(YAMLParsingError):
            self.processor.update_resource_file_libraries(yaml_file, ["new.py"], [])

    def test_update_resource_file_libraries_readonly_file(self):
        """Should handle read-only files appropriately."""
        yaml_content = """
resources:
  pipelines:
    test_pipeline:
      libraries:
        - notebook:
            path: ../generated/test/existing.py
"""
        
        yaml_file = self.temp_dir / "readonly.yml"
        yaml_file.write_text(yaml_content)
        yaml_file.chmod(0o444)  # Read-only
        
        try:
            with pytest.raises(YAMLParsingError) as exc_info:
                self.processor.update_resource_file_libraries(yaml_file, ["new.py"], [])
            
            assert "Permission denied" in str(exc_info.value) or "Read-only" in str(exc_info.value)
        finally:
            # Restore permissions for cleanup
            yaml_file.chmod(0o644)


class TestYAMLProcessorEdgeCases:
    """Test edge cases and error conditions for YAML processing."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.processor = YAMLProcessor()

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_extract_notebook_paths_unicode_content(self):
        """Should handle YAML files with Unicode content."""
        yaml_content = """
# YAML with Unicode: 中文 русский العربية
resources:
  pipelines:
    test_pipeline:
      name: "pipeline_with_unicode_中文"
      libraries:
        - notebook:
            path: ../generated/test/файл.py
        - notebook:
            path: ../generated/test/ملف.py
"""
        
        yaml_file = self.temp_dir / "unicode.yml"
        yaml_file.write_text(yaml_content, encoding='utf-8')
        
        notebook_paths = self.processor.extract_notebook_paths(yaml_file)
        
        expected_paths = [
            "../generated/test/файл.py",
            "../generated/test/ملف.py"
        ]
        
        assert sorted(notebook_paths) == sorted(expected_paths)

    def test_extract_notebook_paths_large_file(self):
        """Should handle large YAML files efficiently."""
        # Create YAML with many library entries
        libraries_section = ""
        expected_paths = []
        
        for i in range(1000):
            path = f"../generated/large/file_{i:04d}.py"
            libraries_section += f"""        - notebook:
            path: {path}
"""
            expected_paths.append(path)
        
        yaml_content = f"""
resources:
  pipelines:
    large_pipeline:
      libraries:
{libraries_section}"""
        
        yaml_file = self.temp_dir / "large.yml"
        yaml_file.write_text(yaml_content)
        
        notebook_paths = self.processor.extract_notebook_paths(yaml_file)
        
        assert len(notebook_paths) == 1000
        assert sorted(notebook_paths) == sorted(expected_paths)

    def test_concurrent_yaml_processing(self):
        """Should handle concurrent YAML processing safely."""
        import threading
        import time
        
        yaml_content = """
resources:
  pipelines:
    test_pipeline:
      libraries:
        - notebook:
            path: ../generated/test/concurrent.py
"""
        
        yaml_file = self.temp_dir / "concurrent.yml"
        yaml_file.write_text(yaml_content)
        
        results = []
        errors = []
        
        def process_yaml():
            try:
                time.sleep(0.01)  # Small delay
                paths = self.processor.extract_notebook_paths(yaml_file)
                results.append(len(paths))
            except Exception as e:
                errors.append(e)
        
        # Run multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=process_yaml)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All operations should complete successfully
        assert len(errors) == 0
        assert len(results) == 5
        assert all(r == 1 for r in results)

    def test_yaml_processing_error_contains_context(self):
        """Should provide helpful context in error messages."""
        yaml_file = self.temp_dir / "context_test.yml"
        yaml_file.write_text("invalid: yaml: content")
        
        try:
            self.processor.extract_notebook_paths(yaml_file)
        except YAMLParsingError as e:
            assert str(yaml_file) in str(e)
            assert "YAML processing error" in str(e)
            assert hasattr(e, 'file_path')
            assert e.file_path == str(yaml_file)

    def test_update_libraries_no_changes_needed(self):
        """Should handle case where no changes are needed."""
        yaml_content = """
resources:
  pipelines:
    test_pipeline:
      libraries:
        - notebook:
            path: ../generated/test/file1.py
        - notebook:
            path: ../generated/test/file2.py
"""
        
        yaml_file = self.temp_dir / "no_changes.yml"
        yaml_file.write_text(yaml_content)
        
        original_mtime = yaml_file.stat().st_mtime
        time.sleep(0.1)  # Ensure time difference
        
        # No changes - empty add/remove lists
        self.processor.update_resource_file_libraries(yaml_file, [], [])
        
        # File should not be modified
        new_mtime = yaml_file.stat().st_mtime
        assert new_mtime == original_mtime 