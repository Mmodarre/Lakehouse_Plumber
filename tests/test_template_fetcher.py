"""
Tests for Databricks template processing functionality.

Tests the local template processing with Jinja2 for bundle file creation,
including success scenarios, edge cases, and error handling.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from lhp.bundle.template_fetcher import DatabricksTemplateFetcher, TemplateError


class TestDatabricksTemplateFetcher:
    """Test suite for local Databricks template processing functionality."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.fetcher = DatabricksTemplateFetcher(self.project_root)

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_template_fetcher_initialization(self):
        """Should initialize with correct project root."""
        assert self.fetcher.project_root == self.project_root

    def test_process_local_template_with_project_name(self):
        """Should process embedded Jinja template with project name substitution."""
        project_name = "my_test_project"
        template_vars = {"project_name": project_name}
        
        # This test will define the expected behavior for local template processing
        result = self.fetcher._process_local_template(project_name, template_vars)
        
        # Verify template processing
        assert "name: my_test_project" in result
        assert "{{" not in result  # No unprocessed template variables
        assert "}}" not in result
        
    def test_process_local_template_special_characters(self):
        """Should handle project names with special characters."""
        project_name = "my-project_123"
        template_vars = {"project_name": project_name}
        
        result = self.fetcher._process_local_template(project_name, template_vars)
        
        assert "name: my-project_123" in result
        assert "{{" not in result
        
    def test_process_local_template_empty_name(self):
        """Should handle empty project name gracefully."""
        project_name = ""
        template_vars = {"project_name": project_name}
        
        result = self.fetcher._process_local_template(project_name, template_vars)
        
        assert "name:" in result  # Should still have the key
        
    def test_create_bundle_files_structure(self):
        """Should create databricks.yml and resources folder in project root."""
        project_name = "test_project"
        template_vars = {"project_name": project_name}
        
        # This test defines expected file creation behavior
        self.fetcher.create_bundle_files(project_name, template_vars)
        
        # Verify files and folders are created
        assert (self.project_root / "databricks.yml").exists()
        assert (self.project_root / "resources").exists()
        assert (self.project_root / "resources").is_dir()
        # Verify LHP subdirectory is created
        assert (self.project_root / "resources" / "lhp").exists()
        assert (self.project_root / "resources" / "lhp").is_dir()
        
        # Verify content
        content = (self.project_root / "databricks.yml").read_text()
        assert "name: test_project" in content
        assert "include:" in content
        assert "resources/lhp/*.yml" in content
        
    def test_create_bundle_files_preserves_existing_structure(self):
        """Should not overwrite existing LHP project structure."""
        # Create existing LHP files
        (self.project_root / "lhp.yaml").write_text("name: existing_project")
        pipelines_dir = self.project_root / "pipelines"
        pipelines_dir.mkdir()
        (pipelines_dir / "test.yaml").write_text("pipeline: test")
        
        project_name = "test_project"
        template_vars = {"project_name": project_name}
        
        self.fetcher.create_bundle_files(project_name, template_vars)
        
        # Verify existing files are preserved
        assert (self.project_root / "lhp.yaml").exists()
        assert (self.project_root / "pipelines" / "test.yaml").exists()
        
        # Verify new bundle files are added
        assert (self.project_root / "databricks.yml").exists()
        assert (self.project_root / "resources").exists()

    def test_create_bundle_files_error_handling(self):
        """Should handle file creation errors gracefully."""
        project_name = "test_project"
        template_vars = {"project_name": project_name}
        
        # Make project root read-only to trigger permission error
        self.project_root.chmod(0o444)
        
        try:
            with pytest.raises(TemplateError) as exc_info:
                self.fetcher.create_bundle_files(project_name, template_vars)
            
            assert "Failed to create bundle files" in str(exc_info.value)
        finally:
            # Restore permissions for cleanup
            self.project_root.chmod(0o755)
            
    def test_create_bundle_files_overwrites_existing_databricks_yml(self):
        """Should overwrite existing databricks.yml with new template."""
        # Create existing databricks.yml with old content
        (self.project_root / "databricks.yml").write_text("bundle:\n  name: old_project")
        
        project_name = "new_project"
        template_vars = {"project_name": project_name}
        
        self.fetcher.create_bundle_files(project_name, template_vars)
        
        # Verify old content is replaced
        content = (self.project_root / "databricks.yml").read_text()
        assert "name: new_project" in content
        assert "old_project" not in content
        
    def test_create_bundle_files_creates_empty_resources_folder(self):
        """Should create resources folder with lhp subdirectory for bundle resources."""
        project_name = "test_project"
        template_vars = {"project_name": project_name}

        self.fetcher.create_bundle_files(project_name, template_vars)

        resources_dir = self.project_root / "resources"
        assert resources_dir.exists()
        assert resources_dir.is_dir()

        # Should contain lhp subdirectory
        lhp_dir = resources_dir / "lhp"
        assert lhp_dir.exists()
        assert lhp_dir.is_dir()
        
        # LHP subdirectory should be empty initially
        assert len(list(lhp_dir.iterdir())) == 0

    def test_jinja_template_unicode_project_name(self):
        """Should handle Unicode characters in project names."""
        project_name = "プロジェクト_测试"
        template_vars = {"project_name": project_name}
        
        result = self.fetcher._process_local_template(project_name, template_vars)
        
        assert f"name: {project_name}" in result
        assert "{{" not in result
        
    def test_jinja_template_very_long_project_name(self):
        """Should handle very long project names."""
        project_name = "a" * 255  # Very long name
        template_vars = {"project_name": project_name}
        
        result = self.fetcher._process_local_template(project_name, template_vars)
        
        assert f"name: {project_name}" in result
        
    def test_jinja_template_special_yaml_characters(self):
        """Should properly escape YAML special characters in project names."""
        project_name = "project:with@special#chars"
        template_vars = {"project_name": project_name}
        
        result = self.fetcher._process_local_template(project_name, template_vars)
        
        # Should be properly quoted or escaped for YAML
        assert project_name in result
        assert "{{" not in result
        
    def test_jinja_template_preserves_yaml_structure(self):
        """Should preserve original YAML structure and formatting."""
        project_name = "test_project"
        template_vars = {"project_name": project_name}
        
        result = self.fetcher._process_local_template(project_name, template_vars)
        
        # Check key YAML structure elements are preserved
        assert "bundle:" in result
        assert "include:" in result
        assert "targets:" in result
        assert "dev:" in result
        assert "prod:" in result
        assert "resources/*.yml" in result
        
    def test_jinja_template_malformed_input(self):
        """Should handle malformed template variables gracefully."""
        project_name = "test_project"
        template_vars = None  # Malformed input
        
        with pytest.raises(TemplateError) as exc_info:
            self.fetcher._process_local_template(project_name, template_vars)
        
        assert "process template" in str(exc_info.value).lower()

    def test_fetch_and_apply_template_calls_create_bundle_files(self):
        """Should call create_bundle_files method for backward compatibility."""
        project_name = "test_project"
        template_vars = {"project_name": project_name}
        
        self.fetcher.fetch_and_apply_template(project_name, template_vars)
        
        # Verify files were created (showing create_bundle_files was called)
        assert (self.project_root / "databricks.yml").exists()
        assert (self.project_root / "resources").exists()
        
        # Verify content
        content = (self.project_root / "databricks.yml").read_text()
        assert "name: test_project" in content


class TestTemplateErrorHandling:
    """Test error handling and edge cases for local template processing."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.fetcher = DatabricksTemplateFetcher(self.project_root)

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_template_error_with_context(self):
        """Should create TemplateError with helpful context."""
        original_error = ValueError("Processing failed")
        
        error = TemplateError("Failed to process template", original_error)
        
        assert "Failed to process template" in str(error)
        assert "Processing failed" in str(error)
        assert error.original_error == original_error

    def test_fetch_template_with_readonly_project_root(self):
        """Should handle permission errors when writing to project root."""
        # Make project root read-only
        self.project_root.chmod(0o444)
        
        try:
            with pytest.raises(TemplateError) as exc_info:
                self.fetcher.fetch_and_apply_template("test_project", {"project_name": "test_project"})
            
            assert "Failed to create bundle files" in str(exc_info.value)
        finally:
            # Restore permissions for cleanup
            self.project_root.chmod(0o755) 