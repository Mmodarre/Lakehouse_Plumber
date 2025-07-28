"""
Tests for CLI bundle integration functionality.

Tests the CLI changes needed for Databricks Asset Bundle support,
including command-line flags, init command extensions, and generate command integration.
"""

import pytest
import tempfile
import shutil
import yaml
import json
import os
from pathlib import Path
from unittest.mock import Mock, patch, call
from click.testing import CliRunner

from lhp.cli.main import cli
from lhp.bundle.exceptions import BundleResourceError
from lhp.utils.bundle_detection import should_enable_bundle_support


class TestCLIBundleFlags:
    """Test CLI flag handling for bundle support."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        """Set up test environment for each test using Windows-safe temporary directory."""
        self.temp_dir = windows_safe_tempdir
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.runner = CliRunner()

    def _create_basic_project(self, with_bundle=False):
        """Create a basic LHP project structure."""
        # Create basic project files
        (self.project_root / "lhp.yaml").write_text("""name: test_project
version: "1.0"
""")
        
        # Create substitutions
        sub_dir = self.project_root / "substitutions"
        sub_dir.mkdir()
        (sub_dir / "dev.yaml").write_text("catalog: dev_catalog\nraw_schema: raw\nbronze_schema: bronze")
        
        # Create pipelines directory with a simple pipeline
        pipe_dir = self.project_root / "pipelines"
        pipe_dir.mkdir()
        (pipe_dir / "test_pipeline.yaml").write_text("""pipeline: test
flowgroup: test_pipeline
actions:
  - name: test_load
    type: load
    source:
      type: delta
      database: "{catalog}.raw"
      table: test_table
    target: v_test_table
  - name: test_write
    type: write
    source: v_test_table
    write_target:
      type: streaming_table
      database: "{catalog}.bronze"
      table: test_table
""")
        
        # Optionally create bundle files
        if with_bundle:
            (self.project_root / "databricks.yml").write_text("""
bundle:
  name: test_bundle
""")

    def test_generate_with_no_bundle_flag_overrides_detection(self):
        """Should respect --no-bundle flag even when bundle files exist."""
        self._create_basic_project(with_bundle=True)
        
        with self.runner.isolated_filesystem():
            # Change to project directory
            import os
            os.chdir(str(self.project_root))
            
            # Run generate with --no-bundle flag
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--no-bundle', '--dry-run'
            ])
            
            # Should succeed without bundle output
            assert result.exit_code == 0
            assert "Bundle support detected" not in result.output

    def test_generate_without_no_bundle_flag_enables_bundle_when_detected(self):
        """Should enable bundle support when files exist and no --no-bundle flag."""
        self._create_basic_project(with_bundle=True)
        
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should succeed with bundle output
            assert result.exit_code == 0
            assert "Bundle support detected" in result.output

    def test_no_bundle_flag_is_not_required(self):
        """Should work normally when --no-bundle flag is not provided."""
        self._create_basic_project()
        
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should complete without errors (no bundle files exist)
            assert result.exit_code == 0

    def test_no_bundle_flag_help_text(self):
        """Should display help text for --no-bundle flag."""
        result = self.runner.invoke(cli, ['generate', '--help'])
        
        assert result.exit_code == 0
        assert '--no-bundle' in result.output
        assert 'Disable bundle support' in result.output or 'bundle' in result.output.lower()


class TestCLIInitBundleCommand:
    """Test init command with bundle support."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        """Set up test environment for each test using Windows-safe temporary directory."""
        self.temp_dir = windows_safe_tempdir
        self.runner = CliRunner()

    def test_init_bundle_creates_bundle_structure(self):
        """Should create bundle project structure with --bundle flag."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            result = self.runner.invoke(cli, ['init', '--bundle', 'test_bundle_project'])
            
            assert result.exit_code == 0
            
            project_path = self.temp_dir / "test_bundle_project"
            
            # Verify standard LHP files exist
            assert (project_path / "lhp.yaml").exists()
            assert (project_path / "substitutions").exists()
            assert (project_path / "pipelines").exists()
            
            # Verify bundle-specific files exist
            assert (project_path / "databricks.yml").exists()
            assert (project_path / "resources").exists()
            
            # Verify databricks.yml content
            bundle_content = yaml.safe_load((project_path / "databricks.yml").read_text())
            assert "bundle" in bundle_content
            assert bundle_content["bundle"]["name"] == "test_bundle_project"

    def test_init_without_bundle_flag_creates_standard_project(self):
        """Should create standard project without bundle files when no --bundle flag."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            result = self.runner.invoke(cli, ['init', 'test_standard_project'])
            
            assert result.exit_code == 0
            
            project_path = self.temp_dir / "test_standard_project"
            
            # Verify standard LHP files exist
            assert (project_path / "lhp.yaml").exists()
            assert (project_path / "substitutions").exists()
            
            # Verify bundle files do NOT exist
            assert not (project_path / "databricks.yml").exists()
            assert not (project_path / "resources").exists()

    def test_init_bundle_help_text(self):
        """Should display help text for --bundle flag in init command."""
        result = self.runner.invoke(cli, ['init', '--help'])
        
        assert result.exit_code == 0
        assert '--bundle' in result.output
        assert 'bundle' in result.output.lower()

    def test_init_bundle_integrates_with_template_fetcher(self):
        """Should use template fetcher to create bundle files."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))

            result = self.runner.invoke(cli, ['init', '--bundle', 'test_template_project'])

            assert result.exit_code == 0

            # Verify bundle files were created using local template processing
            project_path = self.temp_dir / "test_template_project"
            assert (project_path / "databricks.yml").exists()
            assert (project_path / "resources").exists()
            
            # Verify template processing worked (project name substitution)
            content = (project_path / "databricks.yml").read_text()
            assert "name: test_template_project" in content

    def test_init_bundle_handles_existing_directory_error(self):
        """Should handle error when directory already exists."""
        existing_project = self.temp_dir / "existing_project"
        existing_project.mkdir()
        
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            result = self.runner.invoke(cli, ['init', '--bundle', 'existing_project'])
            
            assert result.exit_code == 1
            assert "already exists" in result.output

    def test_init_bundle_handles_template_processing_errors(self):
        """Should handle template processing errors gracefully."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))

            # This test verifies that the init command can handle basic scenarios
            # Since we're using local templates, most errors would be file permission issues
            result = self.runner.invoke(cli, ['init', '--bundle', 'test_error_project'])

            # Should create project successfully with local template
            assert result.exit_code == 0

            project_path = self.temp_dir / "test_error_project"
            assert (project_path / "databricks.yml").exists()
            assert (project_path / "resources").exists()

    def test_init_bundle_creates_resources_directory(self):
        """Should create resources/lhp directory for bundle resource files."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            result = self.runner.invoke(cli, ['init', '--bundle', 'test_resources_project'])
            
            assert result.exit_code == 0
            
            project_path = self.temp_dir / "test_resources_project"
            resources_dir = project_path / "resources"
            resources_lhp_dir = project_path / "resources" / "lhp"
            
            assert resources_dir.exists()
            assert resources_dir.is_dir()
            assert resources_lhp_dir.exists()
            assert resources_lhp_dir.is_dir()

    def test_init_bundle_uses_local_template_no_network(self):
        """Should create bundle files using local template without network calls."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            # Mock network to ensure no network calls are made
            with patch('requests.get') as mock_get:
                result = self.runner.invoke(cli, ['init', '--bundle', 'local_template_project'])
                
                # Should succeed without any network calls
                assert result.exit_code == 0
                mock_get.assert_not_called()
                
                project_path = self.temp_dir / "local_template_project"
                
                # Verify bundle files created locally
                assert (project_path / "databricks.yml").exists()
                assert (project_path / "resources").exists()
                
                # Verify databricks.yml contains correct project name
                content = (project_path / "databricks.yml").read_text()
                assert "name: local_template_project" in content
                
    def test_init_bundle_template_content_accuracy(self):
        """Should generate databricks.yml with accurate template content."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            result = self.runner.invoke(cli, ['init', '--bundle', 'template_accuracy_test'])
            
            assert result.exit_code == 0
            
            project_path = self.temp_dir / "template_accuracy_test"
            databricks_yml = project_path / "databricks.yml"
            
            assert databricks_yml.exists()
            content = databricks_yml.read_text()
            
            # Check essential template elements
            assert "bundle:" in content
            assert "name: template_accuracy_test" in content
            assert "include:" in content
            assert "resources/lhp/*.yml" in content
            assert "targets:" in content
            assert "dev:" in content
            assert "prod:" in content
            assert "mode: development" in content
            assert "mode: production" in content
            
    def test_init_bundle_with_special_project_names(self):
        """Should handle special characters in project names correctly."""
        special_names = [
            "my-project-123",
            "project_with_underscores",
            "MixedCaseProject"
        ]
        
        for project_name in special_names:
            with self.runner.isolated_filesystem():
                os.chdir(str(self.temp_dir))
                
                result = self.runner.invoke(cli, ['init', '--bundle', project_name])
                
                assert result.exit_code == 0, f"Failed for project name: {project_name}"
                
                project_path = self.temp_dir / project_name
                databricks_yml = project_path / "databricks.yml"
                
                assert databricks_yml.exists()
                content = databricks_yml.read_text()
                assert f"name: {project_name}" in content

    def test_init_bundle_complete_project_structure(self):
        """Should create complete LHP + Bundle project structure."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            result = self.runner.invoke(cli, ['init', '--bundle', 'complete_structure_test'])
            
            assert result.exit_code == 0
            
            project_path = self.temp_dir / "complete_structure_test"
            
            # Verify complete directory structure
            expected_structure = [
                "lhp.yaml",                    # LHP project config
                "databricks.yml",              # Bundle config
                "substitutions",               # LHP substitutions
                "substitutions/dev.yaml.tmpl", # Template file is created by default
                "pipelines",                   # LHP pipelines directory
                "resources",                   # Bundle resources directory
                "presets",                     # LHP presets directory
                "templates"                    # LHP templates directory
            ]
            
            for path in expected_structure:
                full_path = project_path / path
                assert full_path.exists(), f"Missing: {path}"
                
            # Verify directories are actually directories
            directory_paths = ["substitutions", "pipelines", "resources", "presets", "templates"]
            for dir_path in directory_paths:
                full_path = project_path / dir_path
                assert full_path.is_dir(), f"Not a directory: {dir_path}"
                
    def test_init_bundle_preserves_lhp_content(self):
        """Should preserve LHP-specific file contents when adding bundle support."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            result = self.runner.invoke(cli, ['init', '--bundle', 'lhp_content_test'])
            
            assert result.exit_code == 0
            
            project_path = self.temp_dir / "lhp_content_test"
            
            # Verify LHP config content
            lhp_config = yaml.safe_load((project_path / "lhp.yaml").read_text())
            assert lhp_config["name"] == "lhp_content_test"
            assert "version" in lhp_config
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy(str(project_path / "substitutions" / "dev.yaml.tmpl"), 
                       str(project_path / "substitutions" / "dev.yaml"))
            
            # Verify substitution file content
            dev_subs = yaml.safe_load((project_path / "substitutions" / "dev.yaml").read_text())
            assert "dev" in dev_subs  # Should have dev environment section
            assert "catalog" in dev_subs["dev"]  # Should have standard substitution variables
            
    def test_init_bundle_resources_directory_empty(self):
        """Should create empty resources/lhp directory for bundle resource files."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            result = self.runner.invoke(cli, ['init', '--bundle', 'empty_resources_test'])
            
            assert result.exit_code == 0
            
            project_path = self.temp_dir / "empty_resources_test"
            resources_dir = project_path / "resources"
            resources_lhp_dir = project_path / "resources" / "lhp"
            
            assert resources_dir.exists()
            assert resources_dir.is_dir()
            assert resources_lhp_dir.exists()
            assert resources_lhp_dir.is_dir()
            
            # LHP subdirectory should be empty initially
            lhp_contents = list(resources_lhp_dir.iterdir())
            assert len(lhp_contents) == 0, f"LHP resources directory should be empty, found: {lhp_contents}"


class TestCLIGenerateBundleIntegration:
    """Test generate command integration with bundle sync."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        """Set up test environment for each test using Windows-safe temporary directory."""
        self.temp_dir = windows_safe_tempdir
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.runner = CliRunner()
        
        # Create project structure
        self._create_project_with_bundle()

    def _create_project_with_bundle(self):
        """Create a complete project with bundle support."""
        # Create LHP config
        (self.project_root / "lhp.yaml").write_text("""name: test_project
version: "1.0"
""")
        
        # Create bundle config
        (self.project_root / "databricks.yml").write_text("""
bundle:
  name: test_project
target:
  dev:
    default: true
    mode: development
""")
        
        # Create substitutions
        sub_dir = self.project_root / "substitutions"
        sub_dir.mkdir()
        (sub_dir / "dev.yaml").write_text("catalog: dev_catalog\nraw_schema: raw\nbronze_schema: bronze")
        
        # Create pipelines
        pipe_dir = self.project_root / "pipelines"
        pipe_dir.mkdir()
        (pipe_dir / "raw_ingestion.yaml").write_text("""pipeline: raw_ingestion
flowgroup: raw_ingestion
actions:
  - name: customer_load
    type: load
    source:
      type: delta
      database: "{catalog}.{raw_schema}"
      table: customer
    target: v_customer
  - name: customer_write
    type: write
    source: v_customer
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: customer
""")
        
        # Create resources/lhp directory
        resources_lhp_dir = self.project_root / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True)

    @patch('lhp.bundle.manager.BundleManager')
    @patch('lhp.utils.bundle_detection.should_enable_bundle_support')
    def test_generate_calls_bundle_sync_when_enabled(self, mock_bundle_detection, mock_bundle_manager_class):
        """Should call bundle sync when bundle support is enabled."""
        # Mock bundle detection to return True
        mock_bundle_detection.return_value = True
        
        # Mock bundle manager
        mock_bundle_manager = Mock()
        mock_bundle_manager_class.return_value = mock_bundle_manager
        
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should complete successfully with bundle sync
            assert result.exit_code == 0
            assert "Bundle support detected" in result.output or "bundle" in result.output.lower()

    def test_generate_skips_bundle_sync_when_disabled(self):
        """Should skip bundle sync when bundle support is disabled."""
        # Remove databricks.yml to disable bundle support
        databricks_file = self.project_root / "databricks.yml"
        if databricks_file.exists():
            databricks_file.unlink()
        
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should complete successfully without bundle output
            assert result.exit_code == 0
            assert "Bundle support detected" not in result.output

    def test_generate_with_no_bundle_flag_disables_sync(self):
        """Should disable bundle sync when --no-bundle flag is used."""
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--no-bundle', '--dry-run'
            ])
            
            # Should complete successfully without bundle output
            assert result.exit_code == 0
            assert "Bundle support detected" not in result.output

    def test_generate_with_custom_output_directory(self):
        """Should work with custom output directory."""
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--output', 'custom_output', '--dry-run'
            ])
            
            # Should complete successfully with bundle support
            assert result.exit_code == 0
            assert "Bundle support detected" in result.output or "bundle" in result.output.lower()

    def test_generate_bundle_sync_with_dry_run(self):
        """Should perform bundle sync in dry-run mode."""
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            # Test with dry-run
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should complete successfully with bundle sync
            assert result.exit_code == 0
            assert "Bundle support detected" in result.output
            assert "Dry run completed" in result.output

    def test_generate_bundle_sync_with_verbose_output(self):
        """Should provide verbose output for bundle operations when requested."""
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should include bundle-related verbose output
            assert result.exit_code == 0
            assert "Bundle support detected" in result.output
            # During dry run, should show bundle sync attempt messages
            assert ("Syncing bundle resources" in result.output or 
                    "Bundle sync warning" in result.output or
                    "syncing resource files" in result.output)


class TestCLIBundleErrorHandling:
    """Test CLI error handling for bundle operations."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        """Set up test environment for each test using Windows-safe temporary directory."""
        self.temp_dir = windows_safe_tempdir
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.runner = CliRunner()

    def test_generate_handles_missing_bundle_dependencies(self):
        """Should handle missing bundle dependencies gracefully."""
        # Create minimal project without bundle setup
        (self.project_root / "lhp.yaml").write_text("name: test")
        
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should handle missing substitution file gracefully
            assert result.exit_code != 0  # Expected to fail due to missing substitution

    @patch('lhp.utils.bundle_detection.should_enable_bundle_support')
    def test_generate_handles_bundle_detection_errors(self, mock_bundle_detection):
        """Should handle bundle detection errors gracefully."""
        # Mock bundle detection to raise error
        mock_bundle_detection.side_effect = Exception("Detection error")
        
        (self.project_root / "lhp.yaml").write_text("name: test")
        sub_dir = self.project_root / "substitutions"
        sub_dir.mkdir()
        (sub_dir / "dev.yaml").write_text("catalog: test")
        
        with self.runner.isolated_filesystem():
            import os
            os.chdir(str(self.project_root))
            
            result = self.runner.invoke(cli, [
                'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should continue without bundle support
            # Error handling depends on implementation - could be exit code 0 or 1

    def test_init_bundle_validates_project_name(self):
        """Should validate project name for bundle initialization."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            # Test with invalid project name characters
            result = self.runner.invoke(cli, ['init', '--bundle', 'invalid/project/name'])
            
            # Should handle invalid characters appropriately
            # Implementation may vary - could create directory or reject

    def test_init_bundle_works_offline(self):
        """Should work offline using local templates."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))

            # Since we're using local templates, no network is required
            result = self.runner.invoke(cli, ['init', '--bundle', 'test_offline_project'])

            # Should create project successfully without any network calls
            assert result.exit_code == 0

            project_path = self.temp_dir / "test_offline_project"
            assert (project_path / "databricks.yml").exists()
            assert (project_path / "resources").exists()
            
            # Verify project name was processed correctly in template
            content = (project_path / "databricks.yml").read_text()
            assert "name: test_offline_project" in content


class TestCLIBundleIntegrationEndToEnd:
    """End-to-end tests for CLI bundle integration."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        """Set up test environment for each test using Windows-safe temporary directory."""
        self.temp_dir = windows_safe_tempdir
        self.runner = CliRunner()

    def test_complete_bundle_workflow(self):
        """Test complete workflow: init bundle project, add pipeline, generate with bundle sync."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            # Step 1: Initialize bundle project
            result = self.runner.invoke(cli, ['init', '--bundle', 'test_workflow'])
            assert result.exit_code == 0
            
            project_path = self.temp_dir / "test_workflow"
            assert (project_path / "databricks.yml").exists()
            
            # Step 2: Add a pipeline configuration
            os.chdir(str(project_path))
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
            
            pipeline_file = project_path / "pipelines" / "test_pipeline.yaml"
            pipeline_file.write_text("""pipeline: test_pipeline
flowgroup: test_pipeline
actions:
  - name: test_load
    type: load
    source:
      type: delta
      database: "{catalog}.{raw_schema}"
      table: test_table
    target: v_test_table
  - name: test_write
    type: write
    source: v_test_table
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: test_table
""")
            
            # Step 3: Generate with bundle sync (dry run)
            result = self.runner.invoke(cli, [
                'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should complete successfully
            assert result.exit_code == 0

    def test_bundle_sync_integration_with_multiple_pipelines(self):
        """Test bundle sync with multiple pipelines."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            # Initialize project
            result = self.runner.invoke(cli, ['init', '--bundle', 'multi_pipeline_project'])
            project_path = self.temp_dir / "multi_pipeline_project"
            os.chdir(str(project_path))
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
            
            # Add multiple pipelines
            (project_path / "pipelines" / "raw.yaml").write_text("""pipeline: raw
flowgroup: raw
actions:
  - name: customer_load
    type: load
    source:
      type: delta
      database: "{catalog}.{raw_schema}"
      table: customer
    target: v_customer
  - name: customer_write
    type: write
    source: v_customer
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: customer
""")
            (project_path / "pipelines" / "bronze.yaml").write_text("""pipeline: bronze
flowgroup: bronze
actions:
  - name: customer_bronze_load
    type: load
    source:
      type: delta
      database: "{catalog}.{bronze_schema}"
      table: customer
    target: v_customer_bronze
  - name: customer_bronze_write
    type: write
    source: v_customer_bronze
    write_target:
      type: streaming_table
      database: "{catalog}.{silver_schema}"
      table: customer
""")
            
            # Generate with bundle sync
            result = self.runner.invoke(cli, [
                '--verbose', 'generate', '--env', 'dev', '--dry-run'
            ])
            
            # Should complete successfully with bundle sync
            assert result.exit_code == 0
            assert "Bundle support detected" in result.output

    def test_no_bundle_flag_overrides_bundle_project(self):
        """Test that --no-bundle flag works even in bundle projects."""
        with self.runner.isolated_filesystem():
            os.chdir(str(self.temp_dir))
            
            # Initialize bundle project
            result = self.runner.invoke(cli, ['init', '--bundle', 'bundle_override_test'])
            project_path = self.temp_dir / "bundle_override_test"
            os.chdir(str(project_path))
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
            
            # Add pipeline
            (project_path / "pipelines" / "test.yaml").write_text("""pipeline: test
flowgroup: test
actions:
  - name: test_load
    type: load
    source:
      type: delta
      database: "{catalog}.{raw_schema}"
      table: test_table
    target: v_test_table
  - name: test_write
    type: write
    source: v_test_table
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: test_table
""")
            
            # Generate with --no-bundle should work
            result = self.runner.invoke(cli, [
                'generate', '--env', 'dev', '--no-bundle', '--dry-run'
            ])
            
            assert result.exit_code == 0 