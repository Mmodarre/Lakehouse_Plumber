"""Comprehensive CLI tests for LakehousePlumber."""

import pytest
import tempfile
import yaml
import json
from pathlib import Path
from click.testing import CliRunner

from lhp.cli.main import cli


class TestCLIComprehensive:
    """Comprehensive CLI tests based on requirements and BurrowBuilder patterns."""
    
    @pytest.fixture
    def runner(self):
        """Create CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def temp_project(self):
        """Create a temporary project directory with full structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create full project structure
            directories = [
                'presets', 'templates', 'pipelines', 'substitutions',
                'schemas', 'expectations', 'generated'
            ]
            
            for dir_name in directories:
                (project_root / dir_name).mkdir(parents=True)
            
            # Create project config
            (project_root / "lhp.yaml").write_text("""
name: test_cli_project
version: "1.0"
description: "Test project for CLI"
""")
            
            # Create basic presets
            (project_root / "presets" / "bronze_layer.yaml").write_text("""
name: bronze_layer
version: "1.0"
defaults:
  operational_metadata: true
  table_properties:
    quality: "bronze"
""")
            
            # Create basic template
            (project_root / "templates" / "basic_ingestion.yaml").write_text("""
name: basic_ingestion
version: "1.0"
parameters:
  - name: table_name
    required: true

actions:
  - name: load_{{ table_name }}
    type: load
    source:
      type: sql
      sql: "SELECT * FROM raw_{{ table_name }}"
    target: v_{{ table_name }}
    
  - name: save_{{ table_name }}
    type: write
    source: v_{{ table_name }}
    write_target:
      type: streaming_table
      database: "bronze"
      table: "{{ table_name }}"
      create_table: true
""")
            
            # Create substitutions
            (project_root / "substitutions" / "dev.yaml").write_text("""
dev:
  env: dev
  catalog: dev_catalog
  bronze_schema: bronze

secrets:
  default_scope: dev_secrets
  scopes:
    database: dev_db_secrets
""")
            
            (project_root / "substitutions" / "prod.yaml").write_text("""
prod:
  env: prod
  catalog: prod_catalog
  bronze_schema: bronze

secrets:
  default_scope: prod_secrets
  scopes:
    database: prod_db_secrets
""")
            
            # Create sample pipeline
            pipeline_dir = project_root / "pipelines" / "test_pipeline"
            pipeline_dir.mkdir(parents=True)
            
            (pipeline_dir / "test_flowgroup.yaml").write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer

actions:
  - name: load_test_data
    type: load
    source:
      type: sql
      sql: "SELECT * FROM test_source"
    target: v_test_data
    
  - name: save_test_data
    type: write
    source: v_test_data
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: test_table
      create_table: true
""")
            
            yield project_root
    
    def test_init_command(self, runner):
        """Test project initialization command."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_name = "my_new_project"
            project_path = Path(tmpdir) / project_name
            
            # Run init command
            result = runner.invoke(cli, ['init', str(project_path)])
            
            assert result.exit_code == 0
            assert "Initialized LakehousePlumber project" in result.output
            assert project_path.exists()
            
            # Check created structure
            assert (project_path / "lhp.yaml").exists()
            assert (project_path / "presets").exists()
            assert (project_path / "templates").exists()
            assert (project_path / "pipelines").exists()
            assert (project_path / "substitutions").exists()
            
            # Check example files were created
            assert (project_path / "substitutions" / "dev.yaml").exists()
            assert (project_path / "presets" / "bronze_layer.yaml").exists()
    
    def test_init_existing_directory(self, runner, temp_project):
        """Test init command on existing directory."""
        result = runner.invoke(cli, ['init', str(temp_project)])
        
        assert result.exit_code != 0
        assert "already exists" in result.output
    
    def test_validate_command_success(self, runner, temp_project):
        """Test validate command with valid configuration."""
        with runner.isolated_filesystem():
            # Change to project directory
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['validate', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "🔍 Validating pipeline configurations" in result.output
            assert "✅ All configurations are valid" in result.output
    
    def test_validate_specific_pipeline(self, runner, temp_project):
        """Test validating a specific pipeline."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['validate', '--env', 'dev', '--pipeline', 'test_pipeline'])
            
            assert result.exit_code == 0
            assert "test_pipeline" in result.output
    
    def test_validate_invalid_pipeline(self, runner, temp_project):
        """Test validation with invalid configuration."""
        # Create invalid flowgroup
        invalid_pipeline = temp_project / "pipelines" / "invalid_pipeline"
        invalid_pipeline.mkdir(parents=True)
        
        (invalid_pipeline / "invalid.yaml").write_text("""
pipeline: invalid_pipeline
flowgroup: invalid_flowgroup

# Missing required actions
actions: []
""")
        
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['validate', '--env', 'dev', '--pipeline', 'invalid_pipeline'])
            
            assert result.exit_code != 0
            assert "error(s) found" in result.output or "Validation failed" in result.output
    
    def test_generate_command(self, runner, temp_project):
        """Test code generation command."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['generate', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "Generating pipeline code" in result.output
            assert "test_pipeline" in result.output
            assert "Generated" in result.output
            
            # Check generated files
            generated_file = temp_project / "generated" / "test_pipeline" / "test_flowgroup.py"
            assert generated_file.exists()
            
            # Verify generated code content
            code = generated_file.read_text()
            assert "import dlt" in code
            assert "@dlt.view()" in code
            assert "dev_catalog.bronze.test_table" in code
    
    def test_generate_dry_run(self, runner, temp_project):
        """Test dry-run generation."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['generate', '--env', 'dev', '--dry-run'])
            
            assert result.exit_code == 0
            assert "Would generate" in result.output
            assert "test_flowgroup.py" in result.output
            
            # Verify no files were actually created
            generated_dir = temp_project / "generated"
            assert not list(generated_dir.glob("**/*.py"))
    
    def test_generate_with_format(self, runner, temp_project):
        """Test generation with code formatting."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['generate', '--env', 'dev', '--format'])
            
            assert result.exit_code == 0
            # Code should be formatted (Black would have been applied)
    
    def test_generate_specific_pipeline(self, runner, temp_project):
        """Test generating a specific pipeline."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['generate', '--env', 'prod', '--pipeline', 'test_pipeline'])
            
            assert result.exit_code == 0
            assert "test_pipeline" in result.output
            
            # Check it used prod substitutions
            generated_file = temp_project / "generated" / "test_pipeline" / "test_flowgroup.py"
            code = generated_file.read_text()
            assert "prod_catalog" in code
    
    def test_list_presets(self, runner, temp_project):
        """Test listing available presets."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['list-presets'])
            
            assert result.exit_code == 0
            assert "Available presets" in result.output
            assert "bronze_layer" in result.output
    
    def test_list_templates(self, runner, temp_project):
        """Test listing available templates."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['list-templates'])
            
            assert result.exit_code == 0
            assert "Available templates" in result.output
            assert "basic_ingestion" in result.output
    
    def test_show_command(self, runner, temp_project):
        """Test show command for resolved configuration."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['show', 'test_flowgroup', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "🔍 Showing resolved configuration for 'test_flowgroup'" in result.output
            assert "📋 FlowGroup Configuration" in result.output
            assert "Pipeline:    test_pipeline" in result.output
            assert "FlowGroup:   test_flowgroup" in result.output
            assert "📊 Actions" in result.output
            assert "load_test_data" in result.output
            assert "save_test_data" in result.output
    
    def test_info_command(self, runner, temp_project):
        """Test info command for project information."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['info'])
            
            assert result.exit_code == 0
            assert "Project Information" in result.output
            assert "test_cli_project" in result.output
            assert "Pipelines:" in result.output
            assert "test_pipeline" in result.output
    
    def test_stats_command(self, runner, temp_project):
        """Test stats command for pipeline statistics."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['stats'])
            
            assert result.exit_code == 0
            assert "Pipeline Statistics" in result.output
            assert "Total pipelines:" in result.output
            assert "Total actions:" in result.output
    
    def test_secret_validation_with_references(self, runner, temp_project):
        """Test secret validation with actual secret references."""
        # Create flowgroup with secrets
        pipeline_dir = temp_project / "pipelines" / "secret_pipeline"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "jdbc_secrets.yaml").write_text("""
pipeline: secret_pipeline
flowgroup: jdbc_secrets

actions:
  - name: load_from_db
    type: load
    source:
      type: jdbc
      url: "jdbc:postgresql://${secret:database/host}:5432/db"
      user: "${secret:database/username}"
      password: "${secret:database/password}"
      driver: "org.postgresql.Driver"
      table: "customers"
    target: v_customers
    
  - name: save_customers
    type: write
    source: v_customers
    write_target:
      type: streaming_table
      database: "bronze"
      table: "customers"
      create_table: true
""")
        
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            # Validate should include secret validation
            result = runner.invoke(cli, ['validate', '--env', 'dev', '--pipeline', 'secret_pipeline'])
            
            # Validation should pass (we have database scope configured)
            assert result.exit_code == 0
    
    def test_no_project_root(self, runner):
        """Test commands when not in a project directory."""
        with runner.isolated_filesystem():
            # Try to validate without being in a project
            result = runner.invoke(cli, ['validate'])
            
            # Should show helpful error message
            assert result.exit_code == 1
            assert "❌ Not in a LakehousePlumber project directory" in result.output
            assert "💡 Run 'lhp init <project_name>' to create a new project" in result.output
    
    def test_verbose_flag(self, runner, temp_project):
        """Test verbose flag for detailed output."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            result = runner.invoke(cli, ['--verbose', 'validate', '--env', 'dev', '--verbose'])
            
            # With verbose, should see more detailed logging
            # (exact output depends on logging configuration)
            assert result.exit_code == 0
    
    def test_version_command(self, runner):
        """Test version command."""
        result = runner.invoke(cli, ['--version'])
        
        assert result.exit_code == 0
        assert "version" in result.output.lower()
    
    def test_custom_output_directory(self, runner, temp_project):
        """Test generation with custom output directory."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            custom_output = "my_output"
            result = runner.invoke(cli, ['generate', '--env', 'dev', '--output', custom_output])
            
            assert result.exit_code == 0
            
            # Check files were generated in custom directory
            output_file = temp_project / custom_output / "test_pipeline" / "test_flowgroup.py"
            assert output_file.exists()
    
    def test_help_commands(self, runner):
        """Test help output for all commands."""
        # Main help
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert "LakehousePlumber" in result.output
        
        # Command-specific help
        commands = ['init', 'validate', 'generate', 'list-presets', 'list-templates', 'show', 'info', 'stats']
        
        for cmd in commands:
            result = runner.invoke(cli, [cmd, '--help'])
            assert result.exit_code == 0
            assert cmd in result.output.lower() or "Usage:" in result.output 