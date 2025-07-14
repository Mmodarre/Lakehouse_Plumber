"""Tests for LakehousePlumber CLI commands."""

import pytest
from click.testing import CliRunner
from pathlib import Path
import tempfile
import yaml
import shutil

from lhp.cli.main import cli, get_version


class TestCLI:
    """Test CLI commands."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def temp_project(self):
        """Create a temporary project directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_cli_version(self, runner):
        """Test version command."""
        result = runner.invoke(cli, ['--version'])
        assert result.exit_code == 0
        expected_version = get_version()
        assert expected_version in result.output
    
    def test_init_command(self, runner, temp_project):
        """Test project initialization."""
        project_name = "test_project"
        
        with runner.isolated_filesystem(temp_dir=temp_project):
            result = runner.invoke(cli, ['init', project_name])
            
            assert result.exit_code == 0
            assert "✅ Initialized LakehousePlumber project" in result.output
            
            # Check project structure
            project_path = Path(project_name)
            assert project_path.exists()
            assert (project_path / "lhp.yaml").exists()
            assert (project_path / "pipelines").exists()
            assert (project_path / "presets").exists()
            assert (project_path / "templates").exists()
            assert (project_path / "substitutions").exists()
            assert (project_path / "substitutions" / "dev.yaml").exists()
            assert (project_path / "README.md").exists()
            assert (project_path / ".gitignore").exists()
    
    def test_init_existing_directory(self, runner, temp_project):
        """Test init with existing directory."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            Path("existing_project").mkdir()
            
            result = runner.invoke(cli, ['init', 'existing_project'])
            
            assert result.exit_code == 1
            assert "❌ Directory existing_project already exists" in result.output
    
    def test_validate_not_in_project(self, runner):
        """Test validate when not in a project directory."""
        result = runner.invoke(cli, ['validate'])
        
        assert result.exit_code == 1
        assert "Not in a LakehousePlumber project directory" in result.output
    
    def test_validate_empty_project(self, runner, temp_project):
        """Test validate with empty project."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            # Initialize project
            runner.invoke(cli, ['init', 'test_project'])
            
            # Change to project directory
            import os
            os.chdir('test_project')
            
            # Run validate
            result = runner.invoke(cli, ['validate'])
            
            assert result.exit_code == 1
            assert "❌ No flowgroups found in project" in result.output
    
    def test_validate_with_pipeline(self, runner, temp_project):
        """Test validate with a valid pipeline."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            # Initialize project
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Create a pipeline
            pipeline_dir = Path("pipelines/test_pipeline")
            pipeline_dir.mkdir(parents=True)
            
            # Create a flowgroup
            flowgroup_content = {
                'pipeline': 'test_pipeline',
                'flowgroup': 'test_flowgroup',
                'actions': [
                    {
                        'name': 'load_data',
                        'type': 'load',
                        'target': 'v_raw_data',
                        'source': {
                            'type': 'cloudfiles',
                            'path': '/mnt/data/raw',
                            'format': 'json'
                        }
                    },
                    {
                        'name': 'write_data',
                        'type': 'write',
                        'source': 'v_raw_data',
                        'write_target': {
                            'type': 'streaming_table',
                            'database': 'bronze',
                            'table': 'test_table',
                            'create_table': True
                        }
                    }
                ]
            }
            
            with open(pipeline_dir / "test_flowgroup.yaml", 'w') as f:
                yaml.dump(flowgroup_content, f)
            
            # Run validate
            result = runner.invoke(cli, ['validate', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "✅ All configurations are valid" in result.output
    
    def test_list_presets(self, runner, temp_project):
        """Test list-presets command."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            result = runner.invoke(cli, ['list-presets'])
            
            assert result.exit_code == 0
            assert "📋 Available presets:" in result.output
            assert "bronze_layer" in result.output
    
    def test_list_templates(self, runner, temp_project):
        """Test list-templates command."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            result = runner.invoke(cli, ['list-templates'])
            
            assert result.exit_code == 0
            assert "📋 Available templates:" in result.output
            assert "standard_ingestion" in result.output
    
    def test_generate_dry_run(self, runner, temp_project):
        """Test generate command with dry-run."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Create a pipeline
            pipeline_dir = Path("pipelines/test_pipeline")
            pipeline_dir.mkdir(parents=True)
            
            flowgroup_content = {
                'pipeline': 'test_pipeline',
                'flowgroup': 'test_flowgroup',
                'actions': [
                    {
                        'name': 'load_data',
                        'type': 'load',
                        'target': 'v_raw_data',
                        'source': {
                            'type': 'sql',
                            'sql': 'SELECT * FROM raw_table'
                        }
                    },
                    {
                        'name': 'write_bronze',
                        'type': 'write',
                        'source': 'v_raw_data',
                        'write_target': {
                            'type': 'streaming_table',
                            'database': 'bronze',
                            'table': 'test_table',
                            'create_table': True
                        }
                    }
                ]
            }
            
            with open(pipeline_dir / "test_flowgroup.yaml", 'w') as f:
                yaml.dump(flowgroup_content, f)
            
            # Run generate with dry-run
            result = runner.invoke(cli, ['generate', '--env', 'dev', '--dry-run'])
            
            assert result.exit_code == 0
            assert "✨ Dry run completed" in result.output
            assert "Would generate" in result.output
    
    def test_show_command(self, runner, temp_project):
        """Test show command."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Create a pipeline with flowgroup
            pipeline_dir = Path("pipelines/test_pipeline")
            pipeline_dir.mkdir(parents=True)
            
            flowgroup_content = {
                'pipeline': 'test_pipeline',
                'flowgroup': 'test_flowgroup',
                'actions': [
                    {
                        'name': 'load_data',
                        'type': 'load',
                        'target': 'v_raw_data',
                        'source': {
                            'type': 'sql',
                            'sql': 'SELECT * FROM {catalog}.{bronze_schema}.source_table'
                        }
                    },
                    {
                        'name': 'write_bronze',
                        'type': 'write',
                        'source': 'v_raw_data',
                        'write_target': {
                            'type': 'streaming_table',
                            'database': '{bronze_schema}',
                            'table': 'processed_data',
                            'create_table': True
                        }
                    }
                ]
            }
            
            with open(pipeline_dir / "test_flowgroup.yaml", 'w') as f:
                yaml.dump(flowgroup_content, f)
            
            # Run show command
            result = runner.invoke(cli, ['show', 'test_flowgroup', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "📋 FlowGroup Configuration" in result.output
            assert "test_flowgroup" in result.output
            assert "📊 Actions" in result.output
    
    def test_validate_with_secrets(self, runner, temp_project):
        """Test validate with secret references."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Create a pipeline with secrets
            pipeline_dir = Path("pipelines/test_pipeline")
            pipeline_dir.mkdir(parents=True)
            
            flowgroup_content = {
                'pipeline': 'test_pipeline',
                'flowgroup': 'test_flowgroup',
                'actions': [
                    {
                        'name': 'load_jdbc',
                        'type': 'load',
                        'target': 'v_jdbc_data',
                        'source': {
                            'type': 'jdbc',
                            'url': 'jdbc:postgresql://${secret:database/host}:5432/db',
                            'user': '${secret:database/username}',
                            'password': '${secret:database/password}',
                            'driver': 'org.postgresql.Driver',
                            'table': 'customers'
                        }
                    },
                    {
                        'name': 'write_customers',
                        'type': 'write',
                        'source': 'v_jdbc_data',
                        'write_target': {
                            'type': 'streaming_table',
                            'database': 'bronze',
                            'table': 'customers_raw',
                            'create_table': True
                        }
                    }
                ]
            }
            
            with open(pipeline_dir / "test_flowgroup.yaml", 'w') as f:
                yaml.dump(flowgroup_content, f)
            
            # Run validate
            result = runner.invoke(cli, ['validate', '--env', 'dev', '--verbose'])
            
            assert result.exit_code == 0
            assert "🔍 Validating pipeline configurations" in result.output
            assert "✅ All configurations are valid" in result.output 