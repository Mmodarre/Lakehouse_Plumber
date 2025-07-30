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
    def temp_project(self, windows_safe_tempdir):
        """Create a temporary project directory with Windows-safe cleanup."""
        return windows_safe_tempdir
    
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
            assert (project_path / "substitutions" / "dev.yaml.tmpl").exists()
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
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
            
            # Run validate
            result = runner.invoke(cli, ['validate'])
            
            assert result.exit_code == 1
            assert "❌ No flowgroups found in project" in result.output

    def test_stats_invalid_pipeline(self, runner, temp_project):
        """Test stats command with non-existent pipeline."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
            
            # Create a valid pipeline first
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
                    }
                ]
            }
            
            with open(pipeline_dir / "test_flowgroup.yaml", 'w') as f:
                yaml.dump(flowgroup_content, f)
            
            # Run stats with non-existent pipeline
            result = runner.invoke(cli, ['stats', '--pipeline', 'UNKNOWN_PIPELINE'])
            
            assert result.exit_code == 0
            assert "❌ Pipeline 'UNKNOWN_PIPELINE' not found" in result.output

    def test_generate_bundle_sync_dry_run(self, runner, temp_project):
        """Test bundle sync detection in dry-run mode."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
            
            # Create databricks.yml to enable bundle support
            databricks_yml_content = """
bundle:
  name: test_bundle
"""
            with open("databricks.yml", 'w') as f:
                f.write(databricks_yml_content)
            
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
            
            # Run generate with verbose and dry-run to see bundle sync message
            result = runner.invoke(cli, ['--verbose', 'generate', '--env', 'dev', '--dry-run'])
            
            assert result.exit_code == 0
            assert "Bundle support detected" in result.output

    def test_load_project_config_malformed(self, runner, temp_project):
        """Test _load_project_config with malformed YAML returns defaults."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Create malformed lhp.yaml
            with open("lhp.yaml", 'w') as f:
                f.write("name: test\nversion: 1.0\ninvalid_yaml: [unclosed list")
            
            # Run info command which uses _load_project_config
            result = runner.invoke(cli, ['info'])
            
            assert result.exit_code == 0
            # Should fall back to defaults when YAML is malformed
            assert "Unknown" in result.output  # Default author or other unknown fields
    
    def test_cli_help(self, runner):
        """Test CLI help command."""
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert "LakehousePlumber" in result.output
        assert "Generate Lakeflow Pipeliness pipelines from YAML configs" in result.output
    
    def test_validate_with_pipeline(self, runner, temp_project):
        """Test validate with a valid pipeline."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            # Initialize project
            runner.invoke(cli, ['init', 'test_project'])
    
            import os
            os.chdir('test_project')
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
    
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
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
    
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

    def test_generate_force_mode(self, runner, temp_project):
        """Test generate command with --force flag."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
    
            import os
            os.chdir('test_project')
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
    
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
    
            # Run generate with --force flag
            result = runner.invoke(cli, ['generate', '--env', 'dev', '--force', '--dry-run'])
    
            assert result.exit_code == 0
            assert "🔄 Force mode: regenerating all files regardless of changes" in result.output
    
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
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
    
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

    def test_get_version_fallbacks(self, runner, temp_project):
        """Test get_version() fallback logic when package metadata is not available."""
        from unittest.mock import patch
        import tempfile
        import shutil
        
        # Test 1: Mock importlib.metadata.version to raise exception, should fall back to pyproject.toml
        with patch('lhp.cli.main.version') as mock_version:
            mock_version.side_effect = Exception("Package not found")
            
            # Create a temporary directory with pyproject.toml
            with tempfile.TemporaryDirectory() as tmpdir:
                pyproject_path = Path(tmpdir) / "pyproject.toml"
                pyproject_path.write_text('''
[tool.poetry]
name = "test-package"
version = "1.2.3"
description = "Test package"
''')
                
                # Temporarily change the module's __file__ to point to our temp dir
                import lhp.cli.main
                original_file = lhp.cli.main.__file__
                try:
                    # Set __file__ to be inside our temp structure
                    lhp.cli.main.__file__ = str(Path(tmpdir) / "src" / "lhp" / "cli" / "main.py")
                    version_result = lhp.cli.main.get_version()
                    assert version_result == "1.2.3"
                finally:
                    lhp.cli.main.__file__ = original_file
        
        # Test 2: No pyproject.toml found, should return default version
        with patch('lhp.cli.main.version') as mock_version:
            mock_version.side_effect = Exception("Package not found")
            
            with tempfile.TemporaryDirectory() as tmpdir:
                # Set __file__ to empty directory with no pyproject.toml
                import lhp.cli.main
                original_file = lhp.cli.main.__file__
                try:
                    lhp.cli.main.__file__ = str(Path(tmpdir) / "deep" / "nested" / "path" / "main.py")
                    version_result = lhp.cli.main.get_version()
                    assert version_result == "0.2.11"
                finally:
                    lhp.cli.main.__file__ = original_file

    def test_cleanup_logging_edge_case(self, runner):
        """Test cleanup_logging() with no handlers attached."""
        import logging
        from unittest.mock import patch
        from lhp.cli.main import cleanup_logging
        
        # Create a fresh logger with no handlers
        test_logger = logging.getLogger("test_empty_logger")
        test_logger.handlers.clear()
        
        # Mock the root logger to return our empty logger
        with patch('lhp.cli.main.logging.getLogger') as mock_get_logger:
            mock_get_logger.return_value = test_logger
            
            # Should not raise any exceptions
            try:
                cleanup_logging()
            except Exception as e:
                pytest.fail(f"cleanup_logging() raised an exception: {e}")
            
            # Verify handlers list is still empty
            assert len(test_logger.handlers) == 0

    def test_list_templates_empty_dir(self, runner, temp_project):
        """Test list-templates command with no template files."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Remove all template files from the templates directory
            templates_dir = Path("templates")
            if templates_dir.exists():
                for template_file in templates_dir.glob("*.yaml"):
                    template_file.unlink()
                for template_file in templates_dir.glob("*.yml"):
                    template_file.unlink()
            
            # Run list-templates
            result = runner.invoke(cli, ['list-templates'])
            
            assert result.exit_code == 0
            assert "📭 No templates found" in result.output
            assert "💡 Create a template file in the 'templates' directory" in result.output

    def test_list_presets_empty_dir(self, runner, temp_project):
        """Test list-presets command with no preset files."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Remove all preset files from the presets directory
            presets_dir = Path("presets")
            if presets_dir.exists():
                for preset_file in presets_dir.glob("*.yaml"):
                    preset_file.unlink()
                for preset_file in presets_dir.glob("*.yml"):
                    preset_file.unlink()
            
            # Run list-presets
            result = runner.invoke(cli, ['list-presets'])
            
            assert result.exit_code == 0
            assert "📭 No presets found" in result.output
            assert "💡 Create a preset file in the 'presets' directory" in result.output

    def test_generate_no_flowgroups_error(self, runner, temp_project):
        """Test generate command when no flowgroups found in project."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ['init', 'test_project'])
            
            import os
            os.chdir('test_project')
            
            # Create dev.yaml for testing by copying the template
            import shutil
            shutil.copy('substitutions/dev.yaml.tmpl', 'substitutions/dev.yaml')
            
            # Create an empty pipeline directory (no YAML files)
            pipeline_dir = Path("pipelines/empty_pipeline")
            pipeline_dir.mkdir(parents=True)
            
            # Run generate - should exit with error when no flowgroups found
            result = runner.invoke(cli, ['generate', '--env', 'dev'])
            
            assert result.exit_code == 1
            assert "❌ No flowgroups found in project" in result.output 