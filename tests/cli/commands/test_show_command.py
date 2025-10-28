"""Tests for show_command CLI implementation."""

import pytest
import os
from pathlib import Path
from unittest.mock import Mock, patch
from click.testing import CliRunner

from lhp.cli.main import cli
from lhp.cli.commands.show_command import ShowCommand
from lhp.models.config import FlowGroup, Action, ActionType


@pytest.fixture
def runner():
    """Create CLI runner."""
    return CliRunner()


@pytest.fixture
def sample_project(tmp_path):
    """Create a sample project structure for testing."""
    # Create directories
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "substitutions").mkdir()
    
    # Create lhp.yaml
    (tmp_path / "lhp.yaml").write_text("""
name: TestProject
version: 1.0.0
description: Test project description
author: Test Author
""")
    
    # Create substitution file
    (tmp_path / "substitutions" / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  schema: dev_schema
""")
    
    return tmp_path


class TestShowFlowgroupCLI:
    """Test 'lhp show' command using CliRunner."""
    
    def test_flowgroup_not_found(self, runner, sample_project):
        """Test error handling when flowgroup not found."""
        with runner.isolated_filesystem(temp_dir=sample_project.parent) as td:
            os.chdir(sample_project)
            result = runner.invoke(cli, ['show', 'nonexistent_fg', '--env', 'dev'])
            
            assert result.exit_code == 1
            assert "not found" in result.output.lower()
    
    def test_show_with_valid_flowgroup(self, runner, sample_project):
        """Test showing a valid flowgroup."""
        # Create a valid flowgroup
        pipelines_dir = sample_project / "pipelines"
        fg_file = pipelines_dir / "test_fg.yaml"
        fg_file.write_text("""
pipeline: test_pipeline
flowgroup: test_fg
actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT * FROM source"
    target: v_data
    
  - name: save_data
    type: write
    source: v_data
    write_target:
      type: streaming_table
      database: "{catalog}.{schema}"
      table: test_table
""")
        
        with runner.isolated_filesystem(temp_dir=sample_project.parent) as td:
            os.chdir(sample_project)
            result = runner.invoke(cli, ['show', 'test_fg', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "test_fg" in result.output
            assert "test_pipeline" in result.output
            assert "load_data" in result.output
            assert "save_data" in result.output
    
    def test_show_with_missing_substitution_file(self, runner, sample_project):
        """Test warning when substitution file is missing."""
        # Create flowgroup
        fg_file = sample_project / "pipelines" / "test_fg.yaml"
        fg_file.write_text("""
pipeline: test_pipeline
flowgroup: test_fg
actions:
  - name: test_action
    type: load
    target: test_table
""")
        
        with runner.isolated_filesystem(temp_dir=sample_project.parent) as td:
            os.chdir(sample_project)
            # Use non-existent environment
            result = runner.invoke(cli, ['show', 'test_fg', '--env', 'nonexistent'])
            
            # Should show warning about missing substitution file
            assert "warning" in result.output.lower() or "not found" in result.output.lower()
    
    def test_show_with_invalid_yaml(self, runner, sample_project):
        """Test error handling with invalid YAML."""
        # Create invalid YAML
        fg_file = sample_project / "pipelines" / "bad_fg.yaml"
        fg_file.write_text("invalid: yaml: [[[")
        
        with runner.isolated_filesystem(temp_dir=sample_project.parent) as td:
            os.chdir(sample_project)
            result = runner.invoke(cli, ['show', 'bad_fg', '--env', 'dev'])
            
            assert result.exit_code == 1


class TestInfoCommandCLI:
    """Test 'lhp info' command using CliRunner."""
    
    def test_info_with_valid_project(self, runner, sample_project):
        """Test info command with valid project."""
        with runner.isolated_filesystem(temp_dir=sample_project.parent) as td:
            os.chdir(sample_project)
            result = runner.invoke(cli, ['info'])
            
            assert result.exit_code == 0
            assert "TestProject" in result.output
            assert "1.0.0" in result.output
            assert "Test Author" in result.output
    
    def test_info_with_missing_config(self, runner, tmp_path):
        """Test info command with missing lhp.yaml."""
        # Create minimal structure without lhp.yaml
        (tmp_path / "pipelines").mkdir()
        
        with runner.isolated_filesystem(temp_dir=tmp_path.parent) as td:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ['info'])
            
            # Should exit with error when not in a project directory
            assert result.exit_code == 1
            assert "not in a lakehouseplumber project" in result.output.lower()
    
    def test_info_with_invalid_yaml(self, runner, tmp_path):
        """Test info command with invalid YAML config."""
        (tmp_path / "pipelines").mkdir()
        (tmp_path / "lhp.yaml").write_text("invalid: yaml: [")
        
        with runner.isolated_filesystem(temp_dir=tmp_path.parent) as td:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ['info'])
            
            assert result.exit_code == 0
            assert "Unknown" in result.output
    
    def test_info_shows_resource_summary(self, runner, sample_project):
        """Test that info command shows resource summary."""
        # Add some resources
        pipelines_dir = sample_project / "pipelines"
        (pipelines_dir / "pipeline1").mkdir()
        (pipelines_dir / "pipeline1" / "fg1.yaml").write_text("test: 1")
        
        presets_dir = sample_project / "presets"
        presets_dir.mkdir()
        (presets_dir / "preset1.yaml").write_text("test: 1")
        
        with runner.isolated_filesystem(temp_dir=sample_project.parent) as td:
            os.chdir(sample_project)
            result = runner.invoke(cli, ['info'])
            
            assert result.exit_code == 0
            assert "Resource Summary" in result.output or "Pipelines" in result.output


class TestFindFlowgroupFile:
    """Test _find_flowgroup_file utility method."""
    
    def test_find_with_include_patterns(self, sample_project):
        """Test file discovery with include patterns."""
        pipelines_dir = sample_project / "pipelines"
        
        # Create test files
        (pipelines_dir / "included.yaml").write_text("""
pipeline: test
flowgroup: included_fg
""")
        (pipelines_dir / "excluded.yaml").write_text("""
pipeline: test
flowgroup: excluded_fg
""")
        
        # Update lhp.yaml with include patterns
        (sample_project / "lhp.yaml").write_text("""
name: test
include:
  - "included.yaml"
""")
        
        command = ShowCommand()
        
        # Test finding included flowgroup
        result = command._find_flowgroup_file("included_fg", sample_project)
        assert result is not None
        assert "included.yaml" in str(result)
    
    def test_find_with_get_include_patterns_error(self, sample_project):
        """Test error handling when getting include patterns fails."""
        command = ShowCommand()
        command.logger = Mock()
        
        with patch('lhp.core.project_config_loader.ProjectConfigLoader', side_effect=Exception("Config error")):
            # Should handle error and return empty list
            result = command._get_include_patterns(sample_project)
            assert result == []
    
    def test_find_in_array_syntax(self, sample_project):
        """Test finding flowgroup in array syntax file."""
        pipelines_dir = sample_project / "pipelines"
        
        fg_file = pipelines_dir / "multi.yaml"
        fg_file.write_text("""
pipeline: test_pipeline
flowgroups:
  - flowgroup: fg1
  - flowgroup: fg2
  - flowgroup: fg3
""")
        
        command = ShowCommand()
        
        result = command._find_flowgroup_file("fg2", sample_project)
        assert result is not None
        assert "multi.yaml" in str(result)


class TestDiscoverYamlFilesWithInclude:
    """Test _discover_yaml_files_with_include method."""
    
    def test_discover_with_no_include_patterns(self, tmp_path):
        """Test discovery with no include patterns."""
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        (pipelines_dir / "test1.yaml").write_text("test: 1")
        (pipelines_dir / "test2.yml").write_text("test: 2")
        
        command = ShowCommand()
        
        result = command._discover_yaml_files_with_include(pipelines_dir, [])
        assert len(result) >= 2
    
    def test_discover_with_include_patterns(self, tmp_path):
        """Test discovery with include patterns."""
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        (pipelines_dir / "included.yaml").write_text("test: 1")
        (pipelines_dir / "excluded.yaml").write_text("test: 2")
        
        command = ShowCommand()
        
        # Mock the actual function where it's imported from
        with patch('lhp.utils.file_pattern_matcher.discover_files_with_patterns') as mock_discover:
            mock_discover.return_value = [pipelines_dir / "included.yaml"]
            
            result = command._discover_yaml_files_with_include(pipelines_dir, ["included.yaml"])
            assert len(result) == 1
            assert "included.yaml" in str(result[0])


class TestResourceSummary:
    """Test _collect_resource_summary method."""
    
    def test_resource_summary_with_no_directories(self, tmp_path):
        """Test resource summary with no pipeline/preset/template directories."""
        command = ShowCommand()
        
        result = command._collect_resource_summary(tmp_path)
        assert result["pipeline_count"] == 0
        assert result["flowgroup_count"] == 0
        assert result["preset_count"] == 0
        assert result["template_count"] == 0
    
    def test_resource_summary_with_resources(self, tmp_path):
        """Test resource summary with actual resources."""
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        pipeline1 = pipelines_dir / "pipeline1"
        pipeline1.mkdir()
        (pipeline1 / "fg1.yaml").write_text("test: 1")
        (pipeline1 / "fg2.yaml").write_text("test: 2")
        
        pipeline2 = pipelines_dir / "pipeline2"
        pipeline2.mkdir()
        (pipeline2 / "fg3.yaml").write_text("test: 3")
        
        presets_dir = tmp_path / "presets"
        presets_dir.mkdir()
        (presets_dir / "preset1.yaml").write_text("test: 1")
        
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        (templates_dir / "template1.yaml").write_text("test: 1")
        (templates_dir / "template2.yaml").write_text("test: 2")
        
        command = ShowCommand()
        
        result = command._collect_resource_summary(tmp_path)
        assert result["pipeline_count"] == 2
        assert result["flowgroup_count"] == 3
        assert result["preset_count"] == 1
        assert result["template_count"] == 2


class TestDisplayMethods:
    """Test display methods."""
    
    def test_display_actions_table_no_actions(self, capsys):
        """Test displaying actions table with no actions."""
        command = ShowCommand()
        
        fg = Mock(spec=FlowGroup)
        fg.actions = []
        
        command._display_actions_table(fg)
        
        captured = capsys.readouterr()
        assert "No actions found" in captured.out
    
    def test_display_action_details_with_sql_action(self, capsys):
        """Test displaying action details with SQL content."""
        command = ShowCommand()
        
        action = Mock(spec=Action)
        action.name = "test_action"
        action.type = ActionType.LOAD
        action.source = "test_source"
        action.sql = "SELECT * FROM table" * 10  # Long SQL
        
        fg = Mock(spec=FlowGroup)
        fg.actions = [action]
        
        command._display_action_details(fg)
        
        captured = capsys.readouterr()
        assert "test_action" in captured.out
        assert "..." in captured.out  # SQL should be truncated
    
    def test_display_action_details_with_transform_type(self, capsys):
        """Test displaying action details with transform type."""
        command = ShowCommand()
        
        action = Mock(spec=Action)
        action.name = "transform_action"
        action.type = ActionType.TRANSFORM
        action.transform_type = "python"
        action.source = None
        
        fg = Mock(spec=FlowGroup)
        fg.actions = [action]
        
        command._display_action_details(fg)
        
        captured = capsys.readouterr()
        assert "transform_action" in captured.out
        assert "python" in captured.out
    
    def test_display_secret_references(self, capsys):
        """Test displaying secret references."""
        command = ShowCommand()
        
        substitution_mgr = Mock()
        
        # Create mock secret reference
        mock_secret = Mock()
        mock_secret.scope = "secrets"
        mock_secret.key = "api_key"
        
        substitution_mgr.get_secret_references.return_value = [mock_secret]
        
        command._display_secret_references(substitution_mgr)
        
        captured = capsys.readouterr()
        assert "Secret References" in captured.out
        assert "api_key" in captured.out
    
    def test_display_substitution_summary_many_tokens(self, capsys):
        """Test displaying substitution summary with many tokens."""
        command = ShowCommand()
        
        substitution_mgr = Mock()
        # Create more than 10 tokens to test truncation
        substitution_mgr.mappings = {f"token{i}": f"value{i}" for i in range(15)}
        
        command._display_substitution_summary(substitution_mgr)
        
        captured = capsys.readouterr()
        assert "Token Substitutions" in captured.out
        assert "and 5 more" in captured.out  # Should show truncation message


class TestEnvironments:
    """Test _display_environments method."""
    
    def test_display_environments_with_files(self, tmp_path, capsys):
        """Test displaying environments with substitution files."""
        subs_dir = tmp_path / "substitutions"
        subs_dir.mkdir()
        (subs_dir / "dev.yaml").write_text("test: 1")
        (subs_dir / "prod.yaml").write_text("test: 2")
        
        command = ShowCommand()
        
        command._display_environments(tmp_path)
        
        captured = capsys.readouterr()
        assert "dev" in captured.out
        assert "prod" in captured.out
    
    def test_display_environments_no_files(self, tmp_path, capsys):
        """Test displaying environments with no substitution files."""
        command = ShowCommand()
        
        command._display_environments(tmp_path)
        
        captured = capsys.readouterr()
        # Should not show environments section
        assert "Environments" not in captured.out or len(captured.out.strip()) == 0


class TestRecentActivity:
    """Test _display_recent_activity method."""
    
    def test_recent_activity_with_files(self, tmp_path, capsys):
        """Test displaying recent activity with files."""
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        (pipelines_dir / "test.yaml").write_text("test: 1")
        
        command = ShowCommand()
        
        command._display_recent_activity(tmp_path)
        
        captured = capsys.readouterr()
        assert "Recent Activity" in captured.out
        assert "test.yaml" in captured.out
    
    def test_recent_activity_no_files(self, tmp_path, capsys):
        """Test displaying recent activity with no files."""
        command = ShowCommand()
        
        command._display_recent_activity(tmp_path)
        
        captured = capsys.readouterr()
        assert "Recent Activity" in captured.out

