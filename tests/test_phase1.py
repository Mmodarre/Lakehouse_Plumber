"""Tests for Phase 1 components of LakehousePlumber."""

import pytest
from pathlib import Path
import tempfile

from lhp.models.config import ActionType, LoadSourceType, TransformType, WriteTargetType, Action, FlowGroup, Template, Preset
from lhp.core.base_generator import BaseActionGenerator
from lhp.cli.main import cli, get_version
from click.testing import CliRunner


class TestModels:
    """Test the core data models."""
    
    def test_action_type_enum(self):
        """Test ActionType enum values."""
        assert ActionType.LOAD.value == "load"
        assert ActionType.TRANSFORM.value == "transform"
        assert ActionType.WRITE.value == "write"
    
    def test_action_model(self):
        """Test Action model creation."""
        action = Action(
            name="test_action",
            type=ActionType.LOAD,
            source={"type": "cloudfiles", "path": "/test/path"},
            target="test_view",
            description="Test action"
        )
        assert action.name == "test_action"
        assert action.type == ActionType.LOAD
        assert action.target == "test_view"
    
    def test_flowgroup_model(self):
        """Test FlowGroup model creation."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[
                Action(name="load_data", type=ActionType.LOAD, target="raw_data"),
                Action(name="clean_data", type=ActionType.TRANSFORM, source="raw_data", target="clean_data")
            ]
        )
        assert flowgroup.pipeline == "test_pipeline"
        assert len(flowgroup.actions) == 2
        assert flowgroup.presets == ["bronze_layer"]
    
    def test_preset_model(self):
        """Test Preset model creation."""
        preset = Preset(
            name="bronze_layer",
            version="1.0",
            extends="base_preset",
            description="Bronze layer preset",
            defaults={"schema_evolution": "addNewColumns"}
        )
        assert preset.name == "bronze_layer"
        assert preset.extends == "base_preset"
        assert preset.defaults["schema_evolution"] == "addNewColumns"


class TestBaseGenerator:
    """Test the base generator framework."""
    
    def test_base_generator_initialization(self):
        """Test that we can't instantiate the abstract base class."""
        with pytest.raises(TypeError):
            BaseActionGenerator()
    
    def test_concrete_generator(self):
        """Test a concrete implementation of BaseActionGenerator."""
        class TestGenerator(BaseActionGenerator):
            def generate(self, action, context):
                return f"Generated code for {action.name}"
        
        generator = TestGenerator()
        generator.add_import("import dlt")
        generator.add_import("import pyspark")
        
        assert "import dlt" in generator.imports
        assert "import pyspark" in generator.imports
        assert generator.imports == ["import dlt", "import pyspark"]  # Should be sorted


class TestCLI:
    """Test the CLI commands."""
    
    def setup_method(self):
        """Set up test runner."""
        self.runner = CliRunner()
    
    def test_cli_help(self):
        """Test CLI help command."""
        result = self.runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert "LakehousePlumber" in result.output
        assert "Generate Delta Live Tables pipelines from YAML configs" in result.output
    
    def test_cli_version(self):
        """Test CLI version command."""
        result = self.runner.invoke(cli, ['--version'])
        assert result.exit_code == 0
        expected_version = get_version()
        assert expected_version in result.output
    
    def test_init_command(self):
        """Test project initialization command."""
        with tempfile.TemporaryDirectory() as temp_dir:
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                project_name = "test_project"
                result = self.runner.invoke(cli, ['init', project_name])
                
                assert result.exit_code == 0
                assert f"Initialized LakehousePlumber project: {project_name}" in result.output
                
                # Check created structure
                project_path = Path(temp_dir) / project_name
                assert project_path.exists()
                assert (project_path / "lhp.yaml").exists()
                assert (project_path / "presets").exists()
                assert (project_path / "templates").exists()
                assert (project_path / "pipelines").exists()
            finally:
                os.chdir(original_cwd)
    
    def test_init_existing_directory(self):
        """Test init command with existing directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                project_name = "existing_project"
                existing_path = Path(temp_dir) / project_name
                existing_path.mkdir()
                
                result = self.runner.invoke(cli, ['init', project_name])
                
                assert result.exit_code == 1  # Should fail for existing directory
                assert f"Directory {project_name} already exists" in result.output
            finally:
                os.chdir(original_cwd)
    
    def test_validate_command(self):
        """Test validate command (placeholder)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                # Initialize a project first
                project_name = "test_project"
                init_result = self.runner.invoke(cli, ['init', project_name])
                assert init_result.exit_code == 0
                
                # Change to project directory
                os.chdir(project_name)
                
                # Now test validate (will fail because no flowgroups, but should reach that stage)
                result = self.runner.invoke(cli, ['validate', '--env', 'dev'])
                assert result.exit_code == 1  # Expected to fail with no flowgroups
                assert "No flowgroups found in project" in result.output
            finally:
                os.chdir(original_cwd)
    
    def test_generate_command(self):
        """Test generate command (placeholder)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                # Initialize a project first
                project_name = "test_project"
                init_result = self.runner.invoke(cli, ['init', project_name])
                assert init_result.exit_code == 0
                
                # Change to project directory
                os.chdir(project_name)
                
                # Now test generate (will fail because no flowgroups, but should reach that stage)
                result = self.runner.invoke(cli, ['generate', '--env', 'dev'])
                assert result.exit_code == 1  # Expected to fail with no flowgroups
                assert "No flowgroups found in project" in result.output
            finally:
                os.chdir(original_cwd)
    
    def test_list_presets_command(self):
        """Test list-presets command (placeholder)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                # Initialize a project first
                project_name = "test_project"
                init_result = self.runner.invoke(cli, ['init', project_name])
                assert init_result.exit_code == 0
                
                # Change to project directory
                os.chdir(project_name)
                
                # Now test list-presets
                result = self.runner.invoke(cli, ['list-presets'])
                assert result.exit_code == 0
                assert "Available presets:" in result.output
            finally:
                os.chdir(original_cwd)
    
    def test_list_templates_command(self):
        """Test list-templates command (placeholder)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                # Initialize a project first
                project_name = "test_project"
                init_result = self.runner.invoke(cli, ['init', project_name])
                assert init_result.exit_code == 0
                
                # Change to project directory
                os.chdir(project_name)
                
                # Now test list-templates
                result = self.runner.invoke(cli, ['list-templates'])
                assert result.exit_code == 0
                assert "Available templates:" in result.output
            finally:
                os.chdir(original_cwd)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 