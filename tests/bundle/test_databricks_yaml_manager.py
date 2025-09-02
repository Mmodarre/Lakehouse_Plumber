"""
Unit tests for DatabricksYAMLManager.

This module tests the DatabricksYAMLManager class which handles databricks.yml
modifications with perfect structure preservation using ruamel.yaml.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import tempfile
import shutil
from ruamel.yaml import YAML

from lhp.bundle.databricks_yaml_manager import DatabricksYAMLManager
from lhp.bundle.exceptions import BundleResourceError, MissingDatabricksTargetError


class TestDatabricksYAMLManager:
    """Test class for DatabricksYAMLManager functionality."""
    
    @pytest.fixture
    def temp_project_root(self):
        """Create a temporary project directory for testing."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def manager(self, temp_project_root):
        """Create a DatabricksYAMLManager instance for testing."""
        return DatabricksYAMLManager(temp_project_root)
    
    @pytest.fixture
    def sample_databricks_yml_content(self):
        """Sample databricks.yml content for testing."""
        return """# This is a Databricks asset bundle definition
bundle:
  name: test_project

include:
  - resources/*.yml

targets:
  dev:
    # Development environment settings
    mode: development
    default: true
    workspace:
      host: <databricks_host>
      root_path: ~/.bundle/${bundle.name}/${bundle.target}

  prod:
    # Production settings - DO NOT MODIFY
    mode: production
    workspace:
      host: <databricks_host>
      root_path: /Workspace/Users/<USERNAME>/.bundle/${bundle.name}/${bundle.target}
    permissions:
      - service_principal_name: <service_principal_id>
        level: CAN_MANAGE
"""

    def test_init(self, temp_project_root):
        """Test DatabricksYAMLManager initialization."""
        manager = DatabricksYAMLManager(temp_project_root)
        
        assert manager.project_root == temp_project_root
        assert manager.databricks_file == temp_project_root / "databricks.yml"
        assert manager.yaml is not None
        assert manager.yaml.preserve_quotes is True
        assert manager.yaml.map_indent == 2
        assert manager.yaml.sequence_indent == 4

    def test_validate_targets_exist_success(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test successful target validation."""
        # Create databricks.yml file
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        # Should not raise any exception
        manager.validate_targets_exist(['dev', 'prod'])

    def test_validate_targets_exist_file_not_found(self, manager):
        """Test error when databricks.yml doesn't exist."""
        with pytest.raises(FileNotFoundError, match="databricks.yml not found"):
            manager.validate_targets_exist(['dev'])

    def test_validate_targets_exist_missing_targets_section(self, manager, temp_project_root):
        """Test error when targets section is missing."""
        # Create databricks.yml without targets
        content = """bundle:
  name: test_project
"""
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(content)
        
        with pytest.raises(MissingDatabricksTargetError, match="missing 'targets' section"):
            manager.validate_targets_exist(['dev'])

    def test_validate_targets_exist_missing_targets(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test error when required targets are missing."""
        # Create databricks.yml file
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        with pytest.raises(MissingDatabricksTargetError, match="Missing targets.*\\['staging'\\]"):
            manager.validate_targets_exist(['dev', 'prod', 'staging'])

    def test_update_target_variables_success(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test successful variable update."""
        # Create databricks.yml file
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        variables = {
            "default_pipeline_catalog": "test_catalog",
            "default_pipeline_schema": "test_schema"
        }
        
        # Update variables
        manager.update_target_variables(['dev', 'prod'], variables)
        
        # Verify the file was updated
        yaml = YAML()
        with open(databricks_file, 'r') as f:
            data = yaml.load(f)
        
        # Check dev target
        assert 'variables' in data['targets']['dev']
        assert data['targets']['dev']['variables']['default_pipeline_catalog'] == "test_catalog"
        assert data['targets']['dev']['variables']['default_pipeline_schema'] == "test_schema"
        
        # Check prod target
        assert 'variables' in data['targets']['prod']
        assert data['targets']['prod']['variables']['default_pipeline_catalog'] == "test_catalog"
        assert data['targets']['prod']['variables']['default_pipeline_schema'] == "test_schema"

    def test_update_target_variables_preserves_structure(self, manager, temp_project_root):
        """Test that variable updates preserve file structure and comments."""
        # Create databricks.yml with comments
        content = """# This is a test file
bundle:
  name: test_project

targets:
  dev:
    # Development settings
    mode: development
    workspace:
      host: dev.databricks.com
"""
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(content)
        
        variables = {"default_pipeline_catalog": "dev_catalog"}
        manager.update_target_variables(['dev'], variables)
        
        # Verify structure is preserved
        updated_content = databricks_file.read_text()
        assert "# This is a test file" in updated_content
        assert "# Development settings" in updated_content
        assert "default_pipeline_catalog: dev_catalog" in updated_content

    def test_update_target_variables_file_not_found(self, manager):
        """Test error when databricks.yml doesn't exist during update."""
        variables = {"default_pipeline_catalog": "test"}
        
        with pytest.raises(FileNotFoundError, match="databricks.yml not found"):
            manager.update_target_variables(['dev'], variables)

    def test_get_target_variables_success(self, manager, temp_project_root):
        """Test getting variables from a target."""
        # Create databricks.yml with existing variables
        content = """bundle:
  name: test_project

targets:
  dev:
    mode: development
    variables:
      default_pipeline_catalog: existing_catalog
      default_pipeline_schema: existing_schema
      other_var: other_value
"""
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(content)
        
        variables = manager.get_target_variables('dev')
        
        assert variables == {
            'default_pipeline_catalog': 'existing_catalog',
            'default_pipeline_schema': 'existing_schema',
            'other_var': 'other_value'
        }

    def test_get_target_variables_no_variables_section(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test getting variables when no variables section exists."""
        # Create databricks.yml file without variables
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        variables = manager.get_target_variables('dev')
        assert variables == {}

    def test_get_target_variables_target_not_found(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test error when target doesn't exist."""
        # Create databricks.yml file
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        with pytest.raises(MissingDatabricksTargetError, match="Target 'nonexistent' not found"):
            manager.get_target_variables('nonexistent')

    def test_bulk_update_all_targets(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test bulk update of multiple targets with environment-specific values."""
        # Create databricks.yml file
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        environment_variables = {
            'dev': {
                'default_pipeline_catalog': 'dev_catalog',
                'default_pipeline_schema': 'dev_schema'
            },
            'prod': {
                'default_pipeline_catalog': 'prod_catalog',
                'default_pipeline_schema': 'prod_schema'
            }
        }
        
        manager.bulk_update_all_targets(['dev', 'prod'], environment_variables)
        
        # Verify the updates
        yaml = YAML()
        with open(databricks_file, 'r') as f:
            data = yaml.load(f)
        
        # Check dev target
        dev_vars = data['targets']['dev']['variables']
        assert dev_vars['default_pipeline_catalog'] == 'dev_catalog'
        assert dev_vars['default_pipeline_schema'] == 'dev_schema'
        
        # Check prod target
        prod_vars = data['targets']['prod']['variables']
        assert prod_vars['default_pipeline_catalog'] == 'prod_catalog'
        assert prod_vars['default_pipeline_schema'] == 'prod_schema'

    def test_bulk_update_missing_targets(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test bulk update error when targets are missing."""
        # Create databricks.yml file
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        environment_variables = {'staging': {'default_pipeline_catalog': 'staging_cat'}}
        
        with pytest.raises(MissingDatabricksTargetError, match="Missing targets.*\\['staging'\\]"):
            manager.bulk_update_all_targets(['staging'], environment_variables)

    @patch('builtins.open', mock_open(read_data="invalid yaml content: ["))
    def test_update_variables_yaml_parse_error(self, manager, temp_project_root):
        """Test error handling for YAML parse errors."""
        # Create empty databricks.yml file (mock will override content)
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.touch()
        
        variables = {"default_pipeline_catalog": "test"}
        
        with pytest.raises(BundleResourceError, match="Unexpected error updating databricks.yml"):
            manager.update_target_variables(['dev'], variables)

    def test_file_permission_error(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test error handling for file permission errors."""
        # Create databricks.yml file
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        variables = {"default_pipeline_catalog": "test"}
        
        # Mock permission error during file write
        with patch('builtins.open', mock_open()) as mock_file:
            mock_file.side_effect = [
                mock_open(read_data=sample_databricks_yml_content).return_value,
                PermissionError("Permission denied")
            ]
            
            with pytest.raises(BundleResourceError, match="Failed to update databricks.yml.*Permission denied"):
                manager.update_target_variables(['dev'], variables)

    def test_yaml_configuration(self, manager):
        """Test that ruamel.yaml is configured correctly for preservation."""
        yaml_processor = manager.yaml
        
        assert yaml_processor.preserve_quotes is True
        assert yaml_processor.map_indent == 2
        assert yaml_processor.sequence_indent == 4
        assert yaml_processor.width == 4096
        assert yaml_processor.allow_duplicate_keys is False

    def test_empty_environment_variables_dict(self, manager, temp_project_root, sample_databricks_yml_content):
        """Test bulk update with empty environment variables."""
        # Create databricks.yml file
        databricks_file = temp_project_root / "databricks.yml"
        databricks_file.write_text(sample_databricks_yml_content)
        
        # Test with empty dict for an environment
        environment_variables = {'dev': {}}
        
        # Should not raise error, but also shouldn't add variables
        manager.bulk_update_all_targets(['dev'], environment_variables)
        
        # Verify no variables were added (or empty variables section)
        yaml = YAML()
        with open(databricks_file, 'r') as f:
            data = yaml.load(f)
        
        dev_vars = data['targets']['dev'].get('variables', {})
        assert dev_vars == {}


if __name__ == '__main__':
    pytest.main([__file__])
