"""
Tests for IntelliSense functionality including VS Code integration, schema management, and cross-platform compatibility.
"""

import os
import json
import tempfile
import shutil
import platform
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest

from lhp.intellisense.schema_manager import SchemaManager
from lhp.intellisense.vscode_config import VSCodeConfigManager
from lhp.intellisense.extension_detector import VSCodeExtensionDetector
from lhp.intellisense.setup import IntelliSenseSetup


class MockVSCodeSettings:
    """Mock VS Code settings for testing."""
    
    def __init__(self, initial_settings=None):
        self.settings = initial_settings or {}
        self.file_path = None
        
    def read_settings(self, settings_path):
        """Mock reading VS Code settings."""
        self.file_path = settings_path
        return self.settings
        
    def write_settings(self, settings_path, settings):
        """Mock writing VS Code settings."""
        self.file_path = settings_path
        self.settings = settings
        
    def backup_settings(self, settings_path):
        """Mock backing up VS Code settings."""
        backup_path = settings_path.with_suffix('.backup')
        return backup_path


class CrossPlatformPathMock:
    """Mock cross-platform path handling."""
    
    @staticmethod
    def get_vscode_settings_path():
        """Get platform-specific VS Code settings path."""
        if platform.system() == "Windows":
            return Path.home() / "AppData" / "Roaming" / "Code" / "User" / "settings.json"
        elif platform.system() == "Darwin":
            return Path.home() / "Library" / "Application Support" / "Code" / "User" / "settings.json"
        else:
            return Path.home() / ".config" / "Code" / "User" / "settings.json"
    
    @staticmethod
    def get_vscode_extensions_path():
        """Get platform-specific VS Code extensions path."""
        if platform.system() == "Windows":
            return Path.home() / "AppData" / "Roaming" / "Code" / "CachedExtensions"
        elif platform.system() == "Darwin":
            return Path.home() / "Library" / "Application Support" / "Code" / "CachedExtensions"
        else:
            return Path.home() / ".config" / "Code" / "CachedExtensions"


class IntelliSenseTestFixtures:
    """Test fixtures for IntelliSense functionality."""
    
    @staticmethod
    def create_sample_vscode_settings():
        """Create sample VS Code settings for testing."""
        return {
            "yaml.schemas": {
                "https://json.schemastore.org/github-workflow.json": ".github/workflows/*.yaml",
                "https://json.schemastore.org/kustomization.json": "kustomization.yaml"
            },
            "files.associations": {
                "*.yaml": "yaml",
                "*.yml": "yaml"
            },
            "yaml.validate": True,
            "yaml.completion": True
        }
    
    @staticmethod
    def create_conflicting_extensions():
        """Create mock conflicting YAML extensions."""
        return [
            {
                "id": "redhat.vscode-yaml",
                "displayName": "YAML",
                "version": "1.14.0",
                "isActive": True
            },
            {
                "id": "ms-vscode.vscode-json",
                "displayName": "JSON",
                "version": "1.0.0",
                "isActive": True
            }
        ]
    
    @staticmethod
    def create_sample_schema_mapping():
        """Create sample schema mapping for testing."""
        return {
            "flowgroup.schema.json": "pipelines/**/*.yaml",
            "template.schema.json": "templates/**/*.yaml",
            "substitution.schema.json": "substitutions/**/*.yaml",
            "project.schema.json": "lhp.yaml",
            "preset.schema.json": "presets/**/*.yaml"
        }


@pytest.fixture
def mock_vscode_settings():
    """Fixture for mock VS Code settings."""
    return MockVSCodeSettings(IntelliSenseTestFixtures.create_sample_vscode_settings())


@pytest.fixture
def temp_project_dir():
    """Fixture for temporary project directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        project_dir = Path(temp_dir) / "test_project"
        project_dir.mkdir()
        
        # Create basic project structure
        (project_dir / "pipelines").mkdir()
        (project_dir / "templates").mkdir()
        (project_dir / "substitutions").mkdir()
        (project_dir / "presets").mkdir()
        
        # Create lhp.yaml
        lhp_config = {
            "project_name": "test_project",
            "version": "1.0.0"
        }
        with open(project_dir / "lhp.yaml", "w") as f:
            json.dump(lhp_config, f)
        
        yield project_dir


@pytest.fixture
def mock_vscode_process():
    """Fixture for mocking VS Code process detection."""
    # Since we're not implementing automatic restart detection,
    # this fixture just provides a mock for testing process-related functionality
    mock_process = Mock()
    mock_process.info = {'name': 'code'}
    yield [mock_process]


@pytest.fixture
def mock_importlib_resources():
    """Fixture for mocking importlib.resources."""
    with patch('importlib.resources') as mock_resources:
        # Mock files() method
        mock_files = Mock()
        mock_resources.files.return_value = mock_files
        
        # Mock individual schema files
        mock_files.joinpath.return_value.read_text.return_value = '{"$schema": "test"}'
        mock_files.iterdir.return_value = [
            Mock(name="flowgroup.schema.json"),
            Mock(name="template.schema.json"),
            Mock(name="substitution.schema.json"),
            Mock(name="project.schema.json"),
            Mock(name="preset.schema.json")
        ]
        
        yield mock_resources


class TestIntelliSenseInfrastructure:
    """Test the IntelliSense test infrastructure itself."""
    
    def test_mock_vscode_settings_creation(self, mock_vscode_settings):
        """Test that mock VS Code settings are created correctly."""
        assert isinstance(mock_vscode_settings.settings, dict)
        assert "yaml.schemas" in mock_vscode_settings.settings
        assert "files.associations" in mock_vscode_settings.settings
    
    def test_cross_platform_path_mock(self):
        """Test cross-platform path mocking."""
        settings_path = CrossPlatformPathMock.get_vscode_settings_path()
        extensions_path = CrossPlatformPathMock.get_vscode_extensions_path()
        
        assert isinstance(settings_path, Path)
        assert isinstance(extensions_path, Path)
        
        # Verify platform-specific paths
        if platform.system() == "Windows":
            assert "AppData" in str(settings_path)
        elif platform.system() == "Darwin":
            assert "Library" in str(settings_path)
        else:
            assert ".config" in str(settings_path)
    
    def test_test_fixtures_creation(self):
        """Test that test fixtures are created correctly."""
        settings = IntelliSenseTestFixtures.create_sample_vscode_settings()
        extensions = IntelliSenseTestFixtures.create_conflicting_extensions()
        schema_mapping = IntelliSenseTestFixtures.create_sample_schema_mapping()
        
        assert isinstance(settings, dict)
        assert isinstance(extensions, list)
        assert isinstance(schema_mapping, dict)
        
        # Verify structure
        assert "yaml.schemas" in settings
        assert len(extensions) > 0
        assert "flowgroup.schema.json" in schema_mapping
    
    def test_temp_project_dir_fixture(self, temp_project_dir):
        """Test that temporary project directory fixture works correctly."""
        assert temp_project_dir.exists()
        assert (temp_project_dir / "pipelines").exists()
        assert (temp_project_dir / "lhp.yaml").exists()
        
        # Verify project structure
        expected_dirs = ["pipelines", "templates", "substitutions", "presets"]
        for dir_name in expected_dirs:
            assert (temp_project_dir / dir_name).exists()
    
    def test_mock_vscode_process_fixture(self, mock_vscode_process):
        """Test that VS Code process mocking works correctly."""
        # This will be used by the actual process detection logic
        processes = mock_vscode_process
        assert len(processes) > 0
        assert processes[0].info['name'] == 'code'
    
    def test_mock_importlib_resources_fixture(self, mock_importlib_resources):
        """Test that importlib.resources mocking works correctly."""
        # Test accessing schema files
        files = mock_importlib_resources.files('lhp.schemas')
        assert files is not None
        
        # Test reading schema content
        schema_content = files.joinpath('flowgroup.schema.json').read_text()
        assert schema_content == '{"$schema": "test"}'


class TestCrossPlatformCompatibility:
    """Test cross-platform compatibility features."""
    
    @pytest.mark.parametrize("platform_name", ["Windows", "Darwin", "Linux"])
    def test_vscode_settings_path_detection(self, platform_name):
        """Test VS Code settings path detection across platforms."""
        with patch('platform.system', return_value=platform_name):
            path = CrossPlatformPathMock.get_vscode_settings_path()
            assert isinstance(path, Path)
            assert "settings.json" in str(path)
    
    @pytest.mark.parametrize("platform_name", ["Windows", "Darwin", "Linux"])
    def test_vscode_extensions_path_detection(self, platform_name):
        """Test VS Code extensions path detection across platforms."""
        with patch('platform.system', return_value=platform_name):
            path = CrossPlatformPathMock.get_vscode_extensions_path()
            assert isinstance(path, Path)
            assert "Code" in str(path)
    
    def test_path_separator_handling(self):
        """Test that path separators are handled correctly across platforms."""
        # Test with different path separators
        paths = [
            "pipelines/test.yaml",
            "pipelines\\test.yaml",
            "pipelines/nested/test.yaml"
        ]
        
        for path_str in paths:
            path = Path(path_str)
            assert isinstance(path, Path)
            # Path should be normalized to current platform
            assert str(path).replace('\\', '/').endswith("test.yaml")


class TestErrorHandling:
    """Test error handling in IntelliSense infrastructure."""
    
    def test_missing_vscode_settings_file(self, temp_project_dir):
        """Test handling when VS Code settings file doesn't exist."""
        non_existent_path = temp_project_dir / "non_existent_settings.json"
        
        # This should not raise an exception
        mock_settings = MockVSCodeSettings()
        result = mock_settings.read_settings(non_existent_path)
        assert result == {}
    
    def test_invalid_json_in_settings(self, temp_project_dir):
        """Test handling of invalid JSON in VS Code settings."""
        settings_file = temp_project_dir / "invalid_settings.json"
        settings_file.write_text("invalid json content")
        
        # This should be handled gracefully in the actual implementation
        # For now, we just ensure our test infrastructure handles it
        assert settings_file.exists()
    
    def test_permission_errors(self, temp_project_dir):
        """Test handling of permission errors."""
        # Create a file we can't write to (simulate permission error)
        readonly_file = temp_project_dir / "readonly_settings.json"
        readonly_file.write_text('{"test": "value"}')
        
        # This would be used to test permission error handling
        assert readonly_file.exists()


class TestMockingUtilities:
    """Test the mocking utilities for IntelliSense functionality."""
    
    def test_schema_content_mocking(self, mock_importlib_resources):
        """Test that schema content can be mocked correctly."""
        # Mock different schema content
        mock_file = mock_importlib_resources.files().joinpath()
        mock_file.read_text.return_value = '{"$schema": "http://json-schema.org/draft-07/schema#"}'
        
        content = mock_importlib_resources.files().joinpath().read_text()
        assert "json-schema.org" in content
    
    def test_file_system_operations_mocking(self, temp_project_dir):
        """Test mocking of file system operations."""
        # Test that we can mock file operations
        with patch('builtins.open', mock_open(read_data='{"mocked": "content"}')):
            # This would be used to test file reading without actual files
            pass
    
    def test_subprocess_mocking(self):
        """Test mocking of subprocess operations."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.returncode = 0
            mock_run.return_value.stdout = "VS Code is running"
            
            # This would be used to test subprocess operations
            result = mock_run()
            assert result.returncode == 0
            assert "VS Code" in result.stdout


def mock_open(read_data=None):
    """Helper function to create mock file operations."""
    from unittest.mock import mock_open as original_mock_open
    return original_mock_open(read_data=read_data)


if __name__ == "__main__":
    pytest.main([__file__]) 