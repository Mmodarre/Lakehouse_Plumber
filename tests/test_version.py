"""Tests for version utility functions."""

import pytest
from unittest.mock import Mock, patch, mock_open
from pathlib import Path

from lhp.utils.version import get_version


class TestVersionUtils:
    """Test cases covering all major business logic branches for version utilities."""

    def test_get_version_from_package_metadata_success(self):
        """Test successful version retrieval from package metadata."""
        # Arrange & Act
        with patch('lhp.utils.version.version') as mock_version:
            mock_version.return_value = "1.2.3"
            result = get_version()
        
        # Assert
        assert result == "1.2.3"
        mock_version.assert_called_once_with("lakehouse-plumber")

    def test_get_version_package_metadata_exception_with_pyproject_fallback(self):
        """Test fallback to pyproject.toml when package metadata fails."""
        # Arrange
        pyproject_content = '''
        [project]
        name = "lakehouse-plumber"
        version = "2.1.0"
        description = "Test project"
        '''
        
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('builtins.open', mock_open(read_data=pyproject_content)):
            
            mock_version.side_effect = Exception("Package not found")
            mock_exists.return_value = True
            
            result = get_version()
        
        # Assert
        assert result == "2.1.0"

    def test_get_version_pyproject_found_current_dir(self):
        """Test finding pyproject.toml in current directory."""
        # Arrange  
        pyproject_content = 'version = "3.4.5"'
        
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('builtins.open', mock_open(read_data=pyproject_content)):
            
            mock_version.side_effect = Exception("Package not found")
            mock_exists.return_value = True
            
            result = get_version()
        
        # Assert
        assert result == "3.4.5"

    def test_get_version_pyproject_found_parent_dir(self):
        """Test finding pyproject.toml in parent directory after search."""
        # Arrange
        pyproject_content = 'version = "4.5.6"'
        
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('builtins.open', mock_open(read_data=pyproject_content)):
            
            mock_version.side_effect = Exception("Package not found")
            # First call returns False (not in current dir), second returns True (found in parent)
            mock_exists.side_effect = [False, True]
            
            result = get_version()
        
        # Assert
        assert result == "4.5.6"

    def test_get_version_pyproject_not_found(self):
        """Test when pyproject.toml is not found after full search."""
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists:
            
            mock_version.side_effect = Exception("Package not found")
            mock_exists.return_value = False  # Never found
            
            result = get_version()
        
        # Assert - should fall back to hardcoded version
        assert result == "0.4.1"

    def test_get_version_pyproject_search_exception(self):
        """Test exception handling during pyproject.toml search."""
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists:
            
            mock_version.side_effect = Exception("Package not found")
            mock_exists.side_effect = Exception("File system error")
            
            result = get_version()
        
        # Assert - should fall back to hardcoded version
        assert result == "0.4.1"

    def test_get_version_pyproject_valid_parsing(self):
        """Test successful version parsing from pyproject.toml."""
        # Arrange - test different version formats
        test_cases = [
            ('version = "1.2.3"', "1.2.3"),
            ("version = '4.5.6'", "4.5.6"), 
            ('version="7.8.9"', "7.8.9"),
            ("version='1.0.0-beta'", "1.0.0-beta"),
        ]
        
        for pyproject_content, expected_version in test_cases:
            # Act
            with patch('lhp.utils.version.version') as mock_version, \
                 patch('pathlib.Path.exists') as mock_exists, \
                 patch('builtins.open', mock_open(read_data=pyproject_content)):
                
                mock_version.side_effect = Exception("Package not found")
                mock_exists.return_value = True
                
                result = get_version()
            
            # Assert
            assert result == expected_version

    def test_get_version_pyproject_invalid_content(self):
        """Test when pyproject.toml exists but has no version match."""
        # Arrange
        pyproject_content = '''
        [project]
        name = "lakehouse-plumber"
        description = "Test project"
        # No version field
        '''
        
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('builtins.open', mock_open(read_data=pyproject_content)):
            
            mock_version.side_effect = Exception("Package not found")
            mock_exists.return_value = True
            
            result = get_version()
        
        # Assert - should fall back to hardcoded version
        assert result == "0.4.1"

    def test_get_version_pyproject_file_read_error(self):
        """Test exception handling during file reading."""
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('builtins.open') as mock_file:
            
            mock_version.side_effect = Exception("Package not found")
            mock_exists.return_value = True
            mock_file.side_effect = IOError("Permission denied")
            
            result = get_version()
        
        # Assert - should fall back to hardcoded version
        assert result == "0.4.1"

    def test_get_version_final_fallback(self):
        """Test final fallback when all other methods fail."""
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists:
            
            mock_version.side_effect = Exception("Package not found")
            mock_exists.return_value = False
            
            result = get_version()
        
        # Assert
        assert result == "0.4.1"

    def test_get_version_multiple_directory_levels(self):
        """Test searching up multiple directory levels (up to 5)."""
        # Arrange
        pyproject_content = 'version = "5.0.0"'
        
        # Act
        with patch('lhp.utils.version.version') as mock_version, \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('builtins.open', mock_open(read_data=pyproject_content)):
            
            mock_version.side_effect = Exception("Package not found")
            # Simulate finding file on the 4th level (3 False calls, then True)
            mock_exists.side_effect = [False, False, False, True]
            
            result = get_version()
        
        # Assert
        assert result == "5.0.0"
        # Verify it tried multiple levels
        assert mock_exists.call_count == 4

    def test_version_function_exists_and_callable(self):
        """Test that the imported version function is callable."""
        # This verifies that one of the import paths worked successfully
        # and the version function is available for use
        from lhp.utils.version import get_version
        
        # The function should exist and be callable
        assert callable(get_version)
        
        # Calling it should return a string version
        result = get_version()
        assert isinstance(result, str)
        assert len(result) > 0
        
        # Version should follow some basic pattern (digits and dots/characters)
        import re
        assert re.match(r'^[\d\w\-\.]+$', result)
