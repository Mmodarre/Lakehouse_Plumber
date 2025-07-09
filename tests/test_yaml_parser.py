"""Tests for YAML parser error handling and edge cases."""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open

from lhp.parsers.yaml_parser import YAMLParser
from lhp.utils.error_formatter import LHPError, ErrorCategory


class TestYAMLParserErrorHandling:
    """Test YAML parser error handling - targeting coverage lines 19-25."""
    
    def test_parse_file_yaml_error(self):
        """Test handling of invalid YAML syntax (line 19-20)."""
        parser = YAMLParser()
        
        # Create a temporary file with invalid YAML
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")  # Missing closing bracket
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            # Should raise ValueError with YAML error message
            with pytest.raises(ValueError) as exc_info:
                parser.parse_file(yaml_file)
            
            assert "Invalid YAML" in str(exc_info.value)
            assert yaml_file.name in str(exc_info.value)
        finally:
            yaml_file.unlink()
    
    def test_parse_file_lhp_error_reraise(self):
        """Test that LHPError is re-raised as-is (lines 21-23)."""
        parser = YAMLParser()
        
        # Create a mock LHPError
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Test LHP Error",
            details="This is a test LHP error"
        )
        
        # Mock yaml.safe_load to raise LHPError
        with patch('yaml.safe_load') as mock_yaml_load:
            mock_yaml_load.side_effect = lhp_error
            
            # Mock file open
            with patch('builtins.open', mock_open(read_data="test: data")):
                # Should re-raise LHPError without modification
                with pytest.raises(LHPError) as exc_info:
                    parser.parse_file(Path("test.yaml"))
                
                # Verify it's the exact same error object
                assert exc_info.value is lhp_error
                assert exc_info.value.title == "Test LHP Error"
    
    def test_parse_file_generic_error(self):
        """Test handling of generic file errors (lines 24-25)."""
        parser = YAMLParser()
        
        # Test with non-existent file
        non_existent_file = Path("/non/existent/file.yaml")
        
        with pytest.raises(ValueError) as exc_info:
            parser.parse_file(non_existent_file)
        
        assert "Error reading" in str(exc_info.value)
        assert str(non_existent_file) in str(exc_info.value)
    
    def test_parse_file_permission_error(self):
        """Test handling of permission errors (lines 24-25)."""
        parser = YAMLParser()
        
        # Mock file open to raise PermissionError
        with patch('builtins.open') as mock_open_func:
            mock_open_func.side_effect = PermissionError("Permission denied")
            
            with pytest.raises(ValueError) as exc_info:
                parser.parse_file(Path("test.yaml"))
            
            assert "Error reading" in str(exc_info.value)
            assert "Permission denied" in str(exc_info.value)
    
    def test_parse_file_success_with_empty_file(self):
        """Test successful parsing of empty YAML file."""
        parser = YAMLParser()
        
        # Create temporary empty file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("")  # Empty file
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            # Should return empty dict for empty file
            result = parser.parse_file(yaml_file)
            assert result == {}
        finally:
            yaml_file.unlink()
    
    def test_parse_file_success_with_null_yaml(self):
        """Test successful parsing of YAML file with null content."""
        parser = YAMLParser()
        
        # Create temporary file with null YAML
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("null")  # YAML null
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            # Should return empty dict for null content
            result = parser.parse_file(yaml_file)
            assert result == {}
        finally:
            yaml_file.unlink()


class TestYAMLParserFlowgroupMethods:
    """Test flowgroup, template, and preset parsing methods."""
    
    def test_parse_flowgroup_success(self):
        """Test successful flowgroup parsing."""
        parser = YAMLParser()
        
        flowgroup_data = {
            "pipeline": "test_pipeline",
            "flowgroup": "test_flowgroup",
            "actions": [
                {
                    "name": "load_data",
                    "type": "load",
                    "source": {"type": "sql", "sql": "SELECT * FROM table"},
                    "target": "v_data"
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(flowgroup_data, f)
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            result = parser.parse_flowgroup(yaml_file)
            assert result.pipeline == "test_pipeline"
            assert result.flowgroup == "test_flowgroup"
            assert len(result.actions) == 1
        finally:
            yaml_file.unlink()
    
    def test_parse_template_success(self):
        """Test successful template parsing."""
        parser = YAMLParser()
        
        template_data = {
            "name": "test_template",
            "version": "1.0",
            "parameters": [
                {"name": "table_name", "required": True}
            ],
            "actions": [
                {
                    "name": "load_{{ table_name }}",
                    "type": "load",
                    "source": {"type": "sql", "sql": "SELECT * FROM {{ table_name }}"},
                    "target": "v_{{ table_name }}"
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(template_data, f)
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            result = parser.parse_template(yaml_file)
            assert result.name == "test_template"
            assert result.version == "1.0"
            assert len(result.parameters) == 1
            assert len(result.actions) == 1
        finally:
            yaml_file.unlink()
    
    def test_parse_preset_success(self):
        """Test successful preset parsing."""
        parser = YAMLParser()
        
        preset_data = {
            "name": "test_preset",
            "version": "1.0",
            "defaults": {
                "operational_metadata": True,
                "table_properties": {
                    "quality": "bronze"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(preset_data, f)
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            result = parser.parse_preset(yaml_file)
            assert result.name == "test_preset"
            assert result.version == "1.0"
            assert result.defaults is not None
        finally:
            yaml_file.unlink() 