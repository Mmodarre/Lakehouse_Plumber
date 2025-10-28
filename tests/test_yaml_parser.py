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
        
        # Mock yaml.safe_load_all to raise LHPError
        with patch('yaml.safe_load_all') as mock_yaml_load_all:
            mock_yaml_load_all.side_effect = lhp_error
            
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
        """Test that empty files raise MultiDocumentError (new behavior with single-doc validation)."""
        parser = YAMLParser()
        
        from lhp.utils.error_formatter import MultiDocumentError
        
        # Create temporary empty file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("")  # Empty file
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            # Empty files (0 documents) should raise MultiDocumentError
            with pytest.raises(MultiDocumentError) as exc_info:
                result = parser.parse_file(yaml_file)
            
            assert "Expected 1, Found 0" in str(exc_info.value)
        finally:
            yaml_file.unlink()
    
    def test_parse_flowgroup_basic(self):
        """Test basic FlowGroup parsing functionality."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
actions:
  - name: load_data
    type: load
    target: raw_data
    description: Load raw data
"""
            f.write(yaml_content)
            f.flush()
            
            try:
                flowgroup = parser.parse_flowgroup(Path(f.name))
                assert flowgroup.pipeline == 'test_pipeline'
                assert flowgroup.presets == ['bronze_layer']
                assert len(flowgroup.actions) == 1
                assert flowgroup.actions[0].name == 'load_data'
            finally:
                Path(f.name).unlink()
    
    # Note: test_discover_flowgroups_basic removed - discover_flowgroups() method
    # has been removed from YAMLParser. Use FlowgroupDiscoverer.discover_flowgroups() instead.
    
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
            result = parser.parse_template_raw(yaml_file)
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


class TestParseFlowgroupsFromFile:
    """Test parse_flowgroups_from_file() method for multi-flowgroup YAML support."""
    
    def test_multi_document_parsing(self):
        """Test parsing multiple documents separated by ---."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: flowgroup1
actions:
  - name: action1
    type: load
    target: table1
---
pipeline: test_pipeline
flowgroup: flowgroup2
actions:
  - name: action2
    type: transform
    source: table1
    target: table2
---
pipeline: test_pipeline
flowgroup: flowgroup3
actions:
  - name: action3
    type: write
    source: table2
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)
            assert len(flowgroups) == 3
            assert flowgroups[0].flowgroup == 'flowgroup1'
            assert flowgroups[1].flowgroup == 'flowgroup2'
            assert flowgroups[2].flowgroup == 'flowgroup3'
            assert flowgroups[0].actions[0].name == 'action1'
            assert flowgroups[1].actions[0].name == 'action2'
            assert flowgroups[2].actions[0].name == 'action3'
        finally:
            yaml_file.unlink()
    
    def test_flowgroups_array_syntax(self):
        """Test parsing flowgroups array syntax."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
use_template: my_template
presets:
  - bronze_preset
flowgroups:
  - flowgroup: flowgroup1
    template_parameters:
      table_name: table1
  - flowgroup: flowgroup2
    template_parameters:
      table_name: table2
  - flowgroup: flowgroup3
    template_parameters:
      table_name: table3
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)
            assert len(flowgroups) == 3
            assert flowgroups[0].flowgroup == 'flowgroup1'
            assert flowgroups[1].flowgroup == 'flowgroup2'
            assert flowgroups[2].flowgroup == 'flowgroup3'
            # Check inheritance
            assert flowgroups[0].pipeline == 'test_pipeline'
            assert flowgroups[0].use_template == 'my_template'
            assert flowgroups[0].presets == ['bronze_preset']
            assert flowgroups[1].pipeline == 'test_pipeline'
            assert flowgroups[2].presets == ['bronze_preset']
        finally:
            yaml_file.unlink()
    
    def test_inheritance_when_key_not_present(self):
        """Test inheritance only happens when key not present in flowgroup."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
presets:
  - bronze_preset
  - data_quality
operational_metadata: true
flowgroups:
  - flowgroup: flowgroup1
    # Should inherit all document-level values
  - flowgroup: flowgroup2
    presets:
      - silver_preset
    # Should NOT inherit presets, but should inherit operational_metadata
  - flowgroup: flowgroup3
    presets: []
    # Empty list means explicit override - no inheritance
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)
            assert len(flowgroups) == 3
            
            # flowgroup1 - inherits everything
            assert flowgroups[0].presets == ['bronze_preset', 'data_quality']
            assert flowgroups[0].operational_metadata == True
            
            # flowgroup2 - overrides presets, inherits operational_metadata
            assert flowgroups[1].presets == ['silver_preset']
            assert flowgroups[1].operational_metadata == True
            
            # flowgroup3 - empty list is explicit override
            assert flowgroups[2].presets == []
            assert flowgroups[2].operational_metadata == True
        finally:
            yaml_file.unlink()
    
    def test_type_agnostic_override(self):
        """Test that different types can override (bool -> list, list -> bool)."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
operational_metadata: true
flowgroups:
  - flowgroup: flowgroup1
    # Inherits bool
  - flowgroup: flowgroup2
    operational_metadata: ["col1", "col2"]
    # Override bool with list
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)
            assert len(flowgroups) == 2
            assert flowgroups[0].operational_metadata == True
            assert flowgroups[1].operational_metadata == ["col1", "col2"]
        finally:
            yaml_file.unlink()
    
    def test_duplicate_flowgroup_name_detection(self):
        """Test that duplicate flowgroup names within same file raise error."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: duplicate_name
actions:
  - name: action1
    type: load
    target: table1
---
pipeline: test_pipeline
flowgroup: duplicate_name
actions:
  - name: action2
    type: load
    target: table2
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(ValueError) as exc_info:
                parser.parse_flowgroups_from_file(yaml_file)
            
            assert "Duplicate flowgroup name" in str(exc_info.value)
            assert "duplicate_name" in str(exc_info.value)
        finally:
            yaml_file.unlink()
    
    def test_mixed_syntax_rejection(self):
        """Test that mixed multi-doc and array syntax raises error."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroups:
  - flowgroup: flowgroup1
---
pipeline: test_pipeline
flowgroup: flowgroup2
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(ValueError) as exc_info:
                parser.parse_flowgroups_from_file(yaml_file)
            
            error_msg = str(exc_info.value).lower()
            assert "mixed syntax" in error_msg or "cannot mix" in error_msg
        finally:
            yaml_file.unlink()
    
    def test_error_messages_include_document_index(self):
        """Test that parsing errors include document index in error message."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: flowgroup1
actions:
  - name: action1
    type: load
    target: table1
---
pipeline: test_pipeline
flowgroup: flowgroup2
actions:
  - name: action2
    type: invalid_type
    target: table2
---
pipeline: test_pipeline
flowgroup: flowgroup3
actions:
  - name: action3
    type: load
    target: table3
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(Exception) as exc_info:
                parser.parse_flowgroups_from_file(yaml_file)
            
            error_msg = str(exc_info.value)
            # Should contain document index (2, since it's the second document)
            assert "document 2" in error_msg or "document" in error_msg
        finally:
            yaml_file.unlink()
    
    def test_single_flowgroup_backward_compatibility(self):
        """Test that single flowgroup files still work (backward compat)."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: single_flowgroup
actions:
  - name: action1
    type: load
    target: table1
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)
            assert len(flowgroups) == 1
            assert flowgroups[0].flowgroup == 'single_flowgroup'
            assert flowgroups[0].pipeline == 'test_pipeline'
        finally:
            yaml_file.unlink()
    
    def test_array_syntax_with_template_parameters(self):
        """Test array syntax with varying template_parameters."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: bronze_sap
use_template: TMPL003_parquet_ingestion
flowgroups:
  - flowgroup: brand_ingestion
    template_parameters:
      source_path: /mnt/raw/sap/brand
      target_table: bronze_sap_brand
  - flowgroup: cat_ingestion
    template_parameters:
      source_path: /mnt/raw/sap/category
      target_table: bronze_sap_cat
  - flowgroup: carrier_ingestion
    template_parameters:
      source_path: /mnt/raw/sap/carrier
      target_table: bronze_sap_carrier
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)
            assert len(flowgroups) == 3
            assert all(fg.use_template == 'TMPL003_parquet_ingestion' for fg in flowgroups)
            assert flowgroups[0].template_parameters['target_table'] == 'bronze_sap_brand'
            assert flowgroups[1].template_parameters['target_table'] == 'bronze_sap_cat'
            assert flowgroups[2].template_parameters['target_table'] == 'bronze_sap_carrier'
        finally:
            yaml_file.unlink()
    
    def test_strict_parsing_malformed_document_fails_entire_file(self):
        """Test that a malformed document fails the entire file (strict parsing)."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: flowgroup1
actions:
  - name: action1
    type: load
    target: table1
---
pipeline: test_pipeline
flowgroup: flowgroup2
actions: [unclosed bracket
---
pipeline: test_pipeline
flowgroup: flowgroup3
actions:
  - name: action3
    type: load
    target: table3
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(ValueError) as exc_info:
                parser.parse_flowgroups_from_file(yaml_file)
            
            # Should fail with YAML error
            assert "Invalid YAML" in str(exc_info.value) or "yaml" in str(exc_info.value).lower()
        finally:
            yaml_file.unlink()


class TestParseFlowgroupErrorOnMultiple:
    """Test that parse_flowgroup() raises error when file contains multiple flowgroups."""
    
    def test_parse_flowgroup_errors_on_multi_document(self):
        """Test parse_flowgroup() raises error with multi-document file."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: flowgroup1
---
pipeline: test_pipeline
flowgroup: flowgroup2
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(ValueError) as exc_info:
                parser.parse_flowgroup(yaml_file)
            
            assert "multiple flowgroups" in str(exc_info.value).lower()
            assert "parse_flowgroups_from_file" in str(exc_info.value)
        finally:
            yaml_file.unlink()
    
    def test_parse_flowgroup_errors_on_array_syntax(self):
        """Test parse_flowgroup() raises error with array syntax."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroups:
  - flowgroup: flowgroup1
  - flowgroup: flowgroup2
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(ValueError) as exc_info:
                parser.parse_flowgroup(yaml_file)
            
            assert "multiple flowgroups" in str(exc_info.value).lower()
            assert "parse_flowgroups_from_file" in str(exc_info.value)
        finally:
            yaml_file.unlink()
    
    def test_parse_flowgroup_still_works_for_single(self):
        """Test parse_flowgroup() still works for single flowgroup files."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
pipeline: test_pipeline
flowgroup: single_flowgroup
actions:
  - name: action1
    type: load
    target: table1
""")
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            # Should work without error
            flowgroup = parser.parse_flowgroup(yaml_file)
            assert flowgroup.flowgroup == 'single_flowgroup'
        finally:
            yaml_file.unlink() 