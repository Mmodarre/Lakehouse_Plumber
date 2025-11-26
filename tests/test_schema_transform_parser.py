"""Tests for schema transform parser."""

import pytest
from pathlib import Path
from lhp.utils.schema_transform_parser import SchemaTransformParser


class TestSchemaTransformParserArrowFormat:
    """Test arrow format parsing."""
    
    def test_parse_arrow_rename_and_cast(self):
        """Test parsing arrow format with rename and cast."""
        parser = SchemaTransformParser()
        
        data = {
            "name": "test_transform",
            "columns": [
                "c_custkey -> customer_id: BIGINT",
                "c_name -> customer_name: STRING"
            ]
        }
        
        result = parser.parse_arrow_format(data)
        
        # Enforcement is no longer returned by parser (action-level only)
        assert "enforcement" not in result
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "customer_name": "STRING"
        }
    
    def test_parse_arrow_rename_only(self):
        """Test parsing arrow format with rename only (no type cast)."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey -> customer_id",
                "c_name -> customer_name"
            ]
        }
        
        result = parser.parse_arrow_format(data)
        
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {}
    
    def test_parse_arrow_cast_only(self):
        """Test parsing arrow format with type cast only (no rename)."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "customer_id: BIGINT",
                "account_balance: DECIMAL(18,2)"
            ]
        }
        
        result = parser.parse_arrow_format(data)
        
        assert result["column_mapping"] == {}
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "account_balance": "DECIMAL(18,2)"
        }
    
    def test_parse_arrow_pass_through(self):
        """Test parsing arrow format with pass-through columns (strict mode only)."""
        parser = SchemaTransformParser()
        
        data = {
            "enforcement": "strict",
            "columns": [
                "c_custkey -> customer_id",
                "address",
                "phone"
            ]
        }
        
        result = parser.parse_arrow_format(data)
        
        assert result["column_mapping"] == {"c_custkey": "customer_id"}
        assert result["type_casting"] == {}
        # Pass-through columns are tracked separately
        assert result["pass_through_columns"] == ["address", "phone"]
    
    def test_parse_arrow_flexible_whitespace_no_spaces(self):
        """Test arrow format parsing with no spaces around arrows and colons."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey->customer_id:BIGINT",
                "c_name->customer_name:STRING"
            ]
        }
        
        result = parser.parse_arrow_format(data)
        
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "customer_name": "STRING"
        }
    
    def test_parse_arrow_flexible_whitespace_multiple_spaces(self):
        """Test arrow format parsing with multiple spaces."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey  ->  customer_id  :  BIGINT",
                "c_name   ->   customer_name   :   STRING"
            ]
        }
        
        result = parser.parse_arrow_format(data)
        
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "customer_name": "STRING"
        }
    
    def test_parse_arrow_duplicate_source_column_error(self):
        """Test that duplicate source columns raise an error."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey -> customer_id: BIGINT",
                "c_custkey -> cust_id: BIGINT"  # Duplicate source
            ]
        }
        
        with pytest.raises(ValueError, match="Duplicate source column"):
            parser.parse_arrow_format(data)
    
    def test_parse_arrow_rename_then_cast_separate_lines(self):
        """Test that renaming and casting same column in separate lines is allowed."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey -> customer_id",
                "customer_id: BIGINT"  # Casting the renamed column separately is allowed
            ]
        }
        
        # This should work - rename then cast is a valid pattern
        result = parser.parse_arrow_format(data)
        assert result["column_mapping"] == {"c_custkey": "customer_id"}
        assert result["type_casting"] == {"customer_id": "BIGINT"}
    
    def test_parse_arrow_invalid_syntax_wrong_arrow(self):
        """Test that invalid arrow syntax raises an error."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey >>> customer_id"  # Wrong arrow
            ]
        }
        
        with pytest.raises(ValueError, match="Invalid arrow format"):
            parser.parse_arrow_format(data)
    
    def test_parse_arrow_invalid_syntax_missing_source(self):
        """Test that missing source column raises an error."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "-> customer_id"  # Missing source
            ]
        }
        
        with pytest.raises(ValueError, match="Invalid arrow format"):
            parser.parse_arrow_format(data)
    
    def test_parse_arrow_invalid_syntax_missing_target(self):
        """Test that missing target column raises an error."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey ->"  # Missing target
            ]
        }
        
        with pytest.raises(ValueError, match="Invalid arrow format"):
            parser.parse_arrow_format(data)
    
    def test_parse_arrow_invalid_syntax_missing_type(self):
        """Test that colon without type raises an error."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "customer_id:"  # Colon but no type
            ]
        }
        
        with pytest.raises(ValueError, match="Invalid arrow format"):
            parser.parse_arrow_format(data)
    
    def test_parse_arrow_default_enforcement_permissive(self):
        """Test that enforcement is not returned by parser (action-level only)."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey -> customer_id"
            ]
        }
        
        result = parser.parse_arrow_format(data)
        
        # Enforcement is action-level only, not returned by parser
        assert "enforcement" not in result
        assert "column_mapping" in result
    
    def test_parse_arrow_mixed_operations(self):
        """Test parsing with mixed operations in single file."""
        parser = SchemaTransformParser()
        
        data = {
            "enforcement": "strict",
            "columns": [
                "c_custkey -> customer_id: BIGINT",  # Rename + cast
                "c_name -> customer_name",  # Rename only
                "account_balance: DECIMAL(18,2)",  # Cast only
                "address"  # Pass-through
            ]
        }
        
        result = parser.parse_arrow_format(data)
        
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "account_balance": "DECIMAL(18,2)"
        }
        assert result["pass_through_columns"] == ["address"]


class TestSchemaTransformParserLegacyFormat:
    """Test legacy format parsing."""
    
    def test_parse_legacy_with_column_mapping_and_type_casting(self):
        """Test parsing legacy format with both column_mapping and type_casting."""
        parser = SchemaTransformParser()
        
        data = {
            "column_mapping": {
                "c_custkey": "customer_id",
                "c_name": "customer_name"
            },
            "type_casting": {
                "customer_id": "BIGINT",
                "customer_name": "STRING"
            }
        }
        
        result = parser.parse_legacy_format(data)
        
        # Enforcement is not returned by parser (action-level only)
        assert "enforcement" not in result
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "customer_name": "STRING"
        }
    
    def test_parse_legacy_column_mapping_only(self):
        """Test parsing legacy format with column_mapping only."""
        parser = SchemaTransformParser()
        
        data = {
            "column_mapping": {
                "c_custkey": "customer_id",
                "c_name": "customer_name"
            }
        }
        
        result = parser.parse_legacy_format(data)
        
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {}
    
    def test_parse_legacy_type_casting_only(self):
        """Test parsing legacy format with type_casting only."""
        parser = SchemaTransformParser()
        
        data = {
            "type_casting": {
                "customer_id": "BIGINT",
                "account_balance": "DECIMAL(18,2)"
            }
        }
        
        result = parser.parse_legacy_format(data)
        
        assert result["column_mapping"] == {}
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "account_balance": "DECIMAL(18,2)"
        }
    
    def test_parse_legacy_default_enforcement_permissive(self):
        """Test that enforcement is not returned by parser (action-level only)."""
        parser = SchemaTransformParser()
        
        data = {
            "column_mapping": {
                "c_custkey": "customer_id"
            }
        }
        
        result = parser.parse_legacy_format(data)
        
        # Enforcement is action-level only, not returned by parser
        assert "enforcement" not in result
        assert "column_mapping" in result
    
    def test_parse_legacy_explicit_permissive_enforcement(self):
        """Test parsing legacy format ignores enforcement (action-level only)."""
        parser = SchemaTransformParser()
        
        data = {
            "enforcement": "permissive",
            "column_mapping": {
                "c_custkey": "customer_id"
            }
        }
        
        result = parser.parse_legacy_format(data)
        
        # Enforcement is ignored by parser (action-level only)
        assert "enforcement" not in result
        assert result["column_mapping"] == {"c_custkey": "customer_id"}


class TestSchemaTransformParserValidation:
    """Test validation and error handling."""
    
    def test_detect_arrow_format(self):
        """Test that arrow format is correctly detected."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey -> customer_id"
            ]
        }
        
        # parse_file should detect arrow format
        result = parser.parse_file_data(data)
        
        assert result["column_mapping"] == {"c_custkey": "customer_id"}
    
    def test_detect_legacy_format(self):
        """Test that legacy format is correctly detected."""
        parser = SchemaTransformParser()
        
        data = {
            "column_mapping": {
                "c_custkey": "customer_id"
            }
        }
        
        # parse_file should detect legacy format
        result = parser.parse_file_data(data)
        
        assert result["column_mapping"] == {"c_custkey": "customer_id"}
    
    def test_mixed_format_error(self):
        """Test that mixing arrow and legacy format raises an error."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey -> customer_id"
            ],
            "column_mapping": {
                "c_name": "customer_name"
            }
        }
        
        with pytest.raises(ValueError, match="Cannot mix arrow format and legacy format"):
            parser.parse_file_data(data)
    
    def test_empty_mappings_in_strict_mode_error(self):
        """Test that empty columns raises an error (regardless of enforcement)."""
        parser = SchemaTransformParser()
        
        # Arrow format with empty columns
        data_arrow = {
            "columns": []
        }
        
        # Empty columns should always raise an error
        with pytest.raises(ValueError, match="No columns defined"):
            parser.parse_arrow_format(data_arrow)
    
    def test_empty_columns_permissive_mode_error(self):
        """Test that empty columns in permissive mode also raises an error."""
        parser = SchemaTransformParser()
        
        data = {
            "enforcement": "permissive",
            "columns": []
        }
        
        # Should error even in permissive mode
        with pytest.raises(ValueError, match="No columns defined"):
            parser.parse_arrow_format(data)
    
    def test_unknown_format_error(self):
        """Test that unknown format raises an error."""
        parser = SchemaTransformParser()
        
        data = {
            "some_other_field": "value"
        }
        
        with pytest.raises(ValueError, match="Unable to detect schema transform format"):
            parser.parse_file_data(data)
    
    def test_pass_through_in_permissive_mode_error(self):
        """Test that pass-through columns are allowed (enforcement is action-level)."""
        parser = SchemaTransformParser()
        
        data = {
            "columns": [
                "c_custkey -> customer_id",
                "address"  # Pass-through column
            ]
        }
        
        # Pass-through columns are now allowed in parser (enforcement validation is action-level)
        result = parser.parse_arrow_format(data)
        assert "address" in result["pass_through_columns"]

