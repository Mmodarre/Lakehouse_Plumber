"""Tests for schema transform file loading and path resolution."""

import pytest
import tempfile
from pathlib import Path
from lhp.utils.schema_transform_parser import SchemaTransformParser


class TestSchemaTransformFileLoading:
    """Test file loading with various path resolutions."""
    
    def test_load_file_from_root_level(self, tmp_path):
        """Test loading schema file from root level."""
        # Create schema file in root (enforcement removed - action-level only)
        schema_file = tmp_path / "customer_transform.yaml"
        schema_file.write_text("""
name: customer_transform
columns:
  - "c_custkey -> customer_id: BIGINT"
  - "c_name -> customer_name"
""")
        
        parser = SchemaTransformParser()
        result = parser.parse_file(schema_file)
        
        # Enforcement is not returned by parser (action-level only)
        assert "enforcement" not in result
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {"customer_id": "BIGINT"}
    
    def test_load_file_from_schemas_directory(self, tmp_path):
        """Test loading schema file from schemas/ directory."""
        # Create schemas directory
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        
        schema_file = schemas_dir / "customer_transform.yaml"
        schema_file.write_text("""
columns:
  - "c_custkey -> customer_id"
  - "c_name -> customer_name"
""")
        
        parser = SchemaTransformParser()
        result = parser.parse_file(schema_file)
        
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
    
    def test_load_file_from_subdirectory(self, tmp_path):
        """Test loading schema file from subdirectory."""
        # Create nested directory structure
        schemas_dir = tmp_path / "schemas" / "bronze"
        schemas_dir.mkdir(parents=True)
        
        schema_file = schemas_dir / "customer_transform.yaml"
        schema_file.write_text("""
columns:
  - "c_custkey -> customer_id: BIGINT"
""")
        
        parser = SchemaTransformParser()
        result = parser.parse_file(schema_file)
        
        # Enforcement is not returned by parser (action-level only)
        assert "enforcement" not in result
        assert result["column_mapping"] == {"c_custkey": "customer_id"}
        assert result["type_casting"] == {"customer_id": "BIGINT"}
    
    def test_load_file_from_nested_subdirectories(self, tmp_path):
        """Test loading schema file from deeply nested subdirectories."""
        # Create deeply nested directory
        schemas_dir = tmp_path / "schemas" / "bronze" / "dimensions"
        schemas_dir.mkdir(parents=True)
        
        schema_file = schemas_dir / "customer_transform.yaml"
        schema_file.write_text("""
columns:
  - "c_custkey -> customer_id: BIGINT"
  - "c_name -> customer_name: STRING"
""")
        
        parser = SchemaTransformParser()
        result = parser.parse_file(schema_file)
        
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "customer_name": "STRING"
        }
    
    def test_load_file_with_absolute_path(self, tmp_path):
        """Test loading schema file with absolute path."""
        schema_file = tmp_path / "transform.yaml"
        schema_file.write_text("""
columns:
  - "customer_id: BIGINT"
""")
        
        parser = SchemaTransformParser()
        # Use absolute path
        result = parser.parse_file(schema_file.absolute())
        
        assert result["type_casting"] == {"customer_id": "BIGINT"}
    
    def test_file_not_found_error(self, tmp_path):
        """Test that non-existent file raises FileNotFoundError."""
        parser = SchemaTransformParser()
        non_existent = tmp_path / "schemas" / "missing.yaml"
        
        with pytest.raises(FileNotFoundError, match="Schema transform file not found"):
            parser.parse_file(non_existent)
    
    def test_file_not_found_error_shows_path(self, tmp_path):
        """Test that FileNotFoundError shows the searched path."""
        parser = SchemaTransformParser()
        non_existent = tmp_path / "missing.yaml"
        
        with pytest.raises(FileNotFoundError) as exc_info:
            parser.parse_file(non_existent)
        
        # Check that the error message contains the path
        assert str(non_existent) in str(exc_info.value)
    
    def test_load_legacy_format_from_file(self, tmp_path):
        """Test loading legacy format from file."""
        schema_file = tmp_path / "legacy_transform.yaml"
        schema_file.write_text("""
column_mapping:
  c_custkey: customer_id
  c_name: customer_name
type_casting:
  customer_id: BIGINT
""")
        
        parser = SchemaTransformParser()
        result = parser.parse_file(schema_file)
        
        # Enforcement is not returned by parser (action-level only)
        assert "enforcement" not in result
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {"customer_id": "BIGINT"}
    
    def test_load_arrow_format_from_file(self, tmp_path):
        """Test loading arrow format from file."""
        schema_file = tmp_path / "arrow_transform.yaml"
        schema_file.write_text("""
name: test_transform
columns:
  - "c_custkey -> customer_id: BIGINT"
  - "c_name -> customer_name"
  - "account_balance: DECIMAL(18,2)"
""")
        
        parser = SchemaTransformParser()
        result = parser.parse_file(schema_file)
        
        # Enforcement is not returned by parser (action-level only)
        assert "enforcement" not in result
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name"
        }
        assert result["type_casting"] == {
            "customer_id": "BIGINT",
            "account_balance": "DECIMAL(18,2)"
        }

