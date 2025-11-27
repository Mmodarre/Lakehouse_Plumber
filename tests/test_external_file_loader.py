"""Tests for external file loader utility."""

import pytest
from pathlib import Path
import tempfile
import os

from lhp.utils.external_file_loader import (
    resolve_external_file_path,
    load_external_file_text,
    is_file_path,
)
from lhp.utils.error_formatter import LHPError


class TestIsFilePath:
    """Test file path detection heuristic."""
    
    def test_detects_yaml_extension(self):
        """Should detect .yaml files."""
        assert is_file_path("schemas/customer.yaml") is True
        assert is_file_path("customer.yaml") is True
        assert is_file_path("CUSTOMER.YAML") is True  # Case insensitive
    
    def test_detects_yml_extension(self):
        """Should detect .yml files."""
        assert is_file_path("schemas/customer.yml") is True
        assert is_file_path("customer.yml") is True
    
    def test_detects_json_extension(self):
        """Should detect .json files."""
        assert is_file_path("expectations/quality.json") is True
        assert is_file_path("quality.json") is True
    
    def test_detects_ddl_extension(self):
        """Should detect .ddl files."""
        assert is_file_path("schemas/customer.ddl") is True
        assert is_file_path("customer.ddl") is True
    
    def test_detects_sql_extension(self):
        """Should detect .sql files."""
        assert is_file_path("sql/query.sql") is True
        assert is_file_path("query.sql") is True
    
    def test_detects_forward_slash(self):
        """Should detect forward slashes as path separators."""
        assert is_file_path("sql/my_query") is True
        assert is_file_path("schemas/silver/customer") is True
    
    def test_detects_backslash(self):
        """Should detect backslashes as path separators."""
        assert is_file_path("sql\\my_query") is True
        assert is_file_path("schemas\\customer") is True
    
    def test_rejects_inline_ddl(self):
        """Should not detect inline DDL as file path."""
        assert is_file_path("customer_id BIGINT, name STRING") is False
        assert is_file_path("customer_id BIGINT") is False
    
    def test_rejects_simple_strings(self):
        """Should not detect simple strings as file paths."""
        assert is_file_path("customer") is False
        assert is_file_path("my_table") is False
        assert is_file_path("") is False


class TestResolveExternalFilePath:
    """Test path resolution logic."""
    
    def test_resolves_relative_path(self, tmp_path):
        """Should resolve relative paths from base directory."""
        # Create file
        schema_file = tmp_path / "schemas" / "customer.yaml"
        schema_file.parent.mkdir(parents=True)
        schema_file.write_text("name: customer")
        
        # Resolve
        resolved = resolve_external_file_path(
            "schemas/customer.yaml",
            tmp_path,
            file_type="schema file"
        )
        
        assert resolved == schema_file
        assert resolved.exists()
    
    def test_resolves_absolute_path(self, tmp_path):
        """Should handle absolute paths."""
        # Create file
        schema_file = tmp_path / "customer.yaml"
        schema_file.write_text("name: customer")
        
        # Resolve with absolute path
        resolved = resolve_external_file_path(
            str(schema_file),
            tmp_path,
            file_type="schema file"
        )
        
        assert resolved == schema_file
        assert resolved.exists()
    
    def test_resolves_subdirectories(self, tmp_path):
        """Should handle multiple levels of subdirectories."""
        # Create file in nested structure
        schema_file = tmp_path / "schemas" / "silver" / "dimensions" / "customer.yaml"
        schema_file.parent.mkdir(parents=True)
        schema_file.write_text("name: customer")
        
        # Resolve
        resolved = resolve_external_file_path(
            "schemas/silver/dimensions/customer.yaml",
            tmp_path,
            file_type="schema file"
        )
        
        assert resolved == schema_file
        assert resolved.exists()
    
    def test_file_not_found_error(self, tmp_path):
        """Should raise LHPError with search locations when file not found."""
        with pytest.raises(LHPError) as exc_info:
            resolve_external_file_path(
                "schemas/missing.yaml",
                tmp_path,
                file_type="schema file"
            )
        
        error = exc_info.value
        error_str = str(error)
        assert "LHP-IO-001" in error_str
        assert "schema file" in error_str.lower()
        assert "schemas/missing.yaml" in error_str
        # Check path is in error (may be word-wrapped, so check components)
        assert "Relative to project root" in error_str
        assert "schemas/missing.yaml" in error_str
    
    def test_handles_path_object(self, tmp_path):
        """Should accept Path objects as input."""
        # Create file
        schema_file = tmp_path / "customer.yaml"
        schema_file.write_text("name: customer")
        
        # Resolve with Path object
        resolved = resolve_external_file_path(
            Path("customer.yaml"),
            tmp_path,
            file_type="schema file"
        )
        
        assert resolved == schema_file


class TestLoadExternalFileText:
    """Test text file loading."""
    
    def test_loads_sql_file(self, tmp_path):
        """Should load SQL file as text."""
        sql_file = tmp_path / "query.sql"
        sql_content = "SELECT * FROM customers WHERE active = true"
        sql_file.write_text(sql_content)
        
        loaded = load_external_file_text(
            "query.sql",
            tmp_path,
            file_type="SQL file"
        )
        
        assert loaded == sql_content
    
    def test_loads_ddl_file(self, tmp_path):
        """Should load DDL file as text."""
        ddl_file = tmp_path / "schema.ddl"
        ddl_content = "customer_id BIGINT NOT NULL,\nname STRING,\nemail STRING"
        ddl_file.write_text(ddl_content)
        
        loaded = load_external_file_text(
            "schema.ddl",
            tmp_path,
            file_type="DDL file"
        )
        
        assert loaded == ddl_content
    
    def test_handles_encoding(self, tmp_path):
        """Should handle different encodings."""
        text_file = tmp_path / "data.txt"
        content = "UTF-8 content with special chars: €£¥"
        text_file.write_text(content, encoding="utf-8")
        
        loaded = load_external_file_text(
            "data.txt",
            tmp_path,
            file_type="text file",
            encoding="utf-8"
        )
        
        assert loaded == content
    
    def test_loads_from_subdirectory(self, tmp_path):
        """Should load files from subdirectories."""
        sql_dir = tmp_path / "sql" / "transforms"
        sql_dir.mkdir(parents=True)
        sql_file = sql_dir / "customer_transform.sql"
        sql_content = "SELECT id, name FROM raw_customers"
        sql_file.write_text(sql_content)
        
        loaded = load_external_file_text(
            "sql/transforms/customer_transform.sql",
            tmp_path,
            file_type="SQL file"
        )
        
        assert loaded == sql_content
    
    def test_file_not_found_error(self, tmp_path):
        """Should raise LHPError when file doesn't exist."""
        with pytest.raises(LHPError) as exc_info:
            load_external_file_text(
                "missing.sql",
                tmp_path,
                file_type="SQL file"
            )
        
        error = exc_info.value
        assert "LHP-IO-001" in str(error)
        assert "missing.sql" in str(error)
    
    def test_preserves_whitespace(self, tmp_path):
        """Should preserve whitespace and line breaks."""
        sql_file = tmp_path / "query.sql"
        sql_content = """
SELECT 
    id,
    name,
    email
FROM 
    customers
WHERE 
    active = true
"""
        sql_file.write_text(sql_content)
        
        loaded = load_external_file_text(
            "query.sql",
            tmp_path,
            file_type="SQL file"
        )
        
        assert loaded == sql_content


class TestIntegration:
    """Integration tests with real file scenarios."""
    
    def test_schema_file_in_nested_directory(self, tmp_path):
        """Test real-world schema file scenario."""
        # Simulate: schemas/silver/dimensions/brand_schema.yaml
        schema_path = tmp_path / "schemas" / "silver" / "dimensions" / "brand_schema.yaml"
        schema_path.parent.mkdir(parents=True)
        schema_content = """name: brand_dimension_schema
columns:
  - name: brand_id
    type: BIGINT
  - name: name
    type: STRING
"""
        schema_path.write_text(schema_content)
        
        # Test is_file_path detection
        file_ref = "schemas/silver/dimensions/brand_schema.yaml"
        assert is_file_path(file_ref) is True
        
        # Test resolution
        resolved = resolve_external_file_path(
            file_ref,
            tmp_path,
            file_type="schema file"
        )
        assert resolved.exists()
        
        # Test loading
        loaded = load_external_file_text(
            file_ref,
            tmp_path,
            file_type="schema file"
        )
        assert "brand_dimension_schema" in loaded
    
    def test_sql_file_loading(self, tmp_path):
        """Test SQL file loading scenario."""
        sql_path = tmp_path / "sql" / "customer_enrichment.sql"
        sql_path.parent.mkdir()
        sql_path.write_text("SELECT * FROM customers")
        
        file_ref = "sql/customer_enrichment.sql"
        assert is_file_path(file_ref) is True
        
        loaded = load_external_file_text(
            file_ref,
            tmp_path,
            file_type="SQL file"
        )
        assert "SELECT * FROM customers" in loaded
    
    def test_ddl_file_for_table_schema(self, tmp_path):
        """Test DDL file loading for table schema."""
        ddl_path = tmp_path / "schemas" / "customer_table.ddl"
        ddl_path.parent.mkdir()
        ddl_content = """customer_id BIGINT NOT NULL,
name STRING,
email STRING,
created_at TIMESTAMP"""
        ddl_path.write_text(ddl_content)
        
        file_ref = "schemas/customer_table.ddl"
        assert is_file_path(file_ref) is True
        
        loaded = load_external_file_text(
            file_ref,
            tmp_path,
            file_type="table schema file"
        )
        assert "customer_id BIGINT NOT NULL" in loaded

