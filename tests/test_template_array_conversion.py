"""Tests for array template parameter conversion in Template Engine."""

import pytest
from pathlib import Path
import tempfile

from lhp.core.template_engine import TemplateEngine


class TestTemplateArrayConversion:
    """Test array template parameter conversion logic."""

    def test_simple_array_conversion(self):
        """Test simple array template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Simple string array
            result = engine._render_value("{{ columns }}", {"columns": ["col1", "col2", "col3"]})
            assert result == ["col1", "col2", "col3"]
            assert isinstance(result, list)
            
            # Single item array
            result = engine._render_value("{{ single_column }}", {"single_column": ["only_col"]})
            assert result == ["only_col"]
            assert isinstance(result, list)

    def test_empty_array_conversion(self):
        """Test empty array template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Empty array should remain empty array
            result = engine._render_value("{{ empty_columns }}", {"empty_columns": []})
            assert result == []
            assert isinstance(result, list)

    def test_mixed_type_array_conversion(self):
        """Test array with mixed data types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Array with mixed types (string, int, boolean)
            mixed_array = ["string_value", 42, True, None]
            result = engine._render_value("{{ mixed_array }}", {"mixed_array": mixed_array})
            assert result == ["string_value", 42, True, None]
            assert isinstance(result, list)
            assert isinstance(result[0], str)
            assert isinstance(result[1], int)
            assert isinstance(result[2], bool)
            assert result[3] is None

    def test_nested_array_conversion(self):
        """Test array containing other arrays (nested arrays)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Nested arrays
            nested_array = [["a", "b"], ["c", "d"], ["e"]]
            result = engine._render_value("{{ nested }}", {"nested": nested_array})
            assert result == [["a", "b"], ["c", "d"], ["e"]]
            assert isinstance(result, list)
            assert isinstance(result[0], list)
            assert isinstance(result[1], list)

    def test_array_in_complex_structure(self):
        """Test array template parameters within complex data structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Array within object structure
            template_data = {
                "table_config": {
                    "name": "{{ table_name }}",
                    "cluster_columns": "{{ cluster_cols }}",
                    "partition_columns": "{{ partition_cols }}"
                },
                "metadata": "{{ meta_fields }}"
            }
            
            params = {
                "table_name": "customer",
                "cluster_cols": ["c_customer_id", "c_region"],
                "partition_cols": ["year", "month"],
                "meta_fields": ["created_at", "updated_at", "source_file"]
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "table_config": {
                    "name": "customer",
                    "cluster_columns": ["c_customer_id", "c_region"],
                    "partition_columns": ["year", "month"]
                },
                "metadata": ["created_at", "updated_at", "source_file"]
            }
            
            assert result == expected
            assert isinstance(result["table_config"]["cluster_columns"], list)
            assert isinstance(result["table_config"]["partition_columns"], list)
            assert isinstance(result["metadata"], list)

    def test_array_with_template_expressions_inside(self):
        """Test arrays that contain template expressions within array items."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Array with template expressions in items
            template_array = [
                "{{ prefix }}_column1",
                "{{ prefix }}_column2", 
                "static_column"
            ]
            
            params = {"prefix": "fact"}
            
            result = engine._render_value(template_array, params)
            
            expected = [
                "fact_column1",
                "fact_column2",
                "static_column"
            ]
            
            assert result == expected
            assert isinstance(result, list)

    def test_array_parameter_with_substitution_tokens(self):
        """Test array containing substitution tokens (should be preserved)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Array with substitution tokens
            template_array = [
                "{catalog}.{schema}.table1",
                "{catalog}.{schema}.table2",
                "{{ dynamic_table }}"
            ]
            
            params = {"dynamic_table": "generated_table"}
            
            result = engine._render_value(template_array, params)
            
            expected = [
                "{catalog}.{schema}.table1",  # Substitution preserved
                "{catalog}.{schema}.table2",  # Substitution preserved  
                "generated_table"             # Template processed
            ]
            
            assert result == expected
            assert isinstance(result, list)

    def test_multiple_array_parameters(self):
        """Test multiple array parameters in same template structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Multiple arrays in same structure
            template_data = {
                "source_tables": "{{ source_list }}",
                "target_columns": "{{ target_cols }}",
                "transformation_steps": "{{ transform_steps }}"
            }
            
            params = {
                "source_list": ["customers", "orders", "products"],
                "target_cols": ["id", "name", "total"],
                "transform_steps": ["clean", "validate", "enrich"]
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "source_tables": ["customers", "orders", "products"],
                "target_columns": ["id", "name", "total"],
                "transformation_steps": ["clean", "validate", "enrich"]
            }
            
            assert result == expected
            assert isinstance(result["source_tables"], list)
            assert isinstance(result["target_columns"], list)
            assert isinstance(result["transformation_steps"], list)

    def test_array_error_cases(self):
        """Test error handling for invalid array parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Note: For now, we expect the current behavior
            # This test documents what should happen but may need adjustment
            # when we implement proper type validation
            
            # Non-array passed to array template parameter
            # Current implementation may convert or pass through
            # This test will be updated based on final implementation
            pass  # Will implement after core logic is finalized 