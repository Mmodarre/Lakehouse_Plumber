"""Tests for string template parameter conversion in Template Engine."""

import pytest
from pathlib import Path
import tempfile

from lhp.core.template_engine import TemplateEngine


class TestTemplateStringConversion:
    """Test string template parameter conversion logic."""

    def test_simple_string_conversion(self):
        """Test simple string template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Simple string
            result = engine._render_value("{{ name }}", {"name": "customer"})
            assert result == "customer"
            assert isinstance(result, str)

    def test_empty_string_conversion(self):
        """Test empty string template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Empty string
            result = engine._render_value("{{ empty }}", {"empty": ""})
            assert result == ""
            assert isinstance(result, str)

    def test_string_with_special_characters(self):
        """Test string with special characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # String with special characters
            special_string = "test@#$%^&*()_+-=[]{}|;:,.<>?"
            result = engine._render_value("{{ special }}", {"special": special_string})
            assert result == special_string
            assert isinstance(result, str)

    def test_string_with_spaces(self):
        """Test string with various whitespace."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # String with spaces
            spaced_string = "  hello   world  "
            result = engine._render_value("{{ spaced }}", {"spaced": spaced_string})
            assert result == spaced_string
            assert isinstance(result, str)

    def test_multiline_string(self):
        """Test multiline string template parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Multiline string
            multiline = "Line 1\nLine 2\nLine 3"
            result = engine._render_value("{{ multiline }}", {"multiline": multiline})
            assert result == multiline
            assert isinstance(result, str)

    def test_string_that_looks_like_other_types(self):
        """Test automatic conversion of strings that look like other data types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Strings that look like other types get automatically converted
            template_data = {
                "string_true": "{{ str_true }}",     # "true" converts to boolean
                "string_false": "{{ str_false }}",   # "false" converts to boolean
                "string_number": "{{ str_number }}", # "123" converts to integer
                "string_array": "{{ str_array }}",   # "[1,2,3]" converts to array
                "string_object": "{{ str_object }}"  # "{\"key\":\"value\"}" converts to object
            }
            
            params = {
                "str_true": "true",
                "str_false": "false",
                "str_number": "123",
                "str_array": "[1,2,3]",
                "str_object": '{"key":"value"}'  # Valid JSON format
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "string_true": True,        # Converted to boolean
                "string_false": False,      # Converted to boolean
                "string_number": 123,       # Converted to integer
                "string_array": [1, 2, 3],  # Converted to array
                "string_object": {"key": "value"}  # Converted to object
            }
            
            assert result == expected
            assert isinstance(result["string_true"], bool)    # Converted
            assert isinstance(result["string_false"], bool)   # Converted
            assert isinstance(result["string_number"], int)   # Converted
            assert isinstance(result["string_array"], list)   # Converted
            assert isinstance(result["string_object"], dict)  # Converted

    def test_string_in_complex_structure(self):
        """Test string template parameters within complex data structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # String within complex structure
            template_data = {
                "metadata": {
                    "table_name": "{{ table_name }}",
                    "description": "{{ description }}",
                    "owner": "{{ owner }}",
                    "environment": "{{ env }}"
                },
                "paths": {
                    "source_path": "{{ source_path }}",
                    "target_path": "{{ target_path }}"
                }
            }
            
            params = {
                "table_name": "customer_data",
                "description": "Customer information table",
                "owner": "data_team",
                "env": "production",
                "source_path": "/data/raw/customer",
                "target_path": "/data/processed/customer"
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "metadata": {
                    "table_name": "customer_data",
                    "description": "Customer information table",
                    "owner": "data_team",
                    "environment": "production"
                },
                "paths": {
                    "source_path": "/data/raw/customer",
                    "target_path": "/data/processed/customer"
                }
            }
            
            assert result == expected
            assert isinstance(result["metadata"]["table_name"], str)
            assert isinstance(result["metadata"]["description"], str)
            assert isinstance(result["metadata"]["owner"], str)
            assert isinstance(result["metadata"]["environment"], str)
            assert isinstance(result["paths"]["source_path"], str)
            assert isinstance(result["paths"]["target_path"], str)

    def test_string_concatenation_in_template(self):
        """Test string concatenation within template expressions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # String concatenation in template
            result = engine._render_value("{{ prefix }}_{{ name }}_{{ suffix }}", {
                "prefix": "bronze",
                "name": "customer",
                "suffix": "table"
            })
            assert result == "bronze_customer_table"
            assert isinstance(result, str)

    def test_string_with_mixed_content(self):
        """Test string template parameters mixed with other types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Mixed structure with various types
            template_data = {
                "config": {
                    "table_name": "{{ table_name }}",  # String
                    "enabled": "{{ is_enabled }}",    # Boolean
                    "batch_size": "{{ batch_size }}",   # Integer
                    "columns": "{{ column_list }}",   # Array
                    "options": "{{ config_options }}" # Object
                }
            }
            
            params = {
                "table_name": "customer_table",
                "is_enabled": True,
                "batch_size": 500,
                "column_list": ["col1", "col2"],
                "config_options": {"key": "value"}
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "config": {
                    "table_name": "customer_table",
                    "enabled": True,
                    "batch_size": 500,
                    "columns": ["col1", "col2"],
                    "options": {"key": "value"}
                }
            }
            
            assert result == expected
            assert isinstance(result["config"]["table_name"], str)
            assert isinstance(result["config"]["enabled"], bool)
            assert isinstance(result["config"]["batch_size"], int)
            assert isinstance(result["config"]["columns"], list)
            assert isinstance(result["config"]["options"], dict)

    def test_string_with_substitution_tokens(self):
        """Test string parameters alongside substitution tokens."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Mixed template expressions and substitution tokens
            template_data = {
                "source": {
                    "path": "{landing_volume}/{{ folder }}/data.csv",  # Mixed
                    "table_name": "{{ table_name }}",                 # String template
                    "database": "{catalog}.{schema}",                 # Substitution
                    "format": "csv"                                   # Static string
                }
            }
            
            params = {
                "folder": "customer_data",
                "table_name": "customer"
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "source": {
                    "path": "{landing_volume}/customer_data/data.csv",  # Mixed processing
                    "table_name": "customer",                          # Template processed
                    "database": "{catalog}.{schema}",                   # Preserved
                    "format": "csv"                                     # Static unchanged
                }
            }
            
            assert result == expected
            assert isinstance(result["source"]["path"], str)
            assert isinstance(result["source"]["table_name"], str)
            assert isinstance(result["source"]["database"], str)
            assert isinstance(result["source"]["format"], str)

    def test_unicode_string_conversion(self):
        """Test Unicode string template parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Unicode strings
            unicode_string = "Hello 世界 🌍 café naïve résumé"
            result = engine._render_value("{{ unicode }}", {"unicode": unicode_string})
            assert result == unicode_string
            assert isinstance(result, str)

    def test_escaped_characters_in_string(self):
        """Test strings with escaped characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # String with escaped characters
            escaped_string = "Line 1\\nLine 2\\tTabbed\\\"Quoted\\\""
            result = engine._render_value("{{ escaped }}", {"escaped": escaped_string})
            assert result == escaped_string
            assert isinstance(result, str)

    def test_very_long_string(self):
        """Test very long string template parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Very long string
            long_string = "x" * 10000  # 10K character string
            result = engine._render_value("{{ long_string }}", {"long_string": long_string})
            assert result == long_string
            assert isinstance(result, str)
            assert len(result) == 10000

    def test_string_error_cases(self):
        """Test error handling for string parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Note: For now, we expect the current behavior
            # This test documents what should happen but may need adjustment
            # when we implement proper type validation
            
            # Non-string passed to string template parameter
            # Current implementation may convert or pass through
            # This test will be updated based on final implementation
            pass  # Will implement after core logic is finalized 