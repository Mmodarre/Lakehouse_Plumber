"""Tests for smart template detection in Template Engine."""

import pytest
from pathlib import Path
import tempfile

from lhp.core.template_engine import TemplateEngine


class TestTemplateSmartDetection:
    """Test smart template detection logic."""

    def test_template_expression_processing(self):
        """Test that {{ }} expressions are processed correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Test simple template expression
            result = engine._render_value("{{ table_name }}", {"table_name": "customer"})
            assert result == "customer"
            
            # Test template expression with array
            result = engine._render_value("{{ columns }}", {"columns": ["col1", "col2"]})
            assert result == ["col1", "col2"]
            
            # Test template expression with object
            result = engine._render_value("{{ props }}", {"props": {"key": "value"}})
            assert result == {"key": "value"}
            
            # Test template expression with boolean
            result = engine._render_value("{{ enabled }}", {"enabled": True})
            assert result == True
            
            # Test template expression with integer
            result = engine._render_value("{{ count }}", {"count": 42})
            assert result == 42

    def test_substitution_token_passthrough(self):
        """Test that substitution tokens {var} are passed through unchanged."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Single substitution token
            result = engine._render_value("{catalog}", {})
            assert result == "{catalog}"
            
            # Multiple substitution tokens
            result = engine._render_value("{catalog}.{schema}", {})
            assert result == "{catalog}.{schema}"
            
            # Substitution in path
            result = engine._render_value("/path/{env}/data", {})
            assert result == "/path/{env}/data"
            
            # Substitution with complex path
            result = engine._render_value("{landing_volume}/customer/*.csv", {})
            assert result == "{landing_volume}/customer/*.csv"

    def test_static_value_passthrough(self):
        """Test that static values are passed through unchanged."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Simple string
            result = engine._render_value("csv", {})
            assert result == "csv"
            
            # String that looks like boolean but should remain string
            result = engine._render_value("true", {})
            assert result == "true"  # Should remain string, not become boolean
            
            # String that looks like integer but should remain string
            result = engine._render_value("123", {})
            assert result == "123"  # Should remain string, not become integer
            
            # Complex static string
            result = engine._render_value("some_static_value", {})
            assert result == "some_static_value"

    def test_mixed_template_and_substitution(self):
        """Test strings with both template expressions and substitution tokens."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Template + substitution in same string
            result = engine._render_value(
                "schemas/{{ table_name }}_schema.yaml", 
                {"table_name": "customer"}
            )
            assert result == "schemas/customer_schema.yaml"
            
            # Path with both template and substitution
            result = engine._render_value(
                "{volume}/{{ folder }}/*.csv",
                {"folder": "landing"}
            )
            assert result == "{volume}/landing/*.csv"
            
            # Multiple templates + substitution
            result = engine._render_value(
                "{env}/{{ prefix }}_{{ table_name }}_{{ suffix }}.csv",
                {"prefix": "load", "table_name": "customer", "suffix": "bronze"}
            )
            assert result == "{env}/load_customer_bronze.csv"

    def test_template_syntax_edge_cases(self):
        """Test edge cases with template syntax."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Template with spaces
            result = engine._render_value("{{  table_name  }}", {"table_name": "customer"})
            assert result == "customer"
            
            # Empty template parameter (should pass None through)
            result = engine._render_value("{{ empty_param }}", {"empty_param": None})
            assert result == None
            
            # Template in middle of string
            result = engine._render_value(
                "prefix_{{ name }}_suffix", 
                {"name": "middle"}
            )
            assert result == "prefix_middle_suffix"

    def test_no_template_parameters_provided(self):
        """Test behavior when no template parameters are provided."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Static strings should pass through even with empty parameters
            result = engine._render_value("static_string", {})
            assert result == "static_string"
            
            # Substitution tokens should pass through even with empty parameters
            result = engine._render_value("{catalog}.{schema}", {})
            assert result == "{catalog}.{schema}"

    def test_complex_nested_processing(self):
        """Test smart detection in nested data structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Complex nested structure with mixed template/substitution/static
            template_data = {
                "source": {
                    "path": "{volume}/{{ folder }}/*.csv",  # Mixed
                    "format": "csv",  # Static
                    "database": "{catalog}.{schema}"  # Substitution only
                },
                "target": "{{ table_name }}",  # Template only
                "metadata": [
                    "{{ field1 }}",  # Template
                    "{substitution_field}",  # Substitution  
                    "static_field"  # Static
                ]
            }
            
            params = {
                "folder": "landing",
                "table_name": "customer",
                "field1": "processed_field"
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "source": {
                    "path": "{volume}/landing/*.csv",  # Template processed, substitution preserved
                    "format": "csv",  # Static unchanged
                    "database": "{catalog}.{schema}"  # Substitution preserved
                },
                "target": "customer",  # Template processed
                "metadata": [
                    "processed_field",  # Template processed
                    "{substitution_field}",  # Substitution preserved
                    "static_field"  # Static unchanged
                ]
            }
            
            assert result == expected 