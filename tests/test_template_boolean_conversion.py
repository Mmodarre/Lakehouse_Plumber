"""Tests for boolean template parameter conversion in Template Engine."""

import pytest
from pathlib import Path
import tempfile

from lhp.core.template_engine import TemplateEngine


class TestTemplateBooleanConversion:
    """Test boolean template parameter conversion logic."""

    def test_true_boolean_conversion(self):
        """Test True boolean template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # True boolean
            result = engine._render_value("{{ enabled }}", {"enabled": True})
            assert result is True
            assert isinstance(result, bool)

    def test_false_boolean_conversion(self):
        """Test False boolean template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # False boolean
            result = engine._render_value("{{ disabled }}", {"disabled": False})
            assert result is False
            assert isinstance(result, bool)

    def test_boolean_in_complex_structure(self):
        """Test boolean template parameters within complex data structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Boolean within complex structure
            template_data = {
                "source": {
                    "format": "csv",
                    "header": "{{ has_header }}",
                    "inferSchema": "{{ infer_schema }}",
                    "multiline": "{{ multiline_enabled }}"
                },
                "processing": {
                    "streaming": "{{ is_streaming }}",
                    "checkpointing": "{{ enable_checkpoints }}"
                }
            }
            
            params = {
                "has_header": True,
                "infer_schema": False,
                "multiline_enabled": True,
                "is_streaming": False,
                "enable_checkpoints": True
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "source": {
                    "format": "csv",
                    "header": True,
                    "inferSchema": False,
                    "multiline": True
                },
                "processing": {
                    "streaming": False,
                    "checkpointing": True
                }
            }
            
            assert result == expected
            assert isinstance(result["source"]["header"], bool)
            assert isinstance(result["source"]["inferSchema"], bool)
            assert isinstance(result["source"]["multiline"], bool)
            assert isinstance(result["processing"]["streaming"], bool)
            assert isinstance(result["processing"]["checkpointing"], bool)

    def test_boolean_in_array(self):
        """Test boolean values within arrays."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Array containing boolean template expressions
            template_array = [
                "{{ flag1 }}",
                "static_string",
                "{{ flag2 }}",
                "{{ flag3 }}"
            ]
            
            params = {
                "flag1": True,
                "flag2": False,
                "flag3": True
            }
            
            result = engine._render_value(template_array, params)
            
            expected = [True, "static_string", False, True]
            
            assert result == expected
            assert isinstance(result[0], bool)
            assert isinstance(result[1], str)
            assert isinstance(result[2], bool)
            assert isinstance(result[3], bool)

    def test_boolean_with_mixed_content(self):
        """Test boolean template parameters mixed with other content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Mixed structure with various types
            template_data = {
                "config": {
                    "table_name": "{{ table_name }}",  # String
                    "enabled": "{{ is_enabled }}",    # Boolean
                    "columns": "{{ column_list }}",   # Array
                    "options": "{{ config_options }}" # Object
                }
            }
            
            params = {
                "table_name": "customer",
                "is_enabled": True,
                "column_list": ["col1", "col2"],
                "config_options": {"key": "value"}
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "config": {
                    "table_name": "customer",
                    "enabled": True,
                    "columns": ["col1", "col2"],
                    "options": {"key": "value"}
                }
            }
            
            assert result == expected
            assert isinstance(result["config"]["table_name"], str)
            assert isinstance(result["config"]["enabled"], bool)
            assert isinstance(result["config"]["columns"], list)
            assert isinstance(result["config"]["options"], dict)

    def test_multiple_boolean_parameters(self):
        """Test multiple boolean parameters in same template."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Multiple boolean parameters
            template_data = {
                "feature_flags": {
                    "enable_caching": "{{ cache_enabled }}",
                    "enable_compression": "{{ compression_enabled }}",
                    "enable_encryption": "{{ encryption_enabled }}",
                    "enable_monitoring": "{{ monitoring_enabled }}"
                }
            }
            
            params = {
                "cache_enabled": True,
                "compression_enabled": False,
                "encryption_enabled": True,
                "monitoring_enabled": False
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "feature_flags": {
                    "enable_caching": True,
                    "enable_compression": False,
                    "enable_encryption": True,
                    "enable_monitoring": False
                }
            }
            
            assert result == expected
            assert isinstance(result["feature_flags"]["enable_caching"], bool)
            assert isinstance(result["feature_flags"]["enable_compression"], bool)
            assert isinstance(result["feature_flags"]["enable_encryption"], bool)
            assert isinstance(result["feature_flags"]["enable_monitoring"], bool)

    def test_boolean_with_substitution_tokens(self):
        """Test boolean parameters alongside substitution tokens."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Mixed template expressions and substitution tokens
            template_data = {
                "source": {
                    "path": "{landing_volume}/data/*.csv",  # Substitution
                    "header": "{{ has_header }}",           # Boolean template
                    "database": "{catalog}.{schema}",       # Substitution
                    "streaming": "{{ is_streaming }}"       # Boolean template
                }
            }
            
            params = {
                "has_header": True,
                "is_streaming": False
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "source": {
                    "path": "{landing_volume}/data/*.csv",  # Preserved
                    "header": True,                          # Template processed
                    "database": "{catalog}.{schema}",        # Preserved
                    "streaming": False                       # Template processed
                }
            }
            
            assert result == expected
            assert isinstance(result["source"]["header"], bool)
            assert isinstance(result["source"]["streaming"], bool)
            assert isinstance(result["source"]["path"], str)  # Should remain string
            assert isinstance(result["source"]["database"], str)  # Should remain string

    def test_boolean_edge_cases(self):
        """Test edge cases for boolean template parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Boolean parameter conversion behavior
            template_data = {
                "bool_true": "{{ bool_true }}",
                "bool_false": "{{ bool_false }}",
                "string_true": "{{ string_true }}",  # String "true" gets converted to boolean
                "string_false": "{{ string_false }}"  # String "false" gets converted to boolean
            }
            
            params = {
                "bool_true": True,     # Actual boolean
                "bool_false": False,   # Actual boolean  
                "string_true": "true", # String value that looks like boolean
                "string_false": "false" # String value that looks like boolean
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "bool_true": True,     # Boolean stays boolean
                "bool_false": False,   # Boolean stays boolean
                "string_true": True,   # String "true" converts to boolean True
                "string_false": False  # String "false" converts to boolean False
            }
            
            assert result == expected
            assert isinstance(result["bool_true"], bool)
            assert isinstance(result["bool_false"], bool)
            assert isinstance(result["string_true"], bool)  # Converted from string
            assert isinstance(result["string_false"], bool)  # Converted from string

    def test_boolean_error_cases(self):
        """Test error handling for invalid boolean parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Note: For now, we expect the current behavior
            # This test documents what should happen but may need adjustment
            # when we implement proper type validation
            
            # Non-boolean passed to boolean template parameter
            # Current implementation may convert or pass through
            # This test will be updated based on final implementation
            pass  # Will implement after core logic is finalized 