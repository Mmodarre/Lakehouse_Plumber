"""Tests for integer template parameter conversion in Template Engine."""

import pytest
from pathlib import Path
import tempfile

from lhp.core.template_engine import TemplateEngine


class TestTemplateIntegerConversion:
    """Test integer template parameter conversion logic."""

    def test_positive_integer_conversion(self):
        """Test positive integer template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Positive integer
            result = engine._render_value("{{ count }}", {"count": 42})
            assert result == 42
            assert isinstance(result, int)

    def test_zero_integer_conversion(self):
        """Test zero integer template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Zero
            result = engine._render_value("{{ zero }}", {"zero": 0})
            assert result == 0
            assert isinstance(result, int)

    def test_negative_integer_conversion(self):
        """Test negative integer template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Negative integer
            result = engine._render_value("{{ negative }}", {"negative": -10})
            assert result == -10
            assert isinstance(result, int)

    def test_large_integer_conversion(self):
        """Test large integer template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Large integer
            large_int = 9223372036854775807  # Max 64-bit signed integer
            result = engine._render_value("{{ large_number }}", {"large_number": large_int})
            assert result == large_int
            assert isinstance(result, int)

    def test_integer_in_complex_structure(self):
        """Test integer template parameters within complex data structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Integer within complex structure
            template_data = {
                "config": {
                    "batch_size": "{{ batch_size }}",
                    "max_retries": "{{ max_retries }}",
                    "timeout_seconds": "{{ timeout }}",
                    "parallelism": "{{ parallel_jobs }}"
                },
                "limits": {
                    "memory_mb": "{{ memory_limit }}",
                    "cpu_cores": "{{ cpu_limit }}"
                }
            }
            
            params = {
                "batch_size": 1000,
                "max_retries": 3,
                "timeout": 300,
                "parallel_jobs": 4,
                "memory_limit": 8192,
                "cpu_limit": 2
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "config": {
                    "batch_size": 1000,
                    "max_retries": 3,
                    "timeout_seconds": 300,
                    "parallelism": 4
                },
                "limits": {
                    "memory_mb": 8192,
                    "cpu_cores": 2
                }
            }
            
            assert result == expected
            assert isinstance(result["config"]["batch_size"], int)
            assert isinstance(result["config"]["max_retries"], int)
            assert isinstance(result["config"]["timeout_seconds"], int)
            assert isinstance(result["config"]["parallelism"], int)
            assert isinstance(result["limits"]["memory_mb"], int)
            assert isinstance(result["limits"]["cpu_cores"], int)

    def test_integer_in_array(self):
        """Test integer values within arrays."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Array containing integer template expressions
            template_array = [
                "{{ count1 }}",
                "static_string",
                "{{ count2 }}",
                "{{ count3 }}"
            ]
            
            params = {
                "count1": 10,
                "count2": 0,
                "count3": -5
            }
            
            result = engine._render_value(template_array, params)
            
            expected = [10, "static_string", 0, -5]
            
            assert result == expected
            assert isinstance(result[0], int)
            assert isinstance(result[1], str)
            assert isinstance(result[2], int)
            assert isinstance(result[3], int)

    def test_integer_with_mixed_content(self):
        """Test integer template parameters mixed with other types."""
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
                "table_name": "customer",
                "is_enabled": True,
                "batch_size": 500,
                "column_list": ["col1", "col2"],
                "config_options": {"key": "value"}
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "config": {
                    "table_name": "customer",
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

    def test_multiple_integer_parameters(self):
        """Test multiple integer parameters in same template."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Multiple integer parameters
            template_data = {
                "performance": {
                    "batch_size": "{{ batch_size }}",
                    "max_files_per_trigger": "{{ max_files }}",
                    "checkpoint_interval": "{{ checkpoint_interval }}",
                    "retention_days": "{{ retention_days }}"
                }
            }
            
            params = {
                "batch_size": 1000,
                "max_files": 50,
                "checkpoint_interval": 10,
                "retention_days": 365
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "performance": {
                    "batch_size": 1000,
                    "max_files_per_trigger": 50,
                    "checkpoint_interval": 10,
                    "retention_days": 365
                }
            }
            
            assert result == expected
            assert isinstance(result["performance"]["batch_size"], int)
            assert isinstance(result["performance"]["max_files_per_trigger"], int)
            assert isinstance(result["performance"]["checkpoint_interval"], int)
            assert isinstance(result["performance"]["retention_days"], int)

    def test_integer_with_substitution_tokens(self):
        """Test integer parameters alongside substitution tokens."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Mixed template expressions and substitution tokens
            template_data = {
                "source": {
                    "path": "{landing_volume}/data/*.csv",  # Substitution
                    "batch_size": "{{ batch_size }}",       # Integer template
                    "database": "{catalog}.{schema}",       # Substitution
                    "max_files": "{{ max_files }}"          # Integer template
                }
            }
            
            params = {
                "batch_size": 1000,
                "max_files": 10
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "source": {
                    "path": "{landing_volume}/data/*.csv",  # Preserved
                    "batch_size": 1000,                     # Template processed
                    "database": "{catalog}.{schema}",        # Preserved
                    "max_files": 10                          # Template processed
                }
            }
            
            assert result == expected
            assert isinstance(result["source"]["batch_size"], int)
            assert isinstance(result["source"]["max_files"], int)
            assert isinstance(result["source"]["path"], str)  # Should remain string
            assert isinstance(result["source"]["database"], str)  # Should remain string

    def test_integer_edge_cases(self):
        """Test edge cases for integer template parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Integer parameter conversion behavior
            template_data = {
                "actual_int": "{{ actual_int }}",
                "string_number": "{{ string_number }}",  # String "123" gets converted to integer
                "zero_int": "{{ zero_int }}",
                "negative_int": "{{ negative_int }}"
            }
            
            params = {
                "actual_int": 42,      # Actual integer
                "string_number": "123", # String value that looks like number
                "zero_int": 0,         # Zero integer
                "negative_int": -100   # Negative integer
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "actual_int": 42,      # Integer stays integer
                "string_number": 123,  # String "123" converts to integer 123
                "zero_int": 0,         # Integer stays integer
                "negative_int": -100   # Integer stays integer
            }
            
            assert result == expected
            assert isinstance(result["actual_int"], int)
            assert isinstance(result["string_number"], int)  # Converted from string
            assert isinstance(result["zero_int"], int)
            assert isinstance(result["negative_int"], int)

    def test_integer_precision_edge_cases(self):
        """Test integer precision and boundary values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Various integer boundary cases
            template_data = {
                "small_positive": "{{ small_pos }}",
                "small_negative": "{{ small_neg }}",
                "large_positive": "{{ large_pos }}",
                "large_negative": "{{ large_neg }}"
            }
            
            params = {
                "small_pos": 1,
                "small_neg": -1,
                "large_pos": 2147483647,  # 32-bit max
                "large_neg": -2147483648  # 32-bit min
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "small_positive": 1,
                "small_negative": -1,
                "large_positive": 2147483647,
                "large_negative": -2147483648
            }
            
            assert result == expected
            assert isinstance(result["small_positive"], int)
            assert isinstance(result["small_negative"], int)
            assert isinstance(result["large_positive"], int)
            assert isinstance(result["large_negative"], int)

    def test_integer_error_cases(self):
        """Test error handling for invalid integer parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Note: For now, we expect the current behavior
            # This test documents what should happen but may need adjustment
            # when we implement proper type validation
            
            # Non-integer passed to integer template parameter
            # Current implementation may convert or pass through
            # This test will be updated based on final implementation
            pass  # Will implement after core logic is finalized 