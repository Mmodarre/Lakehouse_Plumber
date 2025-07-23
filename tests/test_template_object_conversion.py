"""Tests for object template parameter conversion in Template Engine."""

import pytest
from pathlib import Path
import tempfile

from lhp.core.template_engine import TemplateEngine


class TestTemplateObjectConversion:
    """Test object template parameter conversion logic."""

    def test_simple_object_conversion(self):
        """Test simple object template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Simple object
            simple_obj = {"key1": "value1", "key2": "value2"}
            result = engine._render_value("{{ props }}", {"props": simple_obj})
            assert result == {"key1": "value1", "key2": "value2"}
            assert isinstance(result, dict)

    def test_empty_object_conversion(self):
        """Test empty object template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Empty object should remain empty object
            result = engine._render_value("{{ empty_props }}", {"empty_props": {}})
            assert result == {}
            assert isinstance(result, dict)

    def test_nested_object_conversion(self):
        """Test nested object template parameter conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Nested object structure
            nested_obj = {
                "level1": {
                    "level2": {
                        "key": "deep_value",
                        "number": 42
                    },
                    "sibling": "value"
                },
                "top_level": "surface_value"
            }
            
            result = engine._render_value("{{ nested }}", {"nested": nested_obj})
            assert result == nested_obj
            assert isinstance(result, dict)
            assert isinstance(result["level1"], dict)
            assert isinstance(result["level1"]["level2"], dict)

    def test_object_with_mixed_value_types(self):
        """Test object containing various data types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Object with mixed value types
            mixed_obj = {
                "string_val": "hello",
                "integer_val": 123,
                "boolean_val": True,
                "null_val": None,
                "array_val": ["item1", "item2"],
                "nested_obj": {"inner_key": "inner_value"}
            }
            
            result = engine._render_value("{{ mixed }}", {"mixed": mixed_obj})
            assert result == mixed_obj
            assert isinstance(result, dict)
            assert isinstance(result["string_val"], str)
            assert isinstance(result["integer_val"], int)
            assert isinstance(result["boolean_val"], bool)
            assert result["null_val"] is None
            assert isinstance(result["array_val"], list)
            assert isinstance(result["nested_obj"], dict)

    def test_table_properties_object(self):
        """Test table_properties object (the original use case that started this work)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Table properties object
            table_props = {
                "delta.enableChangeDataFeed": "true",
                "delta.columnMapping.mode": "name",
                "PII": "true",
                "data_classification": "sensitive",
                "retention_days": 2555
            }
            
            result = engine._render_value("{{ table_properties }}", {"table_properties": table_props})
            assert result == table_props
            assert isinstance(result, dict)
            assert result["delta.enableChangeDataFeed"] == "true"
            assert result["PII"] == "true"
            assert result["retention_days"] == 2555

    def test_object_in_complex_structure(self):
        """Test object template parameters within complex data structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Object within complex structure
            template_data = {
                "write_target": {
                    "type": "streaming_table",
                    "database": "{{ database }}",
                    "table": "{{ table_name }}",
                    "table_properties": "{{ table_props }}",
                    "spark_conf": "{{ spark_config }}"
                }
            }
            
            params = {
                "database": "catalog.schema",
                "table_name": "customer",
                "table_props": {
                    "PII": "true",
                    "data_classification": "sensitive"
                },
                "spark_config": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                }
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "write_target": {
                    "type": "streaming_table",
                    "database": "catalog.schema",
                    "table": "customer",
                    "table_properties": {
                        "PII": "true",
                        "data_classification": "sensitive"
                    },
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true"
                    }
                }
            }
            
            assert result == expected
            assert isinstance(result["write_target"]["table_properties"], dict)
            assert isinstance(result["write_target"]["spark_conf"], dict)

    def test_object_with_template_expressions_in_values(self):
        """Test objects that contain template expressions within values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Object with template expressions in values
            template_obj = {
                "table_name": "{{ prefix }}_{{ base_name }}",
                "description": "Table for {{ entity_type }} data",
                "location": "/data/{{ env }}/{{ base_name }}",
                "static_value": "always_this"
            }
            
            params = {
                "prefix": "fact",
                "base_name": "customer",
                "entity_type": "customer",
                "env": "prod"
            }
            
            result = engine._render_value(template_obj, params)
            
            expected = {
                "table_name": "fact_customer",
                "description": "Table for customer data",
                "location": "/data/prod/customer",
                "static_value": "always_this"
            }
            
            assert result == expected
            assert isinstance(result, dict)

    def test_object_with_substitution_tokens_in_values(self):
        """Test objects containing substitution tokens (should be preserved)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Object with substitution tokens in values
            template_obj = {
                "database": "{catalog}.{schema}",
                "path": "{landing_volume}/{{ table_name }}/*.csv",
                "format": "csv",
                "generated_name": "{{ prefix }}_table"
            }
            
            params = {
                "table_name": "customer",
                "prefix": "bronze"
            }
            
            result = engine._render_value(template_obj, params)
            
            expected = {
                "database": "{catalog}.{schema}",  # Substitution preserved
                "path": "{landing_volume}/customer/*.csv",  # Mixed: substitution preserved, template processed
                "format": "csv",  # Static unchanged
                "generated_name": "bronze_table"  # Template processed
            }
            
            assert result == expected
            assert isinstance(result, dict)

    def test_multiple_object_parameters(self):
        """Test multiple object parameters in same template structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Multiple objects in same structure
            template_data = {
                "source_config": "{{ source_options }}",
                "target_config": "{{ target_options }}",
                "processing_config": "{{ processing_options }}"
            }
            
            params = {
                "source_options": {
                    "format": "csv",
                    "header": True,
                    "delimiter": ","
                },
                "target_options": {
                    "mode": "append",
                    "partitionBy": ["year", "month"]
                },
                "processing_options": {
                    "batchSize": 1000,
                    "checkpointLocation": "/tmp/checkpoint"
                }
            }
            
            result = engine._render_value(template_data, params)
            
            expected = {
                "source_config": {
                    "format": "csv",
                    "header": True,
                    "delimiter": ","
                },
                "target_config": {
                    "mode": "append",
                    "partitionBy": ["year", "month"]
                },
                "processing_config": {
                    "batchSize": 1000,
                    "checkpointLocation": "/tmp/checkpoint"
                }
            }
            
            assert result == expected
            assert isinstance(result["source_config"], dict)
            assert isinstance(result["target_config"], dict)
            assert isinstance(result["processing_config"], dict)

    def test_object_with_special_keys(self):
        """Test objects with special key formats (dots, underscores, etc.)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Object with special key formats
            special_obj = {
                "cloudFiles.format": "csv",
                "cloudFiles.maxFilesPerTrigger": "10",
                "spark.sql.adaptive.enabled": "true",
                "my_custom_property": "value",
                "CamelCaseProperty": "another_value"
            }
            
            result = engine._render_value("{{ special_props }}", {"special_props": special_obj})
            assert result == special_obj
            assert isinstance(result, dict)
            assert result["cloudFiles.format"] == "csv"
            assert result["spark.sql.adaptive.enabled"] == "true"

    def test_object_error_cases(self):
        """Test error handling for invalid object parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            # Note: For now, we expect the current behavior
            # This test documents what should happen but may need adjustment
            # when we implement proper type validation
            
            # Non-object passed to object template parameter
            # Current implementation may convert or pass through
            # This test will be updated based on final implementation
            pass  # Will implement after core logic is finalized 