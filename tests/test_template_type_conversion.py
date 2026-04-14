"""Consolidated tests for template parameter type conversion in Template Engine.

Tests that _render_value correctly handles all Python types (string, boolean,
integer, array, object) when resolving {{ template_param }} expressions.

Consolidated from five per-type files into one parametrized suite.
"""

import pytest
from pathlib import Path
import tempfile

from lhp.core.template_engine import TemplateEngine


@pytest.fixture
def engine():
    """Create a TemplateEngine with a temporary templates directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield TemplateEngine(Path(tmpdir))


# ---------------------------------------------------------------------------
# Cross-type parametrized tests
# ---------------------------------------------------------------------------


class TestSimpleConversion:
    """Verify that each primitive type round-trips through _render_value."""

    @pytest.mark.parametrize(
        "value, expected_type",
        [
            ("customer", str),
            (True, bool),
            (False, bool),
            (42, int),
            (0, int),
            (-10, int),
            (["col1", "col2"], list),
            ([], list),
            ({"key": "value"}, dict),
            ({}, dict),
        ],
        ids=[
            "string",
            "bool-true",
            "bool-false",
            "int-positive",
            "int-zero",
            "int-negative",
            "array",
            "array-empty",
            "object",
            "object-empty",
        ],
    )
    def test_simple_type_conversion(self, engine, value, expected_type):
        """Each type should pass through _render_value and preserve its Python type."""
        result = engine._render_value("{{ param }}", {"param": value})
        assert result == value
        assert isinstance(result, expected_type)


class TestMixedContent:
    """Test template parameters of different types coexisting in one structure.

    Previously duplicated identically across string, boolean, and integer files.
    """

    def test_mixed_type_structure(self, engine):
        """All five types in a single nested dict resolve correctly."""
        template_data = {
            "config": {
                "table_name": "{{ table_name }}",
                "enabled": "{{ is_enabled }}",
                "batch_size": "{{ batch_size }}",
                "columns": "{{ column_list }}",
                "options": "{{ config_options }}",
            }
        }

        params = {
            "table_name": "customer_table",
            "is_enabled": True,
            "batch_size": 500,
            "column_list": ["col1", "col2"],
            "config_options": {"key": "value"},
        }

        result = engine._render_value(template_data, params)

        expected = {
            "config": {
                "table_name": "customer_table",
                "enabled": True,
                "batch_size": 500,
                "columns": ["col1", "col2"],
                "options": {"key": "value"},
            }
        }

        assert result == expected
        assert isinstance(result["config"]["table_name"], str)
        assert isinstance(result["config"]["enabled"], bool)
        assert isinstance(result["config"]["batch_size"], int)
        assert isinstance(result["config"]["columns"], list)
        assert isinstance(result["config"]["options"], dict)


class TestSubstitutionTokenPreservation:
    """Substitution tokens ({env_token}) must pass through _render_value untouched
    while {{ template_param }} expressions are resolved.

    Previously duplicated across all five type files with minor variations.
    """

    def test_substitution_tokens_preserved_alongside_all_types(self, engine):
        """Mixed substitution tokens and template params of all types."""
        template_data = {
            "source": {
                "path": "{landing_volume}/{{ folder }}/data.csv",
                "database": "{catalog}.{schema}",
                "header": "{{ has_header }}",
                "batch_size": "{{ batch_size }}",
                "columns": "{{ columns }}",
                "options": "{{ opts }}",
                "format": "csv",
            }
        }

        params = {
            "folder": "customer_data",
            "has_header": True,
            "batch_size": 1000,
            "columns": ["id", "name"],
            "opts": {"key": "val"},
        }

        result = engine._render_value(template_data, params)

        expected = {
            "source": {
                "path": "{landing_volume}/customer_data/data.csv",
                "database": "{catalog}.{schema}",
                "header": True,
                "batch_size": 1000,
                "columns": ["id", "name"],
                "options": {"key": "val"},
                "format": "csv",
            }
        }

        assert result == expected
        # Substitution tokens preserved as strings
        assert isinstance(result["source"]["path"], str)
        assert isinstance(result["source"]["database"], str)
        # Template params resolved to native types
        assert isinstance(result["source"]["header"], bool)
        assert isinstance(result["source"]["batch_size"], int)
        assert isinstance(result["source"]["columns"], list)
        assert isinstance(result["source"]["options"], dict)


# ---------------------------------------------------------------------------
# String-specific tests
# ---------------------------------------------------------------------------


class TestStringConversion:
    """String-specific template parameter conversion."""

    def test_empty_string(self, engine):
        result = engine._render_value("{{ empty }}", {"empty": ""})
        assert result == ""
        assert isinstance(result, str)

    def test_special_characters(self, engine):
        special = "test@#$%^&*()_+-=[]{}|;:,.<>?"
        result = engine._render_value("{{ special }}", {"special": special})
        assert result == special

    def test_whitespace_preserved(self, engine):
        spaced = "  hello   world  "
        result = engine._render_value("{{ spaced }}", {"spaced": spaced})
        assert result == spaced

    def test_multiline(self, engine):
        multiline = "Line 1\nLine 2\nLine 3"
        result = engine._render_value("{{ ml }}", {"ml": multiline})
        assert result == multiline

    def test_auto_type_detection(self, engine):
        """Strings that look like other types get auto-converted."""
        template_data = {
            "string_true": "{{ str_true }}",
            "string_false": "{{ str_false }}",
            "string_number": "{{ str_number }}",
            "string_array": "{{ str_array }}",
            "string_object": "{{ str_object }}",
        }

        params = {
            "str_true": "true",
            "str_false": "false",
            "str_number": "123",
            "str_array": "[1,2,3]",
            "str_object": '{"key":"value"}',
        }

        result = engine._render_value(template_data, params)

        assert result["string_true"] is True
        assert result["string_false"] is False
        assert result["string_number"] == 123
        assert result["string_array"] == [1, 2, 3]
        assert result["string_object"] == {"key": "value"}

    def test_string_in_complex_structure(self, engine):
        template_data = {
            "metadata": {
                "table_name": "{{ table_name }}",
                "description": "{{ description }}",
                "owner": "{{ owner }}",
                "environment": "{{ env }}",
            },
            "paths": {
                "source_path": "{{ source_path }}",
                "target_path": "{{ target_path }}",
            },
        }

        params = {
            "table_name": "customer_data",
            "description": "Customer information table",
            "owner": "data_team",
            "env": "production",
            "source_path": "/data/raw/customer",
            "target_path": "/data/processed/customer",
        }

        result = engine._render_value(template_data, params)

        assert result["metadata"]["table_name"] == "customer_data"
        assert result["metadata"]["description"] == "Customer information table"
        assert result["paths"]["source_path"] == "/data/raw/customer"

    def test_string_concatenation_in_template(self, engine):
        result = engine._render_value(
            "{{ prefix }}_{{ name }}_{{ suffix }}",
            {"prefix": "bronze", "name": "customer", "suffix": "table"},
        )
        assert result == "bronze_customer_table"

    def test_unicode(self, engine):
        unicode_string = "Hello 世界 🌍 café naïve résumé"
        result = engine._render_value("{{ u }}", {"u": unicode_string})
        assert result == unicode_string

    def test_escaped_characters(self, engine):
        escaped = 'Line 1\\nLine 2\\tTabbed\\"Quoted\\"'
        result = engine._render_value("{{ e }}", {"e": escaped})
        assert result == escaped

    def test_very_long_string(self, engine):
        long_string = "x" * 10000
        result = engine._render_value("{{ ls }}", {"ls": long_string})
        assert result == long_string
        assert len(result) == 10000


# ---------------------------------------------------------------------------
# Boolean-specific tests
# ---------------------------------------------------------------------------


class TestBooleanConversion:
    """Boolean-specific template parameter conversion."""

    def test_boolean_in_complex_structure(self, engine):
        template_data = {
            "source": {
                "format": "csv",
                "header": "{{ has_header }}",
                "inferSchema": "{{ infer_schema }}",
                "multiline": "{{ multiline_enabled }}",
            },
            "processing": {
                "streaming": "{{ is_streaming }}",
                "checkpointing": "{{ enable_checkpoints }}",
            },
        }

        params = {
            "has_header": True,
            "infer_schema": False,
            "multiline_enabled": True,
            "is_streaming": False,
            "enable_checkpoints": True,
        }

        result = engine._render_value(template_data, params)

        assert result["source"]["header"] is True
        assert result["source"]["inferSchema"] is False
        assert result["processing"]["streaming"] is False
        assert result["processing"]["checkpointing"] is True

    def test_boolean_in_array(self, engine):
        template_array = ["{{ flag1 }}", "static_string", "{{ flag2 }}", "{{ flag3 }}"]
        params = {"flag1": True, "flag2": False, "flag3": True}
        result = engine._render_value(template_array, params)
        assert result == [True, "static_string", False, True]

    def test_multiple_boolean_parameters(self, engine):
        template_data = {
            "feature_flags": {
                "enable_caching": "{{ cache_enabled }}",
                "enable_compression": "{{ compression_enabled }}",
                "enable_encryption": "{{ encryption_enabled }}",
                "enable_monitoring": "{{ monitoring_enabled }}",
            }
        }

        params = {
            "cache_enabled": True,
            "compression_enabled": False,
            "encryption_enabled": True,
            "monitoring_enabled": False,
        }

        result = engine._render_value(template_data, params)

        assert result["feature_flags"]["enable_caching"] is True
        assert result["feature_flags"]["enable_compression"] is False
        assert result["feature_flags"]["enable_encryption"] is True
        assert result["feature_flags"]["enable_monitoring"] is False

    def test_string_to_boolean_edge_cases(self, engine):
        """String 'true'/'false' should auto-convert to boolean."""
        template_data = {
            "bool_true": "{{ bt }}",
            "bool_false": "{{ bf }}",
            "string_true": "{{ st }}",
            "string_false": "{{ sf }}",
        }

        params = {
            "bt": True,
            "bf": False,
            "st": "true",
            "sf": "false",
        }

        result = engine._render_value(template_data, params)

        assert result["bool_true"] is True
        assert result["bool_false"] is False
        assert result["string_true"] is True
        assert result["string_false"] is False


# ---------------------------------------------------------------------------
# Integer-specific tests
# ---------------------------------------------------------------------------


class TestIntegerConversion:
    """Integer-specific template parameter conversion."""

    def test_large_integer(self, engine):
        large_int = 9223372036854775807
        result = engine._render_value("{{ n }}", {"n": large_int})
        assert result == large_int

    def test_integer_in_complex_structure(self, engine):
        template_data = {
            "config": {
                "batch_size": "{{ batch_size }}",
                "max_retries": "{{ max_retries }}",
                "timeout_seconds": "{{ timeout }}",
                "parallelism": "{{ parallel_jobs }}",
            },
            "limits": {
                "memory_mb": "{{ memory_limit }}",
                "cpu_cores": "{{ cpu_limit }}",
            },
        }

        params = {
            "batch_size": 1000,
            "max_retries": 3,
            "timeout": 300,
            "parallel_jobs": 4,
            "memory_limit": 8192,
            "cpu_limit": 2,
        }

        result = engine._render_value(template_data, params)

        assert result["config"]["batch_size"] == 1000
        assert result["config"]["max_retries"] == 3
        assert result["limits"]["memory_mb"] == 8192

    def test_integer_in_array(self, engine):
        template_array = ["{{ c1 }}", "static_string", "{{ c2 }}", "{{ c3 }}"]
        params = {"c1": 10, "c2": 0, "c3": -5}
        result = engine._render_value(template_array, params)
        assert result == [10, "static_string", 0, -5]

    def test_multiple_integer_parameters(self, engine):
        template_data = {
            "performance": {
                "batch_size": "{{ batch_size }}",
                "max_files_per_trigger": "{{ max_files }}",
                "checkpoint_interval": "{{ checkpoint_interval }}",
                "retention_days": "{{ retention_days }}",
            }
        }

        params = {
            "batch_size": 1000,
            "max_files": 50,
            "checkpoint_interval": 10,
            "retention_days": 365,
        }

        result = engine._render_value(template_data, params)

        assert result["performance"]["batch_size"] == 1000
        assert result["performance"]["max_files_per_trigger"] == 50
        assert result["performance"]["retention_days"] == 365

    def test_string_to_integer_edge_cases(self, engine):
        """String '123' should auto-convert to integer."""
        template_data = {
            "actual_int": "{{ ai }}",
            "string_number": "{{ sn }}",
            "zero_int": "{{ zi }}",
            "negative_int": "{{ ni }}",
        }

        params = {
            "ai": 42,
            "sn": "123",
            "zi": 0,
            "ni": -100,
        }

        result = engine._render_value(template_data, params)

        assert result["actual_int"] == 42
        assert result["string_number"] == 123
        assert isinstance(result["string_number"], int)
        assert result["zero_int"] == 0
        assert result["negative_int"] == -100

    def test_integer_precision_boundaries(self, engine):
        template_data = {
            "small_positive": "{{ sp }}",
            "small_negative": "{{ sn }}",
            "large_positive": "{{ lp }}",
            "large_negative": "{{ ln }}",
        }

        params = {
            "sp": 1,
            "sn": -1,
            "lp": 2147483647,
            "ln": -2147483648,
        }

        result = engine._render_value(template_data, params)

        assert result["small_positive"] == 1
        assert result["large_positive"] == 2147483647
        assert result["large_negative"] == -2147483648


# ---------------------------------------------------------------------------
# Array-specific tests
# ---------------------------------------------------------------------------


class TestArrayConversion:
    """Array-specific template parameter conversion."""

    def test_single_item_array(self, engine):
        result = engine._render_value("{{ col }}", {"col": ["only_col"]})
        assert result == ["only_col"]

    def test_mixed_type_array(self, engine):
        mixed = ["string_value", 42, True, None]
        result = engine._render_value("{{ m }}", {"m": mixed})
        assert result == mixed
        assert isinstance(result[0], str)
        assert isinstance(result[1], int)
        assert isinstance(result[2], bool)
        assert result[3] is None

    def test_nested_arrays(self, engine):
        nested = [["a", "b"], ["c", "d"], ["e"]]
        result = engine._render_value("{{ n }}", {"n": nested})
        assert result == nested
        assert isinstance(result[0], list)

    def test_array_in_complex_structure(self, engine):
        template_data = {
            "table_config": {
                "name": "{{ table_name }}",
                "cluster_columns": "{{ cluster_cols }}",
                "partition_columns": "{{ partition_cols }}",
            },
            "metadata": "{{ meta_fields }}",
        }

        params = {
            "table_name": "customer",
            "cluster_cols": ["c_customer_id", "c_region"],
            "partition_cols": ["year", "month"],
            "meta_fields": ["created_at", "updated_at", "source_file"],
        }

        result = engine._render_value(template_data, params)

        assert result["table_config"]["cluster_columns"] == ["c_customer_id", "c_region"]
        assert result["metadata"] == ["created_at", "updated_at", "source_file"]

    def test_array_with_template_expressions_in_items(self, engine):
        template_array = ["{{ prefix }}_column1", "{{ prefix }}_column2", "static_column"]
        params = {"prefix": "fact"}
        result = engine._render_value(template_array, params)
        assert result == ["fact_column1", "fact_column2", "static_column"]

    def test_array_with_substitution_tokens(self, engine):
        template_array = [
            "{catalog}.{schema}.table1",
            "{catalog}.{schema}.table2",
            "{{ dynamic_table }}",
        ]
        params = {"dynamic_table": "generated_table"}
        result = engine._render_value(template_array, params)
        assert result == [
            "{catalog}.{schema}.table1",
            "{catalog}.{schema}.table2",
            "generated_table",
        ]

    def test_multiple_array_parameters(self, engine):
        template_data = {
            "source_tables": "{{ source_list }}",
            "target_columns": "{{ target_cols }}",
            "transformation_steps": "{{ transform_steps }}",
        }

        params = {
            "source_list": ["customers", "orders", "products"],
            "target_cols": ["id", "name", "total"],
            "transform_steps": ["clean", "validate", "enrich"],
        }

        result = engine._render_value(template_data, params)

        assert result["source_tables"] == ["customers", "orders", "products"]
        assert result["target_columns"] == ["id", "name", "total"]


# ---------------------------------------------------------------------------
# Object-specific tests
# ---------------------------------------------------------------------------


class TestObjectConversion:
    """Object/dict-specific template parameter conversion."""

    def test_nested_object(self, engine):
        nested_obj = {
            "level1": {
                "level2": {"key": "deep_value", "number": 42},
                "sibling": "value",
            },
            "top_level": "surface_value",
        }
        result = engine._render_value("{{ nested }}", {"nested": nested_obj})
        assert result == nested_obj
        assert isinstance(result["level1"]["level2"], dict)

    def test_object_with_mixed_value_types(self, engine):
        mixed_obj = {
            "string_val": "hello",
            "integer_val": 123,
            "boolean_val": True,
            "null_val": None,
            "array_val": ["item1", "item2"],
            "nested_obj": {"inner_key": "inner_value"},
        }
        result = engine._render_value("{{ mixed }}", {"mixed": mixed_obj})
        assert result == mixed_obj
        assert result["null_val"] is None
        assert isinstance(result["array_val"], list)
        assert isinstance(result["nested_obj"], dict)

    def test_table_properties_object(self, engine):
        """The original use case that motivated object type conversion."""
        table_props = {
            "delta.enableChangeDataFeed": "true",
            "delta.columnMapping.mode": "name",
            "PII": "true",
            "data_classification": "sensitive",
            "retention_days": 2555,
        }
        result = engine._render_value("{{ tp }}", {"tp": table_props})
        assert result == table_props
        assert result["retention_days"] == 2555

    def test_object_in_complex_structure(self, engine):
        template_data = {
            "write_target": {
                "type": "streaming_table",
                "database": "{{ database }}",
                "table": "{{ table_name }}",
                "table_properties": "{{ table_props }}",
                "spark_conf": "{{ spark_config }}",
            }
        }

        params = {
            "database": "catalog.schema",
            "table_name": "customer",
            "table_props": {"PII": "true", "data_classification": "sensitive"},
            "spark_config": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
        }

        result = engine._render_value(template_data, params)

        assert result["write_target"]["table_properties"] == params["table_props"]
        assert result["write_target"]["spark_conf"] == params["spark_config"]

    def test_object_with_template_expressions_in_values(self, engine):
        template_obj = {
            "table_name": "{{ prefix }}_{{ base_name }}",
            "description": "Table for {{ entity_type }} data",
            "location": "/data/{{ env }}/{{ base_name }}",
            "static_value": "always_this",
        }

        params = {
            "prefix": "fact",
            "base_name": "customer",
            "entity_type": "customer",
            "env": "prod",
        }

        result = engine._render_value(template_obj, params)

        assert result["table_name"] == "fact_customer"
        assert result["description"] == "Table for customer data"
        assert result["location"] == "/data/prod/customer"
        assert result["static_value"] == "always_this"

    def test_object_with_substitution_tokens_in_values(self, engine):
        template_obj = {
            "database": "{catalog}.{schema}",
            "path": "{landing_volume}/{{ table_name }}/*.csv",
            "format": "csv",
            "generated_name": "{{ prefix }}_table",
        }

        params = {"table_name": "customer", "prefix": "bronze"}
        result = engine._render_value(template_obj, params)

        assert result["database"] == "{catalog}.{schema}"
        assert result["path"] == "{landing_volume}/customer/*.csv"
        assert result["generated_name"] == "bronze_table"

    def test_multiple_object_parameters(self, engine):
        template_data = {
            "source_config": "{{ source_options }}",
            "target_config": "{{ target_options }}",
        }

        params = {
            "source_options": {"format": "csv", "header": True, "delimiter": ","},
            "target_options": {"mode": "append", "partitionBy": ["year", "month"]},
        }

        result = engine._render_value(template_data, params)

        assert result["source_config"] == params["source_options"]
        assert result["target_config"] == params["target_options"]

    def test_object_with_special_keys(self, engine):
        special_obj = {
            "cloudFiles.format": "csv",
            "cloudFiles.maxFilesPerTrigger": "10",
            "spark.sql.adaptive.enabled": "true",
            "my_custom_property": "value",
            "CamelCaseProperty": "another_value",
        }
        result = engine._render_value("{{ sp }}", {"sp": special_obj})
        assert result == special_obj
