"""Tests for the StructType code emitter (relocated from test_cloudfiles_options.py).

These tests were previously ``TestSchemaParser.test_to_struct_type_code`` and
``test_type_conversion``. StructType *emission* moved from
``SchemaParser`` into ``lhp.core.codegen.struct_type_emitter`` (P4b.2); the
tests move with it and are retargeted to ``emit_struct_type_code`` /
``_convert_to_spark_type``. Behavior is unchanged — they assert the same
byte-identical output the parser produced.
"""

from lhp.core.codegen.struct_type_emitter import (
    _convert_to_spark_type,
    emit_struct_type_code,
)

SCHEMA_DATA = {
    "name": "test_schema",
    "version": "1.0",
    "description": "Test schema",
    "columns": [
        {"name": "id", "type": "BIGINT", "nullable": False, "comment": "Primary key"},
        {"name": "name", "type": "STRING", "nullable": True, "comment": "Name field"},
        {"name": "amount", "type": "DECIMAL(18,2)", "nullable": True},
        {"name": "is_active", "type": "BOOLEAN", "nullable": False},
        {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
    ],
}


def test_emit_struct_type_code():
    """Conversion to StructType code is byte-identical to the legacy output."""
    variable_name, code_lines = emit_struct_type_code(SCHEMA_DATA)

    assert variable_name == "test_schema_schema"

    # Full byte-identical output (the gate for the codegen relocation).
    assert code_lines == [
        "from pyspark.sql.types import StructType, StructField, StringType, "
        "LongType, IntegerType, DoubleType, FloatType, BooleanType, DateType, "
        "TimestampType, DecimalType, BinaryType, ByteType, ShortType",
        "",
        "test_schema_schema = StructType([",
        '    StructField("id", LongType(), False, {"comment": "Primary key"}),',
        '    StructField("name", StringType(), True, {"comment": "Name field"}),',
        '    StructField("amount", DecimalType(18, 2), True, {}),',
        '    StructField("is_active", BooleanType(), False, {}),',
        '    StructField("created_at", TimestampType(), False, {}),',
        "])",
    ]


def test_type_conversion():
    """Type conversion to Spark types."""
    test_cases = [
        ("STRING", "StringType()"),
        ("BIGINT", "LongType()"),
        ("INT", "IntegerType()"),
        ("DECIMAL(18,2)", "DecimalType(18, 2)"),
        ("BOOLEAN", "BooleanType()"),
        ("TIMESTAMP", "TimestampType()"),
        ("UNKNOWN_TYPE", "StringType()"),  # Should default to StringType
    ]

    for input_type, expected_output in test_cases:
        assert _convert_to_spark_type(input_type) == expected_output
