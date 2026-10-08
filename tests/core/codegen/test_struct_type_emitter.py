"""Tests for the StructType code emitter."""

import ast

import pytest

from lhp.core.codegen.imports import SPARK_TYPE_NAMES
from lhp.core.codegen.struct_type_emitter import (
    _TYPE_MAPPING,
    StructTypeCode,
    _convert_to_spark_type,
    emit_struct_type_code,
)
from lhp.utils.file_header import normalize_content

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

# Issue #287: each of these either broke the emitted literal or silently
# altered it. The emoji is astral (outside the BMP).
_PAYLOADS = {
    "double_quote": 'a "quoted" b',
    "single_quote": "it's",
    "backslash_t_n": "C:\\temp\\new",
    "unc_path": "\\\\server\\share",
    "backslash_U": "C:\\Users\\me",
    "backslash_x": "a\\xyz",
    "backslash_N": "D:\\New",
    "newline": "line1\nline2",
    "blank_line": "p1\n\np2",
    "trailing_backslash": "ends with \\",
    "braces": "{x} {{y}} ${z}",
    "non_ascii": "prix € café 価格",
    "tab": "a\tb",
    "carriage_return": "a\rb",
    "emoji": "smile \U0001f600",
    # str.splitlines() boundaries outside the C0 range: the write path
    # (normalize_content) would split a raw one out of its literal.
    "next_line": "a\x85b",
    "line_separator": "a\u2028b",
    "paragraph_separator": "a\u2029b",
}


def _struct_field_calls(code_lines):
    """Parse emitted code as it is written to disk and return its StructField calls."""
    tree = ast.parse(normalize_content("\n".join(code_lines)))
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "StructField"
    ]


def test_emit_struct_type_code():
    result = emit_struct_type_code(SCHEMA_DATA, "v_test_schema")

    assert isinstance(result, StructTypeCode)
    assert result.code_lines == (
        "v_test_schema = StructType([",
        '    StructField("id", LongType(), False, {"comment": "Primary key"}),',
        '    StructField("name", StringType(), True, {"comment": "Name field"}),',
        '    StructField("amount", DecimalType(18, 2), True, {}),',
        '    StructField("is_active", BooleanType(), False, {}),',
        '    StructField("created_at", TimestampType(), False, {}),',
        "])",
    )


def test_type_names_are_exactly_the_types_used():
    result = emit_struct_type_code(SCHEMA_DATA, "v_test_schema")

    assert result.type_names == (
        "BooleanType",
        "DecimalType",
        "LongType",
        "StringType",
        "StructField",
        "StructType",
        "TimestampType",
    )


def test_type_names_for_single_column_schema():
    schema = {"columns": [{"name": "qty", "type": "INT"}]}

    result = emit_struct_type_code(schema, "v_qty_schema")

    assert result.type_names == ("IntegerType", "StructField", "StructType")


def test_unknown_type_reports_its_string_type_fallback():
    schema = {"columns": [{"name": "blob", "type": "GEOGRAPHY"}]}

    result = emit_struct_type_code(schema, "v_blob_schema")

    assert result.type_names == ("StringType", "StructField", "StructType")


def test_every_emittable_type_name_is_in_the_shared_list():
    """ImportDetector resolves names from SPARK_TYPE_NAMES; the emitter must not outgrow it."""
    sql_types = [*_TYPE_MAPPING, "DECIMAL(10,2)", "NOT_A_TYPE"]
    schema = {
        "columns": [
            {"name": f"c{i}", "type": sql_type} for i, sql_type in enumerate(sql_types)
        ]
    }

    result = emit_struct_type_code(schema, "v_all_schema")

    assert set(result.type_names) <= set(SPARK_TYPE_NAMES)


@pytest.mark.parametrize("payload", _PAYLOADS.values(), ids=_PAYLOADS.keys())
def test_column_name_round_trips(payload):
    schema = {"columns": [{"name": payload, "type": "STRING"}]}

    (call,) = _struct_field_calls(emit_struct_type_code(schema, "v_s").code_lines)

    assert ast.literal_eval(call.args[0]) == payload


@pytest.mark.parametrize("payload", _PAYLOADS.values(), ids=_PAYLOADS.keys())
def test_column_comment_round_trips(payload):
    schema = {"columns": [{"name": "id", "type": "BIGINT", "comment": payload}]}

    (call,) = _struct_field_calls(emit_struct_type_code(schema, "v_s").code_lines)

    assert ast.literal_eval(call.args[3]) == {"comment": payload}


def test_non_ascii_is_emitted_verbatim_not_escaped():
    schema = {
        "columns": [{"name": "prix", "type": "STRING", "comment": "café \U0001f600"}]
    }

    result = emit_struct_type_code(schema, "v_s")

    assert '{"comment": "café \U0001f600"}' in result.code_lines[1]


def test_non_string_scalars_are_stringified():
    schema = {"columns": [{"name": 2024, "type": "INT", "comment": 1.5}]}

    (call,) = _struct_field_calls(emit_struct_type_code(schema, "v_s").code_lines)

    assert ast.literal_eval(call.args[0]) == "2024"
    assert ast.literal_eval(call.args[3]) == {"comment": "1.5"}


def test_type_conversion():
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
