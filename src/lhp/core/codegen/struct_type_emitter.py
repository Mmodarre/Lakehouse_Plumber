"""Emit Spark ``StructType`` Python source from parsed schema data.

Codegen-layer counterpart to :class:`lhp.parsers.schema_parser.SchemaParser`:
the parser owns YAML parsing and validation, while this module owns the
*emission* of executable ``StructType``/``StructField`` source code, rendered
via a Jinja2 template (constitution §2.10 / §9.14, layering §5).
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from ...errors import ErrorFactory, codes
from .template_renderer import TemplateRenderer

logger = logging.getLogger(__name__)

_TEMPLATE_NAME = "load/struct_type.py.j2"

# Referenced by every emitted schema, whatever its column types.
_CONTAINER_TYPE_NAMES = ("StructType", "StructField")

_TYPE_MAPPING = {
    "STRING": "StringType()",
    "BIGINT": "LongType()",
    "INT": "IntegerType()",
    "INTEGER": "IntegerType()",
    "LONG": "LongType()",
    "DOUBLE": "DoubleType()",
    "FLOAT": "FloatType()",
    "BOOLEAN": "BooleanType()",
    "DATE": "DateType()",
    "TIMESTAMP": "TimestampType()",
    "BINARY": "BinaryType()",
    "BYTE": "ByteType()",
    "SHORT": "ShortType()",
}

# DECIMAL types need precision/scale extraction.
_DECIMAL_PATTERN = r"DECIMAL\((\d+),(\d+)\)"

# str.splitlines() boundaries that json.dumps(ensure_ascii=False) leaves raw.
# write_normalized() splits every generated file on them, which would break
# the literal across two physical lines.
_SPLITLINES_ESCAPES = str.maketrans(
    {"\x85": "\\x85", "\u2028": "\\u2028", "\u2029": "\\u2029"}
)


@dataclass(frozen=True)
class StructTypeCode:
    """Emitted ``StructType`` assignment and the type names it references.

    ``type_names`` holds exactly the ``pyspark.sql.types`` names that
    ``code_lines`` uses, sorted. ``code_lines`` carries no import statement;
    the caller registers one import per name.
    """

    code_lines: tuple[str, ...]
    type_names: tuple[str, ...]


def emit_struct_type_code(
    schema_data: dict[str, Any], variable_name: str
) -> StructTypeCode:
    """Emit Spark ``StructType`` source code for a parsed schema.

    Args:
        schema_data: Parsed schema carrying a ``columns`` list.
        variable_name: Module-level name the ``StructType`` is assigned to. The
            caller derives it so that it is unique within the generated module.

    Returns:
        The ``<variable_name> = StructType([...])`` lines and the
        ``pyspark.sql.types`` names they reference.

    Raises:
        LHPValidationError: If the schema has no ``columns`` field.
    """
    if "columns" not in schema_data:
        raise ErrorFactory.validation_error(
            codes.VAL_016,
            title="Missing 'columns' field in schema",
            details="Schema must have a 'columns' field defining the column structure.",
            suggestions=[
                "Add a 'columns' key with a list of column definitions",
                "Each column needs 'name' and 'type' fields",
            ],
            example="columns:\n  - name: id\n    type: BIGINT\n  - name: name\n    type: STRING",
            context={"schema": str(schema_data.get("name", "<unknown>"))},
        )

    struct_fields = []
    type_names = set(_CONTAINER_TYPE_NAMES)
    for column in schema_data["columns"]:
        spark_type = _convert_to_spark_type(column["type"])
        # The constructor name is the import name: DecimalType(18, 2) -> DecimalType.
        type_names.add(spark_type.partition("(")[0])
        struct_fields.append(_generate_struct_field(column, spark_type))

    renderer = TemplateRenderer.from_package()
    rendered = renderer.render_template(
        _TEMPLATE_NAME,
        {
            "variable_name": variable_name,
            "struct_fields": struct_fields,
        },
    )

    return StructTypeCode(
        code_lines=tuple(rendered.split("\n")),
        type_names=tuple(sorted(type_names)),
    )


def _generate_struct_field(column: dict[str, Any], spark_type: str) -> str:
    name = _str_literal(column["name"])
    nullable = column.get("nullable", True)
    comment = column.get("comment", "")

    metadata = "{}" if not comment else f'{{"comment": {_str_literal(comment)}}}'

    return f"StructField({name}, {spark_type}, {nullable}, {metadata})"


def _str_literal(value: Any) -> str:
    """Render a YAML scalar as a Python string literal that round-trips exactly.

    A JSON string is a valid Python string literal, so ``json.dumps`` escapes
    quotes, backslashes and control characters. ``ensure_ascii`` must stay off:
    its surrogate-pair escapes for astral characters (``\\ud83d\\ude00``) decode
    in Python as two lone surrogates, not the original character.
    """
    return json.dumps(str(value), ensure_ascii=False).translate(_SPLITLINES_ESCAPES)


def _convert_to_spark_type(col_type: str) -> str:
    col_type = col_type.upper().strip()

    decimal_match = re.match(_DECIMAL_PATTERN, col_type)
    if decimal_match:
        precision, scale = decimal_match.groups()
        return f"DecimalType({precision}, {scale})"

    if col_type in _TYPE_MAPPING:
        return _TYPE_MAPPING[col_type]

    logger.warning(f"Unknown type '{col_type}', defaulting to StringType")
    return "StringType()"
