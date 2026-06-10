"""Emit Spark ``StructType`` Python source from parsed schema data.

Codegen-layer counterpart to :class:`lhp.parsers.schema_parser.SchemaParser`:
the parser owns YAML parsing and validation, while this module owns the
*emission* of executable ``StructType``/``StructField`` source code, rendered
via a Jinja2 template (constitution §2.10 / §9.14, layering §5).
"""

import logging
import re
from typing import Any

from ...errors import ErrorFactory, codes
from .template_renderer import TemplateRenderer

logger = logging.getLogger(__name__)

_TEMPLATE_NAME = "load/struct_type.py.j2"

# The fixed import line that callers peel off as ``code_lines[0]``.
_IMPORT_LINE = (
    "from pyspark.sql.types import StructType, StructField, StringType, "
    "LongType, IntegerType, DoubleType, FloatType, BooleanType, DateType, "
    "TimestampType, DecimalType, BinaryType, ByteType, ShortType"
)

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


def emit_struct_type_code(schema_data: dict[str, Any]) -> tuple[str, list[str]]:
    """Emit Spark ``StructType`` source code for a parsed schema.

    Returns:
        Tuple of ``(variable_name, code_lines)`` where ``code_lines[0]`` is the
        ``from pyspark.sql.types import ...`` line, followed by a blank line and
        the ``<name>_schema = StructType([...])`` block.

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

    schema_name = schema_data.get("name", "schema")
    variable_name = f"{schema_name}_schema"

    struct_fields = [
        _generate_struct_field(column) for column in schema_data["columns"]
    ]

    renderer = TemplateRenderer.from_package()
    rendered = renderer.render_template(
        _TEMPLATE_NAME,
        {
            "import_line": _IMPORT_LINE,
            "variable_name": variable_name,
            "struct_fields": struct_fields,
        },
    )

    return variable_name, rendered.split("\n")


def _generate_struct_field(column: dict[str, Any]) -> str:
    name = column["name"]
    col_type = column["type"]
    nullable = column.get("nullable", True)
    comment = column.get("comment", "")

    spark_type = _convert_to_spark_type(col_type)
    metadata = "{}" if not comment else f'{{"comment": "{comment}"}}'

    return f'StructField("{name}", {spark_type}, {nullable}, {metadata})'


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
