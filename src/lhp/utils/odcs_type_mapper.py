"""Map ODCS property types to Spark/Databricks DDL type strings.

The emitted strings feed LHP's existing schema format (``columns[].type``),
which downstream becomes ``cloudFiles.schemaHints`` via
:meth:`lhp.parsers.schema_parser.SchemaParser.to_schema_hints`.
"""

from __future__ import annotations

from typing import Any, Dict

from ..errors import ErrorFactory, codes

# Simple ODCS logical types -> Spark DDL type strings.
_SIMPLE_LOGICAL = {
    "string": "STRING",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "STRING",
}


def _unmappable(prop: Dict[str, Any]):
    logical = prop.get("logicalType")
    return ErrorFactory.config_error(
        codes.CFG_063,
        title="Unmappable ODCS column type",
        details=(
            "Could not map ODCS property to a Spark type. Neither a usable "
            f"'physicalType' nor a recognised 'logicalType' was found "
            f"(logicalType={logical!r})."
        ),
        suggestions=[
            "Provide an explicit 'physicalType' (e.g. STRING, BIGINT, DECIMAL(18,2))",
            "Use a supported logicalType: string, integer, number, boolean, "
            "date, timestamp, time, object, array",
        ],
        context={"property": prop},
    )


def odcs_type_to_spark(prop: Dict[str, Any]) -> str:
    """Resolve an ODCS schema property to a Spark DDL type string.

    Resolution order:

    1. If ``physicalType`` is present and looks like a Spark/DDL type
       (e.g. ``BIGINT``, ``DECIMAL(18,2)``, ``STRING``), return it verbatim.
    2. Otherwise map ODCS ``logicalType`` (honouring ``logicalTypeOptions``):
       ``string``→``STRING``, ``integer``→``BIGINT`` (``i32``→``INT``),
       ``number``→``DOUBLE`` (``f32``→``FLOAT``; precision/scale →
       ``DECIMAL(p,s)``), ``boolean``→``BOOLEAN``, ``date``→``DATE``,
       ``timestamp``→``TIMESTAMP``, ``time``→``STRING``, ``object``→
       ``STRUCT<...>`` (recursing over nested ``properties``), ``array``→
       ``ARRAY<itemType>``.

    :raises lhp.errors.LHPError: ``LHP-CFG-063`` when the type is unmappable.
    """
    # 1. Explicit physical/DDL type wins over logicalType.
    physical = prop.get("physicalType")
    if physical:
        return physical

    logical = prop.get("logicalType")
    options = prop.get("logicalTypeOptions") or {}

    if logical in _SIMPLE_LOGICAL:
        return _SIMPLE_LOGICAL[logical]

    if logical == "integer":
        if options.get("format") == "i32":
            return "INT"
        return "BIGINT"

    if logical == "number":
        precision = options.get("precision")
        scale = options.get("scale")
        if isinstance(precision, (int, float)) and isinstance(scale, (int, float)):
            return f"DECIMAL({precision},{scale})"
        if options.get("format") == "f32":
            return "FLOAT"
        return "DOUBLE"

    if logical == "object":
        fields = []
        for sub in prop.get("properties", []) or []:
            fields.append(f"{sub['name']}:{odcs_type_to_spark(sub)}")
        return f"STRUCT<{','.join(fields)}>"

    if logical == "array":
        item_type = odcs_type_to_spark(prop["items"])
        return f"ARRAY<{item_type}>"

    raise _unmappable(prop)
