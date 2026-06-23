"""Map ODCS schema properties to LHP artifacts.

Two pure functions, each operating on a single ODCS schema *property* dict:

- :func:`odcs_type_to_spark` → a Spark/Databricks DDL type string for the
  generated schema (``columns[].type``), consumed downstream by
  :meth:`lhp.parsers.schema_parser.SchemaParser.to_schema_hints`.
- :func:`odcs_property_to_constraints` → row-level ``(predicate, name)`` pairs
  for the generated ``data_quality`` expectations, derived from a property's
  ``logicalTypeOptions``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from ..errors import ErrorFactory, codes

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Type mapping  (ODCS property -> Spark DDL type, for the generated schema)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Constraint mapping  (ODCS property -> data_quality expectation predicates)
# ---------------------------------------------------------------------------


def _num(value: Any) -> str:
    """Render a numeric literal without a spurious trailing ``.0``.

    YAML loads ``0`` as ``int`` and ``0.5`` as ``float``; integers (including
    integer-valued floats) render with no decimal point, genuine floats render
    normally.
    """
    if isinstance(value, bool):
        # bool is a subclass of int; render as its int value.
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    return str(value)


def odcs_property_to_constraints(prop: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Derive row-level expectation predicates for a single ODCS property.

    Returns a list of ``(predicate, name)`` pairs (order preserved), where
    ``predicate`` is a Spark SQL boolean expression and ``name`` is a stable slug.
    Scalar comparisons are emitted bare — DLT/SDP treats a NULL-valued expectation
    expression as passing, so a null column value never fails them. Array
    ``size()`` and object nested ``IS NOT NULL`` checks ARE NULL-guarded
    (``<col> IS NULL OR (<predicate>)``) because there a null value would evaluate
    FALSE and be wrongly flagged. Nullability itself is enforced by the schema's
    ``NOT NULL``, not here. Unknown or non-row-level options are skipped (never
    raised).

    Does not assign a DLT action — the caller derives that from the property's
    ``criticalDataElement`` flag.
    """
    col = prop["name"]
    constraints: List[Tuple[str, str]] = []

    def _guard(predicate: str) -> str:
        # Only used where a bare predicate would evaluate to FALSE (not NULL) on
        # a null value — array ``size()`` (``size(NULL)`` is config-dependent in
        # Spark, can be -1) and object nested ``IS NOT NULL`` (``NULL.field`` →
        # ``NULL IS NOT NULL`` → FALSE). Scalar comparisons need no guard: DLT/SDP
        # treats a NULL-valued expectation expression as passing.
        return f"{col} IS NULL OR ({predicate})"

    # NOTE: the property-level ``required`` flag is intentionally NOT translated
    # here — nullability is enforced by the generated schema's ``nullable: false``
    # (a ``NOT NULL`` column constraint on the Delta / SDP table). Expectations
    # are derived only from ``logicalTypeOptions``.
    logical = prop.get("logicalType")
    options = prop.get("logicalTypeOptions") or {}

    if logical == "string":
        if "minLength" in options:
            constraints.append(
                (f"length({col}) >= {_num(options['minLength'])}", f"{col}_min_length")
            )
        if "maxLength" in options:
            constraints.append(
                (f"length({col}) <= {_num(options['maxLength'])}", f"{col}_max_length")
            )
        if "pattern" in options:
            regex = str(options["pattern"]).replace("'", "''")
            constraints.append((f"{col} RLIKE '{regex}'", f"{col}_pattern"))

    elif logical in ("integer", "number"):
        if "minimum" in options:
            constraints.append((f"{col} >= {_num(options['minimum'])}", f"{col}_min"))
        if "maximum" in options:
            constraints.append((f"{col} <= {_num(options['maximum'])}", f"{col}_max"))
        if "exclusiveMinimum" in options:
            constraints.append(
                (f"{col} > {_num(options['exclusiveMinimum'])}", f"{col}_exclusive_min")
            )
        if "exclusiveMaximum" in options:
            constraints.append(
                (f"{col} < {_num(options['exclusiveMaximum'])}", f"{col}_exclusive_max")
            )
        if "multipleOf" in options:
            constraints.append(
                (f"{col} % {_num(options['multipleOf'])} = 0", f"{col}_multiple_of")
            )

    elif logical in ("date", "timestamp", "time"):
        if "minimum" in options:
            constraints.append((f"{col} >= '{options['minimum']}'", f"{col}_min"))
        if "maximum" in options:
            constraints.append((f"{col} <= '{options['maximum']}'", f"{col}_max"))
        if "exclusiveMinimum" in options:
            constraints.append(
                (f"{col} > '{options['exclusiveMinimum']}'", f"{col}_exclusive_min")
            )
        if "exclusiveMaximum" in options:
            constraints.append(
                (f"{col} < '{options['exclusiveMaximum']}'", f"{col}_exclusive_max")
            )

    elif logical == "array":
        if "minItems" in options:
            constraints.append(
                (_guard(f"size({col}) >= {_num(options['minItems'])}"),
                 f"{col}_min_items")
            )
        if "maxItems" in options:
            constraints.append(
                (_guard(f"size({col}) <= {_num(options['maxItems'])}"),
                 f"{col}_max_items")
            )
        if options.get("uniqueItems") is True:
            constraints.append(
                (_guard(f"size({col}) = size(array_distinct({col}))"),
                 f"{col}_unique_items")
            )

    elif logical == "object":
        for field in options.get("required", []) or []:
            constraints.append(
                (_guard(f"{col}.{field} IS NOT NULL"), f"{col}_{field}_not_null")
            )

    # boolean / unknown logicalType / unique / primaryKey / quality /
    # relationships -> nothing.
    return constraints
