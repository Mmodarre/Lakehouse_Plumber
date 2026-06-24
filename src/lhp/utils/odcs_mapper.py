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
from typing import Any, Dict, List, Optional, Tuple

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


def odcs_quality_to_tests(obj: Dict[str, Any], *, source: str) -> List[Dict[str, Any]]:
    """Map an ODCS schema object's ``quality`` rules to LHP test-action dicts.

    Walks **both** the object's dataset-level ``quality`` array and each
    property's ``quality`` array (binding the property as the column), and
    returns a list of partial LHP ``test`` action dicts — each with a
    ``test_type`` (``uniqueness`` / ``completeness`` / ``custom_sql``), the
    type-specific fields, ``on_violation`` (from rule ``severity``), and an
    optional ``test_id`` (from the rule ``name``/``id``). ``source`` is the
    single table under test, threaded onto every emitted dict.

    Library metrics map by ``metric`` + operator; ``sql`` rules map to
    ``custom_sql``; ``custom`` / ``text`` rules (and column-bound metrics with no
    resolvable column) are skipped (logged), returning no test for that rule.
    """
    tests: List[Dict[str, Any]] = []

    # Dataset-level rules (no enclosing property → column comes from arguments).
    for rule in obj.get("quality", []) or []:
        emitted = _quality_rule_to_test(rule, column=None, source=source)
        if emitted is not None:
            tests.append(emitted)

    # Property-level rules (the enclosing property is the bound column).
    for prop in obj.get("properties", []) or []:
        column = prop.get("name")
        for rule in prop.get("quality", []) or []:
            emitted = _quality_rule_to_test(rule, column=column, source=source)
            if emitted is not None:
                tests.append(emitted)

    return tests


# Operator -> comparison symbol for scalar metric expressions.
_OPERATOR_SYMBOLS = {
    "mustBe": "=",
    "mustNotBe": "!=",
    "mustBeGreaterThan": ">",
    "mustBeGreaterOrEqualTo": ">=",
    "mustBeLessThan": "<",
    "mustBeLessOrEqualTo": "<=",
}


def _severity_to_on_violation(rule: Dict[str, Any]) -> str:
    """Map an ODCS rule ``severity`` to an LHP ``on_violation`` value."""
    severity = rule.get("severity")
    if severity == "error":
        return "fail"
    if severity in ("warning", "info"):
        return "warn"
    # Absent (or anything unrecognised) defaults to fail.
    return "fail"


def _metric_expression(rule: Dict[str, Any]) -> Optional[str]:
    """Build the ``metric <op> <value>`` expression from a rule's operator.

    Returns ``None`` when the rule carries no recognised operator.
    """
    for operator, symbol in _OPERATOR_SYMBOLS.items():
        if operator in rule:
            return f"metric {symbol} {_num(rule[operator])}"
    if "mustBeBetween" in rule:
        low, high = rule["mustBeBetween"]
        return f"metric BETWEEN {_num(low)} AND {_num(high)}"
    if "mustNotBeBetween" in rule:
        low, high = rule["mustNotBeBetween"]
        return f"metric NOT BETWEEN {_num(low)} AND {_num(high)}"
    return None


def _rule_slug(rule: Dict[str, Any], column: Optional[str]) -> str:
    """Name for the emitted test (used as the action-name suffix).

    Uses the rule's ``name`` verbatim when present; otherwise derives a
    lower-cased fallback from the bound column (when any) and the metric / rule
    type, e.g. ``rowcount`` or ``order_id_duplicatevalues`` — never ``None``.
    """
    name = rule.get("name")
    if name:
        return name
    kind = rule.get("metric") if rule.get("type") == "library" else rule.get("type")
    kind = kind or "check"
    col = _resolve_column(rule, column)
    return (f"{col}_{kind}" if col else kind).lower()


def _base_test(
    rule: Dict[str, Any], source: str, on_violation: str, slug: str
) -> Dict[str, Any]:
    """Build the common fields shared by every emitted test dict."""
    test: Dict[str, Any] = {
        "source": source,
        "on_violation": on_violation,
        "name": slug,
    }
    if rule.get("name"):
        test["test_id"] = rule["name"]
    return test


def _custom_sql_test(
    rule: Dict[str, Any], source: str, on_violation: str, sql: str, slug: str
) -> Dict[str, Any]:
    """Build a ``custom_sql`` test dict with a single metric expectation."""
    test = _base_test(rule, source, on_violation, slug)
    test["test_type"] = "custom_sql"
    test["sql"] = sql
    test["expectations"] = [
        {
            "name": slug,
            "expression": _metric_expression(rule),
            "on_violation": on_violation,
        }
    ]
    return test


def _resolve_column(rule: Dict[str, Any], column: Optional[str]) -> Optional[str]:
    """Resolve the single column a metric is bound to.

    Property-level rules use the enclosing ``column``; dataset-level rules fall
    back to ``arguments.column`` (single) or the first of ``arguments.columns``.
    Returns ``None`` when no column can be resolved.
    """
    if column is not None:
        return column
    arguments = rule.get("arguments") or {}
    if arguments.get("column"):
        return arguments["column"]
    columns = arguments.get("columns")
    if columns:
        return columns[0]
    return None


def _resolve_columns(rule: Dict[str, Any], column: Optional[str]) -> Optional[List[str]]:
    """Resolve the column list for a uniqueness metric.

    Property-level rules bind the single enclosing ``column``; dataset-level
    rules use ``arguments.columns`` (list) or ``arguments.column`` (single).
    Returns ``None`` when no column can be resolved.
    """
    if column is not None:
        return [column]
    arguments = rule.get("arguments") or {}
    columns = arguments.get("columns")
    if columns:
        return list(columns)
    if arguments.get("column"):
        return [arguments["column"]]
    return None


def _is_must_be_zero(rule: Dict[str, Any]) -> bool:
    """True when the rule's operator is exactly ``mustBe: 0``."""
    return rule.get("mustBe") == 0


def _quality_rule_to_test(
    rule: Dict[str, Any], *, column: Optional[str], source: str
) -> Optional[Dict[str, Any]]:
    """Map a single ODCS quality rule to a partial test dict (or ``None``)."""
    rule_type = rule.get("type")
    on_violation = _severity_to_on_violation(rule)
    slug = _rule_slug(rule, column)

    if rule_type == "sql":
        query = rule.get("query", "")
        substituted = query.replace("${table}", source)
        if column is not None:
            substituted = substituted.replace("${column}", column)
        sql = f"SELECT ({substituted}) AS metric"
        return _custom_sql_test(rule, source, on_violation, sql, slug)

    if rule_type == "library":
        metric = rule.get("metric")

        if metric == "rowCount":
            sql = f"SELECT COUNT(*) AS metric FROM {source}"
            return _custom_sql_test(rule, source, on_violation, sql, slug)

        if metric == "duplicateValues":
            if _is_must_be_zero(rule):
                columns = _resolve_columns(rule, column)
                if columns is None:
                    return None
                test = _base_test(rule, source, on_violation, slug)
                test["test_type"] = "uniqueness"
                test["columns"] = columns
                return test
            resolved = _resolve_column(rule, column)
            if resolved is None:
                return None
            sql = (
                f"SELECT COUNT(*) AS metric FROM "
                f"(SELECT {resolved} FROM {source} "
                f"GROUP BY {resolved} HAVING COUNT(*) > 1)"
            )
            return _custom_sql_test(rule, source, on_violation, sql, slug)

        if metric in ("nullValues", "missingValues"):
            resolved = _resolve_column(rule, column)
            if resolved is None:
                return None
            if _is_must_be_zero(rule):
                test = _base_test(rule, source, on_violation, slug)
                test["test_type"] = "completeness"
                test["required_columns"] = [resolved]
                return test
            sql = (
                f"SELECT COUNT(*) AS metric FROM {source} "
                f"WHERE {resolved} IS NULL"
            )
            return _custom_sql_test(rule, source, on_violation, sql, slug)

        if metric == "invalidValues":
            resolved = _resolve_column(rule, column)
            if resolved is None:
                return None
            arguments = rule.get("arguments") or {}
            valid_values = arguments.get("validValues")
            if not valid_values:
                return None
            rendered = ", ".join(f"'{value}'" for value in valid_values)
            sql = (
                f"SELECT COUNT(*) AS metric FROM {source} "
                f"WHERE {resolved} NOT IN ({rendered})"
            )
            return _custom_sql_test(rule, source, on_violation, sql, slug)

    # custom / text / unknown rule types and unmappable metrics are skipped.
    return None
