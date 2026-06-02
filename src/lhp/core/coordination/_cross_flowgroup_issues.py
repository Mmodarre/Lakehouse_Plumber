"""Single source of cross-flowgroup error construction (§9.24).

This module owns the *one* place where a
:class:`~lhp.core._interfaces.CrossFlowgroupCheckResult` is turned into
:class:`~lhp.errors.types.LHPError` instances. Both the ``generate``
raise path (:class:`~lhp.core.coordination.processor.PipelineProcessor`)
and the ``validate`` fold path
(:class:`~lhp.core.coordination.executor`) call
:func:`build_cross_flowgroup_issues`; neither hand-reads the
``table_creation_errors`` / ``cdc_fanin_errors`` fields again. The two
paths differ only in how they *surface* the resulting errors (raise the
first vs. fold them all into an issue list), never in how the errors are
*constructed*. This is the §9.24 contract: no duplicated validation
logic across the generate and validate paths.

One aggregated :class:`LHPValidationError` per non-empty family — never
one error per raw string. Families are emitted in precedence order:
table-creation (``LHP-VAL-009``) first, then CDC fan-in
(``LHP-VAL-010``).

This is a pure function (§3.7): no I/O, no logging, deterministic.
"""

from __future__ import annotations

from typing import List, Tuple

from ...errors import ErrorCategory, LHPError, LHPValidationError
from .._interfaces import CrossFlowgroupCheckResult

_TABLE_CREATION_SUGGESTIONS: List[str] = [
    "Ensure each target table has exactly one action with create_table: true",
    "Check for conflicting table creation settings across flowgroups",
    "Run 'lhp validate' for detailed diagnostics",
]

_CDC_FANIN_SUGGESTIONS: List[str] = [
    "All CDC actions sharing a target must agree on table-level and "
    "CDC-key fields (keys, sequence_by, stored_as_scd_type, "
    "track_history_*, partition_columns, table_properties, etc.)",
    "Fields allowed to differ per flow: source, once, "
    "ignore_null_updates, apply_as_deletes, apply_as_truncates, "
    "column_list, except_column_list",
    "Run 'lhp validate' for detailed diagnostics",
]


def _build_family_error(
    *,
    code_number: str,
    title: str,
    errors: List[str],
    suggestions: List[str],
    pipeline: str,
) -> LHPValidationError:
    """Wrap one family of validator strings into a single aggregated LHPValidationError."""
    return LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number=code_number,
        title=title,
        details=f"{title}:\n" + "\n".join(f"  - {e}" for e in errors),
        suggestions=suggestions,
        context={
            "Pipeline": pipeline,
            "Error Count": len(errors),
        },
    )


def build_cross_flowgroup_issues(
    result: CrossFlowgroupCheckResult, pipeline: str
) -> Tuple[LHPError, ...]:
    """Convert a cross-flowgroup check result into aggregated LHP errors.

    Produces exactly one :class:`LHPValidationError` per *non-empty*
    error family, in precedence order:

    1. ``table_creation_errors`` -> ``LHP-VAL-009``
    2. ``cdc_fanin_errors`` -> ``LHP-VAL-010``

    Both families empty returns an empty tuple. Callers decide how to
    surface the result (raise the first vs. fold all into an issue list).
    """
    issues: List[LHPError] = []

    if result.table_creation_errors:
        issues.append(
            _build_family_error(
                code_number="009",
                title="Table creation validation failed",
                errors=result.table_creation_errors,
                suggestions=_TABLE_CREATION_SUGGESTIONS,
                pipeline=pipeline,
            )
        )

    if result.cdc_fanin_errors:
        issues.append(
            _build_family_error(
                code_number="010",
                title="CDC fan-in compatibility validation failed",
                errors=result.cdc_fanin_errors,
                suggestions=_CDC_FANIN_SUGGESTIONS,
                pipeline=pipeline,
            )
        )

    return tuple(issues)
