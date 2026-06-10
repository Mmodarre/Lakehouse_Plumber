"""Unit tests for the cross-flowgroup error converter (§9.24).

Verifies that :func:`build_cross_flowgroup_issues` is the single source
of cross-flowgroup error construction: one aggregated error per
non-empty family, in the processor's precedence order (VAL-009 before
VAL-010), with the correct error codes.
"""

import pytest

from lhp.core._interfaces import CrossFlowgroupCheckResult
from lhp.core.coordination._cross_flowgroup_issues import (
    build_cross_flowgroup_issues,
)
from lhp.errors import LHPValidationError

pytestmark = pytest.mark.unit

PIPELINE = "bronze_pipeline"


def test_both_empty_returns_empty_tuple():
    """No errors in either family -> empty tuple, nothing to surface."""
    result = CrossFlowgroupCheckResult()

    issues = build_cross_flowgroup_issues(result, PIPELINE)

    assert issues == ()


def test_table_creation_only_returns_single_val_009():
    """Only table-creation errors -> exactly one LHP-VAL-009 error."""
    result = CrossFlowgroupCheckResult(
        table_creation_errors=["Table cat.sch.tbl has no creating action"],
    )

    issues = build_cross_flowgroup_issues(result, PIPELINE)

    assert len(issues) == 1
    assert issues[0].code == "LHP-VAL-009"


def test_cdc_fanin_only_returns_single_val_010():
    """Only CDC fan-in errors -> exactly one LHP-VAL-010 error."""
    result = CrossFlowgroupCheckResult(
        cdc_fanin_errors=["CDC actions targeting cat.sch.tbl disagree on keys"],
    )

    issues = build_cross_flowgroup_issues(result, PIPELINE)

    assert len(issues) == 1
    assert issues[0].code == "LHP-VAL-010"


def test_both_populated_orders_val_009_before_val_010():
    """Both families populated -> VAL-009 first, then VAL-010, each correct."""
    result = CrossFlowgroupCheckResult(
        table_creation_errors=["Table cat.sch.tbl has no creating action"],
        cdc_fanin_errors=["CDC actions targeting cat.sch.tbl disagree on keys"],
    )

    issues = build_cross_flowgroup_issues(result, PIPELINE)

    assert [issue.code for issue in issues] == ["LHP-VAL-009", "LHP-VAL-010"]


def test_returned_errors_are_lhp_validation_errors():
    """Each returned error is an aggregated LHPValidationError."""
    result = CrossFlowgroupCheckResult(
        table_creation_errors=["a", "b"],
        cdc_fanin_errors=["c"],
    )

    issues = build_cross_flowgroup_issues(result, PIPELINE)

    assert all(isinstance(issue, LHPValidationError) for issue in issues)


def test_family_is_aggregated_one_error_for_multiple_strings():
    """Multiple strings in a family aggregate into a single error (not one each)."""
    result = CrossFlowgroupCheckResult(
        table_creation_errors=["first error", "second error", "third error"],
    )

    issues = build_cross_flowgroup_issues(result, PIPELINE)

    assert len(issues) == 1
    error = issues[0]
    assert "first error" in error.details
    assert "second error" in error.details
    assert "third error" in error.details
    assert error.context["Error Count"] == 3
    assert error.context["Pipeline"] == PIPELINE
