"""Unit tests for the worker-scoped run-warning collector.

Exercises :func:`lhp.models.run_warnings.record_validation_warning` directly
against a :func:`~lhp.models.run_warnings.collect_deprecations` scope — the
same scope the worker wrapper opens around each flowgroup. The worker-routing
tests (scope -> ``FlowgroupOutcome.warnings``) live beside the wrapper in
``tests/core/coordination/test_flowgroup_pool.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.errors import codes
from lhp.models.processing import ValidationWarningRecord
from lhp.models.run_warnings import (
    collect_deprecations,
    drain_deprecations,
    record_validation_warning,
)

pytestmark = pytest.mark.unit


def test_records_validation_warning_stamped_with_scope_identity() -> None:
    src = Path("pipelines/p/fg.yaml")
    with collect_deprecations(file=src, flowgroup="fg") as collector:
        record_validation_warning(codes.VAL_073, message="write_a, write_b")

    assert drain_deprecations(collector) == (
        ValidationWarningRecord(
            code="LHP-VAL-073",
            message="write_a, write_b",
            file=src,
            flowgroup="fg",
        ),
    )


def test_is_a_no_op_without_an_open_scope() -> None:
    """Outside a worker scope (main thread, direct unit calls) nothing is
    recorded and nothing raises."""
    record_validation_warning(codes.VAL_073, message="ignored")

    with collect_deprecations(file=None, flowgroup="fg") as collector:
        pass
    assert drain_deprecations(collector) == ()


def test_never_raises_on_a_malformed_code() -> None:
    """§5.6: recording a warning must never take the worker down — a code
    object without ``.code`` is dropped instead of raising."""
    with collect_deprecations(file=None, flowgroup="fg") as collector:
        record_validation_warning(object(), message="m")  # type: ignore[arg-type]  # deliberately malformed

    assert drain_deprecations(collector) == ()


def test_dedups_by_code_file_keeping_the_first_message() -> None:
    src = Path("pipelines/p/fg.yaml")
    with collect_deprecations(file=src, flowgroup="fg") as collector:
        record_validation_warning(codes.VAL_073, message="first")
        record_validation_warning(codes.VAL_073, message="second")

    assert [r.message for r in drain_deprecations(collector)] == ["first"]
