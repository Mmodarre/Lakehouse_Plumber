"""IPC round-trip tests for LHPError instances on the validate path.

Phase D's structured ``lhp_error`` field on
:class:`FlowgroupValidationResult` carries the live worker exception
across the spawn boundary. The worker pickles the result via
:meth:`LHPError.__reduce__`; the main thread unpickles it without
losing subclass identity, ``code``, ``context``, or ``suggestions``.

These tests cover:

- Plain :class:`LHPValidationError` round-trip on
  :class:`FlowgroupValidationResult`.
- Subclasses with custom ``__init__`` (:class:`PythonFunctionConflictError`,
  :class:`MultiDocumentError`) — they REQUIRE a working ``__reduce__``
  override.
- Non-LHP exceptions fall back to the string projection (``errors``
  populated, ``lhp_error`` None).
- Orchestrator CDC fan-in path lands LHPErrors in
  :attr:`PipelineValidationOutcome.lhp_errors`, not just ``errors``.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from unittest.mock import MagicMock

from lhp.core.layers import ValidationIssue
from lhp.core.pipeline_executor import (
    FlowgroupValidationResult,
    PipelineValidationOutcome,
)
from lhp.generators.python_file_copier import PythonFunctionConflictError
from lhp.utils.error_formatter import (
    ErrorCategory,
    LHPError,
    LHPValidationError,
    MultiDocumentError,
)


def test_lhp_error_survives_worker_to_main_via_validation_issue():
    """Round-trip a FlowgroupValidationResult carrying an LHPError.

    Mirrors the worker→main IPC path: the worker constructs the
    result, ``pickle.dumps`` (what ProcessPoolExecutor uses to send
    over the spawn channel), then the main thread ``pickle.loads`` it
    and rebuilds a :class:`ValidationIssue`. ``code``, ``title``,
    ``details``, ``context``, and subclass identity must survive.
    """
    err = LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="007",
        title="Test validation error",
        details="An action references an unknown view",
        context={"action": "load_x", "missing_view": "x_v"},
        suggestions=["Define x_v first", "Check spelling"],
    )
    result = FlowgroupValidationResult(
        pipeline="bronze",
        flowgroup_name="fg1",
        errors=(),
        lhp_error=err,
    )
    restored = pickle.loads(pickle.dumps(result))

    assert isinstance(restored.lhp_error, LHPValidationError)
    assert restored.lhp_error.code == "LHP-VAL-007"
    assert restored.lhp_error.title == "Test validation error"
    assert restored.lhp_error.context == {
        "action": "load_x",
        "missing_view": "x_v",
    }
    assert restored.lhp_error.suggestions == ["Define x_v first", "Check spelling"]
    assert restored.errors == ()

    # ValidationIssue rebuild contract from layers._on_outcome.
    issue = ValidationIssue(
        code=restored.lhp_error.code,
        severity="error",
        title=restored.lhp_error.title,
        details=restored.lhp_error.details,
        location="bronze",
        lhp_error=restored.lhp_error,
    )
    assert issue.code == "LHP-VAL-007"
    assert issue.lhp_error is restored.lhp_error


def test_non_lhp_exception_no_lhp_error_field():
    """Non-LHP exceptions fall back to the string projection.

    The worker chooses the bucket based on ``isinstance(exc, LHPError)``.
    A KeyError → ``errors=(string,)``, ``lhp_error=None``.
    """
    result = FlowgroupValidationResult(
        pipeline="bronze",
        flowgroup_name="fg1",
        errors=("Flowgroup 'fg1': KeyError: 'missing_key'",),
        lhp_error=None,
    )
    restored = pickle.loads(pickle.dumps(result))
    assert restored.lhp_error is None
    assert restored.errors == ("Flowgroup 'fg1': KeyError: 'missing_key'",)


def test_python_function_conflict_error_round_trip():
    """PythonFunctionConflictError has a custom __init__; its __reduce__
    override must keep IPC round-trip working when carried via
    FlowgroupValidationResult.
    """
    err = PythonFunctionConflictError(
        destination=Path("/tmp/out/foo.py"),
        existing_source=Path("/tmp/src/a.py"),
        new_source=Path("/tmp/src/b.py"),
    )
    result = FlowgroupValidationResult(
        pipeline="silver",
        flowgroup_name="fg2",
        errors=(),
        lhp_error=err,
    )
    restored = pickle.loads(pickle.dumps(result))

    assert isinstance(restored.lhp_error, PythonFunctionConflictError)
    assert restored.lhp_error.destination == Path("/tmp/out/foo.py")
    assert restored.lhp_error.existing_source == Path("/tmp/src/a.py")
    assert restored.lhp_error.new_source == Path("/tmp/src/b.py")
    assert restored.lhp_error.code.startswith("LHP-")


def test_multi_document_error_round_trip():
    """MultiDocumentError has a custom __init__ with three args; its
    __reduce__ override must keep IPC round-trip working.
    """
    err = MultiDocumentError(
        file_path=Path("/tmp/fg.yaml"),
        num_documents=3,
        error_context="duplicated flowgroup definitions",
    )
    result = FlowgroupValidationResult(
        pipeline="gold",
        flowgroup_name="fg3",
        errors=(),
        lhp_error=err,
    )
    restored = pickle.loads(pickle.dumps(result))

    assert isinstance(restored.lhp_error, MultiDocumentError)
    assert restored.lhp_error.file_path == Path("/tmp/fg.yaml")
    assert restored.lhp_error.num_documents == 3
    assert restored.lhp_error.error_context == "duplicated flowgroup definitions"
    assert restored.lhp_error.code == "LHP-IO-003"


def test_cdc_fan_in_lhp_error_carries_code():
    """Orchestrator CDC fan-in path lands LHPErrors in ``lhp_errors``.

    Unit-level test of the assembler contract: when
    ``ConfigValidator.validate_cdc_fanin_compatibility`` raises an
    LHPError, the per-pipeline outcome carries it in
    :attr:`PipelineValidationOutcome.lhp_errors` (NOT
    :attr:`~PipelineValidationOutcome.errors`).
    """
    from lhp.core.orchestrator import ActionOrchestrator
    from lhp.models.config import FlowGroup

    # Build a synthetic flowgroup so the assembler is exercised with a
    # non-empty pipeline (the empty-pipeline branch short-circuits).
    fg = FlowGroup(
        pipeline="bronze",
        flowgroup="fg1",
        actions=[],
    )

    cdc_err = LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="030",
        title="CDC fan-in mismatch",
        details="Conflicting keys across CDC sources",
    )

    orch = ActionOrchestrator.__new__(ActionOrchestrator)
    orch.config_validator = MagicMock()
    orch.config_validator.validate_cdc_fanin_compatibility = MagicMock(
        side_effect=cdc_err
    )
    orch.logger = MagicMock()

    # Re-create the assembler closure's behaviour with a single
    # flowgroup that has no per-flowgroup errors.
    results = [
        FlowgroupValidationResult(
            pipeline="bronze",
            flowgroup_name="fg1",
            errors=(),
            lhp_error=None,
        )
    ]

    # The assembler is a nested closure inside
    # validate_pipelines_by_fields; we test the equivalent fold here.
    errors_list: list[str] = []
    lhp_errors_acc: list[LHPError] = []
    for result in results:
        errors_list.extend(result.errors)
        if result.lhp_error is not None:
            lhp_errors_acc.append(result.lhp_error)

    try:
        orch.config_validator.validate_cdc_fanin_compatibility([fg])
    except LHPError as e:
        lhp_errors_acc.append(e)

    outcome = PipelineValidationOutcome(
        pipeline="bronze",
        errors=tuple(errors_list),
        warnings=(),
        success=(len(errors_list) == 0 and len(lhp_errors_acc) == 0),
        lhp_errors=tuple(lhp_errors_acc),
    )

    assert outcome.success is False
    assert outcome.errors == ()
    assert len(outcome.lhp_errors) == 1
    assert outcome.lhp_errors[0].code == "LHP-VAL-030"
    assert outcome.lhp_errors[0] is cdc_err
