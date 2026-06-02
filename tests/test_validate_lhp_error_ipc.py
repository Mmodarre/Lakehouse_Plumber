"""IPC round-trip tests for LHPError instances on the validate path.

The consolidated flat engine carries the live worker exception across the
``spawn`` boundary on :class:`~lhp.models.processing.FlowgroupOutcome`'s
structured ``lhp_error`` field. The worker pickles the outcome via the pool's
result channel (which uses ``pickle``); the live :class:`~lhp.errors.LHPError`
inside it travels through :meth:`LHPError.__reduce__`, so the main thread
unpickles it without losing subclass identity, ``code``, ``context``, or
``suggestions``. The validate consumer
(:func:`._pool.assemble_validate_outcomes`) then folds that ``lhp_error`` into
a :class:`~lhp.models.processing.ValidationIssueRecord` on
:attr:`PipelineValidationOutcome.issues`.

These tests cover:

- Plain :class:`LHPValidationError` round-trip on :class:`FlowgroupOutcome`.
- Subclasses with custom ``__init__`` (:class:`PythonFunctionConflictError`,
  :class:`MultiDocumentError`) — they REQUIRE a working ``__reduce__``
  override.
- Non-LHP exceptions fall back to the string projection (``errors``
  populated, ``lhp_error`` None).
- End-to-end: an LHPError on a worker's ``FlowgroupOutcome`` survives the
  pickle and surfaces as a structured ``ValidationIssueRecord`` on
  :attr:`PipelineValidationOutcome.issues` (carrying the live error, not a
  string) through the real assemble fold.
"""

from __future__ import annotations

import pickle
from pathlib import Path

from lhp.core.coordination._pool import _PipelinePoolResult, assemble_validate_outcomes
from lhp.errors import (
    ErrorCategory,
    LHPError,
    LHPValidationError,
    MultiDocumentError,
    PythonFunctionConflictError,
)
from lhp.models.processing import FlowgroupOutcome


def test_lhp_error_survives_worker_to_main_on_flowgroup_outcome():
    """Round-trip a FlowgroupOutcome carrying an LHPError.

    Mirrors the worker->main IPC path: the worker builds a failure outcome
    via :meth:`FlowgroupOutcome.failure`, ``pickle.dumps`` (what the process
    pool uses to send the result over the spawn channel), then the main
    thread ``pickle.loads`` it. ``code``, ``title``, ``details``,
    ``context``, ``suggestions``, and subclass identity must survive.
    """
    err = LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="007",
        title="Test validation error",
        details="An action references an unknown view",
        context={"action": "load_x", "missing_view": "x_v"},
        suggestions=["Define x_v first", "Check spelling"],
    )
    outcome = FlowgroupOutcome.failure(
        "bronze",
        "fg1",
        lhp_error=err,
    )
    restored = pickle.loads(pickle.dumps(outcome))

    assert isinstance(restored, FlowgroupOutcome)
    assert restored.success is False
    assert restored.pipeline == "bronze"
    assert restored.flowgroup_name == "fg1"
    assert isinstance(restored.lhp_error, LHPValidationError)
    assert restored.lhp_error.code == "LHP-VAL-007"
    assert restored.lhp_error.title == "Test validation error"
    assert restored.lhp_error.context == {
        "action": "load_x",
        "missing_view": "x_v",
    }
    assert restored.lhp_error.suggestions == ["Define x_v first", "Check spelling"]
    # Per-fg string and structured channels are mutually exclusive on failure
    # when an LHPError is supplied: the live error rides the structured channel.
    assert restored.errors == ()


def test_non_lhp_exception_no_lhp_error_field():
    """Non-LHP exceptions fall back to the string projection.

    The worker chooses the bucket based on ``isinstance(exc, LHPError)``.
    A KeyError -> ``errors=(string,)``, ``lhp_error=None``.
    """
    outcome = FlowgroupOutcome.failure(
        "bronze",
        "fg1",
        errors=("Flowgroup 'fg1': KeyError: 'missing_key'",),
    )
    restored = pickle.loads(pickle.dumps(outcome))
    assert restored.lhp_error is None
    assert restored.errors == ("Flowgroup 'fg1': KeyError: 'missing_key'",)
    assert restored.success is False


def test_python_function_conflict_error_round_trip():
    """PythonFunctionConflictError has a custom __init__; its __reduce__
    override must keep IPC round-trip working when carried via
    FlowgroupOutcome.
    """
    err = PythonFunctionConflictError(
        destination=Path("/tmp/out/foo.py"),
        existing_source=Path("/tmp/src/a.py"),
        new_source=Path("/tmp/src/b.py"),
    )
    outcome = FlowgroupOutcome.failure("silver", "fg2", lhp_error=err)
    restored = pickle.loads(pickle.dumps(outcome))

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
    outcome = FlowgroupOutcome.failure("gold", "fg3", lhp_error=err)
    restored = pickle.loads(pickle.dumps(outcome))

    assert isinstance(restored.lhp_error, MultiDocumentError)
    assert restored.lhp_error.file_path == Path("/tmp/fg.yaml")
    assert restored.lhp_error.num_documents == 3
    assert restored.lhp_error.error_context == "duplicated flowgroup definitions"
    assert restored.lhp_error.code == "LHP-IO-003"


def test_worker_lhp_error_surfaces_in_pipeline_outcome_issues():
    """An LHPError on a worker outcome lands on ``issues`` post-IPC.

    End-to-end of the validate IPC contract: a worker's
    :class:`FlowgroupOutcome` carrying an LHPError is pickled (the spawn
    channel), unpickled on the main thread, bucketed into a
    :class:`_PipelinePoolResult`, and folded by the REAL
    :func:`assemble_validate_outcomes`. The structured error must surface as a
    :class:`~lhp.models.processing.ValidationIssueRecord` on
    :attr:`PipelineValidationOutcome.issues` whose ``issue`` is the live
    LHPError (NOT a stringified projection); the live instance identity is
    preserved across the fold, and the finding is attributed to its
    originating flowgroup (``fg1``). No ``source_paths`` map is supplied here,
    so ``source_file`` defaults to ``None``.
    """
    err = LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="007",
        title="An action references an unknown view",
        details="v_missing is not defined",
        context={"action": "load_x"},
        suggestions=["Define v_missing first"],
    )
    worker_outcome = FlowgroupOutcome.failure("bronze", "fg1", lhp_error=err)

    # The worker->main spawn hop: pickle the outcome and rebuild it.
    restored_outcome = pickle.loads(pickle.dumps(worker_outcome))

    # The engine buckets per-pipeline results into _PipelinePoolResult; the
    # cross-fg barrier produced no issues here (clean resolved set).
    pool_results = [
        _PipelinePoolResult(
            pipeline="bronze",
            outcomes_in_order=(restored_outcome,),
            cross_fg_issues=(),
            cross_fg_errors=(),
        )
    ]

    outcomes = assemble_validate_outcomes(pool_results, discovery_errors={})

    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert outcome.pipeline == "bronze"
    assert outcome.success is False
    # Exactly one finding, carrying the live structured error (not a string).
    assert len(outcome.issues) == 1
    record = outcome.issues[0]
    assert record.severity == "error"
    assert not isinstance(record.issue, str)
    surfaced: LHPError = record.issue
    assert isinstance(surfaced, LHPValidationError)
    assert surfaced.code == "LHP-VAL-007"
    assert surfaced.context == {"action": "load_x"}
    assert surfaced.suggestions == ["Define v_missing first"]
    # Identity is preserved through the fold (the restored instance is reused).
    assert surfaced is restored_outcome.lhp_error
    # Per-issue attribution: tagged with its flowgroup; no source map → no file.
    assert record.flowgroup_name == "fg1"
    assert record.source_file is None
