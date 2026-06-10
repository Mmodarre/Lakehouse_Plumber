"""Synthetic event-stream fixtures for the event-stream renderers.

Builders here return lists (or, for the failure-rendezvous case, a
generator) of :class:`lhp.api.events.LHPEvent` in constitution §5.7
order: exactly one ``OperationStarted`` first, phase pairs and
per-pipeline pairs in the middle, and exactly one terminal
``*Completed`` last on the non-raising paths.

This is a TEST helper module living under ``tests/`` — not under
``cli/presenters/`` — so it is exempt from the sole-bridge invariant
(§9.5) and MAY import :mod:`lhp.errors` to mint a real
:class:`~lhp.errors.LHPError` for the failure-rendezvous fixture.

Importable as::

    from tests.cli.presenters.event_stream._fixtures import (
        clean_generate_stream,
    )
"""

from __future__ import annotations

import contextlib
import io
from typing import Iterator, List

from lhp.api import (
    BatchGenerationResponse,
    BatchValidationResponse,
    GenerationCompleted,
    GenerationPlan,
    GenerationResponse,
    LHPEvent,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    ValidationCompleted,
    ValidationResponse,
    WarningEmitted,
)
from lhp.api.events import ErrorEmitted, GenerationPlanCompleted
from lhp.errors import ErrorFactory
from lhp.errors.codes import VAL_021


# ---------------------------------------------------------------------------
# Minimal valid response DTOs
# ---------------------------------------------------------------------------
def _generation_response(*, success: bool, error_code: str | None = None):
    """A minimal valid :class:`GenerationResponse`."""
    return GenerationResponse(
        success=success,
        generated_filenames=("bronze_ingest.py",) if success else (),
        files_written=1 if success else 0,
        total_flowgroups=1,
        output_location=None,
        performance_info={},
        error_message=None if success else "pipeline failed",
        error_code=error_code,
    )


def _batch_generation_response(
    *, success: bool, pipeline_responses: dict[str, GenerationResponse]
):
    """A minimal valid :class:`BatchGenerationResponse`."""
    total_written = sum(r.files_written for r in pipeline_responses.values())
    filenames = tuple(
        name for r in pipeline_responses.values() for name in r.generated_filenames
    )
    return BatchGenerationResponse(
        success=success,
        pipeline_responses=pipeline_responses,
        total_files_written=total_written,
        aggregate_generated_filenames=filenames,
        output_location=None,
        error_message=None if success else "one or more pipelines failed",
        error_code=None,
    )


def _batch_validation_response(*, success: bool):
    """A minimal valid :class:`BatchValidationResponse`."""
    return BatchValidationResponse(
        success=success,
        pipeline_responses={
            "bronze": ValidationResponse(
                success=success,
                issues=(),
                validated_pipelines=("bronze",),
            )
        },
        total_errors=0 if success else 1,
        total_warnings=0,
        validated_pipelines=("bronze",),
    )


# ---------------------------------------------------------------------------
# Stream builders (§5.7 order)
# ---------------------------------------------------------------------------
def clean_generate_stream() -> List[LHPEvent]:
    """A fully successful generate stream.

    ``OperationStarted`` -> three phase pairs (discover / preflight /
    generate) -> two pipeline pairs -> terminal ``GenerationCompleted``.
    The last element is the terminal event.
    """
    responses = {
        "bronze": _generation_response(success=True),
        "silver": _generation_response(success=True),
    }
    return [
        OperationStarted(operation_name="generate", env="dev"),
        PhaseStarted(phase="discover"),
        PhaseCompleted(phase="discover", duration_s=0.01, success=True),
        PhaseStarted(phase="preflight"),
        PhaseCompleted(phase="preflight", duration_s=0.02, success=True),
        PhaseStarted(phase="generate"),
        PipelineStarted(pipeline="bronze"),
        PipelineCompleted(pipeline="bronze", duration_s=0.10, files_written=1),
        PipelineStarted(pipeline="silver"),
        PipelineCompleted(pipeline="silver", duration_s=0.12, files_written=1),
        PhaseCompleted(phase="generate", duration_s=0.30, success=True),
        GenerationCompleted(
            response=_batch_generation_response(
                success=True, pipeline_responses=responses
            )
        ),
    ]


def one_failure_stream() -> List[LHPEvent]:
    """A generate stream where one pipeline fails but the run does not raise.

    Includes a ``PipelineFailed`` and a terminal ``GenerationCompleted``
    whose :class:`BatchGenerationResponse` reflects the failure
    (``success=False`` with the failing pipeline's per-pipeline response
    carrying the error code).
    """
    responses = {
        "bronze": _generation_response(success=True),
        "silver": _generation_response(success=False, error_code="LHP-VAL-021"),
    }
    return [
        OperationStarted(operation_name="generate", env="dev"),
        PhaseStarted(phase="generate"),
        PipelineStarted(pipeline="bronze"),
        PipelineCompleted(pipeline="bronze", duration_s=0.10, files_written=1),
        PipelineStarted(pipeline="silver"),
        PipelineFailed(
            pipeline="silver",
            code="LHP-VAL-021",
            message="invalid action configuration",
        ),
        PhaseCompleted(phase="generate", duration_s=0.30, success=False),
        GenerationCompleted(
            response=_batch_generation_response(
                success=False, pipeline_responses=responses
            )
        ),
    ]


def deduped_warnings_stream() -> List[LHPEvent]:
    """A generate stream with repeated ``(code, file)`` warning pairs.

    Several ``WarningEmitted`` events share the same ``(code, file)``
    identity so a renderer's dedup logic has something to collapse. Ends
    with a terminal ``GenerationCompleted``.
    """
    responses = {"bronze": _generation_response(success=True)}
    return [
        OperationStarted(operation_name="generate", env="dev"),
        PhaseStarted(phase="generate"),
        WarningEmitted(
            message="deprecated field 'foo'",
            code="LHP-DEPR-001",
            category="DEPR",
            file=None,
        ),
        WarningEmitted(
            message="deprecated field 'foo'",
            code="LHP-DEPR-001",
            category="DEPR",
            file=None,
        ),
        WarningEmitted(
            message="deprecated field 'bar'",
            code="LHP-DEPR-002",
            category="DEPR",
            file=None,
        ),
        WarningEmitted(
            message="deprecated field 'bar'",
            code="LHP-DEPR-002",
            category="DEPR",
            file=None,
        ),
        PipelineStarted(pipeline="bronze"),
        PipelineCompleted(pipeline="bronze", duration_s=0.10, files_written=1),
        PhaseCompleted(phase="generate", duration_s=0.30, success=True),
        GenerationCompleted(
            response=_batch_generation_response(
                success=True, pipeline_responses=responses
            )
        ),
    ]


def clean_validate_stream() -> List[LHPEvent]:
    """A successful validate stream ending in a terminal ``ValidationCompleted``.

    Validate never raises on issues — even the failure case folds into a
    terminal whose :class:`BatchValidationResponse` carries
    ``success=False``. This builder is the all-clear variant.
    """
    return [
        OperationStarted(operation_name="validate", env="dev"),
        PhaseStarted(phase="validate"),
        PipelineStarted(pipeline="bronze"),
        PipelineCompleted(pipeline="bronze", duration_s=0.05, files_written=0),
        PhaseCompleted(phase="validate", duration_s=0.05, success=True),
        ValidationCompleted(response=_batch_validation_response(success=True)),
    ]


def clean_plan_stream() -> List[LHPEvent]:
    """A successful plan stream ending in a terminal ``GenerationPlanCompleted``.

    Models the path the ``diff`` command drives the live renderer over
    (``facade.generation.plan_generation``): ``OperationStarted`` -> three phase
    pairs (``discover`` / ``preflight`` / ``generate``) -> a pipeline pair ->
    terminal ``GenerationPlanCompleted`` carrying a (here empty) plan. Like the
    real plan stream — and unlike the generate stream — it has NO ``format``
    phase (the dry-run plan never writes or formats files), so a renderer driven
    by it must never surface a ``format`` stage line.
    """
    plan = GenerationPlan(
        files=(),
        output_location=None,
        pipeline_count=1,
        file_count=0,
    )
    return [
        OperationStarted(operation_name="diff", env="dev"),
        PhaseStarted(phase="discover"),
        PhaseCompleted(phase="discover", duration_s=0.01, success=True),
        PhaseStarted(phase="preflight"),
        PhaseCompleted(phase="preflight", duration_s=0.02, success=True),
        PhaseStarted(phase="generate"),
        PipelineStarted(pipeline="bronze"),
        PipelineCompleted(pipeline="bronze", duration_s=0.10, files_written=0),
        PhaseCompleted(phase="generate", duration_s=0.30, success=True),
        GenerationPlanCompleted(response=plan),
    ]


def error_raise_stream() -> Iterator[LHPEvent]:
    """A GENERATOR that yields ``ErrorEmitted`` then RAISES the same ``LHPError``.

    Models the generate/plan failure-rendezvous: the live exception is
    surfaced as data via :class:`ErrorEmitted` immediately before it
    propagates out of the generator. The terminal ``*Completed`` is never
    reached.
    """
    error = ErrorFactory.validation_error(
        VAL_021,
        title="Invalid action configuration",
        details="The 'silver' flowgroup has an unsupported transform type.",
    )
    yield OperationStarted(operation_name="generate", env="dev")
    yield PhaseStarted(phase="generate")
    yield ErrorEmitted(lhp_error=error)
    raise error


# ---------------------------------------------------------------------------
# stderr console capture (parallel of tests.conftest.capture_lhp_console)
# ---------------------------------------------------------------------------
@contextlib.contextmanager
def capture_err_console(width: int = 80):
    """Capture rendered output from ``lhp.cli.console.err_console``.

    The stderr parallel of :func:`tests.conftest.capture_lhp_console`:
    swaps the module-level ``err_console`` singleton for a deterministic
    no-color in-memory ``Console`` at the given width, yields the backing
    StringIO buffer, then restores the prior console on exit. Composes
    with the autouse ``_isolate_lhp_console`` fixture exactly as the
    stdout helper does.
    """
    from rich.console import Console

    import lhp.cli.console as _lhp_console_module

    buf = io.StringIO()
    fake = Console(
        file=buf, stderr=True, force_terminal=False, no_color=True, width=width
    )
    saved = _lhp_console_module.err_console
    _lhp_console_module.err_console = fake
    try:
        yield buf
    finally:
        _lhp_console_module.err_console = saved
