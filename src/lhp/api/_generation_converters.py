"""Private converters for the generation direction of the public API.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Builds the public generation DTOs (:class:`GenerationResponse`,
:class:`BatchGenerationResponse`) from internal worker deltas and
aggregated per-pipeline responses. Shared error-conversion helpers live
in :mod:`lhp.api._converters_common`.

:stability: internal
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

from lhp.api._converters_common import (
    _lhp_error_to_issue_view,
)
from lhp.api.responses import (
    BatchGenerationResponse,
    GenerationPlan,
    GenerationResponse,
    PlannedFileView,
)
from lhp.api.views import ValidationIssueView

if TYPE_CHECKING:
    from lhp.core.codegen import GenerationPlanResult
    from lhp.errors import LHPError
    from lhp.models.processing import PipelineDelta


def _delta_to_generation_response(
    delta: "PipelineDelta", *, output_dir: Optional[Path]
) -> GenerationResponse:
    """Build the public :class:`GenerationResponse` for a worker delta.

    Synthesises an :class:`LHPError` when the worker did not produce one
    so callers' ``except ValueError:`` / ``except FileNotFoundError:``
    keep catching worker failures the same way they catch main-thread
    failures (preserves the dual-inheritance subclass identity).
    """
    from lhp.errors import lhp_error_from_worker_failure

    error_message: Optional[str] = None
    error_code: Optional[str] = None
    error_view: Optional[ValidationIssueView] = None
    if not delta.success:
        error_message = (
            f"{delta.error_type}: {delta.error_message}"
            if delta.error_type
            else delta.error_message
        )
        if delta.lhp_error is not None:
            lhp_err: LHPError = delta.lhp_error
        else:
            lhp_err = lhp_error_from_worker_failure(
                pipeline_name=delta.pipeline_name,
                error_type=delta.error_type or "UnknownError",
                error_message=delta.error_message or "(no message)",
                error_traceback=delta.error_traceback or "",
            )
        error_code = lhp_err.code
        error_view = _lhp_error_to_issue_view(
            lhp_err, pipeline_name=delta.pipeline_name
        )

    return GenerationResponse(
        success=delta.success,
        generated_filenames=delta.generated_filenames,
        files_written=delta.files_written,
        total_flowgroups=len(delta.generated_filenames),
        output_location=output_dir,
        performance_info={"dry_run": output_dir is None},
        duration_s=delta.duration_s,
        error_message=error_message,
        error_code=error_code,
        error=error_view,
    )


def _build_generation_batch_success(
    pipeline_responses: Dict[str, GenerationResponse],
    *,
    output_dir: Optional[Path],
) -> BatchGenerationResponse:
    """Aggregate per-pipeline responses into a successful batch response."""
    aggregate: tuple[str, ...] = ()
    total_written = 0
    for r in pipeline_responses.values():
        aggregate = aggregate + r.generated_filenames
        total_written += r.files_written
    return BatchGenerationResponse(
        success=True,
        pipeline_responses=dict(pipeline_responses),
        total_files_written=total_written,
        aggregate_generated_filenames=aggregate,
        output_location=output_dir,
    )


def _build_generation_batch_failure(
    pipeline_responses: Dict[str, GenerationResponse], exc: Exception
) -> BatchGenerationResponse:
    """Aggregate per-pipeline responses into a failure batch response.

    Only successful pipeline outputs are included in the aggregate
    filename / counter totals; the failing exception's LHP code (if any)
    is propagated as ``error_code`` so the CLI fail-fast boundary can
    map it to an exit code without handling the live exception.
    """
    aggregate: tuple[str, ...] = ()
    total_written = 0
    for r in pipeline_responses.values():
        if r.success:
            aggregate = aggregate + r.generated_filenames
            total_written += r.files_written
    return BatchGenerationResponse(
        success=False,
        pipeline_responses=dict(pipeline_responses),
        total_files_written=total_written,
        aggregate_generated_filenames=aggregate,
        output_location=None,
        error_message=str(exc),
        error_code=getattr(exc, "code", None),
    )


def _generation_result_to_plan(
    result: "GenerationPlanResult", *, output_location: Optional[Path]
) -> GenerationPlan:
    """Map the core :class:`GenerationPlanResult` onto the public DTO.

    Direction is api→core (this converter lives in ``lhp.api`` and reads the
    core dataclass), so the §5.x layering boundary is respected — ``core``
    never imports ``lhp.api``. Each :class:`~lhp.core.codegen.PlannedArtifact`
    is converted 1:1 to a :class:`PlannedFileView` (``path``/``content``/
    ``pipeline``/``kind`` carried verbatim; ``kind`` is the same five-value
    ``Literal``). ``file_count`` is the artifact count; ``pipeline_count`` is
    the result's; ``output_location`` is supplied by the caller — the REAL
    ``generated/<env>`` directory a normal generate WOULD write to (the plan
    reports where the files would land even though it wrote only a temp tree).
    """
    files = tuple(
        PlannedFileView(
            path=artifact.path,
            content=artifact.content,
            pipeline=artifact.pipeline,
            kind=artifact.kind,
        )
        for artifact in result.artifacts
    )
    return GenerationPlan(
        files=files,
        output_location=output_location,
        pipeline_count=result.pipeline_count,
        file_count=len(files),
    )
