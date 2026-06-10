"""Private converters for the validation direction of the public API.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Builds the public validation DTOs (:class:`ValidationResponse`,
:class:`BatchValidationResponse`) from internal per-pipeline outcomes and
preflight issues. Shared error-conversion helpers live in
:mod:`lhp.api._converters_common`.

:stability: internal
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional

from lhp.api._converters_common import _lhp_error_to_issue_view
from lhp.api.responses import (
    BatchValidationResponse,
    ValidationResponse,
)
from lhp.api.views import ValidationIssueView

if TYPE_CHECKING:
    from lhp.core.coordination.executor import PipelineValidationOutcome


def _outcome_to_validation_response(
    outcome: "PipelineValidationOutcome",
) -> ValidationResponse:
    """Build the public :class:`ValidationResponse` for a per-pipeline outcome.

    Each internal
    :class:`~lhp.models.processing.ValidationIssueRecord` on ``outcome.issues``
    becomes one public :class:`ValidationIssueView`, carrying through the
    record's per-issue attribution (``flowgroup_name`` + ``source_file`` →
    ``file_path``). A structured finding (the record's ``issue`` is a live
    :class:`~lhp.errors.LHPError`) projects via
    :func:`_lhp_error_to_issue_view` so ``code`` / ``suggestions`` / ``context``
    survive; a degraded string finding (legacy CDC fan-in, discovery, the
    empty-pipeline message) becomes a code-less ``"VAL"`` view. Cross-flowgroup
    / discovery findings legitimately carry ``flowgroup_name`` / ``file_path``
    ``None``.
    """
    from lhp.errors import LHPError

    issues: List[ValidationIssueView] = []
    for record in outcome.issues:
        if isinstance(record.issue, LHPError):
            issues.append(
                _lhp_error_to_issue_view(
                    record.issue,
                    pipeline_name=outcome.pipeline,
                    flowgroup_name=record.flowgroup_name,
                    file_path=record.source_file,
                    severity=record.severity,
                )
            )
        else:
            issues.append(
                ValidationIssueView(
                    code="",
                    category="VAL",
                    severity=record.severity,
                    title=record.issue,
                    pipeline_name=outcome.pipeline,
                    flowgroup_name=record.flowgroup_name,
                    file_path=record.source_file,
                )
            )
    return ValidationResponse(
        success=outcome.success,
        issues=tuple(issues),
        validated_pipelines=(outcome.pipeline,),
    )


def _build_validation_batch(
    pipeline_responses: Dict[str, ValidationResponse],
    pipeline_fields: tuple[str, ...],
    *,
    exc: Optional[Exception] = None,
) -> BatchValidationResponse:
    """Aggregate per-pipeline responses into a batch validation response.

    When ``exc`` is provided, the batch is marked failed and the
    exception's LHP code (if any) is preserved on the DTO so the CLI
    fail-fast boundary can map it to an exit code (§4.8).
    """
    total_errors = sum(r.error_count for r in pipeline_responses.values())
    total_warnings = sum(r.warning_count for r in pipeline_responses.values())
    if exc is None:
        return BatchValidationResponse(
            success=total_errors == 0,
            pipeline_responses=dict(pipeline_responses),
            total_errors=total_errors,
            total_warnings=total_warnings,
            validated_pipelines=pipeline_fields,
        )
    return BatchValidationResponse(
        success=False,
        pipeline_responses=dict(pipeline_responses),
        total_errors=total_errors,
        total_warnings=total_warnings,
        validated_pipelines=pipeline_fields,
        error_message=str(exc),
        error_code=getattr(exc, "code", None),
    )


def _build_validation_batch_from_issues(
    issues: tuple[ValidationIssueView, ...],
    pipeline_fields: tuple[str, ...],
) -> BatchValidationResponse:
    """Fold project-preflight issues into a failed batch validation response.

    Companion to :func:`_build_validation_batch`. Where that helper marks a
    batch failed from a live ``exc`` (setting ``error_code`` /
    ``error_message`` off the exception), this one does the same from the
    already-decomposed :class:`ValidationIssueView` tuple returned by
    :func:`lhp.api._preflight._run_project_preflight` — the validate path's
    surfacing of the shared §9.24 preflight. The first structured issue (one
    carrying an ``LHP-...`` ``code``) drives ``error_code`` / ``error_message``
    so the CLI fail-fast boundary maps it to a non-zero exit (§4.8); the
    message mirrors ``str(LHPError)`` (``Error [<code>]: <title>``) so the code
    surfaces in the printed batch-failure line. ``pipeline_responses`` is empty
    because preflight issues are project-level, not per-pipeline.

    Precondition: ``issues`` is non-empty (callers short-circuit only when
    preflight returned issues). A defensive empty-input guard still yields a
    failed batch so a folded call never reports success.
    """
    first_structured = next((i for i in issues if i.code), None)
    if first_structured is not None:
        error_code: Optional[str] = first_structured.code
        error_message: Optional[str] = (
            f"Error [{first_structured.code}]: {first_structured.title}"
        )
    else:
        error_code = None
        error_message = (
            issues[0].title if issues else "Project preflight validation failed"
        )
    return BatchValidationResponse(
        success=False,
        pipeline_responses={},
        total_errors=len(issues),
        total_warnings=0,
        validated_pipelines=pipeline_fields,
        error_message=error_message,
        error_code=error_code,
    )
