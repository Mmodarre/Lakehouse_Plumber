"""Fold a terminal validation response into the renderer-accumulated outcome.

The renderers populate :class:`RunOutcome.failures` / ``warnings`` purely
from in-stream events (``PipelineFailed`` / ``ErrorEmitted`` /
``WarningEmitted``). That is complete for generate and plan: those paths
raise on fatal preflight (``render`` re-raises) and surface per-pipeline
failures as ``PipelineFailed`` events. Validate is different: it DOES emit a
``PipelineFailed`` per failing pipeline (for live progress), but it never
raises on findings — it ALSO folds every validation issue, fully attributed
(pipeline / flowgroup / file / code / title), into the terminal
:class:`BatchValidationResponse`. Those two surfaces overlap: each failing
pipeline appears BOTH as an event-derived ``FailureLine`` (no attribution)
AND as a terminal issue, so naively appending would DOUBLE-COUNT every
validate failure (one issue -> "2 failed").

:func:`merge_terminal_validation` resolves that in the single ``RunOutcome``
builder (``renderer_factory.render``): for a :class:`BatchValidationResponse`
terminal the response is the AUTHORITATIVE, fully-attributed failure set, so
this REPLACES the event-accumulated failures with the response-derived ones
(it does not append). The terminal set is complete — every pipeline that
emits ``PipelineFailed`` has its outcome recorded in the batch's
``pipeline_responses`` with at least one error-severity issue (see
``_validation_facade._consume_validate_stream`` /
``_validation_converters._build_validation_batch``), so nothing is lost by
replacing. A batch-level-only failure (``error_code`` / ``error_message`` with
no per-pipeline issues — a project preflight folded for the non-raising
validate path) yields exactly one synthetic failure. WARNINGS are kept as the
UNION of the event-derived (deprecation ``WarningEmitted``) and the terminal
warning-issues, deduped on ``(code, file)``. For any other terminal (generate
/ plan) the outcome is returned unchanged — those failures stay event-derived.

Sole-bridge invariant (constitution §9.5): this module reads the public
``lhp.api`` DTOs (allowed) but MUST NOT import ``lhp.errors``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from lhp.api.responses import BatchValidationResponse

from ._model import FailureLine, RunOutcome, WarningLine

if TYPE_CHECKING:
    from lhp.api.views import ValidationIssueView


def merge_terminal_validation(outcome: RunOutcome, response: object) -> RunOutcome:
    """Return ``outcome`` reconciled with the terminal validation response.

    For a :class:`BatchValidationResponse` terminal the response is the
    authoritative, fully-attributed failure set, so the renderer-accumulated
    (event-derived, unattributed) failures are REPLACED — not appended — with
    one :class:`FailureLine` per error-severity issue. This is what prevents
    double-counting: each failing pipeline already emitted a ``PipelineFailed``
    event that the renderer folded into ``outcome.failures``, and that same
    pipeline also rides the terminal response. A failed batch carrying only a
    batch-level ``error_code`` / ``error_message`` (a preflight folded for
    validate, with no per-pipeline issues) yields exactly one synthetic
    :class:`FailureLine`.

    Warnings are the UNION of the event-derived warnings (deprecation
    ``WarningEmitted``) already on ``outcome`` and every warning-severity issue
    on the response, deduped on ``(code, file)`` — the two warning sources do
    not fully overlap (e.g. the discover-phase ``LHP-DEPR-001`` scan), so they
    must be merged rather than replaced.

    For any non-validation terminal (generate / plan) the outcome is
    returned unchanged — those paths keep their event-derived failures.
    """
    if not isinstance(response, BatchValidationResponse):
        return outcome

    terminal_failures: List[FailureLine] = []
    terminal_warnings: List[WarningLine] = []
    for issue in _iter_validation_issues(response):
        if issue.severity == "error":
            terminal_failures.append(_failure_from_issue(issue))
        elif issue.severity == "warning":
            terminal_warnings.append(_warning_from_issue(issue))

    if not terminal_failures and _is_batch_level_failure(response):
        terminal_failures.append(_failure_from_batch(response))

    return RunOutcome(
        response=outcome.response,
        # UNION the event-derived warnings with the terminal warning-issues.
        warnings=_dedup_warnings(outcome.warnings + tuple(terminal_warnings)),
        # REPLACE event-derived failures: the terminal set is authoritative
        # and complete, so appending would double-count each PipelineFailed.
        failures=tuple(terminal_failures),
        errored=outcome.errored,
    )


def _dedup_warnings(
    warnings: "tuple[WarningLine, ...]",
) -> "tuple[WarningLine, ...]":
    """Drop duplicate warnings sharing a ``(code, file)`` identity.

    Preserves first-seen order so the union of event-derived and
    terminal-derived warnings surfaces each ``(code, file)`` once — mirroring
    the facade's own ``(code, file)`` dedup of the discover-phase and worker
    deprecation scans.
    """
    seen: set[tuple[str, Optional[str]]] = set()
    deduped: List[WarningLine] = []
    for warning in warnings:
        key = (warning.code, warning.file)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(warning)
    return tuple(deduped)


def _iter_validation_issues(
    response: BatchValidationResponse,
) -> "tuple[ValidationIssueView, ...]":
    """Flatten every per-pipeline ``ValidationIssueView`` in arrival order."""
    collected: list = []
    for per_pipeline in response.pipeline_responses.values():
        collected.extend(per_pipeline.issues)
    return tuple(collected)


def _is_batch_level_failure(response: BatchValidationResponse) -> bool:
    """A failed batch whose detail lives only on the batch-level error fields.

    True when the batch failed and carries an ``error_code`` or
    ``error_message`` but has no per-pipeline issues — the shape produced by
    folding a project preflight (e.g. ``CFG-023`` / ``CFG-026``) for the
    non-raising validate path.
    """
    return (
        not response.success
        and (response.error_code is not None or response.error_message is not None)
        and not response.pipeline_responses
    )


def is_preflight_failure(response: object) -> bool:
    """True when ``response`` is a preflight-folded batch-level validation failure.

    The non-raising validate path (``_validation_facade``) folds a project
    preflight (e.g. ``CFG-023`` / ``CFG-026``) into a
    :class:`BatchValidationResponse` that failed, carries an ``error_code`` /
    ``error_message``, and has NO per-pipeline issues — the run stopped before
    any pipeline was validated. The summary presenter uses this to state the
    stop EXPLICITLY ("preflight failed — later stages not run") rather than
    rendering the lone synthetic failure with an empty pipeline name, which
    reads as a missing stage. Returns ``False`` for any non-validation terminal
    or an ordinary per-pipeline failure.
    """
    return isinstance(response, BatchValidationResponse) and _is_batch_level_failure(
        response
    )


_PREFLIGHT_STOP_PHRASE = "preflight failed — later stages not run"


def preflight_failure_text(failure: FailureLine) -> str:
    """Plain text for the explicit preflight-stop summary line.

    Returns ``CODE  preflight failed — later stages not run  ·  <message>`` (the
    code and message segments are dropped when absent). Pure string assembly —
    the summary presenter wraps the result in a Rich ``Text`` for styling, so
    this module stays Rich-free. Pairs with :func:`is_preflight_failure`.
    """
    parts: List[str] = []
    if failure.code:
        parts.append(failure.code)
    parts.append(_PREFLIGHT_STOP_PHRASE)
    text = "  ".join(parts)
    if failure.message:
        text = f"{text}  ·  {failure.message}"
    return text


def _failure_from_issue(issue: "ValidationIssueView") -> FailureLine:
    """Map an error-severity issue to a :class:`FailureLine`."""
    return FailureLine(
        pipeline=issue.pipeline_name or "",
        code=issue.code,
        message=issue.title,
        flowgroup=issue.flowgroup_name,
        file=str(issue.file_path) if issue.file_path else None,
    )


def _warning_from_issue(issue: "ValidationIssueView") -> WarningLine:
    """Map a warning-severity issue to a :class:`WarningLine`."""
    return WarningLine(
        code=issue.code,
        message=issue.title,
        file=str(issue.file_path) if issue.file_path else None,
    )


def _failure_from_batch(response: BatchValidationResponse) -> FailureLine:
    """One synthetic failure for a batch-level-only validation failure."""
    return FailureLine(
        pipeline="",
        code=response.error_code or "",
        message=response.error_message or "Validation failed",
    )
