"""Failure aggregation for the all-or-nothing generate GATE.

Coordinator-side helpers that shape a set of failing units into the
user-facing aggregate error. Split out of
:mod:`.executor` so the gate DRIVER there
(:meth:`~lhp.core.coordination.executor.PipelineExecutionService._run_generate_engine_and_gate`)
stays thin (constitution §3.3 size) and the single-vs-``902`` shaping is
single-sourced in :func:`raise_aggregate_failure`.

Three pieces:

- :class:`_AggFailure` — one failing unit normalized away from its source
  DTO (a :class:`~lhp.models.processing.PipelineDelta` for the legacy path,
  a :class:`._pool._PipelinePoolResult` for the gate).
- :func:`raise_aggregate_failure` — the shaping core: sole failure →
  re-raise verbatim / rebuild; many → synthesize ``LHP-VAL-902``.
- :func:`pipeline_failure_descriptor` — collapse ONE flat-engine
  per-pipeline result into an optional :class:`_AggFailure` (pipeline
  granularity, deterministic representative).

Runs entirely on the coordinator; nothing here crosses the ``spawn``
boundary. Depends only on :mod:`lhp.errors` (a leaf layer) and the
internal :class:`._pool._PipelinePoolResult` shape.

:stability: internal
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Tuple

from ...errors import (
    ErrorCategory,
    LHPError,
    LHPValidationError,
    lhp_error_from_worker_failure,
)
from ..codegen.python_file_copier import PythonFileCopier

if TYPE_CHECKING:
    from ...models.processing import PipelineDelta
    from ._pool import _PipelinePoolResult


@dataclass(frozen=True, slots=True)
class _AggFailure:
    """One failing unit, normalized for the shared single-vs-902 shaping.

    Decouples the aggregate-shaping mechanics (re-raise the live error for a
    sole failure vs. synthesize ``LHP-VAL-902`` for many) from the source
    DTO. The flat-engine generate gate builds one per failing PIPELINE via
    :func:`pipeline_failure_descriptor`.

    Fields:
        label_key: the ``902`` ``context`` map key (a pipeline name).
        label_value: the value shown when this is one of MANY failures —
            ``"LHP-VAL-007 (FlowGroup validation failed)"`` (live) or
            ``"RuntimeError (non-LHP exception)"`` (degraded).
        live_error: the live :class:`~lhp.errors.LHPError` re-raised verbatim
            when this is the SOLE failure. ``None`` for a non-LHP failure.
        rebuild: ``(pipeline_name, error_type, error_message, error_traceback)``
            used to rebuild via :func:`~lhp.errors.lhp_error_from_worker_failure`
            when this is the SOLE failure AND ``live_error`` is ``None``.
    """

    label_key: str
    label_value: str
    live_error: Optional["LHPError"] = None
    rebuild: Optional[Tuple[str, str, str, str]] = None


def raise_aggregate_failure(
    failures: Sequence[_AggFailure],
    *,
    noun: str,
    total: int,
) -> None:
    """Raise the appropriate aggregate error for a set of failing units.

    The single-sourced shaping core shared by the flat-engine generate gate
    (:func:`gate_or_raise`) and the discovery-failure abort in
    :meth:`~lhp.core.coordination.executor.PipelineExecutionService.run_generate`.
    Both reduce their own inputs to :class:`_AggFailure` descriptors and call
    this, so the risk-bearing branch — re-raise-live-vs-rebuild for a sole
    failure, synthesize ``LHP-VAL-902`` for many — never drifts between callers.

    - ``failures`` empty → returns ``None`` (no failure; caller proceeds).
    - exactly one → re-raise its ``live_error`` verbatim (preserving structured
      display + subclass identity), or rebuild via
      :func:`~lhp.errors.lhp_error_from_worker_failure` from its ``rebuild``
      tuple when no live error crossed the boundary.
    - more than one → raise ``LHP-VAL-902`` (:class:`LHPValidationError`)
      whose ``context`` maps each ``label_key`` → ``label_value``.

    ``noun`` / ``total`` parameterize only the ``902`` text (both callers pass
    ``noun="pipeline"``; ``total`` is the count of pipelines in the run).

    :raises LHPError: the sole failure's error, OR ``LHP-VAL-902`` for many.
    """
    if not failures:
        return

    if len(failures) == 1:
        f = failures[0]
        if f.live_error is not None:
            raise f.live_error
        pipeline_name, error_type, error_message, error_traceback = (
            f.rebuild
            if f.rebuild is not None
            else (f.label_key, "UnknownError", "", "")
        )
        raise lhp_error_from_worker_failure(
            pipeline_name=pipeline_name,
            error_type=error_type or "UnknownError",
            error_message=error_message or "(no message)",
            error_traceback=error_traceback or "",
        )

    by_pipeline: Dict[str, str] = {f.label_key: f.label_value for f in failures}
    raise LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="902",
        title=f"{len(failures)} {noun}(s) failed",
        details=(
            f"{len(failures)} of {total} {noun}s "
            f"failed during generation. See per-pipeline rows in the "
            f"summary table for full diagnostics."
        ),
        context={"failure_count": len(failures), **by_pipeline},
        suggestions=[
            "Inspect the summary table above for per-pipeline status",
            "Run 'lhp validate' for detailed per-flowgroup diagnostics",
        ],
    )


def pipeline_failure_descriptor(
    result: "_PipelinePoolResult",
) -> Optional[_AggFailure]:
    """Collapse one pipeline's flat-engine result into an aggregate descriptor.

    Returns ``None`` when the pipeline is clean (no per-flowgroup failure, no
    cross-flowgroup issue/error); otherwise ONE :class:`_AggFailure` keyed by
    pipeline name. The representative error is chosen deterministically
    (``outcomes_in_order`` is already sorted by flowgroup name):

    1. the first FAILING flowgroup carrying a live ``lhp_error`` (richest);
    2. else the first failing flowgroup's degraded ``errors`` (no structured
       type survived the worker — rebuilt from the joined strings);
    3. else the first cross-flowgroup ``lhp_error`` (structured §9.24 issue);
    4. else the first cross-flowgroup degraded string.

    Per-flowgroup failures take precedence over cross-flowgroup ones as the
    representative (the user's primary signal); a pipeline whose flowgroups
    all succeeded but which fails the cross-flowgroup barrier is still
    reported via arms 3/4.
    """
    failing = [o for o in result.outcomes_in_order if not o.success]
    if not failing and not result.cross_fg_issues and not result.cross_fg_errors:
        return None

    pipeline = result.pipeline

    # 1: first failing flowgroup with a live LHPError.
    for outcome in failing:
        if outcome.lhp_error is not None:
            return _AggFailure(
                label_key=pipeline,
                label_value=f"{outcome.lhp_error.code} ({outcome.lhp_error.title})",
                live_error=outcome.lhp_error,
            )

    # 2: first failing flowgroup with degraded string errors.
    for outcome in failing:
        if outcome.errors:
            joined = "; ".join(outcome.errors)
            return _AggFailure(
                label_key=pipeline,
                label_value=f"{pipeline}: worker failure (non-LHP)",
                rebuild=(pipeline, "WorkerError", joined, ""),
            )

    # 3: first structured cross-flowgroup issue.
    if result.cross_fg_issues:
        issue = result.cross_fg_issues[0]
        return _AggFailure(
            label_key=pipeline,
            label_value=f"{issue.code} ({issue.title})",
            live_error=issue,
        )

    # 4: first degraded cross-flowgroup string.
    return _AggFailure(
        label_key=pipeline,
        label_value=f"{pipeline}: {result.cross_fg_errors[0]}",
        rebuild=(pipeline, "CrossFlowgroupError", result.cross_fg_errors[0], ""),
    )


def descriptor_to_exception(descriptor: _AggFailure) -> "LHPError":
    """Materialize a single :class:`_AggFailure` into a live exception.

    The per-PIPELINE counterpart to the sole-failure arm of
    :func:`raise_aggregate_failure`, factored out so the generate driver can
    synthesize a :class:`~lhp.models.processing.PipelineDelta.failure` for EACH
    failing pipeline (feeding the summary table via
    ``on_generate_pipeline_complete``) BEFORE the gate raises the aggregate —
    without duplicating the live-vs-``rebuild`` rule. Prefers the live
    ``LHPError`` (verbatim subclass identity / code / context); otherwise
    rebuilds via :func:`~lhp.errors.lhp_error_from_worker_failure` from the
    degraded ``rebuild`` tuple. The returned exception is NOT raised here — the
    caller wraps it in a failure delta; the actual raise stays with
    :func:`raise_aggregate_failure`.
    """
    if descriptor.live_error is not None:
        return descriptor.live_error
    pipeline_name, error_type, error_message, error_traceback = (
        descriptor.rebuild
        if descriptor.rebuild is not None
        else (descriptor.label_key, "UnknownError", "", "")
    )
    return lhp_error_from_worker_failure(
        pipeline_name=pipeline_name,
        error_type=error_type or "UnknownError",
        error_message=error_message or "(no message)",
        error_traceback=error_traceback or "",
    )


def fire_pipeline_failure_deltas(
    pool_results: Sequence["_PipelinePoolResult"],
    on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]],
) -> None:
    """Fire ``on_pipeline_complete`` with a failure delta per failing pipeline.

    Called by the generate driver BEFORE :func:`gate_or_raise` raises
    so the summary table still lists every failing pipeline. No-op when
    no callback is registered. Each failing pipeline (per
    :func:`pipeline_failure_descriptor`) is collapsed to its representative
    exception (:func:`descriptor_to_exception`) and wrapped in a
    :class:`~lhp.models.processing.PipelineDelta.failure`. Clean pipelines are
    skipped — their success deltas are emitted later, by the commit step, only
    if the gate passes.
    """
    if on_pipeline_complete is None:
        return
    from ...models.processing import PipelineDelta

    for result in pool_results:
        descriptor = pipeline_failure_descriptor(result)
        if descriptor is not None:
            on_pipeline_complete(
                PipelineDelta.failure(
                    result.pipeline, descriptor_to_exception(descriptor)
                )
            )


def gate_or_raise(
    pool_results: Sequence["_PipelinePoolResult"],
) -> List["_PipelinePoolResult"]:
    """Apply the all-or-nothing generate GATE; raise on any failure.

    The gate MECHANICS behind
    :meth:`~lhp.core.coordination.executor.PipelineExecutionService._run_generate_engine_and_gate`
    (the thin driver calls the engine, then hands the per-pipeline results
    here):

    1. Copy-conflict DRY pass — :meth:`PythonFileCopier.plan` over EVERY
       successful outcome's ``copy_records`` across ALL pipelines runs the
       write path's dedup + conflict rule with ZERO disk writes, so a
       cross-source / same-dest collision surfaces as ``LHP-VAL-019`` here,
       before any commit.
    2. Build the global error set = {per-pipeline failures (per-fg +
       cross-fg, collapsed by :func:`pipeline_failure_descriptor`)} ∪ {the
       cross-pipeline ``019``}. Codegen / generated-source parse failures
       already ride the per-fg failures (the worker catches them).
    3. Non-empty → :func:`raise_aggregate_failure` (single → that error
       verbatim; many → ``LHP-VAL-902``) and RAISE. Empty → return
       ``pool_results`` unchanged for the caller's commit to consume.

    Performs ZERO writes in either branch (the gate precedes commit).

    Returns:
        ``list(pool_results)`` on a clean run — the caller's commit reads
        ``outcomes_in_order`` off each.

    :raises LHPError: the sole failure's error (any subclass, e.g.
        ``LHP-VAL-019``), OR ``LHP-VAL-902`` when more than one unit failed.
    """
    all_copy_records = [
        record
        for result in pool_results
        for outcome in result.outcomes_in_order
        if outcome.success
        for record in outcome.copy_records
    ]
    copy_conflict: Optional[LHPError] = None
    try:
        PythonFileCopier().plan(all_copy_records)
    except LHPError as exc:
        # plan() raises PythonFunctionConflictError (an LHPError subclass);
        # keep it live for verbatim single-failure re-raise / 902 listing.
        copy_conflict = exc

    failures: List[_AggFailure] = []
    for result in pool_results:
        descriptor = pipeline_failure_descriptor(result)
        if descriptor is not None:
            failures.append(descriptor)
    if copy_conflict is not None:
        failures.append(
            _AggFailure(
                label_key=getattr(copy_conflict, "destination", None)
                or "<python-function-conflict>",
                label_value=f"{copy_conflict.code} ({copy_conflict.title})",
                live_error=copy_conflict,
            )
        )

    # No-ops on an empty list; otherwise ALWAYS raises (single → verbatim;
    # multi → 902). No files have been written — the gate precedes commit.
    raise_aggregate_failure(failures, noun="pipeline", total=len(pool_results))

    return list(pool_results)
