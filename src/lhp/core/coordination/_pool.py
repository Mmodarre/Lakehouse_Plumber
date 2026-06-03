"""Flat per-flowgroup fan-out engine for the consolidated execution surface.

This module is the coordinator-side driver for the flat
per-flowgroup engine. It owns the
fan-out / bucket / cross-flowgroup-barrier machinery that sits *above*
the single-flowgroup worker seam in :mod:`._flowgroup_pool`:

- :func:`_run_flowgroup_pool_core` — the single flat fan-out engine: one
  future per flowgroup, ``mode`` ("validate"/"generate") the ONLY fork
  (constitution §4.6). The unit of parallelism is always the
  flowgroup, so one engine serves both commands.
- :func:`_run_pipeline_cross_fg_barrier` — the §9.24 closure: the
  cross-flowgroup barrier runs on the RESOLVED flowgroups the workers
  returned, firing UNCONDITIONALLY (even for a single-flowgroup pipeline).
- :class:`_PipelinePoolResult` — the per-pipeline output slice (a plain
  ``NamedTuple`` of existing types, never crossing the spawn boundary).
- :func:`assemble_validate_outcomes` — the VALIDATE consumer that folds
  the per-pipeline results into :class:`PipelineValidationOutcome`s.

The engine runs entirely ON THE COORDINATOR. Only the worker entry
(:func:`~lhp.core.coordination._flowgroup_pool._process_one_flowgroup`)
and its captured :class:`_FlowgroupWorkerState` cross the ``spawn``
boundary; this module ships them via the pool's ``initializer=`` seam and
collects their :class:`~lhp.models.processing.FlowgroupOutcome`s. The
worker seam lives in the sibling :mod:`._flowgroup_pool`; this engine
depends on it one-directionally (importing the worker entry, its state,
its initializer, plus the shared :class:`_PipelineProgress`), and that
module imports NOTHING back — the acyclic edge that keeps the seam clean.

The lazy imports of :class:`~lhp.errors.LHPError` and
:class:`~lhp.core.coordination.executor.PipelineValidationOutcome`
(performed inside the functions, not at module scope) are deliberate:
they keep this module importable without dragging in
:mod:`.executor`, which avoids an ``executor`` ↔ ``_pool`` import cycle.

:stability: internal
"""

from __future__ import annotations

import dataclasses
import logging
import multiprocessing
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from lhp.models import FlowGroupContext

from ...models.processing import (
    DeprecationWarningRecord,
    FlowgroupOutcome,
    ValidationIssueRecord,
)
from ...utils.performance_timer import is_perf_enabled, merge_perf, perf_timer
from ._cross_flowgroup_issues import build_cross_flowgroup_issues
from ._flowgroup_pool import (
    WorkerMode,
    _FlowgroupWorkerState,
    _init_flowgroup_worker,
    _PipelineProgress,
    _process_one_flowgroup,
)
from ._warning_merge import merge_flowgroup_warnings

if TYPE_CHECKING:
    from ...errors import LHPError
    from ._validation_outcome import PipelineValidationOutcome
    from .validation_service import ValidationService


logger = logging.getLogger(__name__)


# Per-pipeline slice of the flat engine's output. NOT a carrier DTO
# (constitution §3.6 forbids a new transport type) — a plain ``NamedTuple``
# of existing types, never crossing the spawn boundary (coordinator-side only).
# Returned in input pipeline order; both consumers (validate; the generate
# gate) read this one shape:
#   - ``outcomes_in_order``: the pipeline's FlowgroupOutcomes SORTED by
#     ``flowgroup_name``. Carries per-fg success/lhp_error/errors
#     (validate + gate) AND formatted_code/copy_records/auxiliary_files
#     (gate-only).
#   - ``cross_fg_issues``  : structured LHPErrors from the per-pipeline
#     barrier on the RESOLVED set (§9.24), via build_cross_flowgroup_issues.
#   - ``cross_fg_errors``  : degraded string errors for the defensive case
#     where the barrier raises a NON-LHPError.
#   - ``warnings``         : per-pipeline deprecation warnings, merged +
#     deduped by ``(code, file)`` in deterministic first-seen order.
class _PipelinePoolResult(NamedTuple):
    pipeline: str
    outcomes_in_order: Tuple[FlowgroupOutcome, ...]
    cross_fg_issues: Tuple["LHPError", ...]
    cross_fg_errors: Tuple[str, ...]
    warnings: Tuple[DeprecationWarningRecord, ...] = ()


def _run_pipeline_cross_fg_barrier(
    pipeline: str,
    outcomes: Sequence[FlowgroupOutcome],
    validation_service: "ValidationService",
) -> Tuple[Tuple["LHPError", ...], Tuple[str, ...]]:
    """Run the cross-flowgroup barrier on a pipeline's RESOLVED flowgroups.

    THE §9.24 closure: the barrier runs on the resolved
    :class:`FlowGroup`s the workers returned
    (``FlowgroupOutcome.resolved_flowgroup``), NOT on the raw flowgroups
    the legacy validate path used — the "validate passes, generate fails"
    gap this consolidation removes.

    Fires UNCONDITIONALLY — even with per-flowgroup failures present, and
    even for a single-flowgroup pipeline (the trivial case is NOT skipped).
    Normal path folds ``validate_cross_flowgroup`` (pipeline-filtered)
    through ``build_cross_flowgroup_issues`` into structured
    :class:`LHPError`s; a NON-``LHPError`` raised by the validators
    degrades to a ``"CDC fan-in validation failed: {e}"`` string so the
    barrier never propagates. Failed-resolve outcomes carry ``None`` and
    are simply absent from the resolved set (their per-fg failure rides
    on their own outcome). Returns ``(cross_fg_issues, cross_fg_errors)``.
    """
    from ...errors import LHPError

    resolved = [
        o.resolved_flowgroup for o in outcomes if o.resolved_flowgroup is not None
    ]
    issues: List["LHPError"] = []
    errors: List[str] = []
    try:
        with perf_timer(
            f"validate_cross_flowgroup [{pipeline}]",
            category="validate_cross_flowgroup",
        ):
            # §9.24: detection logic single-sourced in
            # validate_cross_flowgroup + build_cross_flowgroup_issues; this
            # site only INVOKES it on the resolved set and SURFACES.
            cross_result = validation_service.validate_cross_flowgroup(
                resolved, pipeline_filter=pipeline
            )
        issues.extend(build_cross_flowgroup_issues(cross_result, pipeline))
    except Exception as exc:
        if isinstance(exc, LHPError):
            issues.append(exc)
        else:
            logger.exception(f"Cross-flowgroup barrier raised for pipeline {pipeline}")
            errors.append(f"CDC fan-in validation failed: {exc}")
    return tuple(issues), tuple(errors)


def _run_flowgroup_pool_core(
    *,
    flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
    worker_state: _FlowgroupWorkerState,
    validation_service: "ValidationService",
    max_workers: int,
    mode: WorkerMode,
) -> List[_PipelinePoolResult]:
    """The single flat fan-out engine — one future per flowgroup, both modes.

    The consolidated execution surface: one engine whose unit of parallelism
    is always the flowgroup, replacing the former pipeline-batched-generate +
    flowgroup-flat-validate asymmetry. ``mode`` is the ONLY fork
    (constitution §4.6 — one canonical method, no ``_by_field`` / ``_v2``
    variant); it lives solely on this private core.

    Flow:

    1. Flatten ``flowgroups_by_pipeline`` (the generalized validate shape)
       into a flat ``(pipeline, ctx)`` worklist — no new carrier (§3.6).
    2. Ship ONE :class:`_FlowgroupWorkerState` ONCE via
       ``initializer=_init_flowgroup_worker``; each task carries only its
       :class:`FlowGroupContext`.
    3. Submit ONE future per flowgroup to a
       ``ProcessPoolExecutor(mp_context="spawn")`` capped at
       ``min(max_workers, len(worklist))`` — the throughput win: the cap
       sizes against FLOWGROUP count, not pipeline count.
    4. ``as_completed`` → bucket :class:`FlowgroupOutcome`s by pipeline in
       INPUT order; within a pipeline SORT by ``flowgroup_name`` before the
       barrier.
    5. On a complete bucket, run the cross-flowgroup barrier on the
       RESOLVED set (§9.24 — :func:`_run_pipeline_cross_fg_barrier`),
       firing even for a 1-flowgroup pipeline.

    It does NOT consume the results (no writes, no gate, no
    ``PipelineValidationOutcome`` assembly) — it returns the raw bucketed
    :class:`_PipelinePoolResult`s that the validate consumer
    (:func:`assemble_validate_outcomes`) and the generate gate/commit
    (:mod:`._generate_gate` / :mod:`._commit`) layer on. Workers NEVER raise
    (every failure rides a :class:`FlowgroupOutcome`); the
    ``executor.submit`` and ``fut.result()`` guards synthesize a failure
    outcome so a pickle / pool-shutdown failure stays on the DTO channel.
    ``validation_service`` runs the barrier ON THE COORDINATOR
    (threading-unsafe, hence NOT in ``worker_state``).

    Returns one :class:`_PipelinePoolResult` per pipeline in input pipeline
    order; a zero-flowgroup pipeline yields an empty entry.
    """
    pipelines: List[str] = list(flowgroups_by_pipeline.keys())
    progress: Dict[str, _PipelineProgress] = {
        p: _PipelineProgress(pipeline=p, expected=0) for p in pipelines
    }
    worklist: List[Tuple[str, FlowGroupContext]] = []
    for p in pipelines:
        ctxs = list(flowgroups_by_pipeline[p])
        progress[p].expected = len(ctxs)
        for ctx in ctxs:
            worklist.append((p, ctx))

    results_by_pipeline: Dict[str, _PipelinePoolResult] = {}

    # Resolved-FlowGroup release: once the cross-fg barrier has
    # consumed the resolved set, the only remaining generate consumer is the
    # commit step — which needs ONLY formatted_code / auxiliary_files /
    # copy_records (NOT the resolved FlowGroup). Dropping it here bounds memory
    # on 6000+-flowgroup projects (no resolved graph is held while the whole
    # pool drains). It is released ONLY in generate mode (validate's
    # assemble_validate_outcomes reads neither it nor formatted_code) and ONLY
    # when ``include_tests`` is False: the per-pipeline test-reporting hook
    # walks the resolved flowgroups for their test_ids, so when tests are
    # emitted the resolved set is retained for commit's hook. (The engine
    # cannot see ``project_config.test_reporting``; gating on ``include_tests``
    # alone retains a SUPERSET of the cases the hook needs — never releasing
    # something it would read — at the cost of retaining when tests are on but
    # no reporting provider is configured. Output is unaffected.)
    release_resolved = mode == "generate" and not worker_state.include_tests

    def _finalize(pipeline: str, outcomes: List[FlowgroupOutcome]) -> None:
        ordered = tuple(sorted(outcomes, key=lambda o: o.flowgroup_name))
        cross_issues, cross_errors = _run_pipeline_cross_fg_barrier(
            pipeline, ordered, validation_service
        )
        # Done BEFORE the ``release_resolved`` replace below — warnings are
        # independent of ``resolved_flowgroup``, so collecting first keeps
        # first-seen order stable regardless of pool completion order.
        warnings = merge_flowgroup_warnings(ordered)
        if release_resolved:
            ordered = tuple(
                (
                    dataclasses.replace(o, resolved_flowgroup=None)
                    if o.resolved_flowgroup is not None
                    else o
                )
                for o in ordered
            )
        results_by_pipeline[pipeline] = _PipelinePoolResult(
            pipeline=pipeline,
            outcomes_in_order=ordered,
            cross_fg_issues=cross_issues,
            cross_fg_errors=cross_errors,
            warnings=warnings,
        )

    # Empty pipelines still run the (trivially empty) barrier so a
    # pipeline that discovered zero flowgroups is represented in the
    # output. The barrier on an empty resolved set yields no issues.
    for p in pipelines:
        if progress[p].expected == 0:
            del progress[p]
            _finalize(p, [])

    if worklist:
        # Workload cap: never spawn more workers than flowgroups.
        workers = min(max(1, max_workers), len(worklist))
        ctx_mp = multiprocessing.get_context("spawn")
        parent_level = logging.getLogger().level
        perf_on = is_perf_enabled()
        with (
            perf_timer(
                f"flowgroup_flat_pool [{len(worklist)} flowgroups, "
                f"{workers} workers, mode={mode}]"
            ),
            ProcessPoolExecutor(
                max_workers=workers,
                mp_context=ctx_mp,
                initializer=_init_flowgroup_worker,
                initargs=(parent_level, worker_state, perf_on),
            ) as executor,
        ):
            future_to_key: Dict[Future, Tuple[str, FlowGroupContext]] = {}
            for pipeline, fg_ctx in worklist:
                fg_name = fg_ctx.flowgroup.flowgroup
                # Mirror the validate submit guard: a pickle /
                # post-shutdown failure becomes a failure DTO, never an
                # escaped exception.
                try:
                    fut: Future = executor.submit(
                        _process_one_flowgroup, fg_ctx, mode=mode
                    )
                except Exception as submit_exc:
                    logger.warning(
                        f"executor.submit raised for flowgroup {fg_name} "
                        f"in pipeline {pipeline}: {submit_exc}"
                    )
                    outcome = FlowgroupOutcome.failure(
                        pipeline,
                        fg_name,
                        errors=(f"Flowgroup '{fg_name}': {submit_exc}",),
                    )
                    bucket = progress[pipeline]
                    bucket.results.append(outcome)
                    if bucket.is_complete():
                        del progress[pipeline]
                        _finalize(pipeline, bucket.results)
                    continue
                future_to_key[fut] = (pipeline, fg_ctx)

            for fut in as_completed(future_to_key):
                pipeline, fg_ctx = future_to_key[fut]
                fg_name = fg_ctx.flowgroup.flowgroup
                try:
                    outcome = fut.result()
                except Exception as exc:
                    # _process_one_flowgroup never raises; this guards a
                    # pool-level failure (unpickling the result, worker
                    # death). Keep it on the DTO channel.
                    outcome = FlowgroupOutcome.failure(
                        pipeline,
                        fg_name,
                        errors=(f"Flowgroup '{fg_name}': {exc}",),
                    )

                # Merge the worker's perf payload into the coordinator
                # singleton (no-op when --perf is off / payload is None).
                # Only the normal as_completed path carries worker perf; the
                # submit-failure outcome above is coordinator-constructed with
                # perf=None, so merging it would be a redundant no-op.
                merge_perf(outcome.perf)

                bucket = progress[pipeline]
                bucket.results.append(outcome)
                if bucket.is_complete():
                    del progress[pipeline]
                    _finalize(pipeline, bucket.results)

    # Return in input pipeline order. Every pipeline is present:
    # empty ones were finalized above, non-empty ones on completion.
    return [results_by_pipeline[p] for p in pipelines if p in results_by_pipeline]


def _build_validation_issue_records(
    result: _PipelinePoolResult,
    *,
    discovery_errors: Mapping[str, str],
    source_paths: Mapping[Tuple[str, str], Path],
) -> Tuple[ValidationIssueRecord, ...]:
    """Build the per-issue records for ONE pipeline's validate result.

    The per-pipeline fold for :func:`assemble_validate_outcomes`. Each
    finding becomes one :class:`~lhp.models.processing.ValidationIssueRecord`
    (all error-severity on this path — validate produces no warnings here).
    A finding with no single owning flowgroup carries
    ``flowgroup_name=None`` / ``source_file=None``:

    * a discovery failure (``pipeline in discovery_errors``) short-circuits
      to a single record (discovery error wins over the empty message);
    * an otherwise-empty pipeline surfaces one ``"No flowgroups found ..."``
      record;
    * cross-flowgroup barrier issues/errors (the §9.24 closure) span
      flowgroups, so they too carry no owning flowgroup.

    Per-flowgroup findings ARE attributed: each outcome's string ``errors``
    and structured ``lhp_error`` (mutually exclusive per outcome → no dedup)
    are tagged with that flowgroup and its source YAML, looked up in
    ``source_paths`` by ``(pipeline, flowgroup_name)``.
    """
    pipeline = result.pipeline

    def _record(
        issue: Union["LHPError", str],
        flowgroup_name: Optional[str],
        source_file: Optional[Path],
    ) -> ValidationIssueRecord:
        return ValidationIssueRecord(
            issue=issue,
            flowgroup_name=flowgroup_name,
            source_file=source_file,
            severity="error",
        )

    # Discovery failure / empty pipeline — a single unattributed record.
    if pipeline in discovery_errors:
        return (_record(discovery_errors[pipeline], None, None),)
    if not result.outcomes_in_order:
        return (
            _record(f"No flowgroups found for pipeline field: {pipeline}", None, None),
        )

    issues: List[ValidationIssueRecord] = []
    for outcome in result.outcomes_in_order:
        # Per-fg string and structured channels are mutually exclusive per
        # FlowgroupOutcome failure → no dedup. Tag each with its flowgroup +
        # that flowgroup's source file.
        flowgroup_name = outcome.flowgroup_name
        source_file = source_paths.get((pipeline, flowgroup_name))
        issues.extend(
            _record(err, flowgroup_name, source_file) for err in outcome.errors
        )
        if outcome.lhp_error is not None:
            issues.append(_record(outcome.lhp_error, flowgroup_name, source_file))

    # Cross-flowgroup barrier results (the §9.24 closure on the RESOLVED set):
    # no single owning flowgroup, so flowgroup_name / source_file stay None.
    issues.extend(_record(iss, None, None) for iss in result.cross_fg_issues)
    issues.extend(_record(err, None, None) for err in result.cross_fg_errors)
    return tuple(issues)


def assemble_validate_outcomes(
    pool_results: Sequence[_PipelinePoolResult],
    *,
    discovery_errors: Mapping[str, str],
    source_paths: Optional[Mapping[Tuple[str, str], Path]] = None,
) -> List["PipelineValidationOutcome"]:
    """Fold the flat engine's per-pipeline results into validate outcomes.

    The VALIDATE consumer of :func:`_run_flowgroup_pool_core`: exactly one
    :class:`~lhp.core.coordination.executor.PipelineValidationOutcome` per
    pipeline, in the engine's (input) pipeline order. NO writes, NO gate —
    validate REPORTS findings, never raising on them (the facade decides
    the exit code). ``PipelineExecutionService.run_validate`` points here.

    Each finding becomes one
    :class:`~lhp.models.processing.ValidationIssueRecord` (built by
    :func:`_build_validation_issue_records`) carrying its per-issue
    attribution. ``source_paths`` is the
    ``(pipeline, flowgroup) -> source-YAML path`` map threaded down from the
    orchestrator (built from the discovered flowgroups' ``source_yaml``);
    a per-flowgroup finding looks its source file up there by
    ``(pipeline, outcome.flowgroup_name)``. A finding with no single owning
    flowgroup — a cross-flowgroup fan-in issue, a discovery failure, or the
    empty-pipeline message — carries ``flowgroup_name=None`` /
    ``source_file=None``. (Defaults to ``None``, coerced to an empty map;
    never mutated.)

    ``success`` is ``True`` iff the pipeline produced no error-severity issue.
    """
    if source_paths is None:
        source_paths = {}

    from ._validation_outcome import PipelineValidationOutcome

    outcomes: List["PipelineValidationOutcome"] = []
    for result in pool_results:
        issues = _build_validation_issue_records(
            result, discovery_errors=discovery_errors, source_paths=source_paths
        )
        outcomes.append(
            PipelineValidationOutcome(
                pipeline=result.pipeline,
                issues=issues,
                success=not any(r.severity == "error" for r in issues),
            )
        )
    return outcomes
