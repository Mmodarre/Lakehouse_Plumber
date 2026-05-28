# JUSTIFIED: _pool.py is ~768 lines because cross-pipeline parallel execution
# is one cohesive concern that resists further splitting: the worker entry
# functions (`_generate_one_pipeline`, `_validate_one_fg`), the dispatch
# helpers (`_dispatch_pipeline_for_generate`, `_process_flowgroup_for_validate`),
# the worker-state dataclasses (`_GenerateWorkerState`, `_ValidateWorkerState`),
# and the two pool runners (`run_generate_pool` ~190L, `run_validate_pool`
# ~140L) form a single pickle-by-name contract across the spawn boundary —
# splitting them by visibility risks introducing import-cycle hazards
# (`_pool.py` ↔ `executor.py`) without a meaningful cohesion win. The two
# pool runners themselves each carry the full `ProcessPoolExecutor` +
# `as_completed` aggregation harness with main-thread submit-guards and
# completion-callback exception-trapping that cannot collapse below ~120L
# apiece without losing edge-case coverage (executor.submit pickle failures,
# empty-pipeline short-circuits, validate-assembler raise paths). The
# executor.py file holds only `PipelineExecutionService` (the
# orchestrator-facing facade) plus the two type aliases and the
# `PipelineValidationOutcome` outcome dataclass at ~370L.
# TODO(Phase 9.1): once the orchestrator decomposition lands, split _pool.py into pool dispatch + worker-state envelopes + per-mode pool runners; see LOCAL/REMAINING_WORK.md §9.
"""Internal pool runners and worker entrypoints for cross-pipeline execution.

This module is the implementation half of
:class:`lhp.core.coordination.executor.PipelineExecutionService`. It owns:

- The two worker-state dataclasses (:class:`_GenerateWorkerState`,
  :class:`_ValidateWorkerState`) shipped to spawn workers through the
  pool's ``initializer=`` seam.
- The module-level worker state globals (:data:`_generate_state`,
  :data:`_validate_state`) populated by :func:`_init_generate_worker` /
  :func:`_init_validate_worker` and read by the worker entrypoints.
- The worker entry functions (:func:`_validate_one_fg`,
  :func:`_generate_one_pipeline`) and their dispatch helpers
  (:func:`_dispatch_pipeline_for_generate`,
  :func:`_process_flowgroup_for_validate`,
  :func:`_process_pipeline_for_generate`).
- The two flat pool runners :func:`run_generate_pool` /
  :func:`run_validate_pool` that submit work to
  :class:`ProcessPoolExecutor`.
- The per-flowgroup return type :class:`FlowgroupValidationResult`
  (constructed by :func:`_process_flowgroup_for_validate`).
- The mutable progress tracker :class:`_PipelineProgress`.

The free functions are entrypoints for multiprocessing workers; workers
reference them by import path across the ``spawn`` boundary, so this
module's path / names form a stable pickle-by-name contract. Renaming
breaks workers in flight.

Pickle / spawn invariants:
  - Workers MUST receive only picklable collaborators. The
    orchestrator graph (transitively carrying threading locks) does
    NOT cross the process boundary.
  - Workers MUST NOT raise: every exception inside the worker is
    wrapped into :class:`PipelineDelta.failure` /
    :class:`FlowgroupValidationResult` so the main thread can aggregate
    deterministically.

:stability: internal
"""

from __future__ import annotations

import dataclasses
import logging
import multiprocessing
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from ...models.config import FlowGroupContext
from ...models.processing import PipelineDelta
from ...utils.performance_timer import perf_timer
from ._interfaces import BaseCodeGenerationService, BaseFlowgroupResolutionService

if TYPE_CHECKING:
    from ...errors import LHPError
    from ...models.config import ProjectConfig
    from ..codegen.formatter import CodeFormatter
    from ..processing.substitution import EnhancedSubstitutionManager
    from .processor import ProcessingContext


logger = logging.getLogger(__name__)


def _init_worker_logger(level: int) -> None:
    """Per-worker logging init.

    Silences worker stdlib loggers entirely. Workers MUST NOT write to
    OS stderr — the parent's ``Live(... redirect_stderr=True)`` only
    intercepts the parent's own ``sys.stderr``, not the worker's. All
    worker diagnostics travel back via :class:`PipelineDelta` (which
    carries the live LHPError on ``lhp_error`` plus pre-formatted
    traceback strings).
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.addHandler(logging.NullHandler())
    root.setLevel(level)


@dataclass(frozen=True, slots=True)
class _ValidateWorkerState:
    """Captured collaborators for the validate worker pool.

    Pickled once and shipped to each worker via the pool's ``initializer=``
    seam. Workers read this through the module-level :data:`_validate_state`
    populated by :func:`_init_validate_worker`.
    """

    processor: BaseFlowgroupResolutionService
    substitution_managers: Mapping[str, "EnhancedSubstitutionManager"]
    include_tests: bool


@dataclass(frozen=True, slots=True)
class _GenerateWorkerState:
    """Captured collaborators for the generate worker pool.

    Pickled once and shipped to each worker via the pool's ``initializer=``
    seam — replaces the per-task :class:`functools.partial` capture that
    re-pickled ``FlowgroupResolutionService`` + ``CodeGenerationService`` + ``CodeFormatter``
    on every submit.
    """

    processor: BaseFlowgroupResolutionService
    code_generator: BaseCodeGenerationService
    formatter: "CodeFormatter"
    substitution_managers: Mapping[str, "EnhancedSubstitutionManager"]
    pipeline_output_dirs: Mapping[str, Optional[Path]]
    project_config: Optional["ProjectConfig"]
    environment: str
    project_root: Path
    include_tests: bool


# Module-level worker state. Populated by the pool initializer; read by the
# worker entry functions. Lifetime is one pool only — workers spawn fresh and
# the pool's ``with`` block tears them down.
_validate_state: Optional[_ValidateWorkerState] = None
_generate_state: Optional[_GenerateWorkerState] = None


def _init_validate_worker(level: int, state: _ValidateWorkerState) -> None:
    """Pool initializer: configure logger and stash validate worker state.

    Called once per spawned worker by :class:`ProcessPoolExecutor` via
    ``initializer=`` / ``initargs=(level, state)``. The captured state
    pickles once at pool startup instead of once per submitted task.
    """
    _init_worker_logger(level)
    global _validate_state
    _validate_state = state


def _init_generate_worker(level: int, state: _GenerateWorkerState) -> None:
    """Pool initializer: configure logger and stash generate worker state."""
    _init_worker_logger(level)
    global _generate_state
    _generate_state = state


def _validate_one_fg(ctx: "FlowGroupContext") -> "FlowgroupValidationResult":
    """Pool worker: validate one flowgroup using state from initializer.

    AssertionError (or AttributeError under ``python -O``) if the
    initializer did not run — that signals a pool wiring bug; we do not
    mask it with a defensive fallback.
    """
    state = _validate_state
    assert state is not None, "_init_validate_worker did not populate _validate_state"
    return _process_flowgroup_for_validate(
        ctx,
        processor=state.processor,
        substitution_mgr=state.substitution_managers[ctx.flowgroup.pipeline],
        include_tests=state.include_tests,
    )


def _generate_one_pipeline(
    pipeline_name: str, contexts: Sequence["FlowGroupContext"]
) -> "PipelineDelta":
    """Pool worker: process one pipeline using state from initializer."""
    state = _generate_state
    assert state is not None, "_init_generate_worker did not populate _generate_state"
    return _dispatch_pipeline_for_generate(
        pipeline_name,
        contexts,
        processor=state.processor,
        code_generator=state.code_generator,
        formatter=state.formatter,
        substitution_managers=state.substitution_managers,
        pipeline_output_dirs=state.pipeline_output_dirs,
        environment=state.environment,
        project_root=state.project_root,
        project_config=state.project_config,
        include_tests=state.include_tests,
    )


def _process_pipeline_for_generate(
    pipeline_name: str,
    contexts: Sequence["FlowGroupContext"],
    *,
    environment: str,
    output_dir: Optional[Path],
    project_root: Path,
    project_config: Optional["ProjectConfig"],
    context: "ProcessingContext",
) -> "PipelineDelta":
    """Worker entry: process one whole pipeline and return a delta.

    Picklable, top-level callable suitable for submission to a
    :class:`ProcessPoolExecutor`.

    Args:
        pipeline_name: Pipeline being generated.
        contexts: All FlowGroupContext envelopes belonging to this
            pipeline. The envelope carries the FlowGroup plus its
            source_yaml, synthetic flag, and auxiliary_files.
        environment: Environment name (e.g. ``"dev"``).
        output_dir: Pipeline output directory; ``None`` for dry-run.
        project_root: Project root.
        project_config: Project config (read for ``test_reporting``).
        context: :class:`ProcessingContext` bundling per-pipeline
            collaborators and the ``include_tests`` flag.

    Returns:
        :class:`PipelineDelta` reporting success/failure with file-count
        rollups (or pre-formatted error strings on failure).
    """
    from .processor import PipelineProcessor

    pp = PipelineProcessor(
        pipeline_name=pipeline_name,
        environment=environment,
        output_dir=output_dir,
        project_root=project_root,
        project_config=project_config,
        context=context,
    )
    return pp.run(contexts)


def _dispatch_pipeline_for_generate(
    pipeline_name: str,
    contexts: Sequence["FlowGroupContext"],
    *,
    processor: BaseFlowgroupResolutionService,
    code_generator: BaseCodeGenerationService,
    formatter: "CodeFormatter",
    substitution_managers: Mapping[str, "EnhancedSubstitutionManager"],
    pipeline_output_dirs: Mapping[str, Optional[Path]],
    environment: str,
    project_root: Path,
    project_config: Optional["ProjectConfig"],
    include_tests: bool,
) -> "PipelineDelta":
    """Top-level per-pipeline dispatch entry called from the worker.

    Reachable from :func:`_generate_one_pipeline` (production, via the
    pool initializer) or directly with explicit kwargs (testing). The
    per-pipeline maps (``substitution_managers``, ``pipeline_output_dirs``)
    are read-only here — typed ``Mapping`` to permit the
    :class:`_GenerateWorkerState`'s frozen-dataclass shape on the
    production path.

    Pipelines absent from ``substitution_managers`` (empty flowgroup
    sets) short-circuit to a no-op success delta with
    ``duration_s=0.0`` — no work was done.

    Pure work-time measurement: ``t0 = perf_counter()`` is captured
    after the empty-pipeline short-circuit, then stamped onto the
    returned delta via :func:`dataclasses.replace` at this boundary.
    Keeping the stamp at dispatch (rather than threading the duration
    deeper into :func:`_process_pipeline_for_generate` or
    :class:`PipelineProcessor`) avoids leaking timing concerns into
    domain code. Both success and failure paths receive the same
    measured duration so the Live panel can report real per-pipeline
    work-time regardless of outcome.
    """
    import dataclasses
    from time import perf_counter

    from ...models.processing import PipelineDelta
    from .processor import ProcessingContext

    if pipeline_name not in substitution_managers:
        return PipelineDelta.success_(pipeline_name)

    t0 = perf_counter()
    try:
        context = ProcessingContext(
            processor=processor,
            code_generator=code_generator,
            formatter=formatter,
            substitution_mgr=substitution_managers[pipeline_name],
            include_tests=include_tests,
        )
        delta = _process_pipeline_for_generate(
            pipeline_name=pipeline_name,
            contexts=contexts,
            environment=environment,
            output_dir=pipeline_output_dirs.get(pipeline_name),
            project_root=project_root,
            project_config=project_config,
            context=context,
        )
        return dataclasses.replace(delta, duration_s=perf_counter() - t0)
    except BaseException as exc:
        # Workers MUST NOT raise — the pool aggregator at as_completed
        # synthesizes a failure delta on raise, but that path runs on
        # the main thread without our worker t0.
        # Exception: wrap here so duration is preserved on the failure delta.
        # Non-Exception BaseException (KeyboardInterrupt, SystemExit) propagates
        # from the worker; main-thread synthesis uses duration_s=0.0, which is
        # fine for cancellation paths.
        if isinstance(exc, Exception):
            return PipelineDelta.failure(
                pipeline_name, exc, duration_s=perf_counter() - t0
            )
        raise


@dataclass
class _PipelineProgress:
    """Mutable per-pipeline state inside the executor.

    Used by both :func:`run_generate_pool` (results are
    :class:`FlowgroupResult`) and :func:`run_validate_pool` (results are
    :class:`FlowgroupValidationResult`). The ``results`` annotation is
    intentionally permissive so the same progress tracker can be reused.
    """

    pipeline: str
    expected: int
    results: List[Any] = field(default_factory=list)

    def is_complete(self) -> bool:
        return len(self.results) >= self.expected


@dataclass(frozen=True, slots=True)
class FlowgroupValidationResult:
    """Per-flowgroup validate result.

    Phase A workers return one of these per flowgroup. The main thread
    buckets them by pipeline; Phase B then runs cross-flowgroup
    validation (e.g. CDC fan-in compatibility) on the bucket.

    ``errors`` (string tuple) and ``lhp_error`` (optional instance) are
    MUTUALLY EXCLUSIVE per flowgroup: the worker chooses one based on
    ``isinstance(exc, LHPError)``. This eliminates dedup complexity in
    the main-thread outcome assembler. ``lhp_error`` survives the
    worker→main pickle via the LHPError ``__reduce__`` contract so the
    live subclass identity, ``code``, ``context``, and ``suggestions``
    are preserved verbatim.
    """

    pipeline: str
    flowgroup_name: str
    errors: Tuple[str, ...]
    lhp_error: Optional["LHPError"] = None


def _process_flowgroup_for_validate(
    ctx: "FlowGroupContext",
    *,
    processor: BaseFlowgroupResolutionService,
    substitution_mgr: "EnhancedSubstitutionManager",
    include_tests: bool,
) -> FlowgroupValidationResult:
    """Phase A validate worker.

    Calls :meth:`FlowgroupResolutionService.process_flowgroup` — that method
    runs all per-flowgroup validation (schema, references,
    action-specific). Any exception is caught and returned as a single
    :class:`FlowgroupValidationResult` keyed by the flowgroup name; the
    worker never raises so the pool can aggregate failures.

    LHPError instances are carried via the structured ``lhp_error``
    field so the main thread keeps the live subclass identity,
    ``code``, ``context``, and ``suggestions`` (the ``__str__`` form
    drops these and re-introduces the ``=====`` border that polluted
    earlier validation displays). Non-LHP exceptions fall back to the
    string projection on ``errors``. The two fields are mutually
    exclusive per result.
    """
    from lhp.errors import LHPError

    fg = ctx.flowgroup
    try:
        processor.process_flowgroup(ctx, substitution_mgr, include_tests=include_tests)
        return FlowgroupValidationResult(
            pipeline=fg.pipeline,
            flowgroup_name=fg.flowgroup,
            errors=(),
        )
    except Exception as exc:
        logger.debug(
            "Phase A validate worker: flowgroup '%s' in pipeline '%s' raised: %s",
            fg.flowgroup,
            fg.pipeline,
            type(exc).__name__,
            exc_info=True,
        )
        if isinstance(exc, LHPError):
            return FlowgroupValidationResult(
                pipeline=fg.pipeline,
                flowgroup_name=fg.flowgroup,
                errors=(),
                lhp_error=exc,
            )
        return FlowgroupValidationResult(
            pipeline=fg.pipeline,
            flowgroup_name=fg.flowgroup,
            errors=(f"Flowgroup '{fg.flowgroup}': {type(exc).__name__}: {exc}",),
            lhp_error=None,
        )


def run_generate_pool(
    *,
    flowgroups_by_pipeline: Mapping[str, Sequence["FlowGroupContext"]],
    worker_state: Optional[_GenerateWorkerState] = None,
    max_workers: int,
    on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]] = None,
    on_pipeline_start: Optional[Callable[[str], None]] = None,
    process_one: Optional[
        Callable[[str, Sequence["FlowGroupContext"]], "PipelineDelta"]
    ] = None,
) -> Tuple[List["PipelineDelta"], List["PipelineDelta"]]:
    """Pipeline-batched dispatch. One future per non-empty pipeline.

    Submits one task per non-empty pipeline to a :class:`ProcessPoolExecutor`
    running under a ``spawn`` multiprocessing context. Each worker
    returns a :class:`PipelineDelta`; the main thread aggregates.

    ``worker_state`` is pickled once at pool startup (via the
    ``initializer=`` seam) so per-task submit only ships the pipeline
    name and the flowgroup-context list — eliminating the
    :class:`functools.partial` re-pickle of ``FlowgroupResolutionService`` +
    ``CodeGenerationService`` + ``CodeFormatter`` that used to fire on every
    submit.

    ``process_one`` is a test seam — when ``None`` (production),
    submissions use :func:`_generate_one_pipeline`, which reads
    :data:`_generate_state`. Tests pass a top-level picklable callable
    so the pool's aggregation logic can be exercised without
    constructing real collaborators.

    Invariants:
      - ``worker_state`` is required in production. Either
        ``worker_state`` or ``process_one`` must be supplied; otherwise
        the worker has no captured state and the initializer call
        would fail.
      - Pipelines with zero flowgroup contexts are emitted as no-op
        success deltas before any worker is submitted.
      - The worker functions MUST NOT raise: workers wrap every exception
        into ``PipelineDelta.failure(name, exc)``.
      - If ``executor.submit`` or unpickling fails, the consumer
        synthesizes a ``PipelineDelta.failure`` from the raised
        exception so the aggregate-raise path stays consistent.
      - ``on_pipeline_complete`` exceptions are caught and logged.

    Returns ``(successful_deltas, failed_deltas)``. Order matches
    pipeline completion order (empty-pipeline deltas first).
    """
    from ...models.processing import PipelineDelta

    if worker_state is None and process_one is None:
        raise ValueError(
            "run_generate_pool requires either worker_state (production) "
            "or process_one (test seam)."
        )

    worker_fn = process_one if process_one is not None else _generate_one_pipeline

    worklist: List[Tuple[str, List["FlowGroupContext"]]] = []
    empty_pipelines: List[str] = []
    for pipeline_name, ctxs in flowgroups_by_pipeline.items():
        ctx_list = list(ctxs)
        if ctx_list:
            worklist.append((pipeline_name, ctx_list))
        else:
            empty_pipelines.append(pipeline_name)

    successful: List["PipelineDelta"] = []
    failed: List["PipelineDelta"] = []

    # Empty pipelines: callbacks fire on the main thread before any worker.
    for pipeline_name in empty_pipelines:
        delta = PipelineDelta.success_(pipeline_name)
        successful.append(delta)
        if on_pipeline_start is not None:
            try:
                on_pipeline_start(pipeline_name)
            except Exception as cb_exc:
                logger.warning(
                    f"on_pipeline_start callback raised "
                    f"for pipeline {pipeline_name}: {cb_exc}"
                )
        if on_pipeline_complete is not None:
            try:
                on_pipeline_complete(delta)
            except Exception as cb_exc:
                logger.warning(
                    f"on_pipeline_complete callback raised "
                    f"for pipeline {pipeline_name}: {cb_exc}"
                )

    if not worklist:
        return successful, failed

    # Workload cap: don't spawn more workers than pipelines to dispatch.
    # Spawn'd Python interpreters have non-trivial startup cost; idle
    # workers are pure overhead.
    workers = min(max(1, max_workers), len(worklist))
    ctx = multiprocessing.get_context("spawn")
    parent_level = logging.getLogger().level

    # Test seams pass process_one without worker_state — fall back to the
    # logger-only initializer.
    if worker_state is not None:
        pool_kwargs: Dict[str, Any] = {
            "initializer": _init_generate_worker,
            "initargs": (parent_level, worker_state),
        }
    else:
        pool_kwargs = {
            "initializer": _init_worker_logger,
            "initargs": (parent_level,),
        }

    with (
        perf_timer(f"pipeline_pool [{len(worklist)} pipelines, {workers} workers]"),
        ProcessPoolExecutor(
            max_workers=workers,
            mp_context=ctx,
            **pool_kwargs,
        ) as executor,
    ):
        future_to_pipeline: Dict[Future, str] = {}
        for pipeline_name, ctxs in worklist:
            if on_pipeline_start is not None:
                try:
                    on_pipeline_start(pipeline_name)
                except Exception as cb_exc:
                    logger.warning(
                        f"on_pipeline_start callback raised "
                        f"for pipeline {pipeline_name}: {cb_exc}"
                    )
            try:
                future = executor.submit(worker_fn, pipeline_name, ctxs)
            except Exception as submit_exc:
                # ``executor.submit`` can raise on pickle failure or after
                # the pool has been shut down. Without this guard the
                # exception propagates out before the matching
                # ``on_pipeline_complete`` fires, leaving OverallProgress
                # stalled and the consumer holding partial state. Mirror
                # the as_completed exception branch below: synthesize a
                # failure delta and fire the completion callback so the
                # aggregate counters stay consistent. The continue keeps
                # the loop submitting remaining pipelines.
                logger.warning(
                    f"executor.submit raised for pipeline "
                    f"{pipeline_name}: {submit_exc}"
                )
                delta = PipelineDelta.failure(pipeline_name, submit_exc)
                failed.append(delta)
                if on_pipeline_complete is not None:
                    try:
                        on_pipeline_complete(delta)
                    except Exception as cb_exc:
                        logger.warning(
                            f"on_pipeline_complete callback raised "
                            f"for pipeline {pipeline_name}: {cb_exc}"
                        )
                continue
            future_to_pipeline[future] = pipeline_name

        for fut in as_completed(future_to_pipeline):
            pipeline_name = future_to_pipeline[fut]
            try:
                delta = fut.result()
            except Exception as exc:
                delta = PipelineDelta.failure(pipeline_name, exc)

            bucket = successful if delta.success else failed
            bucket.append(delta)

            logger.info(
                "Pipeline complete: %s (%s)",
                delta.pipeline_name,
                "success" if delta.success else "FAILED",
            )

            if on_pipeline_complete is not None:
                try:
                    on_pipeline_complete(delta)
                except Exception as cb_exc:
                    logger.warning(
                        f"on_pipeline_complete callback raised "
                        f"for pipeline {pipeline_name}: {cb_exc}"
                    )

    return successful, failed


def run_validate_pool(
    *,
    flowgroups_by_pipeline: Mapping[str, Sequence["FlowGroupContext"]],
    worker_state: _ValidateWorkerState,
    assemble_pipeline: Callable[
        [str, List[FlowgroupValidationResult]], "PipelineValidationOutcome"
    ],
    max_workers: int,
    on_pipeline_complete: Optional[
        Callable[["PipelineValidationOutcome"], None]
    ] = None,
) -> List["PipelineValidationOutcome"]:
    """Run validate Phase A as one flat pool; Phase B per pipeline.

    Mirror of :func:`run_generate_pool` for validation — simpler because
    there is no file write or state save, just a cross-flowgroup
    post-barrier hook implemented inside ``assemble_pipeline`` (typically
    ``ValidationService.validate_cross_flowgroup``).

    The captured ``worker_state`` is pickled once at pool startup and
    delivered to each worker via the ``initializer=`` seam, so the per-task
    submit shrinks to a single :class:`FlowGroupContext` argument.

    Pipeline ordering is derived from ``flowgroups_by_pipeline.keys()``
    — dict insertion order is preserved (Python 3.7+), so the caller
    controls display order by the order in which it inserts pipelines
    into the mapping.

    Returns outcomes **ordered by the input pipeline order** (not
    completion order) for stable display. Callers that want completion
    order should consume ``on_pipeline_complete`` instead.
    """
    # Local import: PipelineValidationOutcome lives in executor.py (the
    # service-facing surface), but the pool runner instantiates fallback
    # outcomes on Phase-B assembly failure. Importing at module level would
    # create an executor↔_pool import cycle (executor imports the pool, the
    # pool would import the outcome dataclass from executor).
    from .executor import PipelineValidationOutcome

    pipelines: List[str] = list(flowgroups_by_pipeline.keys())
    progress: Dict[str, _PipelineProgress] = {
        p: _PipelineProgress(pipeline=p, expected=0) for p in pipelines
    }
    worklist: List[Tuple[str, "FlowGroupContext"]] = []
    for p in pipelines:
        ctxs = list(flowgroups_by_pipeline[p])
        progress[p].expected = len(ctxs)
        for ctx in ctxs:
            worklist.append((p, ctx))

    outcomes_by_pipeline: Dict[str, "PipelineValidationOutcome"] = {}

    def _emit_outcome(pipeline: str, results: List[FlowgroupValidationResult]) -> None:
        try:
            outcome = assemble_pipeline(pipeline, results)
        except Exception as exc:
            logger.exception(
                f"Phase B validate assembly raised for pipeline {pipeline}: {exc}"
            )
            outcome = PipelineValidationOutcome(
                pipeline=pipeline,
                errors=(f"Validation assembly failed: {exc}",),
                warnings=(),
                success=False,
            )
        outcomes_by_pipeline[pipeline] = outcome
        if on_pipeline_complete is not None:
            try:
                on_pipeline_complete(outcome)
            except Exception as cb_exc:
                logger.warning(
                    f"on_pipeline_complete callback raised for {pipeline}: {cb_exc}"
                )

    # Pipelines with zero flowgroups still get an empty assembly call
    # (assemble_pipeline decides whether that's an error or a no-op).
    for p in pipelines:
        if progress[p].expected == 0:
            _emit_outcome(p, [])

    if worklist:
        # Workload cap: don't spawn more workers than flowgroups to validate.
        workers = min(max(1, max_workers), len(worklist))
        ctx_mp = multiprocessing.get_context("spawn")
        parent_level = logging.getLogger().level
        with (
            perf_timer(
                f"validate_flat_pool [{len(worklist)} flowgroups, {workers} workers]"
            ),
            ProcessPoolExecutor(
                max_workers=workers,
                mp_context=ctx_mp,
                initializer=_init_validate_worker,
                initargs=(parent_level, worker_state),
            ) as executor,
        ):
            future_to_key: Dict[Future, Tuple[str, "FlowGroupContext"]] = {}
            for pipeline, fg_ctx in worklist:
                # Mirrors the executor.submit guard in run_generate_pool.
                try:
                    fut: Future = executor.submit(_validate_one_fg, fg_ctx)
                except Exception as submit_exc:
                    logger.warning(
                        f"executor.submit raised for flowgroup "
                        f"{fg_ctx.flowgroup.flowgroup} in pipeline "
                        f"{pipeline}: {submit_exc}"
                    )
                    result = FlowgroupValidationResult(
                        pipeline=pipeline,
                        flowgroup_name=fg_ctx.flowgroup.flowgroup,
                        errors=(
                            f"Flowgroup '{fg_ctx.flowgroup.flowgroup}': "
                            f"{submit_exc}",
                        ),
                    )
                    progress[pipeline].results.append(result)
                    if progress[pipeline].is_complete():
                        results = sorted(
                            progress[pipeline].results,
                            key=lambda r: r.flowgroup_name,
                        )
                        del progress[pipeline]
                        _emit_outcome(pipeline, results)
                    continue
                future_to_key[fut] = (pipeline, fg_ctx)

            for fut in as_completed(future_to_key):
                pipeline, fg_ctx = future_to_key[fut]
                try:
                    result = fut.result()
                except Exception as exc:
                    result = FlowgroupValidationResult(
                        pipeline=pipeline,
                        flowgroup_name=fg_ctx.flowgroup.flowgroup,
                        errors=(f"Flowgroup '{fg_ctx.flowgroup.flowgroup}': {exc}",),
                    )

                bucket = progress[pipeline]
                bucket.results.append(result)
                if bucket.is_complete():
                    # Deterministic order for validation aggregation.
                    results = sorted(bucket.results, key=lambda r: r.flowgroup_name)
                    del progress[pipeline]
                    _emit_outcome(pipeline, results)

    # Return outcomes in input pipeline order (stable for display).
    return [outcomes_by_pipeline[p] for p in pipelines if p in outcomes_by_pipeline]
