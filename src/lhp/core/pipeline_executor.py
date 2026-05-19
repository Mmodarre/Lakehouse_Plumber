"""Process-pool executor for cross-pipeline parallel generation.

Uses :class:`ProcessPoolExecutor` over a ``spawn`` multiprocessing
context so each worker has its own interpreter and GIL — code
generation is pure-Python CPU work and would otherwise serialise on
one GIL under a thread pool.

Invariants:
  - Workers MUST receive only picklable collaborators. The orchestrator
    graph (which transitively carries the FlowgroupDiscoverer's
    ``threading.Lock``) does NOT cross the process boundary. The
    source-YAML lookup is therefore done on the main thread before
    submit, with the result stashed on the flowgroup as
    ``_source_yaml``.
  - The ``PythonFileCopier`` is constructed inside each worker; the
    main-thread instance is never pickled.
"""

from __future__ import annotations

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
    TypeAlias,
)

from ..utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from ..models.config import FlowGroup, ProjectConfig
    from ..utils.formatter import CodeFormatter
    from ..utils.substitution import EnhancedSubstitutionManager
    from .pipeline_processor import ProcessingContext
    from .services.blueprint_expander import BlueprintProvenance
    from .services.code_generator import CodeGenerator
    from .services.flowgroup_processor import FlowgroupProcessor
    from .state_models import PipelineDelta


logger = logging.getLogger(__name__)


def _init_worker_logger(level: int) -> None:
    """Per-worker logging init.

    Called once per spawned worker. Configures a stderr handler at the
    parent's effective level so worker diagnostics surface during a
    failing run. ``perf_timer`` data emitted from worker processes
    stays process-local — only main-thread timings drive the release
    perf gate, which is fine because that gate measures wall-clock at
    the top level.
    """
    logging.basicConfig(
        level=level,
        format="[worker %(process)d] %(levelname)s %(name)s: %(message)s",
    )


@dataclass(frozen=True, slots=True)
class PipelineValidationOutcome:
    """Aggregate outcome for a single pipeline's validation."""

    pipeline: str
    errors: Tuple[str, ...]
    warnings: Tuple[str, ...]
    success: bool


def _process_pipeline_for_generate(
    pipeline_name: str,
    flowgroups: Sequence["FlowGroup"],
    *,
    environment: str,
    output_dir: Optional[Path],
    state_dir: Optional[Path],
    project_root: Path,
    project_config: "ProjectConfig",
    context: "ProcessingContext",
    build_state: bool,
    blueprint_provenance: Optional[
        Dict[Tuple[str, str], "BlueprintProvenance"]
    ] = None,
) -> "PipelineDelta":
    """Worker entry: process one whole pipeline and return a delta.

    Picklable, top-level callable suitable for submission to a
    :class:`ProcessPoolExecutor`. State is constructed inside the
    worker via :class:`PipelineStateManager`; ``ProjectStateManager``
    instances never cross the process boundary.

    Args:
        pipeline_name: Pipeline being generated.
        flowgroups: All flowgroups belonging to this pipeline. The
            worker assumes ``fg._source_yaml`` is already populated.
        environment: Environment name (e.g. ``"dev"``).
        output_dir: Pipeline output directory; ``None`` for dry-run.
        state_dir: ``<project_root>/.lhp_state`` directory. May be
            ``None`` when ``build_state=False``.
        project_root: Project root.
        project_config: Project config (read for ``test_reporting``).
        context: :class:`ProcessingContext` bundling per-pipeline
            collaborators and the ``include_tests`` flag.
        build_state: When False, skips state manager and shard.
            Equivalent to ``--no-state``.

    Returns:
        :class:`PipelineDelta` reporting success/failure with file-count
        rollups (or pre-formatted error strings on failure).
    """
    from .pipeline_processor import PipelineProcessor

    pp = PipelineProcessor(
        pipeline_name=pipeline_name,
        environment=environment,
        output_dir=output_dir,
        state_dir=state_dir,
        project_root=project_root,
        project_config=project_config,
        context=context,
        build_state=build_state,
        blueprint_provenance=blueprint_provenance,
    )
    return pp.run(flowgroups)



def _dispatch_pipeline_for_generate(
    pipeline_name: str,
    flowgroups: Sequence["FlowGroup"],
    *,
    processor: "FlowgroupProcessor",
    code_generator: "CodeGenerator",
    formatter: "CodeFormatter",
    substitution_managers: Dict[str, "EnhancedSubstitutionManager"],
    pipeline_output_dirs: Dict[str, Optional[Path]],
    environment: str,
    state_dir: Optional[Path],
    project_root: Path,
    project_config: "ProjectConfig",
    include_tests: bool,
    build_state: bool,
    blueprint_provenance: Optional[
        Dict[Tuple[str, str], "BlueprintProvenance"]
    ] = None,
) -> "PipelineDelta":
    """Top-level per-pipeline dispatch entry submitted to the process pool.

    Picklable callable bound by :class:`functools.partial`. The
    per-pipeline maps (substitution_managers, pipeline_output_dirs)
    are bound once on the main thread; the worker selects its slice
    by ``pipeline_name`` and builds the :class:`ProcessingContext`
    here because ``substitution_mgr`` is per-pipeline.

    Pipelines absent from ``substitution_managers`` (empty flowgroup
    sets) short-circuit to a no-op success delta.
    """
    from .pipeline_processor import ProcessingContext
    from .state_models import PipelineDelta

    if pipeline_name not in substitution_managers:
        return PipelineDelta.success_(pipeline_name)

    context = ProcessingContext(
        processor=processor,
        code_generator=code_generator,
        formatter=formatter,
        substitution_mgr=substitution_managers[pipeline_name],
        include_tests=include_tests,
    )
    return _process_pipeline_for_generate(
        pipeline_name=pipeline_name,
        flowgroups=flowgroups,
        environment=environment,
        output_dir=pipeline_output_dirs.get(pipeline_name),
        state_dir=state_dir,
        project_root=project_root,
        project_config=project_config,
        context=context,
        build_state=build_state,
        blueprint_provenance=blueprint_provenance,
    )


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


def group_by_pipeline(
    flowgroups: Sequence["FlowGroup"],
) -> Dict[str, List["FlowGroup"]]:
    """Group flowgroups by their pipeline field, preserving insertion order.

    Two flowgroups carrying the same ``pipeline`` value are merged
    into one logical pipeline regardless of source directory.
    Returned dict iteration order matches first-occurrence order for
    deterministic reporting.
    """
    result: Dict[str, List["FlowGroup"]] = {}
    for fg in flowgroups:
        result.setdefault(fg.pipeline, []).append(fg)
    return result


def run_generate_pool(
    *,
    flowgroups_by_pipeline: Mapping[str, Sequence["FlowGroup"]],
    process_one: Callable[[str, Sequence["FlowGroup"]], "PipelineDelta"],
    max_workers: int,
    on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]] = None,
) -> Tuple[List["PipelineDelta"], List["PipelineDelta"]]:
    """Pipeline-batched dispatch. One future per non-empty pipeline.

    Submits one task per non-empty pipeline to a :class:`ProcessPoolExecutor`
    running under a ``spawn`` multiprocessing context. Each worker
    returns a :class:`PipelineDelta`; the main thread aggregates.

    Invariants:
      - Pipelines with zero flowgroups are emitted as no-op success
        deltas before any worker is submitted, so ``process_one`` is
        never invoked for them (test mocks of the partial's captured
        services are not picklable across spawn).
      - ``process_one`` MUST be a top-level picklable callable and MUST
        NOT raise: workers wrap every exception into
        ``PipelineDelta.failure(name, exc)``.
      - If ``executor.submit`` or unpickling fails, the consumer
        synthesizes a ``PipelineDelta.failure`` from the raised
        exception so the aggregate-raise path stays consistent.
      - ``on_pipeline_complete`` exceptions are caught and logged.

    Returns ``(successful_deltas, failed_deltas)``. Order matches
    pipeline completion order (empty-pipeline deltas first).
    """
    from .state_models import PipelineDelta

    worklist: List[Tuple[str, List["FlowGroup"]]] = []
    empty_pipelines: List[str] = []
    for pipeline_name, fgs in flowgroups_by_pipeline.items():
        fg_list = list(fgs)
        if fg_list:
            worklist.append((pipeline_name, fg_list))
        else:
            empty_pipelines.append(pipeline_name)

    successful: List["PipelineDelta"] = []
    failed: List["PipelineDelta"] = []

    # Empty pipelines: callbacks fire on the main thread before any worker.
    for pipeline_name in empty_pipelines:
        delta = PipelineDelta.success_(pipeline_name)
        successful.append(delta)
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
    with (
        perf_timer(f"pipeline_pool [{len(worklist)} pipelines, {workers} workers]"),
        ProcessPoolExecutor(
            max_workers=workers,
            mp_context=ctx,
            initializer=_init_worker_logger,
            initargs=(parent_level,),
        ) as executor,
    ):
        future_to_pipeline: Dict[Future, str] = {
            executor.submit(process_one, pipeline_name, fgs): pipeline_name
            for pipeline_name, fgs in worklist
        }

        for fut in as_completed(future_to_pipeline):
            pipeline_name = future_to_pipeline[fut]
            try:
                delta = fut.result()
            except BaseException as exc:
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


@dataclass(frozen=True, slots=True)
class FlowgroupValidationResult:
    """Per-flowgroup validate result.

    Phase A workers return one of these per flowgroup. The main thread
    buckets them by pipeline; Phase B then runs cross-flowgroup
    validation (e.g. CDC fan-in compatibility) on the bucket.
    """

    pipeline: str
    flowgroup_name: str
    errors: Tuple[str, ...]


def _process_flowgroup_for_validate(
    fg: "FlowGroup",
    *,
    processor: "FlowgroupProcessor",
    substitution_mgr: "EnhancedSubstitutionManager",
    include_tests: bool,
) -> FlowgroupValidationResult:
    """Phase A validate worker.

    Calls :meth:`FlowgroupProcessor.process_flowgroup` — that method
    runs all per-flowgroup validation (schema, references,
    action-specific). Any exception is caught and returned as a single
    error string keyed by the flowgroup name; the worker never raises so
    the pool can aggregate failures.
    """
    try:
        processor.process_flowgroup(fg, substitution_mgr, include_tests=include_tests)
        return FlowgroupValidationResult(
            pipeline=fg.pipeline,
            flowgroup_name=fg.flowgroup,
            errors=(),
        )
    except BaseException as exc:
        logger.debug(
            f"Phase A validate worker: flowgroup '{fg.flowgroup}' "
            f"in pipeline '{fg.pipeline}' raised: {exc}",
            exc_info=True,
        )
        return FlowgroupValidationResult(
            pipeline=fg.pipeline,
            flowgroup_name=fg.flowgroup,
            errors=(f"Flowgroup '{fg.flowgroup}': {exc}",),
        )


def _process_one_for_validate(
    fg: "FlowGroup",
    *,
    processor: "FlowgroupProcessor",
    substitution_managers: Dict[str, "EnhancedSubstitutionManager"],
    include_tests: bool,
) -> FlowgroupValidationResult:
    """Top-level validate dispatch entry submitted to the process pool.

    Picklable callable bound by :class:`functools.partial` from the
    orchestrator. The per-pipeline substitution-manager map is bound
    once on the main thread; the worker selects its slice by
    ``fg.pipeline``.
    """
    return _process_flowgroup_for_validate(
        fg,
        processor=processor,
        substitution_mgr=substitution_managers[fg.pipeline],
        include_tests=include_tests,
    )


OnValidationComplete: TypeAlias = Callable[[PipelineValidationOutcome], None]
ValidationAssembler: TypeAlias = Callable[
    [str, List[FlowgroupValidationResult]], PipelineValidationOutcome
]


def run_validate_pool(
    *,
    pipelines: Sequence[str],
    flowgroups_by_pipeline: Mapping[str, Sequence["FlowGroup"]],
    process_one: Callable[["FlowGroup"], FlowgroupValidationResult],
    assemble_pipeline: ValidationAssembler,
    max_workers: int,
    on_pipeline_complete: Optional[OnValidationComplete] = None,
) -> List[PipelineValidationOutcome]:
    """Run validate Phase A as one flat pool; Phase B per pipeline.

    Mirror of :func:`run_generate_pool` for validation — simpler because
    there is no file write or state save, just a cross-flowgroup
    post-barrier hook implemented inside ``assemble_pipeline`` (typically
    ``validate_cdc_fanin_compatibility``).

    Returns outcomes **ordered by the input pipeline order** (not
    completion order) for stable display. Callers that want completion
    order should consume ``on_pipeline_complete`` instead.
    """
    progress: Dict[str, _PipelineProgress] = {
        p: _PipelineProgress(pipeline=p, expected=0) for p in pipelines
    }
    worklist: List[Tuple[str, "FlowGroup"]] = []
    for p in pipelines:
        fgs = list(flowgroups_by_pipeline.get(p, []))
        progress[p].expected = len(fgs)
        for fg in fgs:
            worklist.append((p, fg))

    outcomes_by_pipeline: Dict[str, PipelineValidationOutcome] = {}

    def _emit_outcome(pipeline: str, results: List[FlowgroupValidationResult]) -> None:
        try:
            outcome = assemble_pipeline(pipeline, results)
        except Exception as exc:
            logger.error(
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
        ctx = multiprocessing.get_context("spawn")
        parent_level = logging.getLogger().level
        with (
            perf_timer(
                f"validate_flat_pool [{len(worklist)} flowgroups, {workers} workers]"
            ),
            ProcessPoolExecutor(
                max_workers=workers,
                mp_context=ctx,
                initializer=_init_worker_logger,
                initargs=(parent_level,),
            ) as executor,
        ):
            future_to_key: Dict[Future, Tuple[str, "FlowGroup"]] = {}
            for pipeline, fg in worklist:
                fut: Future = executor.submit(process_one, fg)
                future_to_key[fut] = (pipeline, fg)

            for fut in as_completed(future_to_key):
                pipeline, fg = future_to_key[fut]
                try:
                    result = fut.result()
                except BaseException as exc:
                    result = FlowgroupValidationResult(
                        pipeline=pipeline,
                        flowgroup_name=fg.flowgroup,
                        errors=(f"Flowgroup '{fg.flowgroup}': {exc}",),
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
