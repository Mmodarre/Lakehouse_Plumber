"""Flat-pool flowgroup executor for cross-pipeline parallelism.

Phase A (per-flowgroup processing, codegen, formatting) runs for all
pipelines in a single :class:`ProcessPoolExecutor` over a ``spawn``
multiprocessing context. As each pipeline's last flowgroup completes,
the main thread runs Phase B for that pipeline (validation, file
write, state tracking, atomic save) via a caller-supplied hook.

Why processes rather than threads:

Code generation is pure-Python CPU work. Under a ``ThreadPoolExecutor``
every worker serialised on one GIL, so the wall-clock was flat across
worker counts and per-call ``process_flowgroup.avg`` climbed with the
worker count — the classic GIL-contention fingerprint. Switching to
processes gives each worker its own interpreter and its own GIL.

Invariants:
  - Workers MUST NOT touch the state manager. They emit
    :class:`CopiedModuleRecord` instances that Phase B replays on the
    main thread.
  - Workers MUST NOT call ``smart_writer.write_if_changed``; the disk
    write happens in Phase B.
  - The Phase A record collector is threaded via the ``phase_a_records``
    kwarg on :meth:`CodeGenerator.generate_flowgroup_code` so the
    "workers-never-touch-state-manager" invariant is checkable from
    the call graph.
  - Workers MUST receive only picklable collaborators. The orchestrator
    graph (which transitively carries the FlowgroupDiscoverer's
    ``threading.Lock``) does NOT cross the process boundary. The
    discoverer's source-YAML lookup is therefore done on the main
    thread before submit, with the result stashed on the flowgroup as
    ``_source_yaml`` for the worker to read.
  - The ``PythonFileCopier`` (also lock-bearing) is a main-thread-only
    object. Workers receive ``None`` in its place; ``copy_user_module_for_pipeline``
    routes through ``phase_a_records`` instead.
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

from ..generators.python_file_copier import CopiedModuleRecord
from ..utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from ..models.config import FlowGroup
    from ..utils.formatter import CodeFormatter
    from ..utils.substitution import EnhancedSubstitutionManager
    from .services.code_generator import CodeGenerator
    from .services.flowgroup_processor import FlowgroupProcessor


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
class FlowgroupResult:
    """Result of Phase A processing for a single flowgroup.

    All fields are by-value (``frozen=True``) so workers can hand them back
    to the main thread without further synchronisation. ``error`` carries
    the original exception object — the main thread is responsible for
    wrapping or re-raising as appropriate when aggregating outcomes.
    """

    pipeline: str
    flowgroup_name: str
    processed_flowgroup: Optional["FlowGroup"]
    code: str
    formatted_code: str
    source_yaml: Optional[Path]
    success: bool
    copied_modules: Tuple[CopiedModuleRecord, ...] = ()
    error: Optional[BaseException] = None


@dataclass(frozen=True, slots=True)
class PipelineGenerationOutcome:
    """Aggregate outcome for a single pipeline after Phase B completes."""

    pipeline: str
    generated_files: Mapping[str, str]
    files_written: int
    flowgroups_processed: int
    flowgroups_failed: int
    success: bool
    error: Optional[BaseException] = None


@dataclass(frozen=True, slots=True)
class PipelineValidationOutcome:
    """Aggregate outcome for a single pipeline's validation."""

    pipeline: str
    errors: Tuple[str, ...]
    warnings: Tuple[str, ...]
    success: bool


OnPipelineComplete: TypeAlias = Callable[[PipelineGenerationOutcome], None]
PipelineAssembler: TypeAlias = Callable[
    [str, List[FlowgroupResult]], PipelineGenerationOutcome
]


def _process_flowgroup_for_generate(
    fg: "FlowGroup",
    *,
    processor: "FlowgroupProcessor",
    code_generator: "CodeGenerator",
    formatter: "CodeFormatter",
    substitution_mgr: "EnhancedSubstitutionManager",
    output_dir: Optional[Path],
    env: str,
    include_tests: bool,
) -> FlowgroupResult:
    """Phase A worker: process + codegen + format a single flowgroup.

    Pure with respect to the state manager — never calls it. Any user
    Python modules referenced by the flowgroup are captured as
    :class:`CopiedModuleRecord` instances via the ``phase_a_records``
    list, which is threaded explicitly through
    :meth:`CodeGenerator.generate_flowgroup_code` into the
    generator/context layer. Phase B (main thread) replays the records.

    ``fg._source_yaml`` is pre-populated by the orchestrator on the
    main thread; the worker never calls the discoverer (its index lock
    would break pickling under ``spawn``).
    """
    records: List[CopiedModuleRecord] = []
    try:
        with perf_timer(
            f"process_flowgroup [{fg.flowgroup}]",
            category="process_flowgroup",
        ):
            processed = processor.process_flowgroup(
                fg, substitution_mgr, include_tests=include_tests
            )

        if fg._auxiliary_files:
            processed._auxiliary_files = fg._auxiliary_files
        processed._has_original_test_actions = fg._has_original_test_actions

        # Pre-computed on the main thread; carries through pickling.
        source_yaml: Optional[Path] = getattr(fg, "_source_yaml", None)

        with perf_timer(
            f"generate_code [{fg.flowgroup}]",
            category="generate_code",
        ):
            code = code_generator.generate_flowgroup_code(
                processed,
                substitution_mgr,
                output_dir,
                None,  # state_manager — workers MUST NOT touch it
                source_yaml,
                env,
                include_tests,
                None,  # python_file_copier — main-thread-only (lock-bearing)
                phase_a_records=records,
            )

        with perf_timer(
            f"format_code [{fg.flowgroup}]",
            category="format_code",
        ):
            formatted = formatter.format_code(code)

        return FlowgroupResult(
            pipeline=fg.pipeline,
            flowgroup_name=fg.flowgroup,
            processed_flowgroup=processed,
            code=code,
            formatted_code=formatted,
            source_yaml=source_yaml,
            success=True,
            copied_modules=tuple(records),
        )
    except BaseException as exc:
        logger.error(
            f"Phase A worker failed for flowgroup {fg.flowgroup} "
            f"in pipeline {fg.pipeline}: {exc}"
        )
        return FlowgroupResult(
            pipeline=fg.pipeline,
            flowgroup_name=fg.flowgroup,
            processed_flowgroup=None,
            code="",
            formatted_code="",
            source_yaml=None,
            success=False,
            copied_modules=tuple(records),
            error=exc,
        )


def _process_one_for_pipeline(
    fg: "FlowGroup",
    *,
    processor: "FlowgroupProcessor",
    code_generator: "CodeGenerator",
    formatter: "CodeFormatter",
    substitution_managers: Dict[str, "EnhancedSubstitutionManager"],
    pipeline_output_dirs: Dict[str, Optional[Path]],
    env: str,
    include_tests: bool,
) -> FlowgroupResult:
    """Top-level dispatch entry submitted to the process pool.

    Picklable callable bound by :class:`functools.partial` from the
    orchestrator. The per-pipeline maps are bound once on the main
    thread; the worker selects its slice by ``fg.pipeline``. Kept as
    its own name (rather than collapsing into ``_process_flowgroup_for_generate``)
    so the dispatch boundary is a small, inspectable function whose
    argument set is exactly the picklable surface area workers see.
    """
    return _process_flowgroup_for_generate(
        fg,
        processor=processor,
        code_generator=code_generator,
        formatter=formatter,
        substitution_mgr=substitution_managers[fg.pipeline],
        output_dir=pipeline_output_dirs[fg.pipeline],
        env=env,
        include_tests=include_tests,
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


def run_generate_pool(
    *,
    pipelines: Sequence[str],
    flowgroups_by_pipeline: Mapping[str, Sequence["FlowGroup"]],
    process_one: Callable[["FlowGroup"], FlowgroupResult],
    assemble_pipeline: PipelineAssembler,
    max_workers: int,
    on_pipeline_complete: Optional[OnPipelineComplete] = None,
) -> Tuple[List[PipelineGenerationOutcome], List[PipelineGenerationOutcome]]:
    """Run Phase A as one flat pool; Phase B per pipeline on the main thread.

    Submits every flowgroup across every pipeline to a single
    :class:`ProcessPoolExecutor` running under a ``spawn`` multiprocessing
    context (uniform across macOS/Linux/Windows so behaviour matches CI).
    As workers complete via :func:`as_completed`, results are bucketed by
    pipeline; when a pipeline's bucket is full, the main thread invokes
    ``assemble_pipeline`` for it and then optionally fires
    ``on_pipeline_complete``.

    Invariants:
      - Pipelines with zero flowgroups are emitted BEFORE any worker is
        submitted, so their callbacks always fire first regardless of pool
        size.
      - Within a pipeline, results are sorted by ``flowgroup_name`` before
        Phase B sees them, so Phase B observes deterministic order even
        though worker completion order is not.
      - ``process_one`` MUST be a top-level (importable) picklable callable
        — typically ``functools.partial(_process_one_for_pipeline, ...)``.
        It MUST NOT raise — workers wrap exceptions into
        ``FlowgroupResult(success=False, error=...)``.

    Returns ``(successful_outcomes, failed_outcomes)``. The combined order
    matches pipeline completion order, not the input ``pipelines`` order;
    callers needing input order should re-sort by ``outcome.pipeline``.
    """
    # Materialize the worklist and per-pipeline expected counts.
    progress: Dict[str, _PipelineProgress] = {
        p: _PipelineProgress(pipeline=p, expected=0) for p in pipelines
    }
    worklist: List[Tuple[str, "FlowGroup"]] = []
    for p in pipelines:
        fgs = list(flowgroups_by_pipeline.get(p, []))
        progress[p].expected = len(fgs)
        for fg in fgs:
            worklist.append((p, fg))

    successful: List[PipelineGenerationOutcome] = []
    failed: List[PipelineGenerationOutcome] = []

    def _emit_outcome(pipeline: str, results: List[FlowgroupResult]) -> None:
        try:
            outcome = assemble_pipeline(pipeline, results)
        except BaseException as exc:
            logger.error(f"Phase B assembly raised for pipeline {pipeline}: {exc}")
            outcome = PipelineGenerationOutcome(
                pipeline=pipeline,
                generated_files={},
                files_written=0,
                flowgroups_processed=len(results),
                flowgroups_failed=sum(1 for r in results if not r.success),
                success=False,
                error=exc,
            )
        bucket = successful if outcome.success else failed
        bucket.append(outcome)
        if on_pipeline_complete is not None:
            try:
                on_pipeline_complete(outcome)
            except BaseException as cb_exc:
                logger.warning(
                    f"on_pipeline_complete callback raised "
                    f"for pipeline {pipeline}: {cb_exc}"
                )

    # Pipelines with zero flowgroups still get an empty assembly call so
    # callers see a consistent outcome list.
    for p in pipelines:
        if progress[p].expected == 0:
            _emit_outcome(p, [])

    if not worklist:
        return successful, failed

    workers = max(1, max_workers)
    ctx = multiprocessing.get_context("spawn")
    parent_level = logging.getLogger().level
    with (
        perf_timer(f"flat_pool [{len(worklist)} flowgroups, {workers} workers]"),
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
                # process_one should not raise (workers wrap into
                # FlowgroupResult), but be defensive.
                result = FlowgroupResult(
                    pipeline=pipeline,
                    flowgroup_name=fg.flowgroup,
                    processed_flowgroup=None,
                    code="",
                    formatted_code="",
                    source_yaml=None,
                    success=False,
                    error=exc,
                )

            bucket = progress[pipeline]
            bucket.results.append(result)
            if bucket.is_complete():
                # Sort by flowgroup_name so Phase B (and downstream
                # state-manager writes) observes a deterministic order
                # regardless of which worker finished first.
                results = sorted(bucket.results, key=lambda r: r.flowgroup_name)
                # Free memory once consumed.
                del progress[pipeline]
                _emit_outcome(pipeline, results)

    # Any pipelines still in progress (shouldn't happen) get emitted now.
    for pipeline, bucket in list(progress.items()):
        if bucket.is_complete():
            _emit_outcome(pipeline, bucket.results)

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
        except BaseException as exc:
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
            except BaseException as cb_exc:
                logger.warning(
                    f"on_pipeline_complete callback raised for {pipeline}: {cb_exc}"
                )

    # Pipelines with zero flowgroups still get an empty assembly call
    # (assemble_pipeline decides whether that's an error or a no-op).
    for p in pipelines:
        if progress[p].expected == 0:
            _emit_outcome(p, [])

    if worklist:
        workers = max(1, max_workers)
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
