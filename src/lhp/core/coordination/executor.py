"""Process-pool executor service for cross-pipeline parallel generation.

This module holds :class:`PipelineExecutionService`, the orchestrator-facing
facade that implements :class:`BasePipelineExecutionService`. The worker
entry functions, dispatch helpers, worker-state dataclasses, and the two
:class:`ProcessPoolExecutor` runners live in
:mod:`lhp.core.coordination._pool` — they are re-exported below to
preserve the original import path (``from .executor import
run_generate_pool, _GenerateWorkerState, ...``) for tests and the
orchestrator.

Architectural cohesion:
  - :class:`PipelineExecutionService` carries per-batch reconfiguration
    (callbacks, max_workers, worker states) and the two ABC-typed methods
    :meth:`run_generate` / :meth:`run_validate`.
  - :class:`PipelineValidationOutcome` is the outcome dataclass returned
    from the validate pool to the orchestrator. It stays here (not in
    ``_pool.py``) so the orchestrator's import surface is one module.

Pool-runner invariants (enforced in :mod:`_pool`):
  - Workers MUST receive only picklable collaborators.
  - The ``PythonFileCopier`` is constructed inside each worker; the
    main-thread instance is never pickled.
  - Workers MUST NOT raise: failures are wrapped into
    :class:`PipelineDelta.failure` /
    :class:`FlowgroupValidationResult` so the main thread aggregates
    deterministically.
"""

from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
)

from ...errors import (
    ErrorCategory,
    LHPValidationError,
    lhp_error_from_worker_failure,
)
from lhp.models import FlowGroupContext
from ...models.processing import PipelineDelta, PipelineWorkUnit
from ...utils.performance_timer import perf_timer
from .._interfaces import BasePipelineExecutionService
from ._pool import (
    FlowgroupValidationResult,
    _dispatch_pipeline_for_generate,
    _generate_one_pipeline,
    _GenerateWorkerState,
    _init_generate_worker,
    _init_validate_worker,
    _init_worker_logger,
    _PipelineProgress,
    _process_flowgroup_for_validate,
    _process_pipeline_for_generate,
    _validate_one_fg,
    _ValidateWorkerState,
    run_generate_pool,
    run_validate_pool,
)

if TYPE_CHECKING:
    from ...errors import LHPError
    from ..processing.substitution import EnhancedSubstitutionManager
    from .validation_service import ValidationService


# Public re-exports. Workers reference these by import path across the
# ``spawn`` boundary, and tests + the orchestrator import them from this
# module — the names here are part of the pickle-by-name contract.
__all__ = [
    "FlowgroupValidationResult",
    "OnValidationComplete",
    "PipelineExecutionService",
    "PipelineValidationOutcome",
    "ValidationAssembler",
    "aggregate_generate_outcomes",
    "_GenerateWorkerState",
    "_ValidateWorkerState",
    "_dispatch_pipeline_for_generate",
    "_generate_one_pipeline",
    "_init_generate_worker",
    "_init_validate_worker",
    "_init_worker_logger",
    "_PipelineProgress",
    "_process_flowgroup_for_validate",
    "_process_pipeline_for_generate",
    "_validate_one_fg",
    "run_generate_pool",
    "run_validate_pool",
]


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PipelineValidationOutcome:
    """Aggregate outcome for a single pipeline's validation.

    ``errors`` is the string-projection tuple (legacy plain-string
    errors plus stringified non-LHP exceptions and discovery failures).
    ``lhp_errors`` is the structured tuple of live :class:`LHPError`
    instances aggregated across the pipeline's flowgroups plus the
    cross-flowgroup CDC fan-in check. The two are NOT alternatives at
    the pipeline level — they coexist because plain-string errors and
    structured errors can both originate within the same pipeline (e.g.
    a discovery failure plus an LHPError from a worker). They ARE
    mutually exclusive per individual issue.
    """

    pipeline: str
    errors: Tuple[str, ...]
    warnings: Tuple[str, ...]
    success: bool
    lhp_errors: Tuple["LHPError", ...] = ()


OnValidationComplete: TypeAlias = Callable[[PipelineValidationOutcome], None]
ValidationAssembler: TypeAlias = Callable[
    [str, List[FlowgroupValidationResult]], PipelineValidationOutcome
]


class PipelineExecutionService(BasePipelineExecutionService):
    """Front door for parallel pipeline execution.

    Wraps the free pool-runner functions (:func:`run_generate_pool`,
    :func:`run_validate_pool`, in :mod:`_pool`). The free functions remain
    the worker pickle entry contract (workers reference them by import path
    across the ``spawn`` boundary); this class is the orchestrator-facing
    facade implementing :class:`BasePipelineExecutionService`.

    Heavyweight execution state — captured worker collaborators, the validate
    assembler, completion callbacks, ``max_workers`` — is held on the service
    instance. The ``Sequence[PipelineWorkUnit]`` input to
    :meth:`run_generate` / :meth:`run_validate` carries only the per-call data
    (pipeline name + flowgroup contexts).

    .. note::
       **ABC-contract stubs.** :meth:`run_generate` and :meth:`run_validate`
       exist to satisfy the :class:`BasePipelineExecutionService` interface,
       and they work for the minimal :class:`PipelineWorkUnit` shape they
       were designed for. The orchestrator's ``generate_pipelines`` and
       ``validate_pipelines`` paths still call :func:`run_generate_pool` /
       :func:`run_validate_pool` directly because they need per-call
       collaborators (environment, output dirs, substitution managers,
       callbacks) that the current ``PipelineWorkUnit`` DTO does not carry.
       Collapsing those orchestrator paths through this facade requires
       extending :class:`PipelineWorkUnit` to carry the per-call execution
       context (or splitting it into a constructor-time + per-call DTO
       pair). Until that DTO formalization lands, these methods stay as
       ABC-contract stubs.

    :stability: provisional
    """

    def __init__(
        self,
        *,
        max_workers: int = 4,
        generate_worker_state: Optional[_GenerateWorkerState] = None,
        validate_worker_state: Optional[_ValidateWorkerState] = None,
        validation_service: Optional["ValidationService"] = None,
        on_generate_pipeline_start: Optional[Callable[[str], None]] = None,
        on_generate_pipeline_complete: Optional[
            Callable[["PipelineDelta"], None]
        ] = None,
        on_validate_pipeline_complete: Optional[OnValidationComplete] = None,
    ) -> None:
        self._max_workers = max_workers
        self._generate_worker_state = generate_worker_state
        self._validate_worker_state = validate_worker_state
        self._validation_service = validation_service
        self._on_generate_pipeline_start = on_generate_pipeline_start
        self._on_generate_pipeline_complete = on_generate_pipeline_complete
        self._on_validate_pipeline_complete = on_validate_pipeline_complete

    def run_generate(
        self, work_units: Sequence[PipelineWorkUnit]
    ) -> Tuple[PipelineDelta, ...]:
        """Run generate across the given work units; return one delta per pipeline.

        Pool-constant state (collaborators, environment, project_root,
        project_config, include_tests, callbacks) is captured constructor-time
        on ``self._generate_worker_state``. Per-pipeline state
        (``substitution_manager``, ``output_dir``) lives on the
        :class:`PipelineWorkUnit`; the keyed-dict shape that
        :class:`_GenerateWorkerState` expects is re-derived here on every
        call so the worker pool receives one capture (via the pool's
        ``initializer=`` seam), not one capture per task.
        """
        if self._generate_worker_state is None:
            raise ValueError(
                "PipelineExecutionService.run_generate requires "
                "generate_worker_state at construction or via configure_generate()."
            )
        flowgroups_by_pipeline: Dict[str, Sequence[FlowGroupContext]] = {
            u.pipeline_name: u.flowgroups for u in work_units
        }
        substitution_managers: Dict[str, "EnhancedSubstitutionManager"] = {
            u.pipeline_name: u.substitution_manager
            for u in work_units
            if u.substitution_manager is not None
        }
        pipeline_output_dirs: Dict[str, Optional[Path]] = {
            u.pipeline_name: u.output_dir for u in work_units
        }
        state = dataclasses.replace(
            self._generate_worker_state,
            substitution_managers=substitution_managers,
            pipeline_output_dirs=pipeline_output_dirs,
        )
        successful, failed = run_generate_pool(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=state,
            max_workers=self._max_workers,
            on_pipeline_start=self._on_generate_pipeline_start,
            on_pipeline_complete=self._on_generate_pipeline_complete,
        )
        return tuple(successful) + tuple(failed)

    def run_validate(
        self, work_units: Sequence[PipelineWorkUnit]
    ) -> Tuple[PipelineValidationOutcome, ...]:
        """Run validate across the given work units; return one outcome per pipeline.

        Discovery failures (``unit.discovery_error is not None``) short-circuit
        to a pre-built outcome via the bound assembler; live pipelines run
        through the flat-pool. Cross-flowgroup CDC fan-in is delegated to
        ``self._validation_service.validate_cross_flowgroup`` from inside the
        assembler.
        """
        if self._validate_worker_state is None:
            raise ValueError(
                "PipelineExecutionService.run_validate requires "
                "validate_worker_state at construction or via configure_validate()."
            )
        if self._validation_service is None:
            raise ValueError(
                "PipelineExecutionService.run_validate requires "
                "validation_service at construction or via configure_validate()."
            )
        flowgroups_by_pipeline: Dict[str, Sequence[FlowGroupContext]] = {
            u.pipeline_name: u.flowgroups for u in work_units
        }
        substitution_managers: Dict[str, "EnhancedSubstitutionManager"] = {
            u.pipeline_name: u.substitution_manager
            for u in work_units
            if u.substitution_manager is not None
        }
        discovery_errors: Dict[str, str] = {
            u.pipeline_name: u.discovery_error
            for u in work_units
            if u.discovery_error is not None
        }
        state = dataclasses.replace(
            self._validate_worker_state,
            substitution_managers=substitution_managers,
        )
        validation_service = self._validation_service

        def _assemble(
            pipeline_name: str,
            results: List[FlowgroupValidationResult],
        ) -> PipelineValidationOutcome:
            # Resolved from the call-time flowgroups_by_pipeline map: workers
            # see the FlowGroupContext envelopes, but the cross-flowgroup CDC
            # check needs the raw FlowGroup objects.
            contexts = flowgroups_by_pipeline.get(pipeline_name, ())
            flowgroups = [ctx.flowgroup for ctx in contexts]

            # Discovery failure — report and stop.
            if pipeline_name in discovery_errors:
                return PipelineValidationOutcome(
                    pipeline=pipeline_name,
                    errors=(discovery_errors[pipeline_name],),
                    warnings=(),
                    success=False,
                )

            # Empty discovery — surface as a validation error.
            if not flowgroups:
                return PipelineValidationOutcome(
                    pipeline=pipeline_name,
                    errors=(
                        f"No flowgroups found for pipeline field: {pipeline_name}",
                    ),
                    warnings=(),
                    success=False,
                )

            errors: List[str] = []
            lhp_errors_acc: List["LHPError"] = []
            # ``errors`` (string tuple) and ``lhp_error`` (Optional[LHPError])
            # are mutually exclusive per FlowgroupValidationResult, so this
            # fold needs no dedup: each result contributes to exactly one of
            # the two buckets.
            for result in results:
                errors.extend(result.errors)
                if result.lhp_error is not None:
                    lhp_errors_acc.append(result.lhp_error)

            # Cross-flowgroup CDC fan-in compatibility — runs even when
            # per-flowgroup errors exist (mismatches only surface when
            # flowgroups are considered as a set). Live LHPErrors raised
            # here MUST land in ``lhp_errors_acc`` so the validate CLI
            # can render the structured yellow Panel; stringifying drops
            # ``code``, ``context``, and ``suggestions`` and re-introduces
            # the ``=====`` border in the per-pipeline summary table.
            try:
                with perf_timer(
                    f"validate_cross_flowgroup [{pipeline_name}]",
                    category="validate_cross_flowgroup",
                ):
                    # §9.24: routed through the public ValidationService surface.
                    cross_result = validation_service.validate_cross_flowgroup(
                        flowgroups, pipeline_filter=pipeline_name
                    )
                    cdc_errors = cross_result.cdc_fanin_errors
                errors.extend(cdc_errors)
            except (
                Exception
            ) as e:  # noqa: BLE001 — LHPError caught for structured display
                from ...errors import LHPError as _LHPError

                if isinstance(e, _LHPError):
                    lhp_errors_acc.append(e)
                else:
                    errors.append(f"CDC fan-in validation failed: {e}")

            return PipelineValidationOutcome(
                pipeline=pipeline_name,
                errors=tuple(errors),
                warnings=(),
                success=(len(errors) == 0 and len(lhp_errors_acc) == 0),
                lhp_errors=tuple(lhp_errors_acc),
            )

        outcomes = run_validate_pool(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=state,
            assemble_pipeline=_assemble,
            max_workers=self._max_workers,
            on_pipeline_complete=self._on_validate_pipeline_complete,
        )
        return tuple(outcomes)

    def configure_generate(
        self,
        *,
        max_workers: Optional[int] = None,
        on_pipeline_start: Optional[Callable[[str], None]] = None,
        on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]] = None,
        environment: Optional[str] = None,
        include_tests: Optional[bool] = None,
        worker_state: Optional[_GenerateWorkerState] = None,
    ) -> None:
        """Reconfigure the per-batch parameters for the next ``run_generate``.

        Pool-constant state varies across batches (CLI passes per-invocation
        callbacks; tests pass none). ``None``-valued kwargs are no-ops
        (preserve existing value). Per-pipeline state (substitution managers,
        output dirs) is NOT set here — it travels on the
        :class:`PipelineWorkUnit`.

        ``environment`` and ``include_tests`` update the underlying
        :class:`_GenerateWorkerState`; the per-pipeline maps are left
        untouched (they get rebuilt by :meth:`run_generate` from the work
        units anyway). ``worker_state`` replaces the underlying state
        wholesale (e.g. when collaborators rotate per batch).
        """
        if max_workers is not None:
            self._max_workers = max(1, max_workers)
        if on_pipeline_start is not None:
            self._on_generate_pipeline_start = on_pipeline_start
        if on_pipeline_complete is not None:
            self._on_generate_pipeline_complete = on_pipeline_complete
        if worker_state is not None:
            self._generate_worker_state = worker_state
        if (
            environment is not None or include_tests is not None
        ) and self._generate_worker_state is not None:
            self._generate_worker_state = dataclasses.replace(
                self._generate_worker_state,
                environment=(
                    environment
                    if environment is not None
                    else self._generate_worker_state.environment
                ),
                include_tests=(
                    include_tests
                    if include_tests is not None
                    else self._generate_worker_state.include_tests
                ),
            )

    def configure_validate(
        self,
        *,
        max_workers: Optional[int] = None,
        include_tests: Optional[bool] = None,
        validation_service: Optional["ValidationService"] = None,
        on_pipeline_complete: Optional[OnValidationComplete] = None,
        worker_state: Optional[_ValidateWorkerState] = None,
    ) -> None:
        """Reconfigure the per-batch parameters for the next ``run_validate``.

        ``None``-valued kwargs are no-ops. ``include_tests`` updates the
        underlying :class:`_ValidateWorkerState`. ``validation_service`` is
        captured for the assembler's cross-flowgroup CDC call.
        ``worker_state`` replaces the underlying state wholesale.
        """
        if max_workers is not None:
            self._max_workers = max(1, max_workers)
        if on_pipeline_complete is not None:
            self._on_validate_pipeline_complete = on_pipeline_complete
        if validation_service is not None:
            self._validation_service = validation_service
        if worker_state is not None:
            self._validate_worker_state = worker_state
        if include_tests is not None and self._validate_worker_state is not None:
            self._validate_worker_state = dataclasses.replace(
                self._validate_worker_state,
                include_tests=include_tests,
            )


def aggregate_generate_outcomes(
    deltas: Sequence["PipelineDelta"],
) -> Dict[str, tuple[str, ...]]:
    """Bucket deltas; raise the appropriate aggregate error on any failure.

    Single-failure: re-raise the live :class:`LHPError` from the worker
    (preserves structured display) or rebuild one via
    :func:`lhp_error_from_worker_failure` when the worker raised a
    non-LHP exception. Multi-failure: synthesize :class:`LHPValidationError`
    ``902`` listing each failing pipeline's error code/title.
    """
    successful = [d for d in deltas if d.success]
    failed = [d for d in deltas if not d.success]

    if failed:
        if len(failed) == 1:
            d = failed[0]
            if d.lhp_error is not None:
                raise d.lhp_error
            raise lhp_error_from_worker_failure(
                pipeline_name=d.pipeline_name,
                error_type=d.error_type or "UnknownError",
                error_message=d.error_message or "(no message)",
                error_traceback=d.error_traceback or "",
            )
        by_pipeline: Dict[str, str] = {}
        for d in failed:
            if d.lhp_error is not None:
                by_pipeline[d.pipeline_name] = (
                    f"{d.lhp_error.code} ({d.lhp_error.title})"
                )
            else:
                by_pipeline[d.pipeline_name] = (
                    f"{d.error_type or 'UnknownError'} (non-LHP exception)"
                )
        raise LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="902",
            title=f"{len(failed)} pipeline(s) failed",
            details=(
                f"{len(failed)} of {len(failed) + len(successful)} pipelines "
                f"failed during generation. See per-pipeline rows in the "
                f"summary table for full diagnostics."
            ),
            context={"failure_count": len(failed), **by_pipeline},
            suggestions=[
                "Inspect the summary table above for per-pipeline status",
                "Run 'lhp validate' for detailed per-flowgroup diagnostics",
            ],
        )

    return {delta.pipeline_name: delta.generated_filenames for delta in successful}
