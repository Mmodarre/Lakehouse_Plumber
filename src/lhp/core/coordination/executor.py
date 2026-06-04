"""Process-pool executor service for cross-pipeline parallel execution.

This module holds :class:`PipelineExecutionService`, the orchestrator-facing
facade that implements :class:`BasePipelineExecutionService`. It drives the
consolidated flat per-flowgroup engine
(:func:`._pool._run_flowgroup_pool_core`, ``mode`` the only fork) for both
validate and generate; the single-flowgroup worker entry, its captured
:class:`_FlowgroupWorkerState`, and the worker initializer live on the
worker seam :mod:`lhp.core.coordination._flowgroup_pool`. Those seam symbols
are re-exported below to preserve their import path (``from .executor import
_FlowgroupWorkerState, ...``) for tests and the pickle-by-name contract.

:class:`PipelineExecutionService` carries per-batch reconfiguration and the
two ABC-typed methods :meth:`run_generate` / :meth:`run_validate` — both
generators that ``yield from`` their engine source of truth
(:meth:`_iter_generate_deltas` / :meth:`_iter_validate_outcomes`) and RETURN
the batch's merged worker deprecation warnings via ``StopIteration.value`` for
the facade to re-emit. :class:`PipelineValidationOutcome` (the validate outcome
DTO) lives in the leaf module :mod:`._validation_outcome` and is re-exported
here, so both this module and the engine import it without an
``executor`` ↔ ``_pool`` cycle. Engine / worker invariants (picklable-only
collaborators, never-raise workers) are documented in :mod:`._pool` /
:mod:`._flowgroup_pool`.
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Generator,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from lhp.models import FlowGroupContext

from ...models.processing import (
    DeprecationWarningRecord,
    PipelineDelta,
)
from .._interfaces import BasePipelineExecutionService
from ._commit import commit_generate_results
from ._flowgroup_pool import (
    _FlowgroupWorkerState,
    _init_flowgroup_worker,
    _init_worker_logger,
    _PipelineProgress,
    _process_one_flowgroup,
)
from ._generate_gate import (
    _AggFailure,
    gate_or_raise,
    iter_pipeline_failure_deltas,
    raise_aggregate_failure,
)
from ._pool import _run_flowgroup_pool_core, assemble_validate_outcomes
from ._validation_outcome import PipelineValidationOutcome
from ._warning_merge import merge_pool_warnings

if TYPE_CHECKING:
    from lhp.models import ProjectConfig

    from ..processing.substitution import EnhancedSubstitutionManager
    from .validation_service import ValidationService


# Public re-exports. The worker-seam symbols are referenced by import path
# across the ``spawn`` boundary, and tests import them from this module —
# the names here are part of the pickle-by-name contract. The private flat
# engine (``_run_flowgroup_pool_core``) and its ``mode`` fork are
# deliberately NOT exported (constitution §4.6): callers drive it
# only through :meth:`PipelineExecutionService.run_generate` /
# :meth:`~PipelineExecutionService.run_validate`.
__all__ = [
    "PipelineExecutionService",
    "PipelineValidationOutcome",
    "_FlowgroupWorkerState",
    "_PipelineProgress",
    "_init_flowgroup_worker",
    "_init_worker_logger",
    "_process_one_flowgroup",
]


logger = logging.getLogger(__name__)


class PipelineExecutionService(BasePipelineExecutionService):
    """Orchestrator-facing facade for parallel pipeline execution.

    Heavyweight execution state (worker collaborators, validation service,
    ``max_workers``) is held on the instance. Per-call data (the flat
    four-map worklist and generate-only extras) travels as kwargs to
    :meth:`run_generate` / :meth:`run_validate`.

    :stability: provisional
    """

    def __init__(
        self,
        *,
        max_workers: int = 4,
        generate_worker_state: Optional[_FlowgroupWorkerState] = None,
        validate_worker_state: Optional[_FlowgroupWorkerState] = None,
        validation_service: Optional["ValidationService"] = None,
    ) -> None:
        self._max_workers = max_workers
        self._generate_worker_state = generate_worker_state
        self._validate_worker_state = validate_worker_state
        self._validation_service = validation_service

    def run_generate(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
        substitution_managers: Mapping[str, "EnhancedSubstitutionManager"],
        output_dirs: Mapping[str, Optional[Path]],
        discovery_errors: Mapping[str, str],
        output_dir: Optional[Path],
        project_config: Optional["ProjectConfig"],
        project_root: Path,
        env: str = "dev",
        packaging_modes: Optional[Mapping[str, str]] = None,
        max_workers: Optional[int] = None,
        on_total: Optional[Callable[[int], None]] = None,
        on_flowgroup_done: Optional[Callable[[], None]] = None,
    ) -> Generator[PipelineDelta, None, Tuple[DeprecationWarningRecord, ...]]:
        """Run generate through the unified flat engine and YIELD deltas.

        Non-empty ``discovery_errors`` aborts the WHOLE batch before the engine
        runs (no writes) — generate is all-or-nothing; unlike validate it refuses
        to write a partial tree.

        This is a generator: consumers MUST fully drain it. It writes
        UNFORMATTED source and no longer formats — the single terminal ruff
        pass is the orchestrator CALLER's job, via
        :meth:`~lhp.core.coordination.orchestrator.ActionOrchestrator.format_output_tree`
        (the generate event stream and the plan path each drive it after draining
        this stream cleanly); ``generate_pipelines`` is a pure delta-generator.

        ``on_total`` / ``on_flowgroup_done`` are the optional plain-callable
        flowgroup-progress side channel (§13.5 — not events, no ``lhp.api``
        import) threaded to the engine for a live ``done/total`` counter.

        ``env`` / ``packaging_modes`` are threaded to the coordinator-side commit
        step (NEVER onto ``worker_state`` / the spawn boundary) for the
        per-pipeline wheel packaging branch (a later task); ``None`` modes means
        every pipeline is ``"source"`` → byte-identical to today.

        :raises LHPError: the sole failure's error, or ``LHP-VAL-902`` (many);
            also raised for a non-empty ``discovery_errors``.
        """
        if self._generate_worker_state is None:
            raise ValueError(
                "PipelineExecutionService.run_generate requires "
                "generate_worker_state at construction or via configure_generate()."
            )
        if self._validation_service is None:
            raise ValueError(
                "PipelineExecutionService.run_generate requires "
                "validation_service at construction or via configure_generate()."
            )
        # All-or-nothing: a per-pipeline discovery failure aborts the
        # whole batch before any write — the engine cannot represent it (empty
        # bucket → clean result → gate passes), so it is raised here.
        if discovery_errors:
            raise_aggregate_failure(
                [
                    _AggFailure(
                        label_key=pipeline,
                        label_value=f"{message}",
                        rebuild=(pipeline, "PipelineValidationError", message, ""),
                    )
                    for pipeline, message in discovery_errors.items()
                ],
                noun="pipeline",
                total=len(flowgroups_by_pipeline),
            )
        worker_state = dataclasses.replace(
            self._generate_worker_state,
            substitution_managers=dict(substitution_managers),
            pipeline_output_dirs=dict(output_dirs),
        )
        return (
            yield from self._iter_generate_deltas(
                flowgroups_by_pipeline=flowgroups_by_pipeline,
                worker_state=worker_state,
                validation_service=self._validation_service,
                env=env,
                packaging_modes=packaging_modes,
                output_dir=output_dir,
                project_config=project_config,
                project_root=project_root,
                max_workers=max_workers,
                on_total=on_total,
                on_flowgroup_done=on_flowgroup_done,
            )
        )

    def run_validate(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
        substitution_managers: Mapping[str, "EnhancedSubstitutionManager"],
        output_dirs: Mapping[str, Optional[Path]],
        discovery_errors: Mapping[str, str],
        on_total: Optional[Callable[[int], None]] = None,
        on_flowgroup_done: Optional[Callable[[], None]] = None,
    ) -> Generator[
        PipelineValidationOutcome, None, Tuple[DeprecationWarningRecord, ...]
    ]:
        """Run validate through the unified flat engine and YIELD outcomes.

        Validate REPORTS, never raising on findings. Derives the
        ``(pipeline, flowgroup) -> source-YAML`` map from each
        :attr:`FlowGroupContext.source_yaml` so every finding carries its file.

        ``on_total`` / ``on_flowgroup_done`` are the optional plain-callable
        flowgroup-progress side channel (§13.5 — not events, no ``lhp.api``
        import) threaded to the engine for a live ``done/total`` counter.
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
        worker_state = dataclasses.replace(
            self._validate_worker_state,
            substitution_managers=dict(substitution_managers),
            pipeline_output_dirs=dict(output_dirs),
        )
        source_paths: dict[Tuple[str, str], Path] = {
            (pipeline, ctx.flowgroup.flowgroup): ctx.source_yaml
            for pipeline, contexts in flowgroups_by_pipeline.items()
            for ctx in contexts
            if ctx.source_yaml is not None
        }
        return (
            yield from self._iter_validate_outcomes(
                flowgroups_by_pipeline=flowgroups_by_pipeline,
                worker_state=worker_state,
                validation_service=self._validation_service,
                discovery_errors=discovery_errors,
                source_paths=source_paths,
                on_total=on_total,
                on_flowgroup_done=on_flowgroup_done,
            )
        )

    def _iter_validate_outcomes(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
        worker_state: _FlowgroupWorkerState,
        validation_service: "ValidationService",
        discovery_errors: Mapping[str, str],
        source_paths: Optional[Mapping[Tuple[str, str], Path]] = None,
        on_total: Optional[Callable[[int], None]] = None,
        on_flowgroup_done: Optional[Callable[[], None]] = None,
    ) -> Generator[
        PipelineValidationOutcome, None, Tuple[DeprecationWarningRecord, ...]
    ]:
        """Run the flat engine in validate mode and YIELD per-pipeline outcomes.

        No gate — validate REPORTS findings, never raises on them; this
        generator runs to completion. RETURNS (via ``StopIteration.value``)
        the batch's worker deprecation warnings for the facade to re-emit.

        ``on_total`` / ``on_flowgroup_done`` are the optional plain-callable
        flowgroup-progress side channel threaded straight to the engine
        (§13.5 — not events, no ``lhp.api`` import).
        """
        if source_paths is None:
            source_paths = {}
        pool_results = _run_flowgroup_pool_core(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=validation_service,
            max_workers=self._max_workers,
            mode="validate",
            on_total=on_total,
            on_flowgroup_done=on_flowgroup_done,
        )
        yield from assemble_validate_outcomes(
            pool_results,
            discovery_errors=discovery_errors,
            source_paths=source_paths,
        )
        return merge_pool_warnings(pool_results)

    def _iter_generate_deltas(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
        worker_state: _FlowgroupWorkerState,
        validation_service: "ValidationService",
        env: str = "dev",
        packaging_modes: Optional[Mapping[str, str]] = None,
        output_dir: Optional[Path] = None,
        project_config: Optional["ProjectConfig"] = None,
        project_root: Optional[Path] = None,
        max_workers: Optional[int] = None,
        on_total: Optional[Callable[[int], None]] = None,
        on_flowgroup_done: Optional[Callable[[], None]] = None,
    ) -> Generator[PipelineDelta, None, Tuple[DeprecationWarningRecord, ...]]:
        """Run the flat engine in generate mode and YIELD per-pipeline deltas.

        Per §1.4: every failure delta is yielded BEFORE the gate raises, so the
        stream closes with failures already observed and no writes. On a clean
        gate, the commit generator's lazy whole-env wipe runs before the first
        commit, so a gate failure leaves prior output untouched. Consumers MUST
        fully drain this generator. It writes UNFORMATTED source and does NOT
        format — the single terminal ruff pass is the orchestrator CALLER's job,
        via
        :meth:`~lhp.core.coordination.orchestrator.ActionOrchestrator.format_output_tree`
        (the generate event stream and the plan path each drive it after draining
        this stream cleanly); ``generate_pipelines`` is a pure delta-generator.

        RETURNS (via ``StopIteration.value``) the batch's worker deprecation
        warnings; on the gate-raise path they are simply not delivered.

        ``on_total`` / ``on_flowgroup_done`` are the optional plain-callable
        flowgroup-progress side channel threaded straight to the engine
        (§13.5 — not events, no ``lhp.api`` import).

        ``env`` / ``packaging_modes`` are threaded straight to the commit step
        (coordinator-side, NEVER onto ``worker_state`` / the spawn boundary) for
        the per-pipeline wheel packaging branch (a later task); ``None`` modes
        means every pipeline is ``"source"`` → byte-identical to today.

        :raises LHPError: the sole failure's error, or ``LHP-VAL-902`` (many).
        """
        pool_results = _run_flowgroup_pool_core(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=validation_service,
            max_workers=(
                self._max_workers if max_workers is None else max(1, max_workers)
            ),
            mode="generate",
            on_total=on_total,
            on_flowgroup_done=on_flowgroup_done,
        )
        # Collected up front so it reflects ALL pipelines even though the gate
        # may raise below before the success deltas are yielded.
        warnings = merge_pool_warnings(pool_results)
        # Failure delta per failing pipeline BEFORE the gate raises (§1.4).
        yield from iter_pipeline_failure_deltas(pool_results)
        # Raises on ANY failure (no writes yet); returns the clean results.
        clean = gate_or_raise(pool_results)
        yield from commit_generate_results(
            clean,
            pipeline_output_dirs=worker_state.pipeline_output_dirs,
            substitution_managers=worker_state.substitution_managers,
            include_tests=worker_state.include_tests,
            output_dir=output_dir,
            project_config=project_config,
            project_root=project_root or Path.cwd(),
            env=env,
            packaging_modes=packaging_modes,
        )
        # Commit writes UNFORMATTED source verbatim. The single terminal ruff
        # pass over the whole env tree runs on the orchestrator AFTER this
        # stream drains cleanly (see ActionOrchestrator.format_output_tree); the
        # in-worker AST-parse guard runs regardless, so invalid Python still
        # fails (LHP-CFG-031).
        return warnings

    def configure_generate(
        self,
        *,
        max_workers: Optional[int] = None,
        environment: Optional[str] = None,
        include_tests: Optional[bool] = None,
        validation_service: Optional["ValidationService"] = None,
        worker_state: Optional[_FlowgroupWorkerState] = None,
    ) -> None:
        """Reconfigure the per-batch parameters for the next ``run_generate``.

        ``None``-valued kwargs are no-ops. Per-pipeline state (substitution
        managers, output dirs) is NOT set here — it travels on the worklist
        maps passed to :meth:`run_generate`. ``worker_state`` replaces the
        underlying state wholesale.
        """
        if max_workers is not None:
            self._max_workers = max(1, max_workers)
        if validation_service is not None:
            self._validation_service = validation_service
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
        worker_state: Optional[_FlowgroupWorkerState] = None,
    ) -> None:
        """Reconfigure the per-batch parameters for the next ``run_validate``.

        ``None``-valued kwargs are no-ops. ``worker_state`` replaces the
        underlying state wholesale.
        """
        if max_workers is not None:
            self._max_workers = max(1, max_workers)
        if validation_service is not None:
            self._validation_service = validation_service
        if worker_state is not None:
            self._validate_worker_state = worker_state
        if include_tests is not None and self._validate_worker_state is not None:
            self._validate_worker_state = dataclasses.replace(
                self._validate_worker_state,
                include_tests=include_tests,
            )
