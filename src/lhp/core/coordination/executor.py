"""Process-pool executor service for cross-pipeline parallel execution.

This module holds :class:`PipelineExecutionService`, the orchestrator-facing
facade that implements :class:`BasePipelineExecutionService`. It drives the
consolidated flat per-flowgroup engine
(:func:`._pool._run_flowgroup_pool_core`, ``mode`` the only fork) for both
validate and generate; the single-flowgroup worker entry, its captured
:class:`_FlowgroupWorkerState`, and the worker initializer live on the
worker seam :mod:`lhp.core.coordination._flowgroup_pool`. The worker seam
symbols are re-exported below to preserve their import path
(``from .executor import _FlowgroupWorkerState, _init_worker_logger, ...``)
for tests and the pickle-by-name worker contract.

Architectural cohesion:
  - :class:`PipelineExecutionService` carries per-batch reconfiguration
    (callbacks, max_workers, worker states) and the two ABC-typed methods
    :meth:`run_generate` / :meth:`run_validate`, plus the generate-only
    engine/gate/commit driver :meth:`_run_generate_engine_and_gate`.
  - :class:`PipelineValidationOutcome` is the outcome dataclass returned
    from validate to the orchestrator. It stays here (not in ``_pool.py``)
    so the orchestrator's import surface is one module, and so the engine
    can lazy-import it without an ``executor`` ↔ ``_pool`` cycle.

Engine / worker invariants (enforced in :mod:`._pool` / :mod:`._flowgroup_pool`):
  - Workers MUST receive only picklable collaborators.
  - The ``PythonFileCopier`` is constructed inside each worker; the
    main-thread instance is never pickled.
  - Workers MUST NOT raise: failures are wrapped into a
    :class:`~lhp.models.processing.FlowgroupOutcome` failure so the
    coordinator aggregates deterministically.
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
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
)

from lhp.models import FlowGroupContext

from ...models.processing import PipelineDelta
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
    fire_pipeline_failure_deltas,
    gate_or_raise,
    raise_aggregate_failure,
)
from ._pool import _run_flowgroup_pool_core, assemble_validate_outcomes

if TYPE_CHECKING:
    from lhp.models import ProjectConfig

    from ...errors import LHPError
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
    "OnValidationComplete",
    "PipelineExecutionService",
    "PipelineValidationOutcome",
    "_FlowgroupWorkerState",
    "_PipelineProgress",
    "_init_flowgroup_worker",
    "_init_worker_logger",
    "_process_one_flowgroup",
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


class PipelineExecutionService(BasePipelineExecutionService):
    """Front door for parallel pipeline execution.

    The orchestrator-facing facade implementing
    :class:`BasePipelineExecutionService`. It drives the consolidated flat
    per-flowgroup engine (:func:`._pool._run_flowgroup_pool_core`, ``mode``
    the only fork) for both commands; the single-flowgroup worker entry it
    fans out lives on the worker seam :mod:`._flowgroup_pool` and is
    referenced by import path across the ``spawn`` boundary.

    Heavyweight execution state — captured worker collaborators, the
    validation service, completion callbacks, ``max_workers`` — is held on
    the service instance. The per-call data (the flat four-map worklist +
    the generate-only gate/commit extras) is passed as keyword arguments to
    :meth:`run_generate` / :meth:`run_validate`.

    Both :meth:`run_validate` and :meth:`run_generate` take the flat
    four-map worklist shape and drive the engine; ``mode`` forks
    only inside :func:`._pool._run_flowgroup_pool_core`. :meth:`run_validate`
    folds the per-pipeline results into :class:`PipelineValidationOutcome`s
    via :func:`._pool.assemble_validate_outcomes`; :meth:`run_generate`
    drives the gate/commit via :meth:`_run_generate_engine_and_gate` (engine
    + gate + commit).

    :stability: provisional
    """

    def __init__(
        self,
        *,
        max_workers: int = 4,
        generate_worker_state: Optional[_FlowgroupWorkerState] = None,
        validate_worker_state: Optional[_FlowgroupWorkerState] = None,
        validation_service: Optional["ValidationService"] = None,
        on_generate_pipeline_complete: Optional[
            Callable[["PipelineDelta"], None]
        ] = None,
        on_validate_pipeline_complete: Optional[OnValidationComplete] = None,
    ) -> None:
        self._max_workers = max_workers
        self._generate_worker_state = generate_worker_state
        self._validate_worker_state = validate_worker_state
        self._validation_service = validation_service
        self._on_generate_pipeline_complete = on_generate_pipeline_complete
        self._on_validate_pipeline_complete = on_validate_pipeline_complete

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
        max_workers: Optional[int] = None,
    ) -> Dict[str, tuple[str, ...]]:
        """Run generate through the unified flat engine; all-or-nothing.

        The flat shape mirrors :meth:`run_validate`: the four
        maps come straight from
        :func:`flowgroup_worklist_builder.build_flowgroup_worklist` — but with a
        REAL ``output_dir`` (validate passes ``None``), so each entry in
        ``output_dirs`` is ``output_dir / <pipeline>``. The generate-only
        extras (``output_dir`` env-level, ``project_config``, ``project_root``)
        are what the gate/commit driver
        (:meth:`_run_generate_engine_and_gate`) needs and are forwarded
        through.

        This method assembles the per-batch :class:`_FlowgroupWorkerState`
        (combining the constructor-time leaf-service collaborators —
        ``processor`` / ``code_generator`` / ``formatter`` / ``environment`` /
        ``include_tests`` — with the call-time ``substitution_managers`` /
        ``output_dirs``), then drives the engine in ``mode="generate"`` (codegen
        + Black-format in the worker), GATES (raises on ANY failure, before any
        write), and COMMITS each clean pipeline.

        Discovery failures: ``build_flowgroup_worklist`` routes a pipeline whose
        per-pipeline discovery raised into ``discovery_errors`` (present but
        empty in ``flowgroups_by_pipeline``), so the engine would otherwise emit
        a clean empty result and the gate would pass — silently producing zero
        files for that pipeline. Generate is all-or-nothing, so a
        non-empty ``discovery_errors`` aborts the WHOLE batch HERE, before the
        engine runs (no writes), via the shared
        :func:`._generate_gate.raise_aggregate_failure` shaping. This mirrors the
        legacy generate path, where a discovery exception propagated out of the
        work-unit builder and aborted the run; unlike validate (which REPORTS
        per-pipeline), generate refuses to write a partial tree.

        Returns ``{pipeline -> generated filenames}`` for the committed
        pipelines, in input order — the projection the orchestrator previously
        obtained by aggregating the per-pipeline deltas (the gate now raises
        in-place, so the orchestrator no longer aggregates); the per-pipeline
        :class:`PipelineDelta`s flow out-of-band via
        ``self._on_generate_pipeline_complete`` (fired in the gate-failure and
        commit paths inside the driver), so the public response shapes
        (``BatchGenerationResponse`` / ``GenerationResponse``) are unchanged
        (§1.6).

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
        deltas = self._run_generate_engine_and_gate(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=self._validation_service,
            output_dir=output_dir,
            project_config=project_config,
            project_root=project_root,
            max_workers=max_workers,
        )
        return {delta.pipeline_name: delta.generated_filenames for delta in deltas}

    def run_validate(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
        substitution_managers: Mapping[str, "EnhancedSubstitutionManager"],
        output_dirs: Mapping[str, Optional[Path]],
        discovery_errors: Mapping[str, str],
    ) -> Tuple[PipelineValidationOutcome, ...]:
        """Run validate through the unified flat engine; one outcome per pipeline.

        The flat shape supersedes the legacy
        ``Sequence[PipelineWorkUnit]`` input: the four maps come straight
        from :func:`flowgroup_worklist_builder.build_flowgroup_worklist`
        (``flowgroups_by_pipeline`` keyed in pipeline-input order). This
        method assembles the per-batch :class:`_FlowgroupWorkerState`
        (combining the constructor-time leaf-service collaborators —
        ``processor``, and the unused-in-validate ``code_generator`` /
        ``formatter`` — with the call-time ``substitution_managers`` /
        ``output_dirs``), then drives the consolidated engine in
        ``"validate"`` mode and folds the per-pipeline results into
        :class:`PipelineValidationOutcome`s.

        Cross-flowgroup CDC fan-in (§9.24) now runs INSIDE the engine, on
        the RESOLVED flowgroups the workers return, not on raw
        flowgroups here — closing the validate-under-checks gap. The
        assembly fold lives in
        :func:`._pool.assemble_validate_outcomes`, keeping this
        facade thin.
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
        pool_results = _run_flowgroup_pool_core(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=self._validation_service,
            max_workers=self._max_workers,
            mode="validate",
        )
        outcomes = assemble_validate_outcomes(
            pool_results,
            discovery_errors=discovery_errors,
        )
        if self._on_validate_pipeline_complete is not None:
            for outcome in outcomes:
                self._on_validate_pipeline_complete(outcome)
        return tuple(outcomes)

    def _run_generate_engine_and_gate(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
        worker_state: _FlowgroupWorkerState,
        validation_service: "ValidationService",
        output_dir: Optional[Path] = None,
        project_config: Optional["ProjectConfig"] = None,
        project_root: Optional[Path] = None,
        max_workers: Optional[int] = None,
    ) -> Tuple[PipelineDelta, ...]:
        """Run the flat engine in generate mode, GATE, then COMMIT.

        The generate-path driver, kept thin:
        it drives the generate-only engine
        (:func:`._pool._run_flowgroup_pool_core`, ``mode`` forks
        only there — constitution §4.6), hands the per-pipeline results to
        :func:`._generate_gate.gate_or_raise` (copy-conflict ``019`` dry pass +
        single-vs-``902`` shaping), then commits each clean pipeline via
        :func:`._commit.commit_generate_results`.

        All-or-nothing: on ANY failure NO files are written and the
        aggregate is raised. Writes — and the single whole-env wipe of
        ``output_dir`` (so a gate failure leaves prior output untouched) —
        happen ONLY on the gate-passed path, in input pipeline order.

        ``self._on_generate_pipeline_complete`` fires once per pipeline:
        a :class:`PipelineDelta.failure` per failing pipeline BEFORE
        the raise (via :func:`._generate_gate.fire_pipeline_failure_deltas`, so
        the summary lists failures), or the synthesized success delta per
        committed pipeline (forwarded into the commit step).

        Returns the per-pipeline success deltas on a clean run.

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
        )
        # Failure delta per failing pipeline BEFORE the gate raises.
        fire_pipeline_failure_deltas(pool_results, self._on_generate_pipeline_complete)
        # Raises on ANY failure (no writes yet); returns the clean results.
        clean = gate_or_raise(pool_results)
        return commit_generate_results(
            clean,
            pipeline_output_dirs=worker_state.pipeline_output_dirs,
            substitution_managers=worker_state.substitution_managers,
            include_tests=worker_state.include_tests,
            output_dir=output_dir,
            project_config=project_config,
            project_root=project_root or Path.cwd(),
            on_pipeline_complete=self._on_generate_pipeline_complete,
        )

    def configure_generate(
        self,
        *,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]] = None,
        environment: Optional[str] = None,
        include_tests: Optional[bool] = None,
        validation_service: Optional["ValidationService"] = None,
        worker_state: Optional[_FlowgroupWorkerState] = None,
    ) -> None:
        """Reconfigure the per-batch parameters for the next ``run_generate``.

        ``None``-valued kwargs are no-ops (preserve existing value). Per-pipeline
        state (substitution managers, output dirs) is NOT set here — it travels
        on the worklist maps that :meth:`run_generate` folds into the per-batch
        worker state. ``environment`` / ``include_tests`` update the underlying
        :class:`_FlowgroupWorkerState`; ``validation_service`` is captured for the
        engine's cross-flowgroup CDC barrier (generate runs it on the resolved
        set — same as validate); ``worker_state`` replaces the
        underlying state wholesale.
        """
        if max_workers is not None:
            self._max_workers = max(1, max_workers)
        if on_pipeline_complete is not None:
            self._on_generate_pipeline_complete = on_pipeline_complete
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
        on_pipeline_complete: Optional[OnValidationComplete] = None,
        worker_state: Optional[_FlowgroupWorkerState] = None,
    ) -> None:
        """Reconfigure the per-batch parameters for the next ``run_validate``.

        ``None``-valued kwargs are no-ops. ``include_tests`` updates the
        underlying :class:`_FlowgroupWorkerState`. ``validation_service`` is
        captured for the engine's cross-flowgroup CDC barrier.
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
