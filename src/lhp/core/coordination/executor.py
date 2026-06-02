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
from ...utils.performance_timer import perf_timer
from .._interfaces import BasePipelineExecutionService
from ..codegen.formatter import format_generated_tree
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
    drives the gate/commit via :meth:`_iter_generate_deltas` (engine
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
        max_workers: Optional[int] = None,
        apply_formatting: bool = True,
    ) -> Generator[PipelineDelta, None, Tuple[DeprecationWarningRecord, ...]]:
        """Run generate through the unified flat engine and YIELD deltas.

        The flat shape mirrors :meth:`run_validate`: the four
        maps come straight from
        :func:`flowgroup_worklist_builder.build_flowgroup_worklist` — but with a
        REAL ``output_dir`` (validate passes ``None``), so each entry in
        ``output_dirs`` is ``output_dir / <pipeline>``. The generate-only
        extras (``output_dir`` env-level, ``project_config``, ``project_root``)
        are what the gate/commit driver
        (:meth:`_iter_generate_deltas`) needs and are forwarded
        through.

        This method assembles the per-batch :class:`_FlowgroupWorkerState`
        (constructor-time leaf services — ``processor`` / ``code_generator`` /
        ``formatter`` / ``environment`` / ``include_tests`` — plus call-time
        ``substitution_managers`` / ``output_dirs``), then drives the engine in
        ``mode="generate"`` (codegen + AST-parse guard in the worker; formatting
        is the ``apply_formatting``-gated terminal pass), GATES (raises before
        any write), and COMMITS each clean pipeline.

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

        This is a generator: it ``yield from`` the source-of-truth stream
        :meth:`_iter_generate_deltas`, yielding per-pipeline
        :class:`PipelineDelta`s in DETERMINISTIC INPUT PIPELINE ORDER — every
        failure delta first, then (after the gate raises on any failure, §1.4)
        a success delta per committed pipeline. Consumers MUST fully drain it
        (the terminal whole-env format pass runs after the last success delta).
        The facade/orchestrator consume this stream directly.

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
                output_dir=output_dir,
                project_config=project_config,
                project_root=project_root,
                max_workers=max_workers,
                apply_formatting=apply_formatting,
            )
        )

    def run_validate(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]],
        substitution_managers: Mapping[str, "EnhancedSubstitutionManager"],
        output_dirs: Mapping[str, Optional[Path]],
        discovery_errors: Mapping[str, str],
    ) -> Generator[
        PipelineValidationOutcome, None, Tuple[DeprecationWarningRecord, ...]
    ]:
        """Run validate through the unified flat engine and YIELD outcomes.

        Assembles the per-batch :class:`_FlowgroupWorkerState` (constructor-time
        leaf collaborators + call-time ``substitution_managers`` /
        ``output_dirs``), then ``yield from`` the source-of-truth generator
        :meth:`_iter_validate_outcomes` (so it inherits the
        ``StopIteration.value`` warning-return), surfacing one
        :class:`PipelineValidationOutcome` per pipeline in INPUT ORDER. Validate
        REPORTS, never raising on findings. The
        ``(pipeline, flowgroup) -> source-YAML`` map threaded into the fold is
        derived HERE from each :attr:`FlowGroupContext.source_yaml` (no extra
        discovery pass), so every finding carries its flowgroup's file.
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
    ) -> Generator[
        PipelineValidationOutcome, None, Tuple[DeprecationWarningRecord, ...]
    ]:
        """Run the flat engine in validate mode and YIELD per-pipeline outcomes.

        The source of truth for the validate outcome-stream (mirrors
        :meth:`_iter_generate_deltas` on the generate side). It drives the
        consolidated engine in ``"validate"`` mode
        (:func:`._pool._run_flowgroup_pool_core`, ``mode`` forks only there —
        §4.6), then yields one :class:`PipelineValidationOutcome` per pipeline
        in DETERMINISTIC INPUT PIPELINE ORDER (the order the engine returns its
        buckets, NOT flowgroup-completion order). Validate REPORTS findings and
        never raises on them — there is no gate, so this generator runs to
        completion with no in-stream raise (unlike generate's
        failure-deltas-then-§1.4-raise).

        Cross-flowgroup CDC fan-in (§9.24) runs INSIDE the engine, on the
        RESOLVED flowgroups the workers return, not on raw flowgroups here. The
        per-pipeline fold lives in :func:`._pool.assemble_validate_outcomes`,
        keeping this generator thin; ``source_paths`` (the
        ``(pipeline, flowgroup) -> source-YAML`` map, derived from the
        worklist's ``FlowGroupContext.source_yaml``) is threaded into it so
        each finding is attributed to its flowgroup's file on disk.

        RETURNS (via ``StopIteration.value``) the batch's worker deprecation
        warnings, merged + deduped by ``(code, file)`` across every pipeline
        (:func:`._pool.merge_pool_warnings`), for the facade to re-emit as
        validate-phase :class:`~lhp.api.WarningEmitted` events.
        """
        if source_paths is None:
            source_paths = {}
        pool_results = _run_flowgroup_pool_core(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=validation_service,
            max_workers=self._max_workers,
            mode="validate",
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
        output_dir: Optional[Path] = None,
        project_config: Optional["ProjectConfig"] = None,
        project_root: Optional[Path] = None,
        max_workers: Optional[int] = None,
        apply_formatting: bool = True,
    ) -> Generator[PipelineDelta, None, Tuple[DeprecationWarningRecord, ...]]:
        """Run the flat engine in generate mode and YIELD per-pipeline deltas.

        The source of truth for the generate delta-stream. It drives the
        generate-only engine (:func:`._pool._run_flowgroup_pool_core`, ``mode``
        forks only there — §4.6), then yields per-pipeline
        :class:`~lhp.models.processing.PipelineDelta`s in DETERMINISTIC INPUT
        PIPELINE ORDER (the order the engine returns its buckets, NOT
        flowgroup-completion order): every failure delta first
        (:func:`._generate_gate.iter_pipeline_failure_deltas`); then
        :func:`._generate_gate.gate_or_raise` RAISES on ANY failure, so per §1.4
        the raise closes the stream with the failures already observed and no
        writes; on a clean gate, a success delta per committed pipeline via the
        :func:`._commit.commit_generate_results` generator (whose lazy whole-env
        wipe runs before the first commit, so a gate failure leaves prior output
        untouched). The terminal
        :func:`..codegen.formatter.format_generated_tree` pass runs AFTER the
        success deltas are exhausted (consumers MUST fully drain this
        generator); skipped when ``output_dir`` is ``None`` or
        ``apply_formatting`` is False.

        RETURNS (via ``StopIteration.value``, captured by the caller's
        ``yield from``) the batch's worker-attached deprecation warnings,
        merged + deduped by ``(code, file)`` across every pipeline
        (:func:`._pool.merge_pool_warnings`), so the facade re-emits them as
        generate-phase :class:`~lhp.api.WarningEmitted` events. Computed from
        ALL pipelines' results up front; on the gate-raise path it is simply
        not delivered (the §1.4 raise closes the stream).

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
        # Worker deprecation warnings, merged + deduped across the whole batch
        # (per-pipeline tuples are already deduped by ``(code, file)``; this
        # second fold collapses the same warning across pipelines). Collected
        # up front so it reflects ALL pipelines even though the gate may raise
        # below before the success deltas are yielded.
        warnings = merge_pool_warnings(pool_results)
        # Failure delta per failing pipeline BEFORE the gate raises (§1.4).
        yield from iter_pipeline_failure_deltas(pool_results)
        # Raises on ANY failure (no writes yet); returns the clean results.
        clean = gate_or_raise(pool_results)
        # Success delta per committed pipeline, in input order (the generator's
        # lazy whole-env wipe runs before its first commit).
        yield from commit_generate_results(
            clean,
            pipeline_output_dirs=worker_state.pipeline_output_dirs,
            substitution_managers=worker_state.substitution_managers,
            include_tests=worker_state.include_tests,
            output_dir=output_dir,
            project_config=project_config,
            project_root=project_root or Path.cwd(),
        )
        # Terminal ruff pass over the WHOLE env tree (main thread, timed as
        # ``format_tree``): commit writes UNFORMATTED source verbatim, formatted
        # ONCE here. The in-worker AST-parse guard runs regardless, so invalid
        # Python still fails (LHP-CFG-031) when this is skipped.
        if output_dir is not None and apply_formatting:
            with perf_timer("format_tree", category="format_tree"):
                format_generated_tree(output_dir)
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

        ``None``-valued kwargs are no-ops. ``include_tests`` updates the
        underlying :class:`_FlowgroupWorkerState`. ``validation_service`` is
        captured for the engine's cross-flowgroup CDC barrier.
        ``worker_state`` replaces the underlying state wholesale.
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
