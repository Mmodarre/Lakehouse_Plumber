"""Single-wave per-flowgroup worker for the consolidated execution engine.

This module is the worker-side plumbing for the flat
per-flowgroup engine. It owns exactly
the pieces a *single flowgroup* needs to be processed in a spawn worker:

- One unified worker-state dataclass (:class:`_FlowgroupWorkerState`)
  shipped to each worker through the pool's ``initializer=`` seam: one
  state serves both ``validate`` and ``generate`` because the single-wave
  worker is parameterized by ``mode`` rather than forked per command
  (constitution §4.6 — one canonical method, no ``_by_field`` variant).
- The module-level worker-state global :data:`_flowgroup_state`,
  populated by :func:`_init_flowgroup_worker` and read by the worker.
- The single-wave worker entry :func:`_process_one_flowgroup` which does
  resolve + per-flowgroup validation and, for ``mode == "generate"``,
  codegen + a cheap ``ast.parse`` syntax guard — all in one task, NEVER
  raising (constitution §5.6): every failure is returned as a
  :class:`~lhp.models.processing.FlowgroupOutcome` via its total
  :meth:`~lhp.models.processing.FlowgroupOutcome.failure` constructor.

The fan-out engine (``_run_flowgroup_pool_core`` / the ``as_completed``
loop) deliberately does NOT live here; it is :mod:`._pool`, which imports
this module's worker symbols one-directionally. This module is purely the
worker seam and imports NOTHING from :mod:`._pool` — that one-way edge is
the deliberate acyclic split. It also owns the two helpers the engine
shares across the seam (:func:`_init_worker_logger`, :class:`_PipelineProgress`).

Pickle / spawn invariants (same contract as :mod:`._pool`):
  - Workers MUST receive only picklable collaborators. The orchestrator
    graph (transitively carrying threading locks) does NOT cross the
    process boundary; this state ships the leaf services instead.
  - The worker entry is referenced by import path across the ``spawn``
    boundary, so this module's path / names form a stable
    pickle-by-name contract. Renaming breaks workers in flight.
  - The worker MUST NOT raise: every exception is wrapped into a
    :class:`FlowgroupOutcome` failure so the coordinator can aggregate
    deterministically.

:stability: internal
"""

from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Literal, Mapping, Optional

from lhp.models import FlowGroupContext

from ...models.deprecations import collect_deprecations, drain_deprecations
from ...models.processing import FlowgroupOutcome
from ...utils.performance_timer import (
    enable_perf_timing,
    export_perf_for_merge,
    is_perf_enabled,
    reset_perf_summary,
)
from .._interfaces import BaseCodeGenerationService, BaseFlowgroupResolutionService
from ..codegen.formatter import assert_generated_python_valid

if TYPE_CHECKING:
    from ...models.processing import CopiedModuleRecord
    from ..processing.substitution import EnhancedSubstitutionManager


logger = logging.getLogger(__name__)


def _init_worker_logger(level: int) -> None:
    """Silence worker stdlib loggers entirely.

    Workers MUST NOT write to OS stderr — the parent's
    ``Live(redirect_stderr=True)`` only intercepts the parent's own
    ``sys.stderr``, not the worker's. All worker diagnostics travel back
    via the returned :class:`~lhp.models.processing.FlowgroupOutcome`.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.addHandler(logging.NullHandler())
    root.setLevel(level)


@dataclass
class _PipelineProgress:
    """Mutable per-pipeline completion tracker used by the fan-out engine.

    ``results`` is intentionally permissively typed — it accumulates
    outcomes off the ``as_completed`` loop.
    """

    pipeline: str
    expected: int
    results: List[Any] = field(default_factory=list)

    def is_complete(self) -> bool:
        return len(self.results) >= self.expected


# The two modes the single-wave worker accepts. The flag lives ONLY on
# this private core — no public symbol exposes it.
WorkerMode = Literal["validate", "generate"]


@dataclass(frozen=True, slots=True)
class _FlowgroupWorkerState:
    """Captured collaborators for the single-wave flowgroup worker.

    Pickled once and shipped to each worker via the pool's ``initializer=``
    seam, so collaborators are not re-pickled on every submitted task.

    Field roles:

    - ``processor`` / ``substitution_managers`` — both modes.
    - ``code_generator`` / ``pipeline_output_dirs`` / ``environment`` —
      generate-only (unused in validate mode). The formatter is deliberately
      absent: the worker only ``ast.parse``-validates; the single terminal
      ruff pass runs on the coordinator over the committed tree.
    - ``include_tests`` — consumed in BOTH modes (filters test actions before
      validation).

    ``project_config`` and ``project_root`` are deliberately absent — inputs
    to the per-pipeline commit/test-reporting step (coordinator's job, not
    the worker's); carrying them here would be dead weight across the spawn
    boundary.
    """

    processor: BaseFlowgroupResolutionService
    substitution_managers: Mapping[str, "EnhancedSubstitutionManager"]
    include_tests: bool
    # Generate-only collaborators (unused in validate mode):
    code_generator: BaseCodeGenerationService
    pipeline_output_dirs: Mapping[str, Optional[Path]]
    environment: str


# Populated once per spawned worker by :func:`_init_flowgroup_worker`.
# Read by :func:`_process_one_flowgroup`. ``None`` until the initializer
# runs — the worker asserts on it rather than masking a wiring bug.
_flowgroup_state: Optional[_FlowgroupWorkerState] = None


def _init_flowgroup_worker(
    level: int, state: _FlowgroupWorkerState, perf_on: bool
) -> None:
    """Pool initializer: configure logger and stash flowgroup worker state.

    When ``perf_on`` is set, re-enables perf timing in this spawned worker
    (spawn re-imports the module with ``_enabled = False``). The
    ``project_root=None`` form records ONLY into the in-memory singleton
    (no :class:`FileHandler`) so workers emit no per-line perf log; the
    parent collects the aggregate via the perf envelope in
    :func:`_process_one_flowgroup`.
    """
    _init_worker_logger(level)
    global _flowgroup_state
    _flowgroup_state = state
    if perf_on:
        enable_perf_timing(project_root=None)


def _process_one_flowgroup(
    ctx: FlowGroupContext,
    *,
    mode: WorkerMode,
) -> FlowgroupOutcome:
    """Single-wave worker entry: perf + deprecation envelope around the impl.

    Name is stable — in :mod:`.executor`'s ``__all__`` and monkeypatched
    by engine tests; constitution §5.6 bars renaming it.

    Wraps the impl in a :func:`~lhp.models.deprecations.collect_deprecations`
    scope so deprecation records (otherwise lost under the worker's
    ``NullHandler``) ride back on :attr:`FlowgroupOutcome.warnings`.
    When perf is enabled, :func:`reset_perf_summary` scopes the singleton to
    this flowgroup (one task per worker → race-free) and attaches the
    aggregate to the outcome. NEVER raises.
    """
    perf_on = is_perf_enabled()
    if perf_on:
        reset_perf_summary()
    with collect_deprecations(
        file=ctx.source_yaml,
        flowgroup=ctx.flowgroup.flowgroup,
    ) as collector:
        outcome = _process_one_flowgroup_impl(ctx, mode=mode)
    warnings = drain_deprecations(collector)
    if warnings:
        # The impl never sets ``warnings`` itself (sites use the ambient
        # scope), so replacing the default ``()`` is the whole attach.
        outcome = dataclasses.replace(outcome, warnings=warnings)
    if perf_on:
        outcome = dataclasses.replace(outcome, perf=export_perf_for_merge())
    return outcome


def _process_one_flowgroup_impl(
    ctx: FlowGroupContext,
    *,
    mode: WorkerMode,
) -> FlowgroupOutcome:
    """Single-wave worker: resolve + validate (+ generate) one flowgroup.

    Validate mode returns an ``ok`` outcome carrying the resolved flowgroup
    so the coordinator's cross-flowgroup barrier runs on the RESOLVED set.
    Generate mode also runs codegen (NO disk writes) and the cheap
    ``ast.parse`` guard; formatting is NOT done here — it is the
    coordinator's single terminal ``ruff format`` pass.

    NEVER raises (constitution §5.6): :class:`~lhp.errors.LHPError` is
    carried live on ``lhp_error`` (subclass identity preserved across the
    spawn boundary); any other exception degrades to the ``errors`` channel.
    """
    from lhp.errors import LHPError

    state = _flowgroup_state
    assert state is not None, "_init_flowgroup_worker did not populate _flowgroup_state"

    fg = ctx.flowgroup
    pipeline = fg.pipeline
    flowgroup_name = fg.flowgroup

    try:
        substitution_mgr = state.substitution_managers[pipeline]
        ctx_out = state.processor.process_flowgroup(
            ctx,
            substitution_mgr,
            include_tests=state.include_tests,
        )
        resolved = ctx_out.flowgroup
    except Exception as exc:
        return _to_failure(pipeline, flowgroup_name, exc, lhp_error_cls=LHPError)

    if mode == "validate":
        # Validate-mode: no codegen. The resolved flowgroup MUST ride
        # along so the coordinator's cross-flowgroup barrier runs on the
        # resolved set; formatted_code stays None.
        return FlowgroupOutcome.ok(
            pipeline,
            flowgroup_name,
            resolved_flowgroup=resolved,
        )

    # Generate mode: codegen + syntax guard (no formatting, no disk writes).
    try:
        # ``records`` is mutated in place by generate_flowgroup_code:
        # user-module copies are appended as CopiedModuleRecords instead
        # of being written to disk (the coordinator replays them later).
        records: List["CopiedModuleRecord"] = []
        code = state.code_generator.generate_flowgroup_code(
            resolved,
            substitution_mgr,
            state.pipeline_output_dirs.get(pipeline),
            ctx.source_yaml,
            state.environment,
            state.include_tests,
            phase_a_records=records,
            auxiliary_files=ctx_out.auxiliary_files,
        )
        # Cheap (microsecond ast.parse) syntax guard: the worker ships the
        # UNFORMATTED ``code`` and only asserts it parses. A failed parse
        # raises LHP-CFG-031 — caught below and routed onto the failure DTO.
        assert_generated_python_valid(code, flowgroup_name)
    except Exception as exc:
        # Codegen errors and LHP-CFG-031 both land here. Per §5.6 the worker
        # still does not raise; the resolved flowgroup is not re-attached.
        return _to_failure(pipeline, flowgroup_name, exc, lhp_error_cls=LHPError)

    # Route the monitoring flowgroup's extra inline modules through
    # to the outcome. ctx_out.auxiliary_files is a Mapping; the DTO keeps
    # the (path, content) tuple-of-pairs shape.
    return FlowgroupOutcome.ok(
        pipeline,
        flowgroup_name,
        resolved_flowgroup=resolved,
        # Despite its ``formatted_code`` name the field carries UNFORMATTED
        # source: the terminal ruff pass on the coordinator formats the
        # committed tree in place.
        formatted_code=code,
        auxiliary_files=tuple(ctx_out.auxiliary_files.items()),
        copy_records=tuple(records),
    )


def _to_failure(
    pipeline: str,
    flowgroup_name: str,
    exc: Exception,
    *,
    lhp_error_cls: type,
) -> FlowgroupOutcome:
    """Convert a caught exception into a total :class:`FlowgroupOutcome` failure.

    Live :class:`~lhp.errors.LHPError` instances travel on the structured
    ``lhp_error`` channel (preserving subclass identity / ``code`` /
    ``context`` / ``suggestions``); every other exception degrades to the
    stringified ``errors`` channel. This helper itself never raises —
    :meth:`FlowgroupOutcome.failure` is total — so the worker's
    never-raise contract is upheld even in the error path.

    ``lhp_error_cls`` is injected (rather than imported at module scope)
    only to keep the :class:`~lhp.errors.LHPError` import inside the
    worker call, matching the lazy-import style of the existing workers.
    """
    logger.debug(
        "Flowgroup worker: flowgroup '%s' in pipeline '%s' raised: %s",
        flowgroup_name,
        pipeline,
        type(exc).__name__,
        exc_info=True,
    )
    if isinstance(exc, lhp_error_cls):
        return FlowgroupOutcome.failure(
            pipeline,
            flowgroup_name,
            lhp_error=exc,
        )
    return FlowgroupOutcome.failure(
        pipeline,
        flowgroup_name,
        errors=(f"Flowgroup '{flowgroup_name}': {type(exc).__name__}: {exc}",),
    )
