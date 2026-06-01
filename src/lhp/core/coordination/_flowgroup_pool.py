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
  codegen + Black-format — all in one task, NEVER raising (constitution
  §5.6): every failure is returned as a
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

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Literal, Mapping, Optional

from lhp.models import FlowGroupContext

from ...models.processing import FlowgroupOutcome
from .._interfaces import BaseCodeGenerationService, BaseFlowgroupResolutionService

if TYPE_CHECKING:
    from ...models.processing import CopiedModuleRecord
    from ..codegen.formatter import CodeFormatter
    from ..processing.substitution import EnhancedSubstitutionManager


logger = logging.getLogger(__name__)


def _init_worker_logger(level: int) -> None:
    """Per-worker logging init.

    Silences worker stdlib loggers entirely. Workers MUST NOT write to
    OS stderr — the parent's ``Live(... redirect_stderr=True)`` only
    intercepts the parent's own ``sys.stderr``, not the worker's. All
    worker diagnostics travel back via the returned
    :class:`~lhp.models.processing.FlowgroupOutcome` (which carries the
    live LHPError on ``lhp_error`` plus pre-formatted traceback strings).

    Shared by the engine's pool initializer
    (:func:`_init_flowgroup_worker`); lives here, on the worker seam, so
    the engine in :mod:`._pool` imports it from this module (one
    direction) and this module imports nothing back from :mod:`._pool`.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.addHandler(logging.NullHandler())
    root.setLevel(level)


@dataclass
class _PipelineProgress:
    """Mutable per-pipeline completion tracker used by the fan-out engine.

    The engine (:func:`._pool._run_flowgroup_pool_core`) buckets
    per-flowgroup :class:`~lhp.models.processing.FlowgroupOutcome`s into one
    of these per pipeline and fires the cross-flowgroup barrier once a
    bucket is complete. ``results`` is intentionally permissively typed —
    it accumulates outcomes off the ``as_completed`` loop. Lives on the
    worker seam (with :func:`_init_worker_logger`) so the engine imports it
    one-directionally and this module stays free of any :mod:`._pool`
    dependency.
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

    One state serves both modes because the single-wave worker handles
    both. Pickled once and shipped to each worker via the pool's
    ``initializer=`` seam; workers read it through :data:`_flowgroup_state`
    (populated by :func:`_init_flowgroup_worker`) instead of re-pickling
    collaborators on every submitted task.

    Field roles:

    - ``processor`` / ``substitution_managers`` — both modes (resolve +
      per-flowgroup-validate; the sub manager is looked up by pipeline name).
    - ``code_generator`` / ``formatter`` / ``pipeline_output_dirs`` /
      ``environment`` — generate-only (unused in validate mode).
    - ``include_tests`` — consumed by ``process_flowgroup`` in BOTH modes
      (it filters test actions before validation), so it is genuinely shared.

    ``project_config`` and ``project_root`` are deliberately absent: the
    per-flowgroup codegen seam
    (:meth:`CodeGenerationService.generate_flowgroup_code`) does not consume
    them — they are inputs to the per-pipeline commit / test-reporting step,
    which is the coordinator's job, not the worker's. Carrying them
    here would be dead weight across the spawn boundary.

    ``frozen=True, slots=True`` for immutability across the spawn boundary
    and a smaller per-instance footprint.
    """

    processor: BaseFlowgroupResolutionService
    substitution_managers: Mapping[str, "EnhancedSubstitutionManager"]
    include_tests: bool
    # Generate-only collaborators (unused in validate mode):
    code_generator: BaseCodeGenerationService
    formatter: "CodeFormatter"
    pipeline_output_dirs: Mapping[str, Optional[Path]]
    environment: str


# Populated once per spawned worker by :func:`_init_flowgroup_worker`.
# Read by :func:`_process_one_flowgroup`. ``None`` until the initializer
# runs — the worker asserts on it rather than masking a wiring bug.
_flowgroup_state: Optional[_FlowgroupWorkerState] = None


def _init_flowgroup_worker(level: int, state: _FlowgroupWorkerState) -> None:
    """Pool initializer: configure logger and stash flowgroup worker state.

    Called once per spawned worker by :class:`ProcessPoolExecutor` via
    ``initializer=`` / ``initargs=(level, state)``. The captured state
    pickles once at pool startup instead of once per submitted task.
    Calls the shared :func:`_init_worker_logger` (defined in this module)
    that silences worker stdlib loggers (workers must not write to OS
    stderr; all diagnostics travel back on the returned DTO).
    """
    _init_worker_logger(level)
    global _flowgroup_state
    _flowgroup_state = state


def _process_one_flowgroup(
    ctx: FlowGroupContext,
    *,
    mode: WorkerMode,
) -> FlowgroupOutcome:
    """Single-wave worker: resolve + validate (+ generate) one flowgroup.

    The unit of parallelism for the consolidated engine. Picklable,
    top-level callable suitable for submission to a
    :class:`ProcessPoolExecutor`. Reads its collaborators from the
    module-global :data:`_flowgroup_state` populated by
    :func:`_init_flowgroup_worker`.

    Flow:

    1. Resolve + per-flowgroup-validate via
       :meth:`FlowgroupResolutionService.process_flowgroup`. That single
       call expands templates / presets / substitutions AND runs
       per-flowgroup validation (schema, references, action-specific,
       secret refs), RAISING an :class:`~lhp.errors.LHPError` subclass
       (e.g. :class:`LHPValidationError`) when the resolved flowgroup is
       invalid. It returns a NEW :class:`FlowGroupContext` whose
       ``.flowgroup`` is the resolved :class:`FlowGroup`.
    2. ``validate`` mode stops here and returns an ``ok`` outcome that
       carries the resolved flowgroup (``formatted_code=None``). The
       resolved flowgroup MUST be carried so the coordinator's
       per-pipeline cross-flowgroup barrier runs on the RESOLVED set.
    3. ``generate`` mode then runs codegen
       (:meth:`CodeGenerationService.generate_flowgroup_code` with a
       ``phase_a_records`` list it mutates in place to receive
       :class:`CopiedModuleRecord`s — NO disk writes) and Black-format
       (:meth:`CodeFormatter.format_code`, which raises
       :class:`LHPConfigError` ``LHP-CFG-031`` on unparseable generated
       source). It returns an ``ok`` outcome carrying ``formatted_code``,
       the captured ``copy_records``, the resolved flowgroup, and the
       monitoring-path ``auxiliary_files``.

    A worker that fails resolve/validate never reaches codegen (the early
    failure return short-circuits it) — the local "skip codegen on
    validation failure" optimization falls out naturally; correctness is
    unaffected because the coordinator's gate fails the run regardless.

    NEVER raises (constitution §5.6). Both ``except`` arms
    funnel through the TOTAL
    :meth:`~lhp.models.processing.FlowgroupOutcome.failure` constructor:

    - :class:`~lhp.errors.LHPError` → carried live on ``lhp_error`` (its
      ``__reduce__`` preserves subclass identity / ``code`` / ``context``
      / ``suggestions`` across the spawn boundary for verbatim re-raise).
    - any other :class:`Exception` → degraded string projection on
      ``errors``.

    Args:
        ctx: The per-flowgroup envelope (the raw :class:`FlowGroup` plus
            ``source_yaml``, ``synthetic`` flag, and ``auxiliary_files``).
        mode: ``"validate"`` (resolve + validate only) or ``"generate"``
            (also codegen + format). Keyword-only.

    Returns:
        A :class:`FlowgroupOutcome` — ``ok`` on success, ``failure`` on
        any error. Never propagates an exception.
    """
    from lhp.errors import LHPError

    state = _flowgroup_state
    assert state is not None, "_init_flowgroup_worker did not populate _flowgroup_state"

    fg = ctx.flowgroup
    pipeline = fg.pipeline
    flowgroup_name = fg.flowgroup

    # Resolve + per-flowgroup validation (both modes).
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

    # Generate mode: codegen + format (no disk writes).
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
        formatted = state.formatter.format_code(code)
    except Exception as exc:
        # Codegen errors and Black-parse LHP-CFG-031 both land here. Per
        # §5.6 the worker still does not raise; the resolved flowgroup is
        # not re-attached on the failure DTO (failure() is total and
        # carries only the error channels — the coordinator gate needs
        # only the error, not the resolved FG, on a generate failure).
        return _to_failure(pipeline, flowgroup_name, exc, lhp_error_cls=LHPError)

    # Route the monitoring flowgroup's extra inline modules through
    # to the outcome. ctx_out.auxiliary_files is a Mapping; the DTO keeps
    # the (path, content) tuple-of-pairs shape.
    return FlowgroupOutcome.ok(
        pipeline,
        flowgroup_name,
        resolved_flowgroup=resolved,
        formatted_code=formatted,
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
