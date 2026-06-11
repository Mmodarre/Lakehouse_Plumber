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
from typing import TYPE_CHECKING, Any, List, Literal, Mapping, Optional, Tuple

from lhp.models import FlowGroupContext

from ...models.deprecations import collect_deprecations, drain_deprecations
from ...models.processing import FlowgroupOutcome, SandboxWarningRecord
from ...utils.performance_timer import (
    enable_perf_timing,
    export_perf_for_merge,
    is_perf_enabled,
    reset_perf_summary,
)
from .._interfaces import BaseCodeGenerationService, BaseFlowgroupResolutionService
from ..codegen.formatter import assert_generated_python_valid

if TYPE_CHECKING:
    from lhp.core.sandbox import SandboxTableRenames, UnrewritableTableRead

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
    - ``table_renames`` — both modes; the ``--sandbox`` rename set driving
      the worker's D7 rewrite hooks. ``None`` (the default) outside sandbox
      runs — the hooks are skipped entirely. Frozen and picklable, so it
      crosses the spawn boundary on this carrier.

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
    # Trailing + defaulted so existing positional construction is unaffected
    # (frozen+slots tolerates a defaulted append):
    table_renames: Optional["SandboxTableRenames"] = None


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
        # CONCATENATE, never replace: the impl may have attached sandbox
        # warnings (LHP-VAL-066) already; drained deprecation records are
        # appended after them. Outside sandbox runs ``outcome.warnings`` is
        # always ``()``, so this is behavior-preserving.
        outcome = dataclasses.replace(outcome, warnings=outcome.warnings + warnings)
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
        if state.table_renames is not None:
            # Sandbox hook 1 (D7, BOTH modes): structured rewrite of the
            # RESOLVED flowgroup before any downstream use, so grouping,
            # codegen, the cross-flowgroup barrier and validation all see
            # the sandbox names. Validate mode returns this REWRITTEN
            # flowgroup, which is its whole sandbox story (no text pass,
            # no LHP-VAL-066 — a documented v1 limitation).
            from lhp.core.sandbox import rewrite_flowgroup_tables

            resolved = rewrite_flowgroup_tables(resolved, state.table_renames)
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
    sandbox_warnings: Tuple[SandboxWarningRecord, ...] = ()
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
        if state.table_renames is not None:
            # Sandbox hook 2 (D7, generate only): text pass over the
            # generated module + each copied user module BEFORE the syntax
            # guard, folding any unrewritable in-scope reads into one
            # LHP-VAL-066 record per file. The rewriter's internal
            # ``ast.parse`` self-check ValueError propagates to the
            # catch-all below → clean failure DTO → the all-or-nothing
            # gate aborts the batch. ``auxiliary_files`` (monitoring-only)
            # are deliberately NOT rewritten.
            code, records, sandbox_warnings = _apply_sandbox_python_rewrites(
                code,
                records,
                state.table_renames,
                flowgroup_name=flowgroup_name,
                output_dir=state.pipeline_output_dirs.get(pipeline),
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
        # Sandbox LHP-VAL-066 records (empty outside sandbox runs). The
        # wrapper CONCATENATES its drained deprecation records after these.
        warnings=sandbox_warnings,
    )


def _apply_sandbox_python_rewrites(
    code: str,
    records: List["CopiedModuleRecord"],
    renames: "SandboxTableRenames",
    *,
    flowgroup_name: str,
    output_dir: Optional[Path],
) -> tuple[str, List["CopiedModuleRecord"], Tuple[SandboxWarningRecord, ...]]:
    """Generate-mode sandbox TEXT pass (D7 hook 2) over the worker's Python.

    Runs :func:`~lhp.core.sandbox.rewrite_python_table_literals` over the
    flowgroup's generated module and over each
    :class:`~lhp.models.processing.CopiedModuleRecord`'s content (records are
    frozen — rewritten ones are rebuilt via :func:`dataclasses.replace`).
    Returns ``(code, records, warnings)`` with one ``LHP-VAL-066``
    :class:`~lhp.models.processing.SandboxWarningRecord` per FILE that still
    holds in-scope reads the rewriter could not rename (the ``(code, file)``
    dedup grain the merge chain uses).

    File identity: the generated module is named exactly as the commit step
    writes it (``<pipeline output dir>/<flowgroup_name>.py``; the bare
    relative ``<flowgroup_name>.py`` when the pipeline has no output dir,
    e.g. dry-run); a copied module is its ``record.dest_path``.

    May raise ``ValueError`` — ONLY from the rewriter's internal
    ``ast.parse`` self-check. The caller's catch-all converts it into a
    failure DTO (never raises across the spawn boundary, §5.6), which the
    all-or-nothing gate then surfaces.
    """
    from lhp.core.sandbox import rewrite_python_table_literals

    warnings: List[SandboxWarningRecord] = []

    generated_file = (
        output_dir / f"{flowgroup_name}.py"
        if output_dir is not None
        else Path(f"{flowgroup_name}.py")
    )
    code, unrewritable = rewrite_python_table_literals(code, renames)
    warning = _fold_unrewritable_reads(unrewritable, generated_file, flowgroup_name)
    if warning is not None:
        warnings.append(warning)

    new_records: List["CopiedModuleRecord"] = []
    for record in records:
        content, unrewritable = rewrite_python_table_literals(record.content, renames)
        if content != record.content:
            record = dataclasses.replace(record, content=content)
        new_records.append(record)
        warning = _fold_unrewritable_reads(
            unrewritable, record.dest_path, flowgroup_name
        )
        if warning is not None:
            warnings.append(warning)

    return code, new_records, tuple(warnings)


def _fold_unrewritable_reads(
    reads: "tuple[UnrewritableTableRead, ...]",
    file: Path,
    flowgroup_name: str,
) -> Optional[SandboxWarningRecord]:
    """Fold ONE file's unrewritable in-scope reads into ONE ``LHP-VAL-066``.

    Returns ``None`` when the file has no such reads. Sites are deduped and
    sorted by ``(lineno, table, kind)`` so the message is deterministic.
    """
    if not reads:
        return None
    from lhp.errors.codes import VAL_066

    sites = sorted({(read.lineno, read.table, read.kind) for read in reads})
    detail = "; ".join(
        f"line {lineno}: {table} ({kind})" for lineno, table, kind in sites
    )
    message = (
        f"Sandbox rewrite could not rename {len(sites)} in-scope table "
        f"read(s) in {file.name}: {detail}. These sites still read the "
        f"original (non-sandbox) table(s)."
    )
    return SandboxWarningRecord(
        code=VAL_066.code,
        message=message,
        file=file,
        flowgroup=flowgroup_name,
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
