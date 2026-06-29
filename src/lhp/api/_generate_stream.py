"""Private §5.7 event-stream body for the full pipeline-generation path.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here; they use
:meth:`lhp.api.GenerationFacade.generate_pipelines`, which is a thin
``yield from`` over :func:`_stream_pipeline_generation` here.

This split exists for the same reason as :mod:`lhp.api._plan_stream`: to keep
:mod:`lhp.api._generation_facade` under the constitution §3.3 soft cap. The
generate stream consolidates the FULL generate orchestration — discover,
preflight, generate, format, monitoring-finalize, and (when bundle is enabled)
bundle-sync — into a single §5.7 event stream, which is too large to inline in
the facade module alongside the per-method delegation surface. The facade
method holds the public contract / ``:stability:`` annotation; the stream logic
lives here.

Layering note (constitution §5.1/§5.3, the import-linter "LHP top-down
layering" contract): the bundle-sync phase is composed HERE — in the ``api``
layer, which sits ABOVE ``bundle`` — by calling the ``bundle`` layer directly
(:class:`~lhp.bundle.manager.BundleManager` plus the shared
:func:`~lhp.api._bundle_facade._wipe_resources_lhp` helper). It is NEVER routed
through the orchestrator: the orchestrator is ``core``, which sits BELOW
``bundle``, so a ``core → bundle`` import edge is FORBIDDEN. Discovery and
monitoring-finalize, by contrast, ARE routed through the orchestrator (both are
``core`` work, a legal downward ``api → core`` edge).

:stability: internal
"""

# JUSTIFIED: This module deliberately consolidates the FULL generate
# orchestration (discover, preflight, generate, format, monitoring,
# bundle_sync) into ONE §5.7 event stream — see the module docstring for why
# it is split out of `_generation_facade` rather than living inline. The six
# phases plus their §1.4 error-rendezvous bodies exceed the §3.3 500-line soft
# cap; each phase is a single linear segment of the one stream and cannot be
# meaningfully decomposed without fragmenting the event ordering. Under §9.3's
# 800-line hard cap.

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from lhp.api._bundle_facade import _wipe_resources_lhp
from lhp.api._converters_common import (
    _derive_worklist_fields,
    _emit_warning_records,
    _issue_view_to_lhp_error,
)
from lhp.api._generation_converters import (
    _build_generation_batch_failure,
    _build_generation_batch_success,
    _delta_to_generation_response,
)
from lhp.api._preflight import _run_project_preflight
from lhp.api.events import (
    ErrorEmitted,
    GenerationCompleted,
    LHPEvent,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
)
from lhp.errors import ErrorFactory, LHPError, codes
from lhp.utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from lhp.api._progress import ProgressSink
    from lhp.api.responses import BatchGenerationResponse, GenerationResponse
    from lhp.core.sandbox import SandboxRewritePlan
    from lhp.models import FlowGroup
    from lhp.models.processing import RunWarningRecord, SandboxRunConfig

    # Internal orchestrator type, referenced only as a quoted annotation
    # (§1.10, §9.13) — never named directly in the public API surface.
    _Orchestrator = Any


def _run_bundle_sync(
    orchestrator: "_Orchestrator",
    *,
    env: str,
    output_dir: Path,
    sandbox_plan: Optional["SandboxRewritePlan"] = None,
) -> Tuple[int, int]:
    """Wipe ``resources/lhp/`` and sync bundle resources; return ``(synced, deleted)``.

    Composes the ``bundle`` layer DIRECTLY from the ``api`` layer (the legal
    ``api → bundle`` downward edge): wipes ``resources/lhp/`` via the shared
    :func:`~lhp.api._bundle_facade._wipe_resources_lhp` helper (honoring the
    wipe-and-regenerate contract on :class:`BundleManager`), then drives
    :meth:`BundleManager.sync_resources_with_generated_files`.

    Unlike :meth:`BundleFacade._do_sync_resources` — which intentionally
    SWALLOWS failures into a :class:`BundleSyncResult` DTO for the STANDALONE
    public ``sync_resources`` step (so the CLI can map ``error_code`` → exit
    code) — this in-stream helper lets a structured :class:`LHPError`
    (``LHP-CFG-020`` from :class:`BundleResourceError`) PROPAGATE. The §1.4
    in-stream contract requires the caller to perform the
    ``ErrorEmitted`` + raise rendezvous on a genuine structured error, so the
    swallow-to-DTO behavior is deliberately bypassed here. (Bundle errors are
    CONFIG-category LHPErrors — :class:`BundleResourceError` /
    :class:`BundleConfigurationError` both render as ``LHP-CFG-020``; there is
    no dedicated bundle error category.)

    ``sandbox_plan``: on a ``--sandbox`` run the project-level event-log
    table name is namespaced like any produced table — the composed
    ``{prefix}{pipeline}{suffix}`` is treated as the table LEAF and routed
    through the single :func:`~lhp.core.sandbox.rename_parts` choke point as a
    plain ``str -> str`` closure on :class:`BundleManager`'s
    ``event_log_name_transform`` seam. ``None`` (non-sandbox) constructs the
    manager without a transform — byte-identical to the pre-sandbox behavior.

    NOT routed through the orchestrator: that would create a forbidden
    ``core → bundle`` import edge (the orchestrator is ``core``; ``bundle`` sits
    above it).
    """
    from lhp.bundle.manager import BundleManager

    event_log_name_transform: Optional[Callable[[str], str]] = None
    if sandbox_plan is not None:
        # Deferred like the BundleManager import above: core must not ride on
        # ``import lhp.api`` import time.
        from lhp.core.sandbox import rename_parts

        strategy = sandbox_plan.renames.strategy

        def _rename_event_log_leaf(name: str) -> str:
            return rename_parts(strategy, None, None, name)[2]

        event_log_name_transform = _rename_event_log_leaf

    project_root = orchestrator.project_root
    with perf_timer("facade.generate.bundle_sync"):
        deleted_count = _wipe_resources_lhp(project_root)
        bundle_manager = BundleManager(
            project_root,
            orchestrator.pipeline_config_path,
            project_config=orchestrator.project_config,
            event_log_name_transform=event_log_name_transform,
        )
        synced_count = bundle_manager.sync_resources_with_generated_files(
            output_dir, env
        )
        bundle_manager.emit_wheels_bundle_file(output_dir, env)
    return synced_count, deleted_count


def _consume_generate_stream(
    orchestrator: "_Orchestrator",
    logger: logging.Logger,
    *,
    pipeline_filter: Optional[str] = None,
    pipeline_fields: Sequence[str] = (),
    env: str,
    output_dir: Optional[Path],
    specific_flowgroups: Optional[List[str]] = None,
    include_tests: bool = False,
    pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
    max_workers: Optional[int] = None,
    progress: ProgressSink | None = None,
    sandbox_plan: Optional["SandboxRewritePlan"] = None,
) -> Generator[
    LHPEvent,
    None,
    Tuple["BatchGenerationResponse", Tuple["RunWarningRecord", ...]],
]:
    """Consume the orchestrator delta-stream; yield per-pipeline events.

    Internal generate-phase body for :func:`_stream_pipeline_generation`. Drives
    the orchestrator's ``generate_pipelines`` delta-generator (which itself
    ``yield from`` the engine/gate/commit source of truth in the execution
    service) and, for EACH
    :class:`~lhp.models.processing.PipelineDelta` it yields IN INPUT PIPELINE
    ORDER, emits a :class:`PipelineStarted` immediately followed by exactly ONE
    terminal — :class:`PipelineCompleted` (success) or :class:`PipelineFailed`
    (failure). Because the flat engine crystallises every delta post-pool with
    its whole outcome in hand, Started is never emitted without its terminal (no
    dangling Starts, on either path).

    The orchestrator generator is driven explicitly (``next`` in a loop) rather
    than via ``yield from`` because its yield type (``PipelineDelta``) differs
    from this stream's yield type (``LHPEvent``): each delta is TRANSFORMED into
    the paired progress events here. The orchestrator generator's
    ``StopIteration.value`` — the batch's merged + deduped worker deprecation
    warnings — is captured from the terminating :class:`StopIteration` and
    returned to the caller (see ``warnings`` below).

    Returns (via ``StopIteration.value``, captured by the caller's
    ``yield from``) a 2-tuple ``(response, warnings)``:

    * **response** — the terminal :class:`BatchGenerationResponse`:

      - **Clean run** — a success DTO aggregating the per-pipeline responses.
      - **Non-LHP infra failure** (e.g. an :class:`OSError` from a commit-time
        disk write AFTER the gate passed) — a failure DTO via
        :func:`_build_generation_batch_failure`, so the CLI gets a terminal DTO
        with an ``error_code`` rather than a live exception. This is an
        intentional, documented degradation (commit is a non-transactional
        multi-file write; a partial tree may remain), NOT a §1.4 gap.

    * **warnings** — the batch's merged + deduped worker warning records
      (deprecations, plus sandbox rewrite warnings on a ``sandbox_plan`` run).
      The CALLER (:func:`_stream_pipeline_generation`) emits these as
      :class:`WarningEmitted` events in/after the generate phase; this body
      never emits them itself (the caller owns the shared ``(code, file)`` dedup
      set spanning the discover-phase main-thread warnings). Empty on the
      LHP-gate-raise path (the generator raises instead of returning) and on the
      non-LHP-degradation path (the return value is not delivered).

    * **Structured (:class:`LHPError`) failure** — most importantly the
      all-or-nothing gate aggregate — is NOT returned: the delta-generator
      yields every failure delta first (each paired here as
      ``PipelineStarted`` + ``PipelineFailed``), then raises the aggregate. This
      body emits exactly ONE :class:`ErrorEmitted` then re-raises (a bare
      ``raise`` preserves the cause chain, B904), performing the §1.4
      rendezvous; no terminal DTO is produced.

    The ``generated/<env>`` wipe is NOT performed here: the commit step owns the
    single whole-env wipe, which runs AFTER the gate so a gate failure leaves
    prior output untouched. A dry run threads ``output_dir=None``, so it is
    forwarded as-is and the engine writes nothing.
    """
    pipeline_responses: Dict[str, "GenerationResponse"] = {}
    warnings: Tuple["RunWarningRecord", ...] = ()
    try:
        with perf_timer(f"facade.generate_pipelines [{len(pipeline_fields)}]"):
            delta_stream = orchestrator.generate_pipelines(
                pipeline_filter=pipeline_filter,
                pipeline_fields=(
                    list(pipeline_fields) if pipeline_filter is None else None
                ),
                env=env,
                output_dir=output_dir,
                specific_flowgroups=specific_flowgroups,
                include_tests=include_tests,
                pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                max_workers=max_workers,
                # Adapt the public counter to the plain core callables (§3.6:
                # no Protocol/ABC — a concrete sink suffices for one consumer).
                on_total=None if progress is None else progress.on_total,
                on_flowgroup_done=None if progress is None else progress.on_advance,
                # The --sandbox rewrite plan; None is the legacy no-rewrite
                # path, byte-identical to a run without the kwarg.
                sandbox_plan=sandbox_plan,
            )
            # Drive explicitly (not ``yield from``) to capture ``StopIteration.value``
            # (merged worker warnings) while transforming deltas into §5.7 events.
            while True:
                try:
                    delta = next(delta_stream)
                except StopIteration as stop:
                    warnings = stop.value or ()
                    break
                response = _delta_to_generation_response(delta, output_dir=output_dir)
                pipeline_responses[delta.pipeline_name] = response
                yield PipelineStarted(pipeline=delta.pipeline_name)
                if delta.success:
                    yield PipelineCompleted(
                        pipeline=delta.pipeline_name,
                        duration_s=delta.duration_s,
                        files_written=delta.files_written,
                    )
                else:
                    yield PipelineFailed(
                        pipeline=delta.pipeline_name,
                        code=response.error_code or "LHP-GEN-901",
                        message=response.error_message or "(no message)",
                    )
        return (
            _build_generation_batch_success(pipeline_responses, output_dir=output_dir),
            warnings,
        )
    except LHPError as exc:
        # §1.4 rendezvous: the structured failure (most importantly the
        # all-or-nothing gate aggregate) has already had its per-pipeline
        # failure deltas yielded above; emit exactly one ErrorEmitted then
        # re-raise. The bare ``raise`` keeps the cause chain (B904).
        yield ErrorEmitted(lhp_error=exc)
        raise
    except Exception as exc:
        # A NON-LHP infra failure at/after the gate — e.g. an ``OSError`` from a
        # commit-time disk write once the gate has already passed — is reported
        # as a batch-failure DTO, NOT routed through the ErrorEmitted+raise
        # rendezvous (reserved for LHPError). Such a failure may leave a
        # partially written output tree: commit is a non-transactional
        # multi-file write and making it atomic is explicitly out of scope.
        # Intentional, documented degradation.
        logger.exception("Batch pipeline generation failed")
        return (_build_generation_batch_failure(pipeline_responses, exc), warnings)


def _stream_pipeline_generation(
    orchestrator: "_Orchestrator",
    logger: logging.Logger,
    *,
    pipeline_filter: Optional[str] = None,
    pipeline_fields: Sequence[str] = (),
    env: str,
    output_dir: Optional[Path],
    specific_flowgroups: Optional[List[str]] = None,
    include_tests: bool = False,
    apply_formatting: bool | None = None,
    bundle_enabled: bool = False,
    pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
    max_workers: Optional[int] = None,
    progress: ProgressSink | None = None,
    sandbox: bool = False,
) -> Iterator[LHPEvent]:
    """Yield the full §5.7 progress stream for the full generation path.

    ``sandbox`` switches the run to developer-sandbox mode: AFTER the discover
    phase (and the empty-project guard) the sandbox pre-pass resolves the
    frozen per-run config off the personal profile + team policy
    (:func:`lhp.api._sandbox_run._resolve_sandbox_run`; a structured failure
    performs the §1.4 ``ErrorEmitted`` + raise rendezvous), the profile scope
    becomes the worklist (D6 — only in-scope pipelines are generated; the
    bundle sync runs unchanged and falls out via wipe-and-regenerate), and the
    table-rewrite plan from ``orchestrator.build_sandbox_rewrite_plan`` is
    threaded into the generate phase. The ``monitoring`` phase is SKIPPED on a
    sandbox run (no PhaseStarted — ``finalize_monitoring_artifacts`` would
    clobber the shared committed ``monitoring/<env>`` artifacts with a
    worklist a sandbox run never generates), and the bundle sync namespaces
    the project-level event-log table name. With ``sandbox=False`` (the
    default) the stream is byte-identical to the pre-sandbox behavior.

    Implements :meth:`lhp.api.GenerationFacade.generate_pipelines` (see its
    docstring for the public contract). Consolidates the FULL generate
    orchestration into one event stream, emitting in order:

    1. :class:`OperationStarted`.
    2. ``discover`` phase pair — wraps the orchestrator's
       ``bootstrap.discover_all_flowgroups`` (a legal ``api → core`` edge) when
       the caller did NOT supply ``pre_discovered_all_flowgroups``; the
       resolved list is threaded forward to preflight + generate so discovery
       runs exactly once. A caller-supplied list is honored as-is (no
       re-discovery) and forwarded unchanged. When NEITHER ``pipeline_filter``
       NOR ``pipeline_fields`` was supplied, the all-pipelines worklist is
       auto-derived from that resolved set via
       :func:`~lhp.api._converters_common._derive_worklist_fields` before the
       generate phase. Immediately AFTER the phase pair,
       the MAIN-THREAD bare-``{token}`` deprecation scan
       (``orchestrator.discovery.scan_deprecation_warnings()`` →
       ``LHP-DEPR-001``) is emitted as :class:`WarningEmitted` events (the read
       path runs on the main thread, before any worker pool).
    3. ``preflight`` phase pair — wraps :func:`_run_project_preflight`.
    4. ``generate`` phase pair, with a :class:`PipelineStarted` + single
       terminal per pipeline in input order. AFTER the phase pair, the WORKER
       deprecation warnings merged off the engine (``database`` /
       ``database_suffix`` / schema-transform ``enforcement`` →
       ``LHP-DEPR-002/003/004``), captured from the generate body's
       ``StopIteration.value``, are emitted as :class:`WarningEmitted` events —
       deduped against the discover-phase warnings via a shared ``(code, file)``
       set so the union surfaces once, in deterministic order.
    5. ``format`` phase pair (skipped on dry-run / on a degraded generate / when
       the resolved ``apply_formatting`` is false) — the single terminal
       ``ruff format`` pass over the whole ``generated/<env>`` tree, routed
       through ``orchestrator.format_output_tree`` (a legal ``api → core`` edge).
       A structured formatter failure (``LHP-CFG-033`` / ``LHP-CFG-034``) RAISES
       via the same in-stream rendezvous as ``bundle_sync``.
    6. ``monitoring`` phase pair (skipped on dry-run / on a degraded generate /
       on a sandbox run) — wraps ``orchestrator.finalize_monitoring_artifacts``
       (a legal ``api → core`` edge).
    7. ``bundle_sync`` phase pair, ONLY when ``bundle_enabled`` and the
       generate succeeded and this is not a dry-run — composes the ``bundle``
       layer directly (see :func:`_run_bundle_sync`).
    8. Terminal :class:`GenerationCompleted` carrying the
       :class:`BatchGenerationResponse`.

    Failure surfacing (§1.4): a structured :class:`LHPError` — preflight,
    the all-or-nothing generate gate, or a bundle-sync ``LHP-CFG-020`` — RAISES
    after exactly one :class:`ErrorEmitted` (the raise closes the stream, so no
    terminal :class:`GenerationCompleted` follows). A non-LHP infra failure at
    or after the generate commit degrades to a failed
    :class:`BatchGenerationResponse` carried on a normal terminal
    :class:`GenerationCompleted` (the monitoring + bundle phases are then
    skipped). Files already committed to disk PERSIST in every failure mode
    (commit is a non-transactional multi-file write; the bundle phase runs only
    AFTER a clean generate and never rolls the generated tree back).
    """
    yield OperationStarted(operation_name="generate_pipelines", env=env)

    # Shared ``(code, file)`` dedup set spanning BOTH deprecation-warning
    # sources (main-thread discover-phase scan + worker generate-phase merge),
    # so the union surfaces as ONE deduped, ordered WarningEmitted sequence.
    warnings_seen: Set[Tuple[str, Optional[Path]]] = set()

    # Phase: discover. When no pre_discovered_all_flowgroups is supplied, runs
    # discovery HERE and threads the list forward to preflight + generate so
    # discovery runs exactly once. A caller-supplied list is used as-is.
    discover_start = time.perf_counter()
    yield PhaseStarted(phase="discover")
    resolved_flowgroups: Optional[Sequence["FlowGroup"]] = pre_discovered_all_flowgroups
    if resolved_flowgroups is None:
        resolved_flowgroups = orchestrator.bootstrap.discover_all_flowgroups()
    yield PhaseCompleted(
        phase="discover",
        duration_s=time.perf_counter() - discover_start,
        success=True,
    )

    # Empty-project guard (spec §6.6, GENERATE-only). A project with ZERO
    # flowgroups is a fatal LHP-CFG-014 on generate — intentionally asymmetric
    # with validate, which folds the empty case into a clean ValidationCompleted
    # (exit 0). This keys on the PROJECT-WIDE discovered set, NOT on the
    # ``--pipeline``/``pipeline_fields`` worklist: a valid project whose filter
    # selects nothing keeps its existing "generate nothing" behavior (the engine
    # produces an empty success batch). Following the §1.4/§9.24 generate-fatal
    # pattern, emit exactly one ErrorEmitted then raise so the CLI boundary
    # renders the panel + exits 1; the never-raises preflight contract is
    # untouched (this guard lives in the stream, not in ``_run_project_preflight``).
    if not resolved_flowgroups:
        empty_project_error = ErrorFactory.config_error(
            codes.CFG_014,
            title="No flowgroups found",
            details="No flowgroups found in project",
            suggestions=[
                "Check that the project contains YAML flowgroup files",
                "Run 'lhp info' to see project configuration",
            ],
        )
        yield ErrorEmitted(lhp_error=empty_project_error)
        raise empty_project_error

    # Sandbox pre-pass (runs AFTER the discover phase — the resolver needs the
    # discovered pipeline set — and BEFORE the worklist derivation so the
    # profile scope BECOMES the worklist; D6: only in-scope pipelines are
    # generated). Resolves the frozen per-run config off the personal profile
    # (.lhp/profile.yaml) + the team policy (lhp.yaml ``sandbox:`` block),
    # then builds the table-rewrite plan over the discovered flowgroups via
    # the orchestrator seam. The resolver raises structured LHPErrors
    # (LHP-IO-025 / LHP-CFG-064 / LHP-CFG-065 / LHP-VAL-064); §1.4 rendezvous
    # HERE: exactly one ErrorEmitted then re-raise (bare ``raise`` keeps the
    # cause chain, B904), mirroring the empty-project guard above.
    sandbox_run: Optional["SandboxRunConfig"] = None
    sandbox_plan: Optional["SandboxRewritePlan"] = None
    if sandbox:
        # Deferred: the helper imports lhp.core at module level, so it is
        # imported LAZILY at call time (the same discipline the facade applies
        # to this stream module) — never on ``import lhp.api``.
        from lhp.api._sandbox_run import _resolve_sandbox_run

        try:
            sandbox_run = _resolve_sandbox_run(orchestrator, env, resolved_flowgroups)
        except LHPError as exc:
            yield ErrorEmitted(lhp_error=exc)
            raise
        # The profile's resolved concrete scope is the worklist, through the
        # EXISTING pipeline_fields mechanism (the facade already rejects a
        # sandbox=True run combined with pipeline_filter/pipeline_fields).
        pipeline_fields = sandbox_run.pipelines
        sandbox_plan = orchestrator.build_sandbox_rewrite_plan(
            env, sandbox_run, list(resolved_flowgroups)
        )

    # Auto-derive the all-pipelines worklist from the just-discovered set when
    # the caller supplied no worklist, so passing NEITHER a ``pipeline_filter``
    # NOR a ``pipeline_fields`` batch generates the WHOLE project. A supplied
    # filter/batch is honored unchanged (§4.1 single seam).
    effective_pipeline_fields = _derive_worklist_fields(
        pipeline_filter, pipeline_fields, resolved_flowgroups
    )

    # LHP-DEPR-001 warnings: scanned on the main thread before the worker pool,
    # seeding the shared dedup set.
    yield from _emit_warning_records(
        orchestrator.discovery.scan_deprecation_warnings(
            pipeline_filter=pipeline_filter
        ),
        seen=warnings_seen,
    )

    # Phase: preflight. §9.24 — MUST sit in this outer generator BEFORE generate,
    # because the delta-consumption helper's ``except Exception → return failure``
    # body swallows exceptions into a DTO, which would eat the ErrorEmitted/raise
    # rendezvous. When ``bundle_enabled``, also runs the bundle catalog/schema
    # check (→ LHP-CFG-026), only if orchestrator has a pipeline_config_path.
    preflight_start = time.perf_counter()
    yield PhaseStarted(phase="preflight")
    preflight_issues = _run_project_preflight(
        orchestrator,
        env=env,
        bundle_enabled=bundle_enabled,
        include_tests=include_tests,
        pre_discovered_all_flowgroups=resolved_flowgroups,
    )
    if preflight_issues:
        yield PhaseCompleted(
            phase="preflight",
            duration_s=time.perf_counter() - preflight_start,
            success=False,
        )
        preflight_error = _issue_view_to_lhp_error(preflight_issues[0])
        yield ErrorEmitted(lhp_error=preflight_error)
        raise preflight_error
    yield PhaseCompleted(
        phase="preflight",
        duration_s=time.perf_counter() - preflight_start,
        success=True,
    )

    # Phase: generate. LHPError gate failure raises inside the helper (§1.4),
    # closing the stream so PhaseCompleted/GenerationCompleted below are skipped.
    generate_start = time.perf_counter()
    yield PhaseStarted(phase="generate")
    response, worker_warnings = yield from _consume_generate_stream(
        orchestrator,
        logger,
        pipeline_filter=pipeline_filter,
        pipeline_fields=effective_pipeline_fields,
        env=env,
        output_dir=output_dir,
        specific_flowgroups=specific_flowgroups,
        include_tests=include_tests,
        pre_discovered_all_flowgroups=resolved_flowgroups,
        max_workers=max_workers,
        progress=progress,
        sandbox_plan=sandbox_plan,
    )
    yield PhaseCompleted(
        phase="generate",
        duration_s=time.perf_counter() - generate_start,
        success=response.success,
    )
    # LHP-DEPR-002/003/004 worker warnings — prefixed, on a sandbox run, by
    # the pre-pass plan warnings (mixed-producer sink LHP-VAL-065) so BOTH
    # sources join the run's single (code, file)-deduped WarningEmitted
    # sequence; worker sandbox warnings (LHP-VAL-066) already arrive in
    # ``worker_warnings`` via StopIteration.value.
    after_generate_warnings: Sequence["RunWarningRecord"] = (
        (*sandbox_plan.warnings, *worker_warnings)
        if sandbox_plan is not None
        else worker_warnings
    )
    yield from _emit_warning_records(after_generate_warnings, seen=warnings_seen)

    # Phase: format. The single terminal ``ruff format`` pass over the whole
    # ``generated/<env>`` tree, surfaced as a §5.7 phase HERE — the stream owns
    # the invocation (``generate_pipelines`` is a pure delta-generator that no
    # longer formats at its tail; the read-only plan path drives the same
    # orchestrator primitive itself for byte parity). Guarded on the RESOLVED
    # ``apply_formatting`` bool (the orchestrator owns the tri-state seam — ``None``
    # → the project's ``lhp.yaml`` setting), so ``--no-format`` skips the phase
    # outright (no PhaseStarted). ``response.success`` keeps a degraded generate
    # (a non-LHP commit-time failure that left a partial tree) from formatting an
    # incomplete tree; ``output_dir is None`` already excludes dry-runs. Routed
    # through the orchestrator (a legal api → core edge).
    effective_apply_formatting = orchestrator.resolve_apply_formatting(apply_formatting)
    if output_dir is not None and effective_apply_formatting and response.success:
        format_start = time.perf_counter()
        yield PhaseStarted(phase="format")
        try:
            orchestrator.format_output_tree(output_dir)
        except LHPError as exc:
            # ``format_generated_tree`` RAISES a structured LHPError (LHP-CFG-033
            # on a non-zero ruff exit, LHP-CFG-034 if ruff is missing). Perform
            # the §1.4 / §9.19 in-stream rendezvous — PhaseCompleted(success=False)
            # then exactly one ErrorEmitted then re-raise (bare ``raise`` keeps the
            # cause chain, B904) — mirroring the bundle_sync phase. The generated
            # tree already written to disk persists; the format pass never rolls
            # it back. The raise closes the stream, so the monitoring / bundle_sync
            # phases and the terminal GenerationCompleted below are skipped.
            yield PhaseCompleted(
                phase="format",
                duration_s=time.perf_counter() - format_start,
                success=False,
            )
            yield ErrorEmitted(lhp_error=exc)
            raise
        yield PhaseCompleted(
            phase="format",
            duration_s=time.perf_counter() - format_start,
            success=True,
        )

    # Phase: monitoring. Skipped on dry-run (no real tree to reconcile), on a
    # degraded generate (mirrors CLI behavior), and on a SANDBOX run — the
    # finalizer wipes monitoring/<env>/ + the monitoring resources/*.job.yml
    # and rewrites them referencing ALL pipelines plus a monitoring pipeline a
    # sandbox run never generates, which would clobber shared committed
    # artifacts. Routed through the orchestrator (api → core) —
    # MonitoringFinalizerService must stay below api.
    if output_dir is not None and response.success and sandbox_run is None:
        monitoring_start = time.perf_counter()
        yield PhaseStarted(phase="monitoring")
        orchestrator.finalize_monitoring_artifacts(env, output_dir)
        yield PhaseCompleted(
            phase="monitoring",
            duration_s=time.perf_counter() - monitoring_start,
            success=True,
        )

    # Phase: bundle_sync. Composed directly in the api layer (never through the
    # orchestrator — that would be a forbidden core → bundle edge). A bundle-sync
    # LHPError (LHP-CFG-020) performs the §1.4 rendezvous; the generated tree
    # already written to disk persists — the bundle phase never rolls it back.
    if bundle_enabled and output_dir is not None and response.success:
        bundle_start = time.perf_counter()
        yield PhaseStarted(phase="bundle_sync")
        try:
            synced_count, deleted_count = _run_bundle_sync(
                orchestrator,
                env=env,
                output_dir=output_dir,
                sandbox_plan=sandbox_plan,
            )
        except LHPError as exc:
            yield PhaseCompleted(
                phase="bundle_sync",
                duration_s=time.perf_counter() - bundle_start,
                success=False,
            )
            yield ErrorEmitted(lhp_error=exc)
            raise
        logger.debug(
            f"Bundle resources synced: {synced_count} written, {deleted_count} removed"
        )
        yield PhaseCompleted(
            phase="bundle_sync",
            duration_s=time.perf_counter() - bundle_start,
            success=True,
        )

    yield GenerationCompleted(response=response)
