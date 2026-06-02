"""Private §5.7 event-stream body for the full pipeline-generation path.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here; they use
:meth:`lhp.api.GenerationFacade.generate_pipelines`, which is a thin
``yield from`` over :func:`_stream_pipeline_generation` here.

This split exists for the same reason as :mod:`lhp.api._plan_stream`: to keep
:mod:`lhp.api._generation_facade` under the constitution §3.3 soft cap. The
generate stream consolidates the FULL generate orchestration — discover,
preflight, generate, monitoring-finalize, and (when bundle is enabled)
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

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
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
    _emit_deprecation_warnings,
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
from lhp.errors import LHPError
from lhp.utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from lhp.api.responses import BatchGenerationResponse, GenerationResponse
    from lhp.models import FlowGroup
    from lhp.models.processing import DeprecationWarningRecord

    # Internal orchestrator type, referenced only as a quoted annotation
    # (§1.10, §9.13) — never named directly in the public API surface.
    _Orchestrator = Any


def _run_bundle_sync(
    orchestrator: "_Orchestrator",
    *,
    env: str,
    output_dir: Path,
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

    NOT routed through the orchestrator: that would create a forbidden
    ``core → bundle`` import edge (the orchestrator is ``core``; ``bundle`` sits
    above it).
    """
    from lhp.bundle.manager import BundleManager

    project_root = orchestrator.project_root
    with perf_timer("facade.generate.bundle_sync"):
        deleted_count = _wipe_resources_lhp(project_root)
        bundle_manager = BundleManager(
            project_root,
            orchestrator.pipeline_config_path,
            project_config=orchestrator.project_config,
        )
        synced_count = bundle_manager.sync_resources_with_generated_files(
            output_dir, env
        )
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
    apply_formatting: bool | None = None,
    pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
    max_workers: Optional[int] = None,
) -> Generator[
    LHPEvent,
    None,
    Tuple["BatchGenerationResponse", Tuple["DeprecationWarningRecord", ...]],
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

    * **warnings** — the batch's merged + deduped worker deprecation warnings.
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
    warnings: Tuple["DeprecationWarningRecord", ...] = ()
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
                apply_formatting=apply_formatting,
                pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                max_workers=max_workers,
            )
            # Drive the delta-generator explicitly so its ``StopIteration.value``
            # (the merged worker warnings) is captured while each ``PipelineDelta``
            # is transformed into the paired §5.7 progress events.
            while True:
                try:
                    delta = next(delta_stream)
                except StopIteration as stop:
                    warnings = stop.value or ()
                    break
                response = _delta_to_generation_response(delta, output_dir=output_dir)
                pipeline_responses[delta.pipeline_name] = response
                # Started + its single terminal, as a pair (no dangling Start).
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
) -> Iterator[LHPEvent]:
    """Yield the full §5.7 progress stream for the full generation path.

    Implements :meth:`lhp.api.GenerationFacade.generate_pipelines` (see its
    docstring for the public contract). Consolidates the FULL generate
    orchestration into one event stream, emitting in order:

    1. :class:`OperationStarted`.
    2. ``discover`` phase pair — wraps the orchestrator's
       ``bootstrap.discover_all_flowgroups`` (a legal ``api → core`` edge) when
       the caller did NOT supply ``pre_discovered_all_flowgroups``; the
       resolved list is threaded forward to preflight + generate so discovery
       runs exactly once. A caller-supplied list is honored as-is (no
       re-discovery) and forwarded unchanged. Immediately AFTER the phase pair,
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
    5. ``monitoring`` phase pair (skipped on dry-run / on a degraded generate)
       — wraps ``orchestrator.finalize_monitoring_artifacts`` (a legal
       ``api → core`` edge).
    6. ``bundle_sync`` phase pair, ONLY when ``bundle_enabled`` and the
       generate succeeded and this is not a dry-run — composes the ``bundle``
       layer directly (see :func:`_run_bundle_sync`).
    7. Terminal :class:`GenerationCompleted` carrying the
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

    # Phase: discover. Reconcile the caller-supplied ``pre_discovered_all_flowgroups``
    # with facade-owned discovery: when the caller did NOT supply a list, run
    # the orchestrator's ``bootstrap.discover_all_flowgroups`` HERE (a legal
    # ``api → core`` downward edge — the same call ``_inspection_facade`` makes)
    # and thread the resolved list forward to BOTH preflight and generate. This
    # makes the discover phase wrap real work (closing E3's GAP) AND guarantees
    # discovery runs exactly once across the three downstream consumers. A
    # caller-supplied list WINS and is forwarded unchanged (no double-discover).
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
    # Main-thread bare-``{token}`` deprecation warnings (LHP-DEPR-001). Scanned
    # on the main thread (the read path precedes the worker pool); emitted right
    # after the discover phase and seeding the shared dedup set.
    yield from _emit_deprecation_warnings(
        orchestrator.discovery.scan_deprecation_warnings(),
        seen=warnings_seen,
    )

    # Phase: preflight. §9.24 — the preflight LOGIC is single-sourced in
    # ``_run_project_preflight`` (shared with the validate path). This call site
    # only SURFACES failures: generate raises (validate folds them into a failed
    # batch instead). It MUST sit in this OUTER generator — BEFORE the generate
    # phase below — because the delta-consumption helper's
    # ``except Exception → return failure`` body SWALLOWS exceptions into a DTO,
    # which would eat the ErrorEmitted/raise rendezvous.
    #
    # ``bundle_enabled`` is threaded from the CLI (``--no-bundle`` +
    # databricks.yml auto-detect): when ``True`` this preflight also runs the
    # bundle catalog/schema check (→ ``LHP-CFG-026``), single-sourced with
    # validate. It only fires if the orchestrator was constructed with a
    # ``pipeline_config_path``. Covers DUPLICATE + TEST-REPORTING + bundle.
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

    # Phase: generate. Consume the orchestrator's delta-generator end-to-end,
    # emitting the paired per-pipeline events as each delta crystallises. The
    # helper RETURNS ``(response, warnings)`` (success or graceful non-LHP
    # failure); an LHPError gate failure is surfaced via ErrorEmitted+raise
    # INSIDE the helper (per §1.4 the raise closes the stream, so the
    # PhaseCompleted/GenerationCompleted below are skipped).
    generate_start = time.perf_counter()
    yield PhaseStarted(phase="generate")
    response, worker_warnings = yield from _consume_generate_stream(
        orchestrator,
        logger,
        pipeline_filter=pipeline_filter,
        pipeline_fields=pipeline_fields,
        env=env,
        output_dir=output_dir,
        specific_flowgroups=specific_flowgroups,
        include_tests=include_tests,
        apply_formatting=apply_formatting,
        pre_discovered_all_flowgroups=resolved_flowgroups,
        max_workers=max_workers,
    )
    yield PhaseCompleted(
        phase="generate",
        duration_s=time.perf_counter() - generate_start,
        success=response.success,
    )
    # Worker deprecation warnings (LHP-DEPR-002/003/004), deduped against the
    # main-thread set via the shared ``warnings_seen``.
    yield from _emit_deprecation_warnings(worker_warnings, seen=warnings_seen)

    # Phase: monitoring (absorbed from the former standalone
    # ``finalize_monitoring_artifacts`` public step). Routed through the
    # orchestrator (``api → core``, legal) so the ``core`` MonitoringFinalizerService
    # stays below ``api``. Skipped on a dry-run (``output_dir is None`` — there is
    # no real tree to reconcile) and on a degraded generate (``response.success``
    # is ``False`` — mirrors the CLI, which finalized monitoring only after a
    # clean batch). On the LHPError gate-failure path the generate phase above
    # already raised, so this is never reached.
    if output_dir is not None and response.success:
        monitoring_start = time.perf_counter()
        yield PhaseStarted(phase="monitoring")
        orchestrator.finalize_monitoring_artifacts(env, output_dir)
        yield PhaseCompleted(
            phase="monitoring",
            duration_s=time.perf_counter() - monitoring_start,
            success=True,
        )

    # Phase: bundle_sync (D2). ONLY when bundle is enabled, the generate
    # succeeded, and this is not a dry-run. Composed in the ``api`` layer by
    # calling the ``bundle`` layer DIRECTLY (NEVER through the orchestrator —
    # that would be a forbidden ``core → bundle`` edge). A bundle-sync
    # ``LHPError`` (``LHP-CFG-020`` from BundleResourceError) performs the §1.4
    # rendezvous here: emit one ErrorEmitted then re-raise (the raise is the
    # terminal, NOT ``GenerationCompleted``). The Python files already written to
    # ``generated/<env>`` by the generate commit PERSIST — the bundle phase only
    # writes ``resources/lhp/`` and never rolls the generated tree back (mirrors
    # the commit-time OSError degradation note in ``_consume_generate_stream``).
    if bundle_enabled and output_dir is not None and response.success:
        bundle_start = time.perf_counter()
        yield PhaseStarted(phase="bundle_sync")
        try:
            synced_count, deleted_count = _run_bundle_sync(
                orchestrator, env=env, output_dir=output_dir
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
