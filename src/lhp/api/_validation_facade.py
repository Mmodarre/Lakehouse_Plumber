"""Private module — implementation of the public :class:`ValidationFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`ValidationFacade` from :mod:`lhp.api` (re-exported
via :mod:`lhp.api.facade`). This split exists purely to keep
``lhp/api/facade.py`` under the constitution §3.3 soft cap (500 lines)
while the validation surface absorbs the stream-protocol generator
wrapper plus its ``_consume_validate_stream`` private helper.

Heavy DTO-conversion bodies live in :mod:`lhp.api._validation_converters`
(and the shared error helpers in :mod:`lhp.api._converters_common`); what
remains here is the per-method delegation surface for validation.

:stability: internal
"""

# JUSTIFIED: This module deliberately pairs the ValidationFacade's
# delegation surface with its long-running §5.7 validate event stream —
# discover (+ DEPR-001 scan), sandbox pre-pass, preflight, validate — in
# ONE module. The phase machinery and its single shared event/warning
# state (the §1.4 ErrorEmitted+raise rendezvous, the discover-phase
# warning dedup) are one cohesive responsibility; splitting facade from
# stream (as the generate side does with `_generate_stream`) is the known
# decomposition, but it slices that rendezvous state across modules for
# ~50 lines of relief at 558. Under §9.3's 800-line hard cap.

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
    Optional,
    Sequence,
    Set,
    Tuple,
)

from lhp.api._converters_common import (
    _derive_worklist_fields,
    _emit_warning_records,
)
from lhp.api._preflight import _run_project_preflight
from lhp.api._stream_guard import _cap_event_stream
from lhp.api._validation_converters import (
    _build_validation_batch,
    _build_validation_batch_from_issues,
    _outcome_to_validation_response,
)
from lhp.api.events import (
    ErrorEmitted,
    LHPEvent,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    ValidationCompleted,
)
from lhp.api.responses import (
    BatchValidationResponse,
    ValidationResponse,
)
from lhp.errors import LHPError
from lhp.utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from lhp.api._progress import ProgressSink
    from lhp.core.sandbox import SandboxRewritePlan
    from lhp.models import FlowGroup
    from lhp.models.processing import RunWarningRecord, SandboxWarningRecord

    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10,
    # §9.13). Composition is delegated to ``core/coordination/layers``.
    _Orchestrator = Any


class ValidationFacade:
    """:stability: provisional"""

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self._logger = logging.getLogger(__name__)

    def validate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        max_workers: Optional[int] = None,
        include_tests: bool = True,
        bundle_enabled: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        progress: ProgressSink | None = None,
        sandbox: bool = False,
    ) -> Iterator[LHPEvent]:
        """Stream-protocol wrapper around batch pipeline validation (§5.7).

        Yields the full §5.7 progress stream, in order (mirroring the
        generate path, but REPORT-only — validate never aborts/raises on
        findings):

        1. :class:`OperationStarted` (the mandated first event).
        2. :class:`PhaseStarted` / :class:`PhaseCompleted` for ``"discover"``
           — this facade now performs project-wide flowgroup discovery itself
           inside the phase (see "Discover phase" below). Immediately AFTER the
           phase pair, the MAIN-THREAD bare-``{token}`` deprecation scan
           (``orchestrator.discovery.scan_deprecation_warnings()`` →
           ``LHP-DEPR-001``) is emitted as :class:`WarningEmitted` events.
        3. :class:`PhaseStarted` / :class:`PhaseCompleted` for ``"preflight"``
           (wraps :func:`_run_project_preflight`, fed the discovered set).
        4. :class:`PhaseStarted` for ``"validate"``, then — as the
           outcome-generator is consumed — a :class:`PipelineStarted` paired
           with exactly ONE terminal (:class:`PipelineCompleted` when that
           pipeline validated clean, or :class:`PipelineFailed` when it had
           validation errors) PER PIPELINE in input order, then
           :class:`PhaseCompleted` for ``"validate"``. AFTER the phase pair,
           the WORKER deprecation warnings merged off the engine
           (``database`` / ``database_suffix`` / schema-transform
           ``enforcement`` → ``LHP-DEPR-002/003/004``), captured from the
           validate body's ``StopIteration.value``, are emitted as
           :class:`WarningEmitted` events — deduped against the discover-phase
           warnings via a shared ``(code, file)`` set so the union surfaces
           once, in deterministic order.
        5. Terminal :class:`ValidationCompleted` carrying the
           :class:`BatchValidationResponse`.

        Discover phase: it resolves the project-wide flowgroup set EXACTLY ONCE
        and threads the SAME list into both the preflight phase (so
        :func:`_run_project_preflight` does not re-discover) and the validate
        phase (so the orchestrator's worklist builder does not re-discover):

        * **Pre-discovered path supplied** — when the caller passes
          ``pre_discovered_all_flowgroups`` (e.g. to inject duplicates not yet
          on disk), that list is adopted VERBATIM; the facade does NOT
          re-discover. The discover phase still emits its Start/Complete pair
          (carrying the supplied count) so the §5.7 phase-pairing invariant
          holds.
        * **No pre-discovered path** — the facade discovers via
          ``orchestrator.bootstrap.discover_all_flowgroups()`` (the existing
          project-wide discovery surface, the same one
          :func:`_run_project_preflight` used as its lazy fallback). That
          surface is memoized, so even though the resolved list is also reused
          downstream the filesystem is scanned only once.

        Per-pipeline pairing has NO dangling Starts: a ``PipelineStarted`` is
        emitted only once its terminal outcome is already in hand (the engine
        crystallises outcomes post-pool in input order, each carrying the whole
        per-pipeline result). Because validate REPORTS — there is no
        all-or-nothing gate, so EVERY pipeline yields a Started+terminal pair —
        the load-bearing invariant
        ``count(PipelineStarted) == count(PipelineCompleted) + count(PipelineFailed)``
        holds with equality across the whole batch (unlike generate, where an
        abort can suppress the non-failing pipelines' events).

        Failure surfacing (constitution §1.4, §5.7): validation FINDINGS are
        REPORTED, never raised — a pipeline with errors becomes a
        ``PipelineFailed`` event plus a non-zero-exit
        :class:`BatchValidationResponse` (the terminal still emits). The
        :class:`ErrorEmitted` + ``raise`` rendezvous below is reserved for a
        genuinely catastrophic :class:`LHPError` that escapes the report-only
        stream consumer (e.g. a worklist-build failure), mirroring the
        generate path's §1.4 contract.

        Callers that don't need event-level visibility use
        :func:`lhp.api.collect_response` to walk the stream and return the
        terminal response DTO.

        ``bundle_enabled`` mirrors the generate path: when ``True`` the
        shared preflight also runs the bundle catalog/schema check
        (→ ``LHP-CFG-026``), but only if the orchestrator was constructed
        with a ``pipeline_config_path``. Defaults to ``False`` so the
        non-bundle behavior is unchanged (§9.24).

        ``progress`` is an optional :class:`~lhp.api.ProgressSink` advanced
        as flowgroups complete (``on_total`` once with the total flowgroup
        count, then ``on_advance`` per finished flowgroup), adapted to the
        plain core callables the coordinator added; read its ``total`` /
        ``done`` fields while iterating the stream to drive a progress bar.
        ``None`` (the default) wires no progress callbacks.

        ``sandbox`` switches the run to developer-sandbox mode: the pipeline
        scope and namespace come from the personal ``.lhp/profile.yaml`` plus
        the team ``sandbox:`` policy in ``lhp.yaml``, resolved by the sandbox
        preflight at the start of the run. Because sandbox scope is
        profile-driven, ``sandbox=True`` CANNOT be combined with
        ``pipeline_filter`` / ``pipeline_fields``.

        The §5.7 stream body lives in :meth:`_validate_pipelines_stream`;
        this method restates the canonical signature (§4.2) and forwards it
        through :func:`lhp.api._stream_guard._cap_event_stream` (the §13.4
        event-buffer soft cap) via ``yield from``.

        :stability: provisional
        :raises ValueError: if ``sandbox=True`` is combined with
            ``pipeline_filter`` or ``pipeline_fields`` — API misuse, no
            structured ``LHP-*`` code (the codebase convention for
            invalid-argument combinations).
        :raises lhp.errors.LHPError: ``LHP-VAL-*`` (config/action/schema
            validation), ``LHP-CFG-*`` (project config + substitution),
            ``LHP-FILE-*`` (missing files), ``LHP-MULT-*`` (multi-document
            YAML), and ``LHP-TPL-*`` (template expansion) propagated from
            the per-pipeline workers; on a ``sandbox=True`` run also the
            sandbox preflight errors ``LHP-IO-025`` (missing
            ``.lhp/profile.yaml``), ``LHP-CFG-064`` (malformed profile),
            ``LHP-CFG-065`` (environment not sandbox-enabled), and
            ``LHP-VAL-064`` (profile scope matched no pipelines, or an
            exact entry names the monitoring pipeline). An
            :class:`ErrorEmitted` event is yielded before the exception
            escapes (§1.4 stream protocol).
        """
        if sandbox and (pipeline_filter is not None or pipeline_fields):
            raise ValueError(
                "`sandbox` cannot be combined with `pipeline_filter` or "
                "`pipeline_fields`: sandbox scope comes from the personal "
                "profile (.lhp/profile.yaml)."
            )
        yield from _cap_event_stream(
            self._validate_pipelines_stream(
                pipeline_filter=pipeline_filter,
                pipeline_fields=pipeline_fields,
                env=env,
                max_workers=max_workers,
                include_tests=include_tests,
                bundle_enabled=bundle_enabled,
                pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                progress=progress,
                sandbox=sandbox,
            )
        )

    def _validate_pipelines_stream(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        max_workers: Optional[int] = None,
        include_tests: bool = True,
        bundle_enabled: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        progress: ProgressSink | None = None,
        sandbox: bool = False,
    ) -> Iterator[LHPEvent]:
        """Yield the full §5.7 validate progress stream (the stream body).

        Private generator so the public method can route through
        :func:`lhp.api._stream_guard._cap_event_stream` (§13.4 event-buffer soft cap).

        ``sandbox`` runs the sandbox pre-pass between the discover phase
        (+ its DEPR-001 scan) and the preflight phase, mirroring the
        generate stream: resolve the frozen
        :class:`~lhp.models.processing.SandboxRunConfig` off the personal
        profile + team policy
        (:func:`lhp.api._sandbox_run._resolve_sandbox_run`), scope the
        worklist to the profile's resolved pipelines, and build the
        rewrite plan via the orchestrator seam
        (:meth:`build_sandbox_rewrite_plan`) so the validate workers apply
        the STRUCTURED rewrite and cross-flowgroup validation sees the
        sandbox names. A pre-pass :class:`~lhp.errors.LHPError` is a MODE
        error, not a pipeline finding: §1.4 ``ErrorEmitted`` + raise — it
        never folds into the terminal :class:`BatchValidationResponse`. The
        plan's carried warnings (e.g. mixed-producer ``LHP-VAL-065``) join
        the post-validate-phase warning slot; the generate-only
        ``LHP-VAL-066`` text pass does not run here (documented v1
        limitation). With ``sandbox=False`` (the default) the stream is
        byte-identical to the pre-sandbox behavior.

        :stability: internal
        """
        yield OperationStarted(operation_name="validate_pipelines", env=env)

        # Shared ``(code, file)`` dedup set spanning both deprecation-warning
        # sources (main-thread discover phase + worker validate phase) so the
        # union surfaces deduplicated.
        warnings_seen: Set[Tuple[str, Optional[Path]]] = set()

        discover_start = time.perf_counter()
        yield PhaseStarted(phase="discover")
        if pre_discovered_all_flowgroups is not None:
            resolved_flowgroups: Sequence["FlowGroup"] = pre_discovered_all_flowgroups
        else:
            resolved_flowgroups = self._orchestrator.bootstrap.discover_all_flowgroups()
        # Auto-derive the all-pipelines worklist from the just-discovered set
        # when the caller supplied no worklist: passing NEITHER a
        # ``pipeline_filter`` NOR a ``pipeline_fields`` batch validates the WHOLE
        # project. A supplied filter/batch is honored unchanged (§4.1: the same
        # ``_derive_worklist_fields`` seam the generate/plan streams use).
        effective_pipeline_fields = _derive_worklist_fields(
            pipeline_filter, pipeline_fields, resolved_flowgroups
        )
        yield PhaseCompleted(
            phase="discover",
            duration_s=time.perf_counter() - discover_start,
            success=True,
        )
        # Main-thread LHP-DEPR-001 warnings, seeding the shared dedup set.
        yield from _emit_warning_records(
            self._orchestrator.discovery.scan_deprecation_warnings(
                pipeline_filter=pipeline_filter
            ),
            seen=warnings_seen,
        )

        # Sandbox pre-pass (D1: --sandbox covers validate too), mirroring the
        # generate stream: after discover + DEPR-001, before preflight. A
        # failing pre-pass is a MODE error, not a pipeline finding — §1.4
        # ErrorEmitted + raise, never a BatchValidationResponse.
        sandbox_plan: Optional[SandboxRewritePlan] = None
        sandbox_warnings: Tuple[SandboxWarningRecord, ...] = ()
        if sandbox:
            # Lazy import (that module's contract): _sandbox_run pulls
            # lhp.core at module level and must not load at api-import time.
            from lhp.api._sandbox_run import _resolve_sandbox_run

            try:
                sandbox_run = _resolve_sandbox_run(
                    self._orchestrator, env, resolved_flowgroups
                )
                sandbox_plan = self._orchestrator.build_sandbox_rewrite_plan(
                    env, sandbox_run, list(resolved_flowgroups)
                )
            except LHPError as exc:
                yield ErrorEmitted(lhp_error=exc)
                raise
            # Profile-driven scope (D5/D6): the resolved concrete pipeline
            # set REPLACES the derived all-pipelines worklist.
            effective_pipeline_fields = sandbox_run.pipelines
            sandbox_warnings = sandbox_plan.warnings

        preflight_start = time.perf_counter()
        yield PhaseStarted(phase="preflight")
        preflight_issues = _run_project_preflight(
            self._orchestrator,
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
            yield ValidationCompleted(
                response=_build_validation_batch_from_issues(
                    preflight_issues, tuple(effective_pipeline_fields)
                )
            )
            return
        yield PhaseCompleted(
            phase="preflight",
            duration_s=time.perf_counter() - preflight_start,
            success=True,
        )

        validate_start = time.perf_counter()
        yield PhaseStarted(phase="validate")
        try:
            response, worker_warnings = yield from self._consume_validate_stream(
                pipeline_filter=pipeline_filter,
                pipeline_fields=effective_pipeline_fields,
                env=env,
                max_workers=max_workers,
                include_tests=include_tests,
                pre_discovered_all_flowgroups=resolved_flowgroups,
                progress=progress,
                sandbox_plan=sandbox_plan,
            )
        except LHPError as exc:
            yield PhaseCompleted(
                phase="validate",
                duration_s=time.perf_counter() - validate_start,
                success=False,
            )
            yield ErrorEmitted(lhp_error=exc)
            raise
        yield PhaseCompleted(
            phase="validate",
            duration_s=time.perf_counter() - validate_start,
            success=response.success,
        )
        # Worker LHP-DEPR-002/003/004 warnings plus the sandbox plan's carried
        # warnings (LHP-VAL-065), concatenated BEFORE the shared dedup so the
        # union surfaces once.
        yield from _emit_warning_records(
            sandbox_warnings + worker_warnings, seen=warnings_seen
        )
        yield ValidationCompleted(response=response)

    def _consume_validate_stream(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        max_workers: Optional[int] = None,
        include_tests: bool = True,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        progress: ProgressSink | None = None,
        sandbox_plan: Optional["SandboxRewritePlan"] = None,
    ) -> Generator[
        LHPEvent,
        None,
        Tuple["BatchValidationResponse", Tuple["RunWarningRecord", ...]],
    ]:
        """Consume the orchestrator outcome-stream; yield per-pipeline events.

        Internal validate-phase body for :meth:`validate_pipelines` (which has
        already run the shared §9.24 project preflight — duplicate-(pipeline,
        flowgroup) + test-reporting + optional bundle — and short-circuited on
        any issues). Drives the orchestrator's ``validate_pipelines``
        outcome-generator (which itself ``yield from`` the engine source of
        truth :meth:`PipelineExecutionService._iter_validate_outcomes`) and, for
        EACH :class:`~lhp.core.coordination.PipelineValidationOutcome` it yields
        IN INPUT PIPELINE ORDER, emits a :class:`PipelineStarted` immediately
        followed by exactly ONE terminal — :class:`PipelineCompleted` (the
        pipeline validated clean) or :class:`PipelineFailed` (it had validation
        errors). The engine crystallises every outcome post-pool with the whole
        per-pipeline result in hand, so Started is never emitted without its
        terminal (no dangling Starts).

        The orchestrator generator is driven explicitly (``next`` in a loop)
        rather than via ``yield from`` because its yield type
        (``PipelineValidationOutcome``) differs from this stream's yield type
        (``LHPEvent``): each outcome is TRANSFORMED into the paired progress
        events here. The generator's ``StopIteration.value`` — the batch's
        merged + deduped worker deprecation warnings — is captured and returned
        to the caller (see ``warnings`` below).

        Returns (via ``StopIteration.value``, captured by the caller's
        ``yield from``) a 2-tuple ``(response, warnings)``:

        * **response** — the terminal :class:`BatchValidationResponse`:

          - **Clean / report-only failure** — a DTO aggregating the per-pipeline
            responses; ``success`` is ``False`` when any pipeline reported errors
            (validate REPORTS — the findings ride the DTO and the paired
            ``PipelineFailed`` events, NOT a raise).
          - **Non-LHP / catastrophic failure** — the graceful-DTO path (catch
            ``Exception`` → return a DTO carrying ``error_code`` /
            ``error_message``) is intentional and orthogonal to the
            stream-protocol ``ErrorEmitted`` rendezvous. A genuinely
            catastrophic :class:`LHPError` is RE-RAISED here so the public
            wrapper performs the §1.4 ``ErrorEmitted`` + ``raise`` rendezvous;
            everything else folds into the DTO so the CLI gets a terminal DTO
            with a code rather than a live exception.

        * **warnings** — the batch's merged + deduped worker warning records
          (deprecation and sandbox); the CALLER (:meth:`validate_pipelines`)
          emits these as :class:`WarningEmitted` events in/after the validate
          phase — concatenated after the sandbox plan's carried warnings —
          deduped against the discover-phase main-thread warnings via a shared
          ``(code, file)`` set. Empty on the LHPError re-raise path and the
          non-LHP-degradation path.

        ``sandbox_plan`` is the ``--sandbox`` structured-rewrite plan built by
        the stream's sandbox pre-pass (``None`` outside sandbox runs),
        threaded verbatim into ``orchestrator.validate_pipelines`` so the
        workers apply the rewrite before validation.

        ``files_written`` on the per-pipeline :class:`PipelineCompleted` is
        always ``0``: validate writes nothing.
        """
        pipeline_responses: Dict[str, "ValidationResponse"] = {}
        warnings: Tuple["RunWarningRecord", ...] = ()
        try:
            with perf_timer(f"facade.validate_pipelines [{len(pipeline_fields)}]"):
                outcome_stream = self._orchestrator.validate_pipelines(
                    pipeline_filter=pipeline_filter,
                    pipeline_fields=(
                        list(pipeline_fields) if pipeline_filter is None else None
                    ),
                    env=env,
                    include_tests=include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                    max_workers=max_workers,
                    # Adapt the public counter to the plain core callables (§3.6:
                    # no Protocol/ABC — a concrete sink suffices for one consumer).
                    on_total=None if progress is None else progress.on_total,
                    on_flowgroup_done=(
                        None if progress is None else progress.on_advance
                    ),
                    # --sandbox structured-rewrite plan (None outside sandbox
                    # runs): the workers rewrite the resolved flowgroups so
                    # cross-flowgroup validation sees the sandbox names.
                    sandbox_plan=sandbox_plan,
                )
                # Drive the outcome-generator explicitly so its
                # ``StopIteration.value`` (the merged worker warnings) is captured
                # while each outcome is transformed into the paired §5.7 events.
                while True:
                    try:
                        outcome = next(outcome_stream)
                    except StopIteration as stop:
                        warnings = stop.value or ()
                        break
                    response = _outcome_to_validation_response(outcome)
                    pipeline_responses[outcome.pipeline] = response
                    # Started + its single terminal, as a pair (no dangling
                    # Start). Validate REPORTS, so EVERY pipeline pairs.
                    yield PipelineStarted(pipeline=outcome.pipeline)
                    if outcome.success:
                        yield PipelineCompleted(
                            pipeline=outcome.pipeline,
                            duration_s=0.0,
                            files_written=0,
                        )
                    else:
                        # First structured issue code drives the failure line;
                        # the first error-severity title the message.
                        code = (
                            next((i.code for i in response.issues if i.code), None)
                            or "LHP-VAL-901"
                        )
                        message = (
                            next(
                                (
                                    i.title
                                    for i in response.issues
                                    if i.severity == "error"
                                ),
                                None,
                            )
                            or "(no message)"
                        )
                        yield PipelineFailed(
                            pipeline=outcome.pipeline,
                            code=code,
                            message=message,
                        )
            return (
                _build_validation_batch(pipeline_responses, tuple(pipeline_fields)),
                warnings,
            )
        except LHPError:
            # §1.4 rendezvous: catastrophic LHPError re-raised so the public
            # wrapper emits ErrorEmitted then re-raises.
            raise
        except Exception as exc:
            # Non-LHP failure folds into a batch-failure DTO rather than the
            # ErrorEmitted+raise rendezvous — validate reports, never aborts.
            self._logger.exception("Batch pipeline validation failed")
            return (
                _build_validation_batch(
                    pipeline_responses, tuple(pipeline_fields), exc=exc
                ),
                warnings,
            )
