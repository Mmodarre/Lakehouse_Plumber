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

from lhp.api._converters_common import _emit_deprecation_warnings
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
    from lhp.models import FlowGroup
    from lhp.models.processing import DeprecationWarningRecord

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

        Discover phase (D3): the discover phase is no longer a no-op
        placeholder. It resolves the project-wide flowgroup set EXACTLY ONCE
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

        The §5.7 stream body lives in :meth:`_validate_pipelines_stream`;
        this method restates the canonical signature (§4.2) and forwards it
        through :func:`lhp.api._stream_guard._cap_event_stream` (the §13.4
        event-buffer soft cap) via ``yield from``.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-VAL-*`` (config/action/schema
            validation), ``LHP-CFG-*`` (project config + substitution),
            ``LHP-FILE-*`` (missing files), ``LHP-MULT-*`` (multi-document
            YAML), and ``LHP-TPL-*`` (template expansion) propagated from
            the per-pipeline workers. An :class:`ErrorEmitted` event is
            yielded before the exception escapes (§1.4 stream protocol).
        """
        yield from _cap_event_stream(
            self._validate_pipelines_stream(
                pipeline_filter=pipeline_filter,
                pipeline_fields=pipeline_fields,
                env=env,
                max_workers=max_workers,
                include_tests=include_tests,
                bundle_enabled=bundle_enabled,
                pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
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
    ) -> Iterator[LHPEvent]:
        """Yield the full §5.7 validate progress stream (the stream body).

        Private generator so the public method can route through
        :func:`lhp.api._stream_guard._cap_event_stream` (§13.4 event-buffer soft cap).

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
        yield PhaseCompleted(
            phase="discover",
            duration_s=time.perf_counter() - discover_start,
            success=True,
        )
        # Main-thread LHP-DEPR-001 warnings, seeding the shared dedup set.
        yield from _emit_deprecation_warnings(
            self._orchestrator.discovery.scan_deprecation_warnings(),
            seen=warnings_seen,
        )

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
                    preflight_issues, tuple(pipeline_fields)
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
                pipeline_fields=pipeline_fields,
                env=env,
                max_workers=max_workers,
                include_tests=include_tests,
                pre_discovered_all_flowgroups=resolved_flowgroups,
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
        # Worker LHP-DEPR-002/003/004 warnings, deduped against the main-thread set.
        yield from _emit_deprecation_warnings(worker_warnings, seen=warnings_seen)
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
    ) -> Generator[
        LHPEvent,
        None,
        Tuple["BatchValidationResponse", Tuple["DeprecationWarningRecord", ...]],
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

        * **warnings** — the batch's merged + deduped worker deprecation
          warnings; the CALLER (:meth:`validate_pipelines`) emits these as
          :class:`WarningEmitted` events in/after the validate phase, deduped
          against the discover-phase main-thread warnings via a shared
          ``(code, file)`` set. Empty on the LHPError re-raise path and the
          non-LHP-degradation path.

        ``files_written`` on the per-pipeline :class:`PipelineCompleted` is
        always ``0``: validate writes nothing.
        """
        pipeline_responses: Dict[str, "ValidationResponse"] = {}
        warnings: Tuple["DeprecationWarningRecord", ...] = ()
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
