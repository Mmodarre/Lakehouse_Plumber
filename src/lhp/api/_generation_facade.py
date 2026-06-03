"""Private module — implementation of the public :class:`GenerationFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`GenerationFacade` from :mod:`lhp.api` (re-exported
via :mod:`lhp.api.facade`). This split exists purely to keep
``lhp/api/facade.py`` under the constitution §3.3 soft cap (500 lines).

What remains here is the per-method delegation surface for generation.
The full §5.7 generate-stream body (discover → preflight → generate →
monitoring → bundle_sync → terminal) lives in
:mod:`lhp.api._generate_stream`; the plan-only stream body lives in
:mod:`lhp.api._plan_stream`; the heavy DTO-conversion bodies live in
:mod:`lhp.api._generation_converters` (with the shared error helpers in
:mod:`lhp.api._converters_common`) — all split out per §3.3 so this module
stays a lean facade.

:stability: internal
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    List,
    Optional,
    Sequence,
)

from lhp.api._generate_stream import _stream_pipeline_generation
from lhp.api._plan_stream import _stream_plan_generation
from lhp.api._stream_guard import _cap_event_stream
from lhp.api.events import LHPEvent

if TYPE_CHECKING:
    from lhp.models import FlowGroup

    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10,
    # §9.13). Composition is delegated to ``core/coordination/layers``.
    _Orchestrator = Any


class GenerationFacade:
    """Generation operations on a constructed project.

    :stability: provisional
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self._logger = logging.getLogger(__name__)

    def generate_pipelines(
        self,
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
        """Stream-protocol wrapper around the FULL generate orchestration (§5.7).

        Consolidates discover, preflight, generate, monitoring-finalize, and
        (when bundle is enabled) bundle-sync into a SINGLE §5.7 event stream
        with ONE terminal. Yields, in order:

        1. :class:`OperationStarted` (the mandated first event).
        2. :class:`PhaseStarted` / :class:`PhaseCompleted` for ``"discover"`` —
           runs the orchestrator's full-flowgroup discovery when the caller did
           not supply ``pre_discovered_all_flowgroups``, threading the resolved
           list forward so discovery runs exactly once; a caller-supplied list
           is honored as-is (no re-discovery).
        3. :class:`PhaseStarted` / :class:`PhaseCompleted` for ``"preflight"``
           (wraps the §9.24-shared project preflight).
        4. :class:`PhaseStarted` for ``"generate"``, then — as the
           delta-generator is consumed — a :class:`PipelineStarted` paired with
           exactly ONE terminal (:class:`PipelineCompleted` on success or
           :class:`PipelineFailed` on failure) PER PIPELINE in input order, then
           :class:`PhaseCompleted` for ``"generate"``.
        5. :class:`PhaseStarted` / :class:`PhaseCompleted` for ``"monitoring"``
           — the absorbed monitoring-finalize step (skipped on a dry-run or a
           degraded generate; routed through the orchestrator into the core
           ``MonitoringFinalizerService``).
        6. :class:`PhaseStarted` / :class:`PhaseCompleted` for ``"bundle_sync"``
           — ONLY when ``bundle_enabled`` and the generate succeeded and this is
           not a dry-run. Composed in THIS api layer by calling the ``bundle``
           layer directly (never through the orchestrator — that would be a
           forbidden ``core → bundle`` edge).
        7. Terminal :class:`GenerationCompleted` carrying the
           :class:`BatchGenerationResponse`.

        Per-pipeline pairing has NO dangling Starts: a ``PipelineStarted`` is
        emitted only once its terminal is already in hand (the flat engine
        crystallises deltas post-pool in input order, so each delta carries the
        whole outcome). The load-bearing invariant
        ``count(PipelineStarted) == count(PipelineCompleted) + count(PipelineFailed)``
        therefore holds on BOTH the success and the abort paths.

        Failure surfacing (constitution §1.4, §5.7):

        * **Structured (:class:`LHPError`) failure** — a preflight failure, the
          all-or-nothing generate gate aggregate, or a bundle-sync
          ``LHP-CFG-020`` — RAISES. Exactly ONE :class:`ErrorEmitted` is emitted
          first; the raise closes the stream, so neither the remaining phase
          ``PhaseCompleted`` events nor the terminal ``GenerationCompleted`` is
          emitted. Files already committed to disk PERSIST in every failure mode
          (the generate commit and the bundle ``resources/lhp/`` write are both
          non-transactional; a bundle failure leaves the already-written
          ``generated/<env>`` tree untouched).
        * **Non-LHP infra failure** (e.g. an :class:`OSError` from a
          commit-time disk write AFTER the gate has passed) degrades to a
          failed :class:`BatchGenerationResponse`: ``PhaseCompleted("generate",
          …, success=False)`` then a terminal ``GenerationCompleted`` carrying
          the failed DTO (the monitoring + bundle phases are skipped) — NOT an
          ``ErrorEmitted`` / raise.

        Callers that don't need event-level visibility use
        :func:`lhp.api.collect_response` to walk the stream and return the
        terminal response DTO.

        ``apply_formatting`` is a tri-state OVERRIDE for the terminal
        code-formatting pass: ``None`` (the default) means "use the
        project's ``lhp.yaml`` ``apply_formatting`` setting"; ``True`` /
        ``False`` override that key. The resolution to a concrete bool
        happens in the orchestrator (which holds the loaded project
        config); this layer only forwards the override.

        The §5.7 stream body lives in :func:`lhp.api._generate_stream
        ._stream_pipeline_generation` (split out per §3.3 to keep this module
        lean, mirroring :mod:`lhp.api._plan_stream`); this method restates the
        canonical signature (§4.2) and forwards via ``yield from``.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-VAL-*`` (config/action/schema
            validation), ``LHP-CFG-*`` (project config + substitution),
            ``LHP-FILE-*`` (missing files), ``LHP-MULT-*`` (multi-document
            YAML), ``LHP-TPL-*`` (template expansion) propagated from the
            per-pipeline workers, and ``LHP-CFG-020`` from the bundle-sync phase
            (:class:`~lhp.bundle.exceptions.BundleResourceError`). An
            :class:`ErrorEmitted` event is yielded before the exception
            escapes (§1.4 stream protocol).
        """
        yield from _cap_event_stream(
            _stream_pipeline_generation(
                self._orchestrator,
                self._logger,
                pipeline_filter=pipeline_filter,
                pipeline_fields=pipeline_fields,
                env=env,
                output_dir=output_dir,
                specific_flowgroups=specific_flowgroups,
                include_tests=include_tests,
                apply_formatting=apply_formatting,
                bundle_enabled=bundle_enabled,
                pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                max_workers=max_workers,
            )
        )

    def plan_generation(
        self,
        env: str,
        *,
        pipeline_filter: Optional[str] = None,
        include_tests: bool = False,
    ) -> Iterator[LHPEvent]:
        """Stream-protocol wrapper around plan-only generation (§5.7).

        Mirrors :meth:`generate_pipelines` event-for-event but produces a
        :class:`~lhp.api.GenerationPlan` instead of writing files: the run
        codegens every flowgroup to a throwaway temp dir (via the
        parity-guaranteed :func:`~lhp.core.codegen.build_generation_plan`
        primitive) and DISCARDS it. Nothing is written to the real
        ``generated/<env>`` tree (monitoring *finalization* is likewise
        skipped); the plan still REPORTS that directory as its
        ``output_location`` so callers learn where the files would land.

        Yields the full §5.7 progress stream, in order:

        1. :class:`OperationStarted` (the mandated first event).
        2. :class:`PhaseStarted` / :class:`PhaseCompleted` for ``"discover"``
           (the same thin placeholder seam as :meth:`generate_pipelines`).
        3. :class:`PhaseStarted` / :class:`PhaseCompleted` for ``"preflight"``.
        4. :class:`PhaseStarted` for ``"generate"``, then — per pipeline in
           input order — a :class:`PipelineStarted` paired with exactly ONE
           terminal (:class:`PipelineCompleted` / :class:`PipelineFailed`),
           then :class:`PhaseCompleted` for ``"generate"``.
        5. Terminal :class:`GenerationPlanCompleted` carrying the plan.

        Per-pipeline pairing has NO dangling Starts and upholds
        ``count(PipelineStarted) == count(PipelineCompleted) + count(PipelineFailed)``
        on BOTH the success and gate-abort paths (the primitive forwards a
        crystallised :class:`~lhp.models.processing.PipelineDelta` per
        pipeline, so the whole outcome is in hand before its Started).

        Structured failure matches :meth:`generate_pipelines`: an
        :class:`LHPError` (most importantly the gate aggregate) RAISES after
        the per-pipeline failure events + one :class:`ErrorEmitted` (§1.4),
        before any temp write. There is NO non-LHP infra-degradation path — a
        plan performs no commit-time disk write, so the ``:raises:`` list below
        OMITS the ``LHP-FILE-*`` disk-write codes.

        The §5.7 stream body lives in :func:`lhp.api._plan_stream
        ._stream_plan_generation` (split out per §3.3 to keep this module
        lean, mirroring :mod:`lhp.api._generation_converters`); this method
        restates the canonical signature (§4.2) and forwards via
        ``yield from``.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-VAL-*`` (config/action/schema
            validation), ``LHP-CFG-*`` (project config + substitution),
            ``LHP-MULT-*`` (multi-document YAML), and ``LHP-TPL-*`` (template
            expansion) propagated from the per-pipeline workers and the
            all-or-nothing gate. An :class:`ErrorEmitted` event is yielded
            before the exception escapes (§1.4 stream protocol).
        """
        yield from _cap_event_stream(
            _stream_plan_generation(
                self._orchestrator,
                env=env,
                pipeline_filter=pipeline_filter,
                include_tests=include_tests,
            )
        )
