"""Private §5.7 event-stream body for the plan-only generation path.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here; they use
:meth:`lhp.api.GenerationFacade.plan_generation`, which is a thin
``yield from`` over :func:`_stream_plan_generation` here.

This split exists for the same reason as :mod:`lhp.api._generation_converters`:
to keep :mod:`lhp.api._generation_facade` under the constitution §3.3 soft cap
(the plan path mirrors the generate path's stream wrapper, which would push the
facade module past 500 lines if inlined). The facade method holds the public
contract / ``:stability:`` annotation; the stream logic lives here.

The body mirrors :meth:`GenerationFacade._consume_generate_stream`
event-for-event, but the source of truth is
:func:`~lhp.core.codegen.build_generation_plan` (generate-to-temp) rather than
the orchestrator's commit-writing generator: it drives the UNMODIFIED real
generate flow to a throwaway temp dir, reads the formatted tree back into a
:class:`~lhp.api.GenerationPlan`, and discards the temp dir — the real
``generated/<env>`` tree is never touched.

:stability: internal
"""

from __future__ import annotations

import time
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    List,
    Optional,
    Sequence,
)

from lhp.api._converters_common import (
    _derive_worklist_fields,
    _issue_view_to_lhp_error,
)
from lhp.api._generation_converters import (
    _delta_to_generation_response,
    _generation_result_to_plan,
)
from lhp.api._preflight import _run_project_preflight
from lhp.api.events import (
    ErrorEmitted,
    GenerationPlanCompleted,
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
    from lhp.api._progress import ProgressSink
    from lhp.models.processing import PipelineDelta

    # Internal orchestrator type, referenced only as a quoted annotation
    # (§1.10, §9.13) — never named directly in the public API surface.
    _Orchestrator = Any


def _emit_pipeline_events(
    deltas: List["PipelineDelta"],
) -> Iterator[LHPEvent]:
    """Yield a ``PipelineStarted`` + single terminal per delta (no dangling).

    Each delta is rendered as a :class:`PipelineStarted` immediately followed
    by exactly ONE terminal — :class:`PipelineCompleted` (success) or
    :class:`PipelineFailed` (failure) — so the §5.7 no-dangling-Starts and
    ``count(PipelineStarted) == count(PipelineCompleted) + count(PipelineFailed)``
    invariants hold. Shared by both the clean and the gate-abort paths.
    """
    for delta in deltas:
        yield PipelineStarted(pipeline=delta.pipeline_name)
        if delta.success:
            yield PipelineCompleted(
                pipeline=delta.pipeline_name,
                duration_s=delta.duration_s,
                files_written=delta.files_written,
            )
        else:
            response = _delta_to_generation_response(delta, output_dir=None)
            yield PipelineFailed(
                pipeline=delta.pipeline_name,
                code=response.error_code or "LHP-GEN-901",
                message=response.error_message or "(no message)",
            )


def _stream_plan_generation(
    orchestrator: "_Orchestrator",
    *,
    env: str,
    pipeline_filter: Optional[str] = None,
    pipeline_fields: Sequence[str] = (),
    include_tests: bool = False,
    progress: ProgressSink | None = None,
) -> Iterator[LHPEvent]:
    """Yield the full §5.7 progress stream for plan-only generation.

    Implements :meth:`lhp.api.GenerationFacade.plan_generation` (see its
    docstring for the public contract). Emits, in order:
    :class:`OperationStarted` → paired ``discover`` / ``preflight`` phases →
    ``generate`` phase with per-pipeline ``PipelineStarted`` + terminal pairs →
    terminal :class:`GenerationPlanCompleted` carrying the
    :class:`~lhp.api.GenerationPlan`.

    The worklist is keyed by pipeline name and forwarded to
    :func:`~lhp.core.codegen.build_generation_plan` mutually-exclusively (the
    same shape as the real generate path): a single ``pipeline_filter`` OR a
    ``pipeline_fields`` batch (forwarded only when ``pipeline_filter`` is
    ``None``). Passing NEITHER auto-derives the full project worklist from the
    discover-phase set, so a plan over the whole project no longer requires
    the caller to enumerate the pipeline names.

    A structured (:class:`LHPError`) failure — most importantly the
    all-or-nothing gate aggregate — RAISES after the per-pipeline failure
    events and a single :class:`ErrorEmitted` (§1.4), before any temp write.
    There is NO non-LHP infra-degradation path: a plan performs no commit-time
    disk write.

    ``progress`` is driven exactly as on the real generate stream: its
    :meth:`~lhp.api.ProgressSink.on_total` / :meth:`~lhp.api.ProgressSink.on_advance`
    are forwarded as plain ``Callable``\\ s into
    :func:`~lhp.core.codegen.build_generation_plan`, which threads them into the
    same per-FLOWGROUP ``on_total`` / ``on_flowgroup_done`` hooks
    ``generate_pipelines`` fires off the flat worklist (the flowgroup count, not
    the pipeline count). The pipeline-grained ``on_pipeline_complete`` delta sink
    is a separate concern (it feeds the §5.7 ``PipelineStarted`` / terminal
    pairs), so the two hooks do not collide. ``progress=None`` forwards ``None``,
    wiring no callbacks.
    """
    yield OperationStarted(operation_name="plan_generation", env=env)

    # Phase: discover. Resolve the project-wide flowgroup set here (the same
    # memoized ``bootstrap.discover_all_flowgroups`` surface the generate /
    # validate streams use) so the worklist can be auto-derived when the
    # caller supplied no worklist. The resolved list is threaded into
    # ``build_generation_plan`` so discovery runs exactly once (memoized).
    discover_start = time.perf_counter()
    yield PhaseStarted(phase="discover")
    resolved_flowgroups = orchestrator.bootstrap.discover_all_flowgroups()
    effective_pipeline_fields = _derive_worklist_fields(
        pipeline_filter, pipeline_fields, resolved_flowgroups
    )
    yield PhaseCompleted(
        phase="discover",
        duration_s=time.perf_counter() - discover_start,
        success=True,
    )

    # Phase: preflight. §9.24 — MUST sit in the outer generator (before generate)
    # so failures surface via ErrorEmitted/raise rather than being swallowed.
    # bundle_enabled=False: a plan never writes bundle resources.
    preflight_start = time.perf_counter()
    yield PhaseStarted(phase="preflight")
    preflight_issues = _run_project_preflight(
        orchestrator,
        env=env,
        bundle_enabled=False,
        include_tests=include_tests,
        pre_discovered_all_flowgroups=None,
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

    # Phase: generate (plan-only). The sink only COLLECTS deltas because a callback
    # cannot ``yield`` into this generator and the formatted plan tree does not
    # exist until the primitive fully drains (ruff formats the whole temp tree at
    # the end). Paired events are emitted from the collected list after the
    # primitive returns, preserving §5.7 ordering.
    generate_start = time.perf_counter()
    yield PhaseStarted(phase="generate")
    collected_deltas: List["PipelineDelta"] = []
    # Deferred so ``import lhp.api`` does not eagerly pull the codegen stack
    # (and jinja2 via it); consume the package via its public surface (§5.4).
    from lhp.core.codegen import build_generation_plan

    try:
        with perf_timer("facade.plan_generation"):
            result = build_generation_plan(
                orchestrator,
                env=env,
                pipeline_filter=pipeline_filter,
                pipeline_fields=(
                    list(effective_pipeline_fields) if pipeline_filter is None else None
                ),
                include_tests=include_tests,
                pre_discovered_all_flowgroups=resolved_flowgroups,
                on_pipeline_complete=collected_deltas.append,
                on_total=None if progress is None else progress.on_total,
                on_flowgroup_done=None if progress is None else progress.on_advance,
            )
    except LHPError as exc:
        # §1.4 rendezvous: the gate raised after forwarding every failure delta
        # to the sink. Emit the paired per-pipeline events, then exactly one
        # ErrorEmitted, then re-raise (bare ``raise`` keeps the cause chain,
        # B904). No PhaseCompleted/terminal follows (the raise closes the
        # stream). Unlike the real generate there is NO non-LHP commit-time
        # degradation path: a plan never writes to ``generated/<env>``.
        yield from _emit_pipeline_events(collected_deltas)
        yield ErrorEmitted(lhp_error=exc)
        raise

    yield from _emit_pipeline_events(collected_deltas)
    yield PhaseCompleted(
        phase="generate",
        duration_s=time.perf_counter() - generate_start,
        success=True,
    )
    # Report where files would land even though only a discarded temp tree was written.
    output_location = orchestrator.project_root / "generated" / env
    plan = _generation_result_to_plan(result, output_location=output_location)
    yield GenerationPlanCompleted(response=plan)
