"""Mode-agnostic flat-worklist builder for the consolidated execution engine.

Single responsibility: turn a list of pipeline fields into the four flat
maps the flat per-flowgroup engine
(:func:`lhp.core.coordination._pool._run_flowgroup_pool_core`)
consumes — *without* materializing the legacy per-pipeline
:class:`~lhp.models.processing.PipelineWorkUnit` carrier.

The engine's input shape is a *flat* worklist of
``(pipeline, FlowGroupContext)`` plus the per-pipeline substitution
managers and output dirs. This builder produces exactly that, as four
plain maps returned as a tuple — NOT a new carrier DTO (constitution
§3.6: no needless carrier; the four maps are passed downstream as
separate kwargs). It is the flat analogue of
:func:`work_unit_builder.build_validate_work_units`: it reuses the same
``ActionOrchestrator``-owned discovery / grouping primitives
(``discover_all_flowgroups``, ``discover_flowgroups_by_pipeline_field``,
``_make_context``, ``create_substitution_manager``), only emitting the
flat shape the unified engine wants instead of work units.

Mode-agnostic by design: the same builder feeds the validate
path (``output_dir=None``, so every ``output_dirs`` entry is ``None``) and
the generate path (``output_dir`` set, so ``output_dirs`` carries the
per-pipeline ``output_dir / <pipeline>`` directory, created on disk,
mirroring :func:`work_unit_builder.build_generate_work_units`). ``mode``
itself is NOT a parameter here — the only fork lives on the engine core
(constitution §4.6); this builder is identical for both.

Like the work-unit builders, this function is
``ActionOrchestrator``-aware (it calls main-process-only helpers that own
the synthetic-context state), so the orchestrator argument is required.
It is NOT part of the public API; the orchestrator calls it directly.

:stability: internal
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple

from lhp.models import FlowGroup, FlowGroupContext

from ...utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from ..orchestrator import ActionOrchestrator
    from ..processing.substitution import EnhancedSubstitutionManager


def build_flowgroup_worklist(
    orchestrator: "ActionOrchestrator",
    *,
    pipeline_fields: Sequence[str],
    env: str,
    output_dir: Optional[Path],
    pre_discovered_all_flowgroups: Optional[List[FlowGroup]],
) -> Tuple[
    Dict[str, List[FlowGroupContext]],
    Dict[str, "EnhancedSubstitutionManager"],
    Dict[str, Optional[Path]],
    Dict[str, str],
]:
    """Discover per-pipeline slices; emit the flat engine's four input maps.

    The flat analogue of
    :func:`work_unit_builder.build_validate_work_units`: same discovery /
    grouping logic, flat output. Returns a 4-tuple of dicts, each keyed by
    pipeline field in INPUT order (Python ``dict`` preserves insertion
    order, so the engine — which iterates ``flowgroups_by_pipeline.keys()``
    — observes pipelines in ``pipeline_fields`` order, preserving the
    deterministic pipeline ordering):

    1. ``flowgroups_by_pipeline``: ``{pipeline -> [FlowGroupContext, ...]}``
       — every requested pipeline is present, even one that discovered zero
       flowgroups (its list is empty) or whose discovery failed (also empty;
       see ``discovery_errors``). The engine submits one future per context
       and runs an empty-but-present cross-fg barrier for empty pipelines.
    2. ``substitution_managers``: ``{pipeline -> EnhancedSubstitutionManager}``
       — only for pipelines with at least one flowgroup (an empty or
       failed-discovery pipeline never needs one; the worker only indexes
       this map for pipelines that have contexts).
    3. ``output_dirs``: ``{pipeline -> Optional[Path]}`` — present for every
       pipeline that has flowgroups. ``None`` for every entry when
       ``output_dir is None`` (the validate path); when ``output_dir`` is
       set (the generate path) each entry is ``output_dir / <pipeline>``,
       created on disk here — mirroring
       :func:`work_unit_builder.build_generate_work_units`.
    4. ``discovery_errors``: ``{pipeline -> str}`` — only for pipelines whose
       per-pipeline discovery raised. The string is the same
       ``"Pipeline validation failed: {e}"`` projection the work-unit
       builder put on ``PipelineWorkUnit.discovery_error``; the engine's
       validate consumer
       (:func:`._pool.assemble_validate_outcomes`) short-circuits
       these to a single-error :class:`PipelineValidationOutcome`.

    Args:
        orchestrator: The owning :class:`ActionOrchestrator` (required —
            this builder calls its main-process-only discovery helpers).
        pipeline_fields: The pipeline field values to build a worklist for,
            in the order they should be processed.
        env: The environment name; selects ``substitutions/<env>.yaml``.
        output_dir: The generate base output dir, or ``None`` for validate.
            When set, each pipeline's ``output_dirs`` entry is
            ``output_dir / <pipeline>`` and is ``mkdir``-ed; when ``None``
            every entry is ``None`` and no directories are touched.
        pre_discovered_all_flowgroups: A pre-computed full flowgroup list to
            reuse (single-parse memoization); ``None`` triggers a
            fresh ``discover_all_flowgroups`` here.

    Returns:
        The 4-tuple ``(flowgroups_by_pipeline, substitution_managers,
        output_dirs, discovery_errors)`` described above.
    """
    if pre_discovered_all_flowgroups is not None:
        all_flowgroups = pre_discovered_all_flowgroups
    else:
        with perf_timer("discover_all_flowgroups"):
            all_flowgroups = orchestrator.bootstrap.discover_all_flowgroups()

    substitution_file = orchestrator.project_root / "substitutions" / f"{env}.yaml"

    flowgroups_by_pipeline: Dict[str, List[FlowGroupContext]] = {}
    substitution_managers: Dict[str, "EnhancedSubstitutionManager"] = {}
    output_dirs: Dict[str, Optional[Path]] = {}
    discovery_errors: Dict[str, str] = {}

    for pipeline_field in pipeline_fields:
        try:
            flowgroups = [fg for fg in all_flowgroups if fg.pipeline == pipeline_field]
        except Exception as e:
            orchestrator.logger.debug(
                f"Pipeline '{pipeline_field}' discovery failed",
                exc_info=True,
            )
            # Present-but-empty in the worklist so the engine yields a
            # per-pipeline result; the error message rides the separate
            # discovery_errors map (the validate consumer short-circuits).
            flowgroups_by_pipeline[pipeline_field] = []
            discovery_errors[pipeline_field] = f"Pipeline validation failed: {e}"
            continue

        contexts = [orchestrator.bootstrap.make_context(fg) for fg in flowgroups]
        flowgroups_by_pipeline[pipeline_field] = contexts
        if not flowgroups:
            # Empty discovery: present in the worklist, no sub-manager / dir.
            continue

        pipeline_out_dir = output_dir / pipeline_field if output_dir else None
        if pipeline_out_dir is not None:
            pipeline_out_dir.mkdir(parents=True, exist_ok=True)
        output_dirs[pipeline_field] = pipeline_out_dir

        sub_mgr = orchestrator._orchestration_dependencies.create_substitution_manager(
            substitution_file, env
        )
        # Bare-``{token}`` deprecation detection is single-sourced in
        # ``FlowgroupDiscoveryService.scan_deprecation_warnings`` (scans every
        # file once, one ``DeprecationWarningRecord`` per offending file, which
        # the facade re-emits as ``WarningEmitted``) — not duplicated here.
        substitution_managers[pipeline_field] = sub_mgr

    return (
        flowgroups_by_pipeline,
        substitution_managers,
        output_dirs,
        discovery_errors,
    )
