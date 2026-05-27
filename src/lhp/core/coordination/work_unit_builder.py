"""Builders for :class:`PipelineWorkUnit` sequences (generate + validate).

Extracted from :class:`ActionOrchestrator` in Phase D8b so the
orchestrator stays under the §9.3 800-line hard cap. The builders
remain ``ActionOrchestrator``-aware (they call back into
``self._make_context``, ``self._discover_and_filter_flowgroups``,
``self._lookup_pipeline_slice`` — main-process-only helpers that own
the ``_synthetic_contexts`` mutable dict — see manifest §10 risk #1)
so the orchestrator argument is required, not optional.

These free functions are NOT part of the public API. The orchestrator
calls them; tests reach them through the orchestrator's two thin
delegators (``_build_generate_work_units``, ``_build_validate_work_units``)
to preserve back-compat with the ~5 unit tests that exercise the
work-unit shape directly.

:stability: internal
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

from ...models.config import FlowGroup
from ...models.processing import PipelineWorkUnit
from ...utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from ...cli.warning_collector import WarningCollector
    from ..orchestrator import ActionOrchestrator


def build_generate_work_units(
    orchestrator: "ActionOrchestrator",
    *,
    pipeline_fields: Sequence[str],
    env: str,
    output_dir: Optional[Path],
    specific_flowgroups: Optional[List[str]],
    include_tests: bool,
    pre_discovered_all_flowgroups: Optional[List[FlowGroup]],
    warning_collector: Optional["WarningCollector"],
) -> Tuple[PipelineWorkUnit, ...]:
    """Per-pipeline slice → context wrap → output dir → substitution manager.

    Builds one :class:`PipelineWorkUnit` per requested pipeline. Empty
    slices still produce a unit (with ``substitution_manager=None`` and
    ``output_dir=None``) so the downstream pool emits a no-op success
    delta on the main thread (matches the pre-D5 contract).
    """
    if pre_discovered_all_flowgroups is not None:
        all_flowgroups = pre_discovered_all_flowgroups
    else:
        with perf_timer("discover_all_flowgroups"):
            all_flowgroups = orchestrator.discover_all_flowgroups()
        with perf_timer("validate_duplicates"):
            orchestrator.validate_duplicate_pipeline_flowgroup_combinations(
                all_flowgroups
            )

    substitution_file = orchestrator.project_root / "substitutions" / f"{env}.yaml"
    units: List[PipelineWorkUnit] = []

    for pipeline_field in pipeline_fields:
        slice_for_pipeline = orchestrator._lookup_pipeline_slice(
            all_flowgroups, pipeline_field
        )
        with perf_timer(
            f"discover_and_filter_flowgroups [{pipeline_field}]",
            category="discover_and_filter_flowgroups",
        ):
            flowgroups = orchestrator._discover_and_filter_flowgroups(
                env=env,
                pipeline_identifier=pipeline_field,
                include_tests=include_tests,
                specific_flowgroups=specific_flowgroups,
                use_directory_discovery=False,
                pre_discovered_flowgroups=slice_for_pipeline,
            )
        contexts = tuple(orchestrator._make_context(fg) for fg in flowgroups)

        if not flowgroups:
            units.append(
                PipelineWorkUnit(
                    pipeline_name=pipeline_field,
                    flowgroups=contexts,
                    substitution_manager=None,
                    output_dir=None,
                )
            )
            continue

        pipeline_out_dir = output_dir / pipeline_field if output_dir else None
        if pipeline_out_dir is not None:
            pipeline_out_dir.mkdir(parents=True, exist_ok=True)

        with perf_timer(
            f"create_substitution_manager [{pipeline_field}]",
            category="create_substitution_manager",
        ):
            sub_mgr = orchestrator._orchestration_dependencies.create_substitution_manager(
                substitution_file, env
            )
        if warning_collector is not None and sub_mgr.has_deprecated_bare_tokens:
            warning_collector.add(
                "deprecation",
                "The bare {token} substitution syntax is deprecated and will "
                "be removed in v1.0. Use ${token} instead.",
            )
        units.append(
            PipelineWorkUnit(
                pipeline_name=pipeline_field,
                flowgroups=contexts,
                substitution_manager=sub_mgr,
                output_dir=pipeline_out_dir,
            )
        )
    return tuple(units)


def build_validate_work_units(
    orchestrator: "ActionOrchestrator",
    *,
    pipeline_fields: Sequence[str],
    env: str,
    pre_discovered_all_flowgroups: Optional[List[FlowGroup]],
    warning_collector: Optional["WarningCollector"],
) -> Tuple[PipelineWorkUnit, ...]:
    """Discover per-pipeline slices; emit work units with discovery error or substitution manager.

    Discovery failures (each pipeline's
    :meth:`discover_flowgroups_by_pipeline_field` call) are captured on
    ``unit.discovery_error`` so the executor's assembler short-circuits
    to a ``PipelineValidationOutcome`` with that message.
    """
    if pre_discovered_all_flowgroups is not None:
        all_flowgroups = pre_discovered_all_flowgroups
    else:
        with perf_timer("discover_all_flowgroups"):
            all_flowgroups = orchestrator.discover_all_flowgroups()

    substitution_file = orchestrator.project_root / "substitutions" / f"{env}.yaml"
    units: List[PipelineWorkUnit] = []

    for pipeline_field in pipeline_fields:
        try:
            flowgroups = orchestrator.discover_flowgroups_by_pipeline_field(
                pipeline_field, pre_discovered_all_flowgroups=all_flowgroups,
            )
        except Exception as e:  # noqa: BLE001 — caller-facing discovery error
            orchestrator.logger.debug(
                f"Pipeline '{pipeline_field}' discovery failed", exc_info=True,
            )
            units.append(
                PipelineWorkUnit(
                    pipeline_name=pipeline_field, flowgroups=(),
                    substitution_manager=None, output_dir=None,
                    discovery_error=f"Pipeline validation failed: {e}",
                )
            )
            continue

        contexts = tuple(orchestrator._make_context(fg) for fg in flowgroups)
        if not flowgroups:
            units.append(
                PipelineWorkUnit(
                    pipeline_name=pipeline_field, flowgroups=contexts,
                    substitution_manager=None, output_dir=None,
                )
            )
            continue

        sub_mgr = orchestrator._orchestration_dependencies.create_substitution_manager(
            substitution_file, env
        )
        if warning_collector is not None and sub_mgr.has_deprecated_bare_tokens:
            warning_collector.add(
                "deprecation",
                "The bare {token} substitution syntax is deprecated and will "
                "be removed in v1.0. Use ${token} instead.",
            )
        units.append(
            PipelineWorkUnit(
                pipeline_name=pipeline_field, flowgroups=contexts,
                substitution_manager=sub_mgr, output_dir=None,
            )
        )
    return tuple(units)
