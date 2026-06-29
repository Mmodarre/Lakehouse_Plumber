"""Private converters for the inspection direction of the public API.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Holds the per-type DTO projections used by the inspection / listing
facades (:class:`FlowgroupView`, :class:`ActionView`,
:class:`ProjectConfigView`, :class:`BlueprintView`, :class:`PresetView`,
:class:`TemplateView`, :class:`ProcessedFlowgroupView`,
:class:`DependencyAnalysisResult`, :class:`StatsResult`) plus the
inspection helpers (``_locate_flowgroup_by_name``,
``_build_substitution_manager_for_env``, ``_flowgroup_file_paths``,
``_duplicates_to_validation_response``). Bundle-specific converters live
in :mod:`lhp.api._bundle_facade`.

:stability: internal
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence

from lhp.api.responses import (
    DependencyAnalysisResult,
    DependencyWarningView,
    StatsResult,
    ValidationResponse,
)
from lhp.api.views import (
    ActionView,
    BlueprintInstanceView,
    BlueprintView,
    FlowgroupView,
    PipelineStats,
    PresetView,
    ProcessedFlowgroupView,
    ProjectConfigView,
    TemplateParameterView,
    TemplateView,
    ValidationIssueView,
)

if TYPE_CHECKING:
    from lhp.core.processing.substitution import EnhancedSubstitutionManager
    from lhp.models import (
        Action,
        Blueprint,
        FlowGroup,
        Preset,
        ProjectConfig,
        Template,
    )
    from lhp.models.dependencies import DependencyAnalysisResult as _InternalDepResult

    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10, §9.13).
    _Orchestrator = Any


def _write_target_as_dict(write_target: Any) -> Dict[str, Any]:
    """Normalise a ``write_target`` (Pydantic model or raw dict) to a dict.

    Resolved write actions reach the inspection converter with their
    ``write_target`` still a raw dict (CDC keys such as ``mode`` /
    ``cdc_config`` are not declared on the :class:`WriteTarget` model, so
    they only survive as dict members). The model branch is kept for
    robustness against callers that pass a coerced model.
    """
    if isinstance(write_target, dict):
        return write_target
    if hasattr(write_target, "model_dump"):
        dumped = write_target.model_dump()
        return dumped if isinstance(dumped, dict) else {}
    return {}


def _scd_type_from_write_target(wt: Dict[str, Any]) -> Optional[int]:
    """Read the SCD type from a CDC or snapshot-CDC config, if present."""
    for cfg_key in ("cdc_config", "snapshot_cdc_config"):
        cfg = wt.get(cfg_key)
        if isinstance(cfg, dict):
            scd = cfg.get("scd_type")
            if scd is None:
                scd = cfg.get("stored_as_scd_type")
            if isinstance(scd, int) and not isinstance(scd, bool):
                return scd
            if isinstance(scd, str):
                try:
                    return int(scd)
                except ValueError:
                    return None
    return None


def _full_name_from_write_target(wt: Dict[str, Any], action_name: str) -> str:
    """Build the fully-qualified target name for a write target.

    Sinks render as ``sink:<sink_type>/<id>``. Table targets prefer the
    canonical ``catalog.schema.table`` (matching the dependency
    producer index); they fall back to the deprecated ``database.table``
    pairing, then the bare ``table``, then the action name. Substitution
    tokens present in any field are passed through verbatim.
    """
    write_type = wt.get("type")
    if write_type == "sink":
        sink_type = wt.get("sink_type") or "unknown"
        sink_id = wt.get("topic") or wt.get("sink_name") or "unnamed"
        return f"sink:{sink_type}/{sink_id}"

    catalog = wt.get("catalog") or ""
    schema = wt.get("schema") or ""
    database = wt.get("database") or ""
    table = wt.get("table") or ""
    if catalog and schema and table:
        return f"{catalog}.{schema}.{table}"
    if database and table:
        return f"{database}.{table}"
    return table or database or action_name


def _write_metadata(
    action: "Action",
) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """Derive ``(write_mode, scd_type, target_full_name)`` for an action.

    Returns ``(None, None, None)`` for non-write actions and for write
    actions with no ``write_target``. The write mode defaults to
    ``"standard"`` for table targets that omit ``mode`` (matching the
    streaming-table generator); sink targets report no mode.
    """
    if action.type.value != "write" or action.write_target is None:
        return None, None, None

    wt = _write_target_as_dict(action.write_target)
    write_type = wt.get("type")

    write_mode = wt.get("mode")
    if write_mode is None and write_type != "sink":
        write_mode = "standard"

    scd_type = _scd_type_from_write_target(wt)
    target_full_name = _full_name_from_write_target(wt, action.name)
    return write_mode, scd_type, target_full_name


def _action_to_view(action: "Action") -> ActionView:
    write_mode, scd_type, target_full_name = _write_metadata(action)
    return ActionView(
        name=action.name,
        action_type=action.type.value,
        target=action.target,
        description=action.description,
        transform_type=action.transform_type,
        test_type=action.test_type,
        write_mode=write_mode,
        scd_type=scd_type,
        target_full_name=target_full_name,
    )


def _flowgroup_to_view(
    flowgroup: "FlowGroup", *, file_path: Optional[Path] = None
) -> FlowgroupView:
    load_count = 0
    transform_count = 0
    write_count = 0
    test_count = 0
    for action in flowgroup.actions:
        type_value = action.type.value
        if type_value == "load":
            load_count += 1
        elif type_value == "transform":
            transform_count += 1
        elif type_value == "write":
            write_count += 1
        elif type_value == "test":
            test_count += 1
    return FlowgroupView(
        name=flowgroup.flowgroup,
        pipeline=flowgroup.pipeline,
        file_path=file_path,
        presets=tuple(flowgroup.presets or ()),
        template=flowgroup.use_template,
        load_action_count=load_count,
        transform_action_count=transform_count,
        write_action_count=write_count,
        test_action_count=test_count,
        job_name=getattr(flowgroup, "job_name", None),
    )


def _flowgroup_to_processed_view(
    flowgroup: "FlowGroup", *, file_path: Optional[Path] = None
) -> ProcessedFlowgroupView:
    variables = dict(flowgroup.variables) if flowgroup.variables else {}
    return ProcessedFlowgroupView(
        flowgroup=_flowgroup_to_view(flowgroup, file_path=file_path),
        actions=tuple(_action_to_view(action) for action in flowgroup.actions),
        job_name=flowgroup.job_name,
        variables=variables,
    )


def _project_config_to_view(
    project_config: Optional["ProjectConfig"],
) -> ProjectConfigView:
    """Project an internal :class:`ProjectConfig` onto a public view.

    A missing config (``None``) yields a placeholder view with an
    empty name and ``"unknown"`` version; the CLI rendering layer is
    responsible for surfacing the absence to the user.
    """
    if project_config is None:
        return ProjectConfigView(name="", version="unknown")
    return ProjectConfigView(
        name=project_config.name,
        version=project_config.version,
        description=project_config.description,
        author=project_config.author,
        created_date=project_config.created_date,
        required_lhp_version=project_config.required_lhp_version,
        include=tuple(project_config.include or ()),
        blueprint_include=tuple(project_config.blueprint_include or ()),
        instance_include=tuple(project_config.instance_include or ()),
        has_operational_metadata=project_config.operational_metadata is not None,
        has_event_log=project_config.event_log is not None,
        has_monitoring=project_config.monitoring is not None,
        has_test_reporting=project_config.test_reporting is not None,
    )


def _blueprint_to_view(
    name: str,
    blueprint: "Blueprint",
    file_path: Path,
    *,
    instance_count: int = 0,
    instances: Sequence["BlueprintInstanceView"] = (),
) -> BlueprintView:
    return BlueprintView(
        name=name,
        file_path=file_path,
        version=str(blueprint.version),
        description=blueprint.description,
        parameter_count=len(blueprint.parameters),
        flowgroup_count=len(blueprint.flowgroups),
        instance_count=instance_count,
        instances=tuple(instances),
    )


def _preset_to_view(preset: "Preset", file_path: Path) -> PresetView:
    return PresetView(
        name=preset.name,
        file_path=file_path,
        version=str(preset.version),
        extends=preset.extends,
        description=preset.description,
    )


def _template_to_view(template: "Template", file_path: Path) -> TemplateView:
    """Required-parameter count uses ``required`` key defaulting to ``False`` when absent."""
    parameters = template.parameters or []
    required_count = sum(
        1 for param in parameters if bool(param.get("required", False))
    )
    parameter_views = tuple(
        TemplateParameterView(
            name=str(param.get("name", "unknown")),
            type_=str(param.get("type", "string")),
            required=bool(param.get("required", False)),
            description=param.get("description") or None,
            default=param.get("default"),
        )
        for param in parameters
    )
    return TemplateView(
        name=template.name,
        file_path=file_path,
        version=str(template.version),
        description=template.description,
        parameter_count=len(parameters),
        required_parameter_count=required_count,
        action_count=len(template.actions),
        parameters=parameter_views,
    )


def _build_stats_result(flowgroups: Sequence["FlowGroup"]) -> StatsResult:
    """Aggregate a list of flowgroups into a :class:`StatsResult`.

    Walks every action exactly once. ``action_counts_by_type`` keys are
    the lowercase :class:`ActionType` enum values (``"load"``,
    ``"transform"``, ``"write"``, ``"test"``) plus the sub-keys the
    legacy CLI tracks for load source types and transform subtypes
    (e.g. ``"load_cloudfiles"``, ``"transform_sql"``).
    """
    pipelines: Dict[str, Dict[str, int]] = {}
    action_counts: Dict[str, int] = {}
    templates_used: set[str] = set()
    presets_used: set[str] = set()
    total_actions = 0

    for fg in flowgroups:
        pipeline_row = pipelines.setdefault(
            fg.pipeline, {"flowgroups": 0, "actions": 0}
        )
        pipeline_row["flowgroups"] += 1
        if fg.use_template:
            templates_used.add(fg.use_template)
        for preset in fg.presets or ():
            presets_used.add(preset)
        for action in fg.actions:
            type_value = action.type.value
            action_counts[type_value] = action_counts.get(type_value, 0) + 1
            pipeline_row["actions"] += 1
            total_actions += 1
            if type_value == "load" and isinstance(action.source, dict):
                subtype = str(action.source.get("type", "unknown"))
                key = f"load_{subtype}"
                action_counts[key] = action_counts.get(key, 0) + 1
            elif type_value == "transform" and action.transform_type:
                key = f"transform_{action.transform_type}"
                action_counts[key] = action_counts.get(key, 0) + 1

    breakdown = tuple(
        PipelineStats(
            pipeline_name=name,
            flowgroup_count=row["flowgroups"],
            total_actions=row["actions"],
        )
        for name, row in sorted(pipelines.items())
    )
    return StatsResult(
        pipeline_count=len(pipelines),
        flowgroup_count=sum(row["flowgroups"] for row in pipelines.values()),
        total_actions=total_actions,
        action_counts_by_type=action_counts,
        pipeline_breakdown=breakdown,
        templates_used=tuple(sorted(templates_used)),
        presets_used=tuple(sorted(presets_used)),
    )


def _dependency_result_to_view(
    internal: "_InternalDepResult",
) -> DependencyAnalysisResult:
    # Drops the live DiGraph objects from internal.graphs — not JSON-serialisable.

    pipeline_deps: Dict[str, tuple[str, ...]] = {
        name: tuple(dep.depends_on)
        for name, dep in internal.pipeline_dependencies.items()
    }
    execution_stages = tuple(tuple(stage) for stage in internal.execution_stages)
    circular = tuple(tuple(cycle) for cycle in internal.circular_dependencies)
    warnings = tuple(
        DependencyWarningView(
            code=warning.code,
            message=warning.message,
            flowgroup=warning.flowgroup,
            action=warning.action,
            suggestion=warning.suggestion,
            file_path=warning.file_path,
            line=warning.line,
        )
        for warning in internal.warnings
    )
    return DependencyAnalysisResult(
        pipeline_dependencies=pipeline_deps,
        execution_stages=execution_stages,
        circular_dependencies=circular,
        external_sources=tuple(internal.external_sources),
        total_pipelines=internal.total_pipelines,
        total_external_sources=internal.total_external_sources,
        warnings=warnings,
    )


def _flowgroups_to_views(
    flowgroups: Iterable["FlowGroup"],
    *,
    file_paths: Optional[Dict[tuple[str, str], Path]] = None,
) -> tuple[FlowgroupView, ...]:
    views: List[FlowgroupView] = []
    for fg in flowgroups:
        path: Optional[Path] = None
        if file_paths is not None:
            path = file_paths.get((fg.pipeline, fg.flowgroup))
        views.append(_flowgroup_to_view(fg, file_path=path))
    return tuple(views)


def _duplicates_to_validation_response(
    flowgroups: Sequence[FlowgroupView],
) -> ValidationResponse:
    """Detect duplicate ``(pipeline, flowgroup)`` pairs in a sequence of views.

    Returns a :class:`ValidationResponse` carrying one
    :class:`ValidationIssueView` per duplicate. The first occurrence of
    each pair is treated as the canonical entry; every subsequent
    occurrence emits a diagnostic referencing the prior file path.
    Does not raise.
    """
    seen: Dict[tuple[str, str], FlowgroupView] = {}
    issues: List[ValidationIssueView] = []
    pipelines_observed: set[str] = set()
    for view in flowgroups:
        pipelines_observed.add(view.pipeline)
        key = (view.pipeline, view.name)
        prior = seen.get(key)
        if prior is None:
            seen[key] = view
            continue
        issues.append(
            ValidationIssueView(
                code="LHP-VAL-DUPFG",
                category="VAL",
                severity="error",
                title=(
                    f"Duplicate flowgroup '{view.name}' in pipeline '{view.pipeline}'"
                ),
                details=(
                    "Two flowgroups in the same pipeline share the same "
                    "flowgroup name; each pair must be unique."
                ),
                pipeline_name=view.pipeline,
                flowgroup_name=view.name,
                suggestions=(
                    "Rename one of the duplicate flowgroups.",
                    "Or move one to a different pipeline.",
                ),
                context={
                    "first": str(prior.file_path) if prior.file_path else "",
                    "duplicate": str(view.file_path) if view.file_path else "",
                },
            )
        )
    return ValidationResponse(
        success=len(issues) == 0,
        issues=tuple(issues),
        validated_pipelines=tuple(sorted(pipelines_observed)),
    )


def _build_substitution_manager_for_env(
    project_root: Path,
    env: str,
) -> "EnhancedSubstitutionManager":
    # Falls back to an empty manager when ``substitutions/<env>.yaml`` is absent.
    from lhp.core.processing.substitution import EnhancedSubstitutionManager

    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        return EnhancedSubstitutionManager(env=env)
    return EnhancedSubstitutionManager(substitution_file, env)


def _locate_flowgroup_by_name(
    orchestrator: "_Orchestrator", flowgroup_name: str
) -> "FlowGroup":
    # Raises LookupError when not found; callers translate to None or their public outcome.
    flowgroups: Sequence["FlowGroup"] = orchestrator.bootstrap.discover_all_flowgroups()
    for fg in flowgroups:
        if fg.flowgroup == flowgroup_name:
            return fg
    raise LookupError(f"Flowgroup '{flowgroup_name}' not found in project.")


def _flowgroup_file_paths(
    orchestrator: "_Orchestrator",
    flowgroups: Sequence["FlowGroup"],
) -> Dict[tuple[str, str], Path]:
    paths: Dict[tuple[str, str], Path] = {}
    for fg in flowgroups:
        path = orchestrator.discovery.find_source_yaml_for_flowgroup(fg)
        if path is not None:
            paths[(fg.pipeline, fg.flowgroup)] = path
    return paths
