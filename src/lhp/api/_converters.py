"""Private converters from internal types to public DTO views.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

:stability: internal
"""

# JUSTIFIED: This module is ~711 lines because it co-locates every
# internal-type → public-DTO conversion for the inspection / generation
# / validation API surface. There is one converter per public DTO
# (FlowgroupView, ActionView, ProjectConfigView, BlueprintView,
# PresetView, TemplateView, ProcessedFlowgroupView,
# DependencyAnalysisResult, StatsResult, FinalizeMonitoringResult)
# plus the generation/validation response-aggregators plus inspection
# helpers (_locate_flowgroup_by_name, _build_substitution_manager_for_env,
# _flowgroup_file_paths, _duplicates_to_validation_response) plus the
# monitoring result builder. Bundle-specific converters live in
# :mod:`lhp.api._bundle_facade`. Splitting further would scatter the
# public-API conversion rules across multiple files, breaking the
# §1.10 single-import-surface invariant and forcing facade.py to
# import from four or five sub-paths.
# TODO(Phase 9.5): split into per-DTO-family converter modules (flowgroup / blueprint / preset / template / processing / dependency / stats / monitoring) once the public DTO surface stabilises; see LOCAL/REMAINING_WORK.md §9.5.
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterable, List, Literal, Optional, Sequence

from lhp.api.responses import (
    BatchGenerationResponse,
    BatchValidationResponse,
    DependencyAnalysisResult,
    FinalizeMonitoringResult,
    GenerationResponse,
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
    from lhp.core.coordination.executor import PipelineValidationOutcome
    from lhp.core.processing.substitution import EnhancedSubstitutionManager
    from lhp.errors import LHPError
    from lhp.models import (
        Action,
        Blueprint,
        FlowGroup,
        Preset,
        ProjectConfig,
        Template,
    )
    from lhp.models.dependencies import DependencyAnalysisResult as _InternalDepResult
    from lhp.models.processing import PipelineDelta


def _lhp_error_to_issue_view(
    lhp_err: "LHPError",
    *,
    pipeline_name: Optional[str] = None,
    flowgroup_name: Optional[str] = None,
    severity: Literal["error", "warning"] = "error",
) -> ValidationIssueView:
    """Project an :class:`LHPError` onto a public :class:`ValidationIssueView`.

    Shared between the generation and validation paths.
    """
    return ValidationIssueView(
        code=lhp_err.code,
        category=lhp_err.category.value,
        severity=severity,
        title=lhp_err.title,
        details=lhp_err.details or None,
        pipeline_name=pipeline_name,
        flowgroup_name=flowgroup_name,
        suggestions=tuple(lhp_err.suggestions or ()),
        context=dict(lhp_err.context or {}),
        doc_link=lhp_err.doc_link,
    )


def _delta_to_generation_response(
    delta: "PipelineDelta", *, output_dir: Optional[Path]
) -> GenerationResponse:
    """Build the public :class:`GenerationResponse` for a worker delta.

    Synthesises an :class:`LHPError` when the worker did not produce one
    so callers' ``except ValueError:`` / ``except FileNotFoundError:``
    keep catching worker failures the same way they catch main-thread
    failures (preserves the dual-inheritance subclass identity).
    """
    from lhp.errors import LHPError, lhp_error_from_worker_failure

    error_message: Optional[str] = None
    error_code: Optional[str] = None
    error_view: Optional[ValidationIssueView] = None
    if not delta.success:
        error_message = (
            f"{delta.error_type}: {delta.error_message}"
            if delta.error_type
            else delta.error_message
        )
        if delta.lhp_error is not None:
            lhp_err: LHPError = delta.lhp_error
        else:
            lhp_err = lhp_error_from_worker_failure(
                pipeline_name=delta.pipeline_name,
                error_type=delta.error_type or "UnknownError",
                error_message=delta.error_message or "(no message)",
                error_traceback=delta.error_traceback or "",
            )
        error_code = lhp_err.code
        error_view = _lhp_error_to_issue_view(
            lhp_err, pipeline_name=delta.pipeline_name
        )

    return GenerationResponse(
        success=delta.success,
        generated_filenames=delta.generated_filenames,
        files_written=delta.files_written,
        total_flowgroups=len(delta.generated_filenames),
        output_location=output_dir,
        performance_info={"dry_run": output_dir is None},
        duration_s=delta.duration_s,
        error_message=error_message,
        error_code=error_code,
        error=error_view,
    )


def _outcome_to_validation_response(
    outcome: "PipelineValidationOutcome",
) -> ValidationResponse:
    """Build the public :class:`ValidationResponse` for a per-pipeline outcome.

    ``outcome.lhp_errors`` and ``outcome.errors`` are NOT alternatives —
    they can coexist when an LHPError-raising flowgroup sits in the
    same pipeline as a string-only discovery or CDC failure.
    """
    issues: List[ValidationIssueView] = []
    for lhp_err in outcome.lhp_errors:
        issues.append(_lhp_error_to_issue_view(lhp_err, pipeline_name=outcome.pipeline))
    for err in outcome.errors:
        issues.append(
            ValidationIssueView(
                code="",
                category="VAL",
                severity="error",
                title=err,
                pipeline_name=outcome.pipeline,
            )
        )
    for warn in outcome.warnings:
        issues.append(
            ValidationIssueView(
                code="",
                category="VAL",
                severity="warning",
                title=warn,
                pipeline_name=outcome.pipeline,
            )
        )
    return ValidationResponse(
        success=outcome.success,
        issues=tuple(issues),
        validated_pipelines=(outcome.pipeline,),
    )


def _build_generation_batch_success(
    pipeline_responses: Dict[str, GenerationResponse],
    *,
    output_dir: Optional[Path],
) -> BatchGenerationResponse:
    """Aggregate per-pipeline responses into a successful batch response."""
    aggregate: tuple[str, ...] = ()
    total_written = 0
    for r in pipeline_responses.values():
        aggregate = aggregate + r.generated_filenames
        total_written += r.files_written
    return BatchGenerationResponse(
        success=True,
        pipeline_responses=dict(pipeline_responses),
        total_files_written=total_written,
        aggregate_generated_filenames=aggregate,
        output_location=output_dir,
    )


def _build_generation_batch_failure(
    pipeline_responses: Dict[str, GenerationResponse], exc: Exception
) -> BatchGenerationResponse:
    """Aggregate per-pipeline responses into a failure batch response.

    Only successful pipeline outputs are included in the aggregate
    filename / counter totals; the failing exception's LHP code (if any)
    is propagated as ``error_code`` so the CLI fail-fast boundary can
    map it to an exit code without handling the live exception.
    """
    aggregate: tuple[str, ...] = ()
    total_written = 0
    for r in pipeline_responses.values():
        if r.success:
            aggregate = aggregate + r.generated_filenames
            total_written += r.files_written
    return BatchGenerationResponse(
        success=False,
        pipeline_responses=dict(pipeline_responses),
        total_files_written=total_written,
        aggregate_generated_filenames=aggregate,
        output_location=None,
        error_message=str(exc),
        error_code=getattr(exc, "code", None),
    )


def _build_validation_batch(
    pipeline_responses: Dict[str, ValidationResponse],
    pipeline_fields: tuple[str, ...],
    *,
    exc: Optional[Exception] = None,
) -> BatchValidationResponse:
    """Aggregate per-pipeline responses into a batch validation response.

    When ``exc`` is provided, the batch is marked failed and the
    exception's LHP code (if any) is preserved on the DTO so the CLI
    fail-fast boundary can map it to an exit code (§4.8).
    """
    total_errors = sum(r.error_count for r in pipeline_responses.values())
    total_warnings = sum(r.warning_count for r in pipeline_responses.values())
    if exc is None:
        return BatchValidationResponse(
            success=total_errors == 0,
            pipeline_responses=dict(pipeline_responses),
            total_errors=total_errors,
            total_warnings=total_warnings,
            validated_pipelines=pipeline_fields,
        )
    return BatchValidationResponse(
        success=False,
        pipeline_responses=dict(pipeline_responses),
        total_errors=total_errors,
        total_warnings=total_warnings,
        validated_pipelines=pipeline_fields,
        error_message=str(exc),
        error_code=getattr(exc, "code", None),
    )


def _action_to_view(action: "Action") -> ActionView:
    """Project an internal :class:`Action` Pydantic model onto a view."""
    return ActionView(
        name=action.name,
        action_type=action.type.value,
        target=action.target,
        description=action.description,
        transform_type=action.transform_type,
        test_type=action.test_type,
    )


def _flowgroup_to_view(
    flowgroup: "FlowGroup", *, file_path: Optional[Path] = None
) -> FlowgroupView:
    """Project an internal :class:`FlowGroup` onto a public view.

    Action counts are computed from ``flowgroup.actions``; type-keys
    use the lowercased :class:`ActionType` enum value to match the
    documented YAML surface.
    """
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
    """Project a fully-resolved :class:`FlowGroup` onto a processed view.

    The flowgroup must already be post-process (template expanded,
    presets merged, substitutions resolved). The conversion enumerates
    every action and copies the surface-relevant fields.
    """
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
    """Project an internal :class:`Blueprint` onto a public view."""
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
    """Project an internal :class:`Preset` onto a public view."""
    return PresetView(
        name=preset.name,
        file_path=file_path,
        version=str(preset.version),
        extends=preset.extends,
        description=preset.description,
    )


def _template_to_view(template: "Template", file_path: Path) -> TemplateView:
    """Project an internal :class:`Template` onto a public view.

    Required-parameter count is derived from each parameter mapping's
    ``required`` key (defaulting to ``False`` when absent), mirroring
    the legacy CLI parsing in :meth:`ListCommand._parse_template_information`.
    ``parameters`` carries the per-parameter view list — the CLI
    presenter renders this when displaying template details.
    """
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
    """Project the internal :class:`DependencyAnalysisResult` onto the public view.

    Drops the live :class:`networkx.DiGraph` objects (carried in
    ``internal.graphs``) and flattens
    :class:`PipelineDependency` rows into a name-to-tuple mapping.
    """
    pipeline_deps: Dict[str, tuple[str, ...]] = {
        name: tuple(dep.depends_on)
        for name, dep in internal.pipeline_dependencies.items()
    }
    execution_stages = tuple(tuple(stage) for stage in internal.execution_stages)
    circular = tuple(tuple(cycle) for cycle in internal.circular_dependencies)
    return DependencyAnalysisResult(
        pipeline_dependencies=pipeline_deps,
        execution_stages=execution_stages,
        circular_dependencies=circular,
        external_sources=tuple(internal.external_sources),
        total_pipelines=internal.total_pipelines,
        total_external_sources=internal.total_external_sources,
    )


def _flowgroups_to_views(
    flowgroups: Iterable["FlowGroup"],
    *,
    file_paths: Optional[Dict[tuple[str, str], Path]] = None,
) -> tuple[FlowgroupView, ...]:
    """Convert an iterable of flowgroups to a tuple of views.

    ``file_paths`` maps ``(pipeline, flowgroup)`` to the source file
    path; when supplied, each view's ``file_path`` is populated from
    this map.
    """
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
                    f"Duplicate flowgroup '{view.name}' in pipeline "
                    f"'{view.pipeline}'"
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
    """Build a substitution manager for the named env, falling back when missing.

    Mirrors :meth:`ShowCommand._load_substitution_manager`: when the
    ``substitutions/<env>.yaml`` file does not exist, an empty
    manager is returned so callers can still process flowgroups that
    do not reference env tokens.
    """
    from lhp.core.processing.substitution import EnhancedSubstitutionManager

    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        return EnhancedSubstitutionManager(env=env)
    return EnhancedSubstitutionManager(substitution_file, env)


def _locate_flowgroup_by_name(orchestrator: object, flowgroup_name: str) -> "FlowGroup":
    """Locate the first flowgroup by name across the project, or raise.

    Discovery is unfiltered (walks every pipeline directory). Raises
    :class:`LookupError` when no flowgroup matches; callers translate
    this into the appropriate public outcome (``None`` for
    :meth:`InspectionFacade.find_source_yaml_for_flowgroup`).
    """
    flowgroups: Sequence["FlowGroup"] = orchestrator.discover_all_flowgroups()  # type: ignore[attr-defined]
    for fg in flowgroups:
        if fg.flowgroup == flowgroup_name:
            return fg
    raise LookupError(f"Flowgroup '{flowgroup_name}' not found in project.")


def _flowgroup_file_paths(
    orchestrator: object,
    flowgroups: Sequence["FlowGroup"],
) -> Dict[tuple[str, str], Path]:
    """Build a ``(pipeline, flowgroup) -> path`` map for a sequence of flowgroups.

    Delegates to the orchestrator's underlying source-path lookup;
    flowgroups with no resolvable source path are simply absent from
    the resulting map.
    """
    paths: Dict[tuple[str, str], Path] = {}
    for fg in flowgroups:
        # Intentional underscore-prefixed lookup: this closure is the
        # single reach-through site until ``find_source_yaml_for_flowgroup``
        # is promoted on the orchestrator.
        path = orchestrator._find_source_yaml_for_flowgroup(fg)  # type: ignore[attr-defined]
        if path is not None:
            paths[(fg.pipeline, fg.flowgroup)] = path
    return paths


def _lhp_error_code_and_message(exc: BaseException) -> tuple[Optional[str], str]:
    """Extract ``(error_code, error_message)`` from an arbitrary exception.

    Returns the LHP error code (e.g. ``"LHP-CFG-026"``) and ``title``
    when ``exc`` is an :class:`LHPError`, otherwise ``(None, str(exc))``.
    Shared by the C4/C5 result converters so monitoring / bundle
    failures surface a stable code on the public DTO (§4.8) without
    leaking the live exception instance.
    """
    from lhp.errors import LHPError

    if isinstance(exc, LHPError):
        return exc.code, exc.title
    return None, str(exc)


def _finalize_monitoring_to_result(
    *,
    monitoring_pipeline_path: Optional[Path],
    event_log_table_created: bool,
    exc: Optional[BaseException] = None,
) -> FinalizeMonitoringResult:
    """Build a :class:`FinalizeMonitoringResult` for the C4 facade path.

    ``exc=None`` indicates success; the ``monitoring_pipeline_path``
    may still be ``None`` when monitoring is not configured (legitimate
    no-op). When ``exc`` is provided the code / title are surfaced as
    flat fields per §4.8.
    """
    if exc is None:
        return FinalizeMonitoringResult(
            success=True,
            monitoring_pipeline_path=monitoring_pipeline_path,
            event_log_table_created=event_log_table_created,
        )
    code, message = _lhp_error_code_and_message(exc)
    return FinalizeMonitoringResult(
        success=False,
        monitoring_pipeline_path=monitoring_pipeline_path,
        event_log_table_created=event_log_table_created,
        error_message=message,
        error_code=code,
    )
