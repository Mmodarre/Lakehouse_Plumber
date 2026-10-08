"""Project editor projection over the canonical discovery and resolver graph."""

from __future__ import annotations

import json
import math
from dataclasses import replace
from importlib.resources import files
from pathlib import Path
from typing import Any, Dict, Literal, Mapping, Optional, Tuple, cast

import yaml

from lhp.api._editor_generated import generated_flowgroup_view
from lhp.api._inspection_converters import _build_substitution_manager_for_env
from lhp.api.editor_views import (
    EditorActionView,
    EditorCatalogView,
    EditorDiagnosticView,
    EditorFlowgroupView,
    EditorProjectView,
    EditorRelatedFileView,
    EditorSourceView,
)
from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.api.responses import JSONValue
from lhp.core.discovery import EditorYamlIndex, RawSourceEntry, action_file_references
from lhp.errors import LHPError
from lhp.models import Action


def _json_value(value: Any) -> JSONValue:
    """Project raw YAML can contain dates and NaN; keep the DTO JSON-safe."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)


def _mapping(value: Any) -> Mapping[str, JSONValue]:
    result = _json_value(value)
    return cast(Mapping[str, JSONValue], result if isinstance(result, dict) else {})


def _relative(root: Path, path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    resolved_root = root.resolve()
    resolved = path.resolve()
    if not resolved.is_relative_to(resolved_root):
        return None
    return resolved.relative_to(resolved_root).as_posix()


def _source(
    path: str,
    index: Optional[EditorYamlIndex],
    entry: Optional[RawSourceEntry],
    suffix: Tuple[str | int, ...] = (),
) -> EditorSourceView:
    if entry is None:
        return EditorSourceView(path=path)
    yaml_path = (*entry.yaml_path, *suffix)
    span = index.span(entry.document_index, yaml_path) if index is not None else None
    if span is None and suffix:
        # A preset or template may supply the resolved field. Point at the
        # authored action rather than inventing an editable YAML field path.
        return _source(path, index, entry)
    if span is None:
        return EditorSourceView(
            path=path, document_index=entry.document_index, yaml_path=yaml_path
        )
    return EditorSourceView(
        path=path,
        document_index=entry.document_index,
        yaml_path=yaml_path,
        line=span.line,
        column=span.column,
        end_line=span.end_line,
        end_column=span.end_column,
    )


def _index_for(
    root: Path,
    path: Optional[str],
    cache: Dict[str, Optional[EditorYamlIndex]],
    diagnostics: list[EditorDiagnosticView],
) -> Optional[EditorYamlIndex]:
    if path is None:
        return None
    if path not in cache:
        try:
            cache[path] = EditorYamlIndex((root / path).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError, yaml.YAMLError) as exc:
            cache[path] = None
            diagnostics.append(
                EditorDiagnosticView(
                    severity="error",
                    message=str(exc),
                    layer="syntax",
                    source=EditorSourceView(path=path),
                )
            )
    return cache[path]


def editor_catalog_from_facade(
    root: Path,
    facade: LakehousePlumberApplicationFacade,
    *,
    visible_root: Optional[Path] = None,
) -> EditorCatalogView:
    """Project installed authoring assets and canonical action metadata.

    :stability: provisional
    """
    schemas: Dict[str, JSONValue] = {}
    for resource in files("lhp.schemas").iterdir():
        if resource.name.endswith(".schema.json"):
            try:
                schemas[resource.name.removesuffix(".schema.json")] = _json_value(
                    json.loads(resource.read_text(encoding="utf-8"))
                )
            except (OSError, ValueError):
                continue
    blueprints = facade.inspection.list_blueprints(include_instances=True)
    parameters: Dict[str, JSONValue] = {}
    for blueprint in blueprints:
        path = _relative(root, blueprint.file_path)
        if path is None:
            continue
        try:
            index = EditorYamlIndex((root / path).read_text(encoding="utf-8"))
            first = index.first_mapping()
            parameters[blueprint.name] = _json_value(
                first.raw.get("parameters", []) if first else []
            )
        except (OSError, ValueError, yaml.YAMLError):
            parameters[blueprint.name] = []
    output_root = visible_root or root

    def visible(path: Path) -> Path:
        relative = _relative(root, path)
        return output_root / relative if relative is not None else path

    return EditorCatalogView(
        schemas=schemas,
        action_schema=_json_value(Action.model_json_schema()),
        action_help=_json_value(
            json.loads(
                files("lhp.schemas")
                .joinpath("help")
                .joinpath("flowgroup.json")
                .read_text(encoding="utf-8")
            )
        ),
        templates=tuple(
            replace(item, file_path=visible(item.file_path))
            for item in facade.inspection.list_templates()
        ),
        presets=tuple(
            replace(item, file_path=visible(item.file_path))
            for item in facade.inspection.list_presets()
        ),
        blueprints=tuple(
            replace(
                item,
                file_path=visible(item.file_path),
                instances=tuple(
                    replace(
                        instance,
                        instance_file_path=visible(instance.instance_file_path),
                    )
                    for instance in item.instances
                ),
            )
            for item in blueprints
        ),
        blueprint_parameters=parameters,
    )


def project_view(
    root: Path,
    env: str,
    facade: LakehousePlumberApplicationFacade,
    service_graph: Any,
    *,
    visible_root: Optional[Path] = None,
) -> EditorProjectView:
    """Compose reusable core source indexes with canonical resolved flowgroups.

    :stability: provisional
    """
    config = facade.inspection.get_project_config()
    catalog = editor_catalog_from_facade(root, facade, visible_root=visible_root)
    diagnostics: list[EditorDiagnosticView] = []
    try:
        dependencies = facade.dependency.analyze_dependencies(
            include_graphs=True, force_rebuild=True
        )
    except LHPError as exc:
        dependencies = None
        diagnostics.append(
            EditorDiagnosticView(
                severity="error",
                code=exc.code,
                message=exc.title,
                layer="configuration",
            )
        )
    if visible_root is not None and dependencies is not None:
        dependencies = replace(
            dependencies,
            warnings=tuple(
                replace(
                    warning,
                    file_path=_visible_warning_path(
                        root, visible_root, warning.file_path
                    ),
                    edit_yaml_path=_visible_warning_path(
                        root, visible_root, warning.edit_yaml_path
                    ),
                )
                for warning in dependencies.warnings
            ),
        )
    orchestrator = service_graph  # volatile core graph stays within lhp.api
    flowgroups = orchestrator.bootstrap.discover_all_flowgroups()
    substitution = _build_substitution_manager_for_env(root, env)
    indexes: Dict[str, Optional[EditorYamlIndex]] = {}
    projected: list[EditorFlowgroupView] = []

    for flowgroup in flowgroups:
        provenance = orchestrator.bootstrap.blueprint_provenance(
            flowgroup.pipeline, flowgroup.flowgroup
        )
        context = orchestrator.bootstrap.make_context(flowgroup)
        path = _relative(root, context.source_yaml)
        if path is None:
            # Generated flowgroups have no authored action YAML.
            config_index = _index_for(root, "lhp.yaml", indexes, diagnostics)
            config_entry = config_index.first_mapping() if config_index else None
            source = _source(
                "lhp.yaml",
                config_index,
                config_entry,
                ("monitoring",) if config.has_monitoring else (),
            )
            try:
                resolved_generated = orchestrator.processing.resolve(
                    context, substitution
                ).flowgroup
            except LHPError as exc:
                resolved_generated = flowgroup
                diagnostics.append(
                    EditorDiagnosticView(
                        severity="error",
                        code=exc.code,
                        message=exc.title,
                        layer="configuration",
                        source=source,
                    )
                )
            projected.append(
                generated_flowgroup_view(flowgroup, resolved_generated, source)
            )
            diagnostics.append(
                EditorDiagnosticView(
                    severity="information",
                    message="Generated flowgroup has no editable action YAML; edit its lhp.yaml configuration",
                    layer="configuration",
                    source=source,
                )
            )
            continue
        index = _index_for(root, path, indexes, diagnostics)
        entry = (
            index.blueprint_spec(provenance.spec_index)
            if index is not None and provenance
            else index.flowgroup(flowgroup.pipeline, flowgroup.flowgroup)
            if index is not None
            else None
        )
        if entry is None:
            diagnostics.append(
                EditorDiagnosticView(
                    severity="warning",
                    message="Could not map flowgroup to a YAML node",
                    layer="configuration",
                    source=EditorSourceView(path=path),
                )
            )
            continue
        instance_path = (
            _relative(root, provenance.instance_path) if provenance else path
        )
        instance_index = _index_for(root, instance_path, indexes, diagnostics)
        instance_entry = (
            instance_index.first_mapping()
            if instance_index is not None and provenance
            else entry
        )
        template_name = flowgroup.use_template
        template = next(
            (
                item
                for item in catalog.templates
                if template_name is not None
                and (relative := _relative(visible_root or root, item.file_path))
                is not None
                and Path(relative).is_relative_to("templates")
                and Path(relative).relative_to("templates").with_suffix("").as_posix()
                == template_name
            ),
            None,
        )
        template_path = (
            _relative(visible_root or root, template.file_path)
            if template is not None
            else None
        )
        template_index = _index_for(root, template_path, indexes, diagnostics)
        template_entry = (
            template_index.first_mapping() if template_index is not None else None
        )
        definition = (
            _source(path, index, entry)
            if provenance
            else _source(template_path, template_index, template_entry)
            if template_path is not None and template_entry is not None
            else None
        )
        flowgroup_source = (
            _source(instance_path, instance_index, instance_entry)
            if provenance and instance_path is not None
            else _source(path, index, entry)
        )
        try:
            resolved_ctx = orchestrator.processing.resolve(context, substitution)
            resolved = _mapping(
                resolved_ctx.flowgroup.model_dump(mode="json", exclude_none=True)
            )
            resolved_actions = resolved_ctx.flowgroup.actions
        except LHPError as exc:
            diagnostics.append(
                EditorDiagnosticView(
                    severity="error",
                    code=exc.code,
                    message=exc.title,
                    layer="configuration",
                    source=flowgroup_source,
                )
            )
            resolved = {}
            resolved_actions = flowgroup.actions
        direct_entries = index.action_entries(entry) if index is not None else ()
        template_entries = (
            template_index.action_entries(template_entry)
            if template_index is not None and template_entry is not None
            else ()
        )
        action_views: list[EditorActionView] = []
        for action_number, action in enumerate(resolved_actions):
            from_template = action_number >= len(flowgroup.actions) and bool(
                template_entries
            )
            raw_entry = (
                template_entries[action_number - len(flowgroup.actions)]
                if from_template
                and action_number - len(flowgroup.actions) < len(template_entries)
                else direct_entries[action_number]
                if action_number < len(direct_entries)
                else None
            )
            action_path = (
                template_path if from_template and template_path is not None else path
            )
            action_index = template_index if from_template else index
            action_source = _source(action_path, action_index, raw_entry)
            resolved_action = _mapping(
                action.model_dump(mode="json", exclude_none=True)
            )
            raw_action = _mapping(raw_entry.raw) if raw_entry is not None else {}
            refs = tuple(
                EditorRelatedFileView(
                    kind=ref.kind,
                    path=ref.path,
                    exists=ref.exists,
                    source=_source(
                        action_path, action_index, raw_entry, ref.field_path
                    ),
                    action_name=ref.action_name,
                )
                for ref in action_file_references(resolved_action, root)
                if not Path(ref.path).is_absolute() and ".." not in Path(ref.path).parts
            )
            origin: Literal["direct", "template", "blueprint"] = (
                "template" if from_template else "blueprint" if provenance else "direct"
            )
            action_views.append(
                EditorActionView(
                    name=action.name,
                    action_type=str(action.type.value),
                    source=action_source,
                    origin=origin,
                    raw=raw_action,
                    resolved=resolved_action,
                    related_files=refs,
                    editable=origin == "direct" and raw_entry is not None,
                )
            )
        origin_fg: Literal["direct", "template", "blueprint"] = (
            "blueprint" if provenance else "template" if template_name else "direct"
        )
        projected.append(
            EditorFlowgroupView(
                pipeline=flowgroup.pipeline,
                name=flowgroup.flowgroup,
                source=flowgroup_source,
                origin=origin_fg,
                raw=_mapping(
                    instance_entry.raw if provenance and instance_entry else entry.raw
                ),
                definition_raw=_mapping(entry.raw) if provenance else None,
                resolved=resolved,
                actions=tuple(action_views),
                definition=definition,
                instance=_source(instance_path, instance_index, instance_entry)
                if instance_path is not None
                else None,
                editable=provenance is None and entry is not None,
            )
        )

    substitutions = root / "substitutions"
    environments = tuple(
        sorted(
            {path.stem for path in substitutions.glob("*.yaml")}
            | {path.stem for path in substitutions.glob("*.yml")}
            | {env}
        )
    )
    return EditorProjectView(
        project=config,
        environment=env,
        environments=environments,
        flowgroups=tuple(projected),
        catalog=catalog,
        dependencies=dependencies,
        diagnostics=tuple(diagnostics),
    )


def _visible_warning_path(
    root: Path, visible_root: Path, value: Optional[str]
) -> Optional[str]:
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute():
        return value
    relative = _relative(root, path)
    return str(visible_root / relative) if relative is not None else value
