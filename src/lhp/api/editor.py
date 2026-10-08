"""Public editor operations over the LHP project model.

These standalone functions keep the constitution's seven-facade composition
unchanged. They compose the same public facade and resolver graph the CLI uses;
unsaved inputs are confined to a temporary project mirror.

:stability: provisional
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Literal, Mapping, Optional, Tuple

import yaml
from pydantic import ValidationError

from lhp.api._composition import build_api_service_graph
from lhp.api._editor_event_paths import visible_editor_event
from lhp.api._editor_overlay import _checked_overlay_path, mirrored_project
from lhp.api._editor_projection import editor_catalog_from_facade, project_view
from lhp.api.editor_views import (
    EditorCatalogView,
    EditorDiagnosticView,
    EditorDocumentOverlay,
    EditorDocumentView,
    EditorProjectView,
    EditorScaffoldResult,
    EditorSourceView,
)
from lhp.api.events import ErrorEmitted, LHPEvent, OperationStarted
from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.api.responses import JSONValue
from lhp.errors import LHPError


def _runtime(
    root: Path, pipeline_config_path: Optional[str] = None
) -> Tuple[LakehousePlumberApplicationFacade, Any]:
    if pipeline_config_path is not None:
        relative = Path(pipeline_config_path)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or "\\" in pipeline_config_path
            or not (root / relative).resolve().is_relative_to(root.resolve())
        ):
            raise ValueError("Pipeline config path must stay inside the project")
    graph = build_api_service_graph(
        root,
        pipeline_config_path=pipeline_config_path,
        no_cache=True,
        enforce_version=False,
        max_workers=None,
    )
    return LakehousePlumberApplicationFacade(graph), graph


def _facade(
    root: Path, pipeline_config_path: Optional[str] = None
) -> LakehousePlumberApplicationFacade:
    return _runtime(root, pipeline_config_path)[0]


def _safe_project_root(project_root: Path) -> Path:
    root = Path(project_root).resolve()
    if not (root / "lhp.yaml").is_file():
        raise FileNotFoundError(f"No lhp.yaml at {root}")
    return root


def _syntax_diagnostic(
    overlay: EditorDocumentOverlay, exc: yaml.YAMLError
) -> EditorDiagnosticView:
    mark = getattr(exc, "problem_mark", None)
    source = EditorSourceView(path=overlay.path)
    if mark is not None:
        lines = overlay.text.splitlines(keepends=True)
        text_before = lines[mark.line][: mark.column] if mark.line < len(lines) else ""
        column = len(text_before.encode("utf-16-le")) // 2
        source = EditorSourceView(
            path=overlay.path,
            line=mark.line,
            column=column,
            end_line=mark.line,
            end_column=column + 1,
        )
    return EditorDiagnosticView(
        severity="error",
        message=str(exc),
        layer="syntax",
        source=source,
    )


def inspect_editor_project(
    project_root: Path,
    *,
    env: str = "dev",
    overlays: Tuple[EditorDocumentOverlay, ...] = (),
    pipeline_config_path: Optional[str] = None,
) -> EditorProjectView:
    """Discover, resolve and locate a project without writing user files.

    Invalid unsaved YAML retains the saved graph and adds a source diagnostic;
    ``stale=True`` tells callers that the displayed graph does not include the
    invalid overlay. Complete overlays are resolved in an isolated mirror.

    :stability: provisional
    :raises lhp.errors.LHPError: ``LHP-CFG-*``/``LHP-VAL-*`` for an invalid
        saved project when no complete fallback can be built.
    :raises ValueError: unsafe or oversized overlay paths/content.
    """
    root = _safe_project_root(project_root)
    if not overlays:
        facade, graph = _runtime(root, pipeline_config_path)
        return project_view(root, env, facade, graph)
    from lhp.core.discovery import EditorYamlIndex

    syntax_diagnostics: list[EditorDiagnosticView] = []
    for overlay in overlays:
        _checked_overlay_path(root, overlay)
        if Path(overlay.path).suffix.lower() not in {".yaml", ".yml"}:
            continue
        try:
            EditorYamlIndex(overlay.text)
        except yaml.YAMLError as exc:
            syntax_diagnostics.append(_syntax_diagnostic(overlay, exc))
    if syntax_diagnostics:
        facade, graph = _runtime(root, pipeline_config_path)
        saved = project_view(root, env, facade, graph)
        return EditorProjectView(
            project=saved.project,
            environment=saved.environment,
            environments=saved.environments,
            flowgroups=saved.flowgroups,
            catalog=saved.catalog,
            dependencies=saved.dependencies,
            diagnostics=(*saved.diagnostics, *syntax_diagnostics),
            stale=True,
        )
    try:
        with mirrored_project(root, overlays) as mirror:
            facade, graph = _runtime(mirror, pipeline_config_path)
            return project_view(mirror, env, facade, graph, visible_root=root)
    except (yaml.YAMLError, LHPError, ValidationError) as exc:
        # Preserve partial authoring context on a malformed draft. The saved
        # project can itself be invalid, in which case its original exception
        # propagates rather than returning an invented empty graph.
        facade, graph = _runtime(root, pipeline_config_path)
        saved = project_view(root, env, facade, graph)
        # A domain failure may come from any included project document.
        # Without a reliable path in its context, keep it project-level.
        source = None
        if isinstance(exc, LHPError):
            reported_file = exc.context.get("file")
            if isinstance(reported_file, str):
                for overlay in overlays:
                    if reported_file.endswith("/" + overlay.path):
                        source = EditorSourceView(path=overlay.path)
                        break
        diagnostic = EditorDiagnosticView(
            severity="error",
            message=getattr(exc, "title", str(exc)),
            layer="configuration",
            source=source,
            code=getattr(exc, "code", None),
        )
        return EditorProjectView(
            project=saved.project,
            environment=saved.environment,
            environments=saved.environments,
            flowgroups=saved.flowgroups,
            catalog=saved.catalog,
            dependencies=saved.dependencies,
            diagnostics=(*saved.diagnostics, diagnostic),
            stale=True,
        )


def inspect_editor_document(
    project_root: Path, *, path: str, text: str
) -> EditorDocumentView:
    """Inspect one unsaved YAML document and return editor diagnostics.

    The project-level resolver is also exercised against the overlay when
    possible. Other saved project files remain authoritative.

    :stability: provisional
    :raises ValueError: unsafe/oversized document path or content.
    """
    root = _safe_project_root(project_root)
    overlay = EditorDocumentOverlay(path=path, text=text, version=0)
    _checked_overlay_path(root, overlay)
    from lhp.core.discovery import EditorYamlIndex

    try:
        index = EditorYamlIndex(text)
    except yaml.YAMLError as exc:
        return EditorDocumentView(
            path=path,
            flowgroups=(),
            diagnostics=(_syntax_diagnostic(overlay, exc),),
        )
    # Parsed but possibly incomplete. Run the canonical project resolver in a
    # mirror; a failed overlay retains a saved graph and reports its error.
    view = inspect_editor_project(root, overlays=(overlay,))
    flowgroups = tuple(
        flowgroup
        for flowgroup in view.flowgroups
        if flowgroup.source.path == path
        or (flowgroup.instance is not None and flowgroup.instance.path == path)
    )
    diagnostics = tuple(
        item
        for item in view.diagnostics
        if item.source is None or item.source.path == path
    )
    if not index.documents:
        diagnostics += (
            EditorDiagnosticView(
                severity="error",
                message="Empty YAML document",
                layer="syntax",
                source=EditorSourceView(path=path),
            ),
        )
    return EditorDocumentView(path=path, flowgroups=flowgroups, diagnostics=diagnostics)


def editor_catalog(project_root: Path) -> EditorCatalogView:
    """Read schemas and project authoring assets through LHP. :stability: provisional

    :raises lhp.errors.LHPError: ``LHP-CFG-*`` if project config cannot load.
    """
    root = _safe_project_root(project_root)
    return editor_catalog_from_facade(root, _facade(root))


def validate_editor_project(
    project_root: Path,
    *,
    env: str,
    overlays: Tuple[EditorDocumentOverlay, ...] = (),
    pipeline_config_path: Optional[str] = None,
    include_tests: bool = True,
) -> Iterator[LHPEvent]:
    """Run canonical validation over saved files or isolated unsaved drafts.

    The terminal ``ValidationCompleted`` carries the canonical batch response.
    The temporary mirror stays alive until the stream is fully consumed.

    :stability: provisional
    :raises ValueError: unsafe or oversized overlay paths/content.
    :raises lhp.errors.LHPError: ``LHP-CFG-*`` project-load failures.
    """
    yield OperationStarted(operation_name="validate_editor_project", env=env)
    error_emitted = False
    try:
        root = _safe_project_root(project_root)

        def drive(active_root: Path) -> Iterator[LHPEvent]:
            facade = _facade(active_root, pipeline_config_path)
            yield from facade.validation.validate_pipelines(
                env=env,
                include_tests=include_tests,
                bundle_enabled=False,
            )

        with mirrored_project(root, overlays) as mirror:
            for event in drive(mirror):
                if isinstance(event, OperationStarted):
                    continue
                error_emitted |= isinstance(event, ErrorEmitted)
                yield visible_editor_event(event, mirror, root)
    except LHPError as exc:
        if not error_emitted:
            yield ErrorEmitted(lhp_error=exc)
        raise


def preview_editor_project(
    project_root: Path,
    *,
    env: str,
    overlays: Tuple[EditorDocumentOverlay, ...] = (),
    pipeline_config_path: Optional[str] = None,
) -> Iterator[LHPEvent]:
    """Render source-mode generated text from a saved or unsaved project.

    This is the canonical generation plan in a temporary project mirror. It
    excludes bundle resources, monitoring finalisation, sandbox rewrites and
    wheel artifacts. No claim of full generation parity is made.

    :stability: provisional
    :raises ValueError: wheel projects or unsafe/oversized overlays.
    :raises lhp.errors.LHPError: ``LHP-VAL-*``/``LHP-CFG-*`` on generation
        preflight or action failures, after the event stream reports the error.
    """
    yield OperationStarted(operation_name="preview_editor_project", env=env)
    error_emitted = False
    try:
        root = _safe_project_root(project_root)
        with mirrored_project(root, overlays) as mirror:
            from lhp.core.loaders import PipelineConfigLoader

            facade = _facade(mirror, pipeline_config_path)
            pipeline_names = sorted(
                {item.pipeline for item in facade.inspection.list_flowgroups()}
            )
            modes = PipelineConfigLoader(
                mirror, pipeline_config_path
            ).resolve_packaging_modes(pipeline_names)
            if "wheel" in modes.values():
                raise ValueError(
                    "Wheel projects are not supported by text-only editor preview"
                )
            for event in facade.generation.plan_generation(env=env):
                if isinstance(event, OperationStarted):
                    continue
                error_emitted |= isinstance(event, ErrorEmitted)
                yield visible_editor_event(event, mirror, root)
    except LHPError as exc:
        if not error_emitted:
            yield ErrorEmitted(lhp_error=exc)
        raise


def scaffold_editor_instance(
    project_root: Path,
    *,
    kind: Literal["template", "blueprint"],
    reference: str,
    parameters: Mapping[str, JSONValue],
    pipeline: str = "",
    flowgroup: str = "",
) -> EditorScaffoldResult:
    """Return canonical YAML for one template or blueprint invocation.

    The caller owns the native editor document and decides where to save it.

    :stability: provisional
    :raises ValueError: unknown reference, missing identity or parameters.
    :raises lhp.errors.LHPError: ``LHP-CFG-*`` if project config cannot load.
    """
    root = _safe_project_root(project_root)
    catalog = editor_catalog(root)
    if kind == "template":
        if not pipeline or not flowgroup:
            raise ValueError("Template instances require pipeline and flowgroup")
        template_root = root / "templates"
        template = next(
            (
                item
                for item in catalog.templates
                if item.name == reference
                or (
                    item.file_path.is_relative_to(template_root)
                    and item.file_path.relative_to(template_root)
                    .with_suffix("")
                    .as_posix()
                    == reference
                )
            ),
            None,
        )
        if template is None:
            raise ValueError(f"Unknown template: {reference}")
        required = {item.name for item in template.parameters if item.required}
        missing = sorted(required - parameters.keys())
        if missing:
            raise ValueError(f"Missing template parameters: {', '.join(missing)}")
        data: Mapping[str, JSONValue] = {
            "pipeline": pipeline,
            "flowgroup": flowgroup,
            "use_template": reference,
            "template_parameters": dict(parameters),
        }
    else:
        if reference not in {item.name for item in catalog.blueprints}:
            raise ValueError(f"Unknown blueprint: {reference}")
        declared = catalog.blueprint_parameters.get(reference, [])
        required_blueprint = (
            {
                str(item["name"])
                for item in declared
                if isinstance(item, dict)
                and item.get("required") is True
                and isinstance(item.get("name"), str)
            }
            if isinstance(declared, list)
            else set()
        )
        missing = sorted(required_blueprint - parameters.keys())
        if missing:
            raise ValueError(f"Missing blueprint parameters: {', '.join(missing)}")
        data = {"use_blueprint": reference, "parameters": dict(parameters)}
    return EditorScaffoldResult(
        kind=kind,
        content=yaml.safe_dump(dict(data), sort_keys=False, allow_unicode=True),
    )


def scaffold_editor_bronze(
    *,
    name: str,
    pipeline: str,
    source_path: str,
    format: str,
    target: str,
) -> EditorScaffoldResult:
    """Return a minimal CloudFiles bronze flowgroup as editable YAML.

    ``target`` must be ``catalog.schema.table``. Names and shape are checked
    by LHP's input model. The resulting file
    remains a draft until the caller saves and validates it in a project.

    :stability: provisional
    :raises ValueError: missing fields, unqualified target or unsupported format.
    """
    from lhp.models import FlowGroup

    if not all((name.strip(), pipeline.strip(), source_path.strip(), target.strip())):
        raise ValueError("Bronze flowgroup fields must be nonempty")
    if format not in {"csv", "json", "parquet", "avro", "orc", "text"}:
        raise ValueError(f"Unsupported CloudFiles format: {format}")
    target_parts = target.split(".")
    if len(target_parts) != 3 or not all(part.strip() for part in target_parts):
        raise ValueError("Bronze target must be catalog.schema.table")
    catalog_name, schema_name, table_name = target_parts
    source_name = f"{name}_source"
    document = {
        "pipeline": pipeline,
        "flowgroup": name,
        "actions": [
            {
                "name": f"load_{name}",
                "type": "load",
                "readMode": "stream",
                "source": {
                    "type": "cloudfiles",
                    "path": source_path,
                    "format": format,
                    "options": {"cloudFiles.format": format},
                },
                "target": source_name,
            },
            {
                "name": f"write_{name}",
                "type": "write",
                "source": source_name,
                "write_target": {
                    "type": "streaming_table",
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "table": table_name,
                },
            },
        ],
    }
    FlowGroup.model_validate(document)
    return EditorScaffoldResult(
        kind="bronze",
        content=yaml.safe_dump(document, sort_keys=False, allow_unicode=True),
    )
