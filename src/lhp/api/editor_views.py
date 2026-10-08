"""Frozen, serialisable projections for editor integrations.

The existing ``views.py`` registry is near its architecture size grant; these
editor-specific projections form one bounded public contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Mapping, Optional, Tuple

from lhp.api.responses import DependencyAnalysisResult, JSONValue, SandboxScopeResult
from lhp.api.views import BlueprintView, PresetView, ProjectConfigView, TemplateView


@dataclass(frozen=True)
class EditorDocumentOverlay:
    """One unsaved project-relative UTF-8 document. :stability: provisional"""

    path: str
    text: str
    version: int


@dataclass(frozen=True)
class EditorSourceView:
    """Project-relative YAML source in zero-based UTF-16. :stability: provisional"""

    path: str
    document_index: int = 0
    yaml_path: Tuple[str | int, ...] = ()
    line: Optional[int] = None
    column: Optional[int] = None
    end_line: Optional[int] = None
    end_column: Optional[int] = None


@dataclass(frozen=True)
class EditorDiagnosticView:
    """A source-aware editor finding. :stability: provisional"""

    severity: Literal["error", "warning", "information"]
    message: str
    layer: Literal["syntax", "configuration", "generation"]
    code: Optional[str] = None
    source: Optional[EditorSourceView] = None


@dataclass(frozen=True)
class EditorRelatedFileView:
    """A source field referring to another project file. :stability: provisional"""

    kind: Literal[
        "sql", "python", "schema", "expectations", "config", "template", "blueprint"
    ]
    path: str
    exists: bool
    source: EditorSourceView
    action_name: Optional[str] = None


@dataclass(frozen=True)
class EditorActionView:
    """Raw and resolved action with its editable origin. :stability: provisional"""

    name: str
    action_type: str
    source: EditorSourceView
    origin: Literal["direct", "template", "blueprint", "generated"]
    raw: Mapping[str, JSONValue]
    resolved: Mapping[str, JSONValue]
    related_files: Tuple[EditorRelatedFileView, ...] = ()
    editable: bool = True


@dataclass(frozen=True)
class EditorFlowgroupView:
    """A discovered flowgroup and its source ownership. :stability: provisional"""

    pipeline: str
    name: str
    source: EditorSourceView
    origin: Literal["direct", "template", "blueprint", "generated"]
    raw: Mapping[str, JSONValue]
    definition_raw: Optional[Mapping[str, JSONValue]]
    resolved: Mapping[str, JSONValue]
    actions: Tuple[EditorActionView, ...]
    definition: Optional[EditorSourceView] = None
    instance: Optional[EditorSourceView] = None
    editable: bool = True


@dataclass(frozen=True)
class EditorCatalogView:
    """Installed schemas and project authoring assets. :stability: provisional"""

    schemas: Mapping[str, JSONValue]
    action_schema: JSONValue
    action_help: JSONValue
    templates: Tuple[TemplateView, ...]
    presets: Tuple[PresetView, ...]
    blueprints: Tuple[BlueprintView, ...]
    blueprint_parameters: Mapping[str, JSONValue]
    template_related_files: Mapping[str, Tuple[EditorRelatedFileView, ...]] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class EditorProjectView:
    """One coherent project snapshot for an editor. :stability: provisional"""

    project: ProjectConfigView
    environment: str
    environments: Tuple[str, ...]
    flowgroups: Tuple[EditorFlowgroupView, ...]
    catalog: EditorCatalogView
    dependencies: Optional[DependencyAnalysisResult] = None
    diagnostics: Tuple[EditorDiagnosticView, ...] = ()
    stale: bool = False
    sandbox_enabled: bool = False
    sandbox: Optional[SandboxScopeResult] = None


@dataclass(frozen=True)
class EditorDocumentView:
    """Unsaved YAML structure and diagnostics. :stability: provisional"""

    path: str
    flowgroups: Tuple[EditorFlowgroupView, ...]
    diagnostics: Tuple[EditorDiagnosticView, ...]


@dataclass(frozen=True)
class EditorScaffoldResult:
    """Native-editor YAML scaffold; no file was written. :stability: provisional"""

    kind: Literal["template", "blueprint", "bronze"]
    content: str
