"""Path-based template authoring and draft preview HTTP contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class TemplateDiagnostic(BaseModel):
    severity: Literal["error", "warning", "info"]
    code: str
    stage: Literal["inspect", "expanded", "resolved"]
    message: str
    suggestion: str | None = None
    source_path: str
    field_path: list[str | int] | None = None
    line: int | None = None
    column: int | None = None


class TemplateAuthoringParameter(BaseModel):
    name: str
    required: bool
    has_default: bool
    default: Any = None
    declared_type: str | None = None
    description: str | None = None


class TemplateCatalogEntry(BaseModel):
    source_path: str
    reference: str | None
    declared_name: str | None
    version: str | None
    description: str | None
    state: Literal["ready", "invalid", "unsupported_extension"]
    parameters: list[TemplateAuthoringParameter]
    presets: list[str]
    action_count: int | None
    diagnostics: list[TemplateDiagnostic]


class TemplateCatalogResponse(BaseModel):
    templates: list[TemplateCatalogEntry]
    total: int


class TemplateSourceResponse(BaseModel):
    template: TemplateCatalogEntry


class TemplatePreviewContext(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pipeline: str = Field(default="", max_length=256)
    flowgroup: str = Field(default="", max_length=256)
    environment: str = Field(default="", max_length=128)
    presets: list[str] = Field(default_factory=list, max_length=100)
    variables: dict[str, str] = Field(default_factory=dict)


class TemplatePreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_path: str = Field(min_length=1, max_length=2048)
    source_yaml: str = Field(max_length=524288)
    request_revision: str = Field(max_length=256)
    stage: Literal["inspect", "expanded", "resolved"]
    sample_parameters: dict[str, Any] = Field(default_factory=dict)
    context: TemplatePreviewContext | None = None


class TemplateSavedDependency(BaseModel):
    path: str
    fingerprint: str | None


class TemplatePreviewResponse(BaseModel):
    request_revision: str
    source_hash: str
    stage: Literal["inspect", "expanded", "resolved"]
    status: Literal["ready", "needs_parameters", "needs_context", "invalid", "stale"]
    diagnostics: list[TemplateDiagnostic]
    missing_parameters: list[str]
    effective_parameters: dict[str, Any] | None = None
    expanded_actions: list[dict[str, Any]] | None = None
    resolved_flowgroup: dict[str, Any] | None = None
    saved_dependencies: list[TemplateSavedDependency]
