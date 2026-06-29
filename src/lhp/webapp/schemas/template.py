"""Template list / detail response schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class TemplateInfoResponse(BaseModel):
    """Template metadata returned by TemplateEngine.get_template_info()."""

    name: str
    version: str
    description: str
    parameters: list[dict[str, Any]]
    action_count: int


class TemplateSummary(BaseModel):
    """Lightweight template summary for list views."""

    name: str
    description: str | None = None
    parameter_count: int
    action_count: int
    action_types: list[str]


class TemplateListResponse(BaseModel):
    """List of available template names."""

    templates: list[str]
    total: int


class TemplateListDetailResponse(BaseModel):
    """List of templates with summary metadata."""

    templates: list[TemplateSummary]
    total: int


class TemplateDetailResponse(BaseModel):
    """Single template with full metadata."""

    name: str
    template: TemplateInfoResponse
