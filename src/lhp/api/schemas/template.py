from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class TemplateInfoResponse(BaseModel):
    """Template metadata returned by TemplateEngine.get_template_info()."""
    name: str
    version: str
    description: str
    parameters: List[Dict[str, Any]]
    action_count: int


class TemplateSummary(BaseModel):
    """Lightweight template summary for list views."""
    name: str
    description: Optional[str] = None
    parameter_count: int
    action_count: int
    action_types: List[str]


class TemplateListResponse(BaseModel):
    """List of available template names."""
    templates: List[str]
    total: int


class TemplateListDetailResponse(BaseModel):
    """List of templates with summary metadata."""
    templates: List[TemplateSummary]
    total: int


class TemplateDetailResponse(BaseModel):
    """Single template with full metadata."""
    name: str
    template: TemplateInfoResponse
