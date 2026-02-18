from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class TemplateInfoResponse(BaseModel):
    """Template metadata returned by TemplateEngine.get_template_info()."""
    name: str
    version: str
    description: str
    parameters: List[Dict[str, Any]]
    action_count: int


class TemplateListResponse(BaseModel):
    """List of available template names."""
    templates: List[str]
    total: int


class TemplateDetailResponse(BaseModel):
    """Single template with full metadata."""
    name: str
    template: TemplateInfoResponse
