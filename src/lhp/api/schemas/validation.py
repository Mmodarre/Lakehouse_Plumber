from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class ValidateRequest(BaseModel):
    """Request body for POST /validate."""
    environment: str = "dev"
    pipeline: Optional[str] = None


class ValidateResponse(BaseModel):
    success: bool
    errors: List[str]
    warnings: List[str]
    validated_pipelines: List[str]
    error_message: Optional[str] = None


class ValidateYAMLRequest(BaseModel):
    """Request body for POST /validate/yaml."""
    content: str                       # Raw YAML string
    environment: str = "dev"
