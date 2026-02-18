from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class ErrorDetail(BaseModel):
    """Structured error response matching the spec."""
    code: str                           # e.g. "LHP-VAL-003"
    category: str                       # e.g. "VALIDATION"
    message: str                        # Human-readable title
    details: str                        # Detailed explanation
    suggestions: List[str] = []
    context: Dict[str, Any] = {}
    http_status: int


class ErrorResponse(BaseModel):
    """Top-level error envelope."""
    error: ErrorDetail


class SuccessResponse(BaseModel):
    """Generic success message."""
    message: str
    details: Optional[Dict[str, Any]] = None
