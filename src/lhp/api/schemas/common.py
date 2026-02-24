from typing import Any, Dict, Generic, List, Optional, TypeVar

from fastapi import Query
from pydantic import BaseModel, Field


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


T = TypeVar("T")


class PaginationParams(BaseModel):
    """Reusable pagination parameters."""

    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=200)


def get_pagination(
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(50, ge=1, le=200, description="Max items to return"),
) -> PaginationParams:
    """FastAPI dependency for extracting pagination parameters."""
    return PaginationParams(offset=offset, limit=limit)


class PaginatedResponse(BaseModel, Generic[T]):
    """Base model for paginated list responses."""

    items: List[T]
    total: int
    offset: int
    limit: int
