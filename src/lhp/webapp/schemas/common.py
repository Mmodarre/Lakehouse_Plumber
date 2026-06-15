"""Shared response envelopes and pagination schemas (pure pydantic)."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field


class ErrorDetail(BaseModel):
    """Structured error response matching the spec."""

    code: str  # e.g. "LHP-VAL-003"
    category: str  # e.g. "VALIDATION"
    message: str  # Human-readable title
    details: str  # Detailed explanation
    suggestions: list[str] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)
    http_status: int


class ErrorResponse(BaseModel):
    """Top-level error envelope."""

    error: ErrorDetail


class SuccessResponse(BaseModel):
    """Generic success message."""

    message: str
    details: dict[str, Any] | None = None


T = TypeVar("T")


class PaginationParams(BaseModel):
    """Reusable pagination parameters."""

    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=200)


class PaginatedResponse(BaseModel, Generic[T]):
    """Base model for paginated list responses."""

    items: list[T]
    total: int
    offset: int
    limit: int
