"""Health and version response schemas for the web IDE HTTP contract."""

from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str


class VersionResponse(BaseModel):
    lhp_version: str
    python_version: str
    dependencies: dict[str, str]  # package name → installed version
