"""Health and version response schemas for the web IDE HTTP contract."""

from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str
    # "ok" | "no_project" — resolved by the app lifespan (fail-closed project
    # root); the SPA renders init guidance on "no_project".
    project_state: str = "ok"
    # Project-root path as a string; shown by the SPA in the no-project notice.
    root: str = ""


class VersionResponse(BaseModel):
    lhp_version: str
    python_version: str
    dependencies: dict[str, str]  # package name → installed version
