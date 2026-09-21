"""Health and version response schemas for the web IDE HTTP contract."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str
    # "ok" | "no_project" — resolved by the app lifespan (fail-closed project
    # root); the SPA renders init guidance on "no_project".
    project_state: str = "ok"
    # Project-root path as a string; shown by the SPA in the no-project notice.
    root: str = ""
    # Whether this server process emits anonymous usage telemetry. Consent is
    # resolved once per process, so the SPA can reflect the state without
    # reading any telemetry file of its own.
    telemetry_enabled: bool = False
    # A release newer than the installed one, or None. None is the answer
    # whenever telemetry is off, the update check is opted out, or the
    # installed version is already current — the field is never the merely
    # "last seen" release.
    latest_version: Optional[str] = None


class VersionResponse(BaseModel):
    lhp_version: str
    python_version: str
    dependencies: dict[str, str]  # package name → installed version
