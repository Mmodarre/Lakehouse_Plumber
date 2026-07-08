"""Health-check router for the ``lhp web`` local IDE backend.

Exposes a single ``GET /api/health`` endpoint returning a minimal
:class:`HealthResponse` (``{status, version, project_state, root}``). The SPA
polls this to render the connection indicator, the version badge, and — when
``project_state`` is ``"no_project"`` — the "no LHP project found" guidance.

This endpoint is deliberately exempt from the session-token guard: the SPA
must always be able to render guidance, and the ``lhp web`` launch
readiness-poll hits it before a browser (and thus a token) is involved.

Per the ``webapp-uses-public-api`` import contract this module imports
only the standard library and FastAPI — no ``lhp`` internals.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from fastapi import APIRouter, Request

from lhp.webapp.schemas.health import HealthResponse

# Health is the router convention's exception: no constructor prefix. The
# route is declared as "/health" and the app factory mounts it under "/api",
# giving the final path /api/health.
router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    """Liveness probe — no auth, no project access required."""
    try:
        pkg_version = version("lakehouse-plumber")
    except PackageNotFoundError:
        pkg_version = "unknown"
    # Defaults cover apps not built by create_app (e.g. a bare router mount):
    # missing lifespan state reads as "ok", missing settings as an empty root.
    project_state = getattr(request.app.state, "project_state", "ok")
    settings = getattr(request.app.state, "settings", None)
    root = str(settings.project_root) if settings is not None else ""
    return HealthResponse(
        status="healthy",
        version=pkg_version,
        project_state=project_state,
        root=root,
    )
