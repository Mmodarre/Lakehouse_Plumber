"""Health-check router for the ``lhp web`` local IDE backend.

Exposes a single ``GET /api/health`` endpoint returning a minimal
:class:`HealthResponse` (``{status, version}``). The SPA polls this to
render the connection indicator and the version badge in the header.

Per the ``webapp-uses-public-api`` import contract this module imports
only the standard library and FastAPI — no ``lhp`` internals.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from fastapi import APIRouter

from lhp.webapp.schemas.health import HealthResponse

# Health is the router convention's exception: no constructor prefix. The
# route is declared as "/health" and the app factory mounts it under "/api",
# giving the final path /api/health.
router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Liveness probe — no auth, no project access required."""
    try:
        pkg_version = version("lakehouse-plumber")
    except PackageNotFoundError:
        pkg_version = "unknown"
    return HealthResponse(status="healthy", version=pkg_version)
