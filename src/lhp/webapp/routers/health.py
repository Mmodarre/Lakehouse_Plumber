"""Health-check router for the ``lhp web`` local IDE backend.

Exposes a single ``GET /api/health`` endpoint returning a minimal
:class:`HealthResponse` (``{status, version, project_state, root,
telemetry_enabled, latest_version}``). The SPA polls this to render the
connection indicator, the version badge — including the "a newer release
exists" hint — and, when ``project_state`` is ``"no_project"``, the "no LHP
project found" guidance.

This endpoint is deliberately exempt from the session-token guard: the SPA
must always be able to render guidance, and the ``lhp web`` launch
readiness-poll hits it before a browser (and thus a token) is involved.

Per the ``webapp-uses-public-api`` import contract this module imports the
standard library, FastAPI and :mod:`lhp.telemetry` — no other ``lhp``
internals. The telemetry import buys exactly one read:
:func:`~lhp.telemetry.newer_version_available`, which reports the release the
background update check last saw when it is newer than the installed one. It
touches no project file and never raises. Because every open tab polls health
every 30 s, its answer is cached on ``app.state`` for
:data:`HEALTH_UPDATE_TTL_SECONDS`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from importlib.metadata import PackageNotFoundError, version
from typing import Optional

from fastapi import APIRouter, Request

from lhp import telemetry
from lhp.webapp.schemas.health import HealthResponse

logger = logging.getLogger(__name__)

# Health is the router convention's exception: no constructor prefix. The
# route is declared as "/health" and the app factory mounts it under "/api",
# giving the final path /api/health.
router = APIRouter(tags=["health"])

#: Seconds a resolved ``latest_version`` stays valid on ``app.state``. The
#: answer changes at most once a day, while the SPA polls every 30 s per tab,
#: so anything shorter would buy nothing and cost a state-file read per poll.
HEALTH_UPDATE_TTL_SECONDS = 300

#: The TTL's clock, as an injectable module attribute so tests can expire the
#: cache without sleeping. Monotonic by design: a wall-clock jump must not
#: extend or void a cache entry.
_monotonic: Callable[[], float] = time.monotonic


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    """Liveness probe — no auth, no project access required."""
    try:
        pkg_version = version("lakehouse-plumber")
    except PackageNotFoundError:
        pkg_version = "unknown"
    # Defaults cover apps not built by create_app (e.g. a bare router mount):
    # missing lifespan state reads as "ok", missing settings as an empty root,
    # and absent consent as telemetry off.
    project_state = getattr(request.app.state, "project_state", "ok")
    settings = getattr(request.app.state, "settings", None)
    root = str(settings.project_root) if settings is not None else ""
    telemetry_enabled = bool(getattr(request.app.state, "telemetry_enabled", False))
    return HealthResponse(
        status="healthy",
        version=pkg_version,
        project_state=project_state,
        root=root,
        telemetry_enabled=telemetry_enabled,
        # A disabled process must not read the telemetry state file at all,
        # so the lookup sits behind the flag rather than inside it.
        latest_version=(await _latest_version(request) if telemetry_enabled else None),
    )


async def _latest_version(request: Request) -> Optional[str]:
    """The newer release to advertise, refreshed at most once per TTL.

    The cache lives on ``app.state`` as ``(value, expires_at)`` in the
    monotonic timebase and is created on first use, so an app assembled
    without ``create_app`` needs no seeding. "No newer release" is cached
    like any other answer: it is the common case, and leaving it uncached
    would make the TTL worthless.

    Unsynchronised on purpose: polls that overlap across the ``await`` can
    each miss and each look up, which costs one extra state-file read and
    converges on the same value — cheaper than holding a lock on the path
    every tab hits twice a minute.
    """
    now = _monotonic()
    cached: Optional[tuple[Optional[str], float]] = getattr(
        request.app.state, "latest_version_cache", None
    )
    if cached is not None and now < cached[1]:
        return cached[0]
    try:
        # A state-file read: off the event loop, like every other blocking
        # telemetry call in the webapp.
        latest: Optional[str] = await asyncio.to_thread(
            telemetry.newer_version_available
        )
    except Exception:  # a health poll must never fail over an update hint
        logger.debug("telemetry: update lookup failed", exc_info=True)
        latest = None
    request.app.state.latest_version_cache = (latest, now + HEALTH_UPDATE_TTL_SECONDS)
    return latest
