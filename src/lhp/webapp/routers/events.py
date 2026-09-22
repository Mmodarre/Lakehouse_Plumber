"""Server-Sent Events endpoint pushing live updates to the web IDE SPA.

``GET /api/events`` holds one long-lived ``text/event-stream`` response per
browser tab. Each connection subscribes a bounded queue on the app-wide
:class:`~lhp.webapp.services.event_bus.EventBus` and relays every published
event as an SSE frame::

    event: <name>
    data: <compact JSON>

Producers today: the run recorder (``run-updated``) and the file watcher
(``file-changed`` on any change, plus ``graph-stale`` when a graph-relevant
edit means the dependency graph should be manually refreshed). While the bus
is idle, a comment heartbeat (``: ping``)
is emitted every :data:`HEARTBEAT_SECONDS` so proxies and the browser can
tell a quiet stream from a dead one. On reconnect the SPA re-fetches all
state, so dropped events (bounded queue, see the bus) are recoverable and
no replay/Last-Event-ID support is needed.

Authentication: the existing ``TokenGuardMiddleware`` already covers
``/api/events`` and accepts the session token via the ``token`` query
parameter (``EventSource`` cannot set headers), so this router does no auth
of its own.

Queue reads are pure-async (``await queue.get()``) — the handler never
occupies a threadpool worker, so any number of concurrent SSE connections
cannot starve the sync-handler pool.

ROUTER CONVENTION: routes carry their sub-path (``/events``); the app mounts
this router with ``prefix="/api"``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import StreamingResponse

from lhp.webapp.services._telemetry_events import on_sse_disconnect, session_for
from lhp.webapp.services.telemetry_sessions import WebSessionRegistry

router = APIRouter(tags=["events"])

logger = logging.getLogger(__name__)

# Idle-stream heartbeat cadence in seconds. Module-level (not a parameter or
# query arg) so tests can monkeypatch it; read on every loop iteration.
HEARTBEAT_SECONDS: float = 15.0

_SSE_MEDIA_TYPE = "text/event-stream"
# no-cache: SSE responses must never be cached; X-Accel-Buffering disables
# proxy response buffering (nginx et al.) that would batch the push frames.
_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
_HEARTBEAT_FRAME = ": ping\n\n"


def _format_frame(event: dict[str, Any]) -> str:
    """Render one bus event dict as an SSE ``event:``/``data:`` frame."""
    name = event.get("event", "message")
    data = json.dumps(event.get("data", {}), separators=(",", ":"))
    return f"event: {name}\ndata: {data}\n\n"


async def _event_stream(request: Request) -> AsyncIterator[str]:
    """Yield SSE frames from a fresh bus subscription until disconnect.

    The subscription is released in ``finally`` — Starlette closes this
    generator when the client disconnects, so no queue is ever leaked.

    The connection also bounds the tab's telemetry session: opening it marks
    the session live (cancelling any grace timer a reconnect is racing), and
    the last close starts the grace timer that ends the session unless the
    tab comes back. Both hooks are no-ops when telemetry is off or the
    ``session`` query parameter is absent or malformed.
    """
    event_bus = request.app.state.event_bus
    queue = event_bus.subscribe()
    attributed = session_for(request)
    if attributed is not None:
        registry, sid = attributed
        registry.sse_connected(sid)
    logger.debug("SSE client connected")
    try:
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
            except TimeoutError:
                yield _HEARTBEAT_FRAME
                continue
            yield _format_frame(event)
    finally:
        event_bus.unsubscribe(queue)
        if attributed is not None:
            _schedule_grace(request.app, *attributed)
        logger.debug("SSE client disconnected")


def _schedule_grace(app: FastAPI, registry: WebSessionRegistry, sid: str) -> None:
    """Start the grace timer once the tab's LAST SSE connection has closed.

    ``sse_disconnected`` is ``True`` only for the last connection, so a tab
    holding two streams keeps its session on the first close. The task is
    created on the loop running this handler and registered at once so a
    reconnect or the lifespan shutdown can cancel it before it fires.
    """
    try:
        if registry.sse_disconnected(sid):
            task = asyncio.create_task(
                on_sse_disconnect(app, sid), name="lhp-telemetry-grace"
            )
            registry.grace_started(sid, task)
    except Exception:  # telemetry never reaches the stream that triggered it
        logger.debug("telemetry: SSE grace task not scheduled", exc_info=True)


@router.get("/events")
async def events(request: Request) -> StreamingResponse:
    """Open the live-update SSE stream (``run-updated`` / ``file-changed`` /
    ``graph-stale``)."""
    return StreamingResponse(
        _event_stream(request),
        media_type=_SSE_MEDIA_TYPE,
        headers=_SSE_HEADERS,
    )
