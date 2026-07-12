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

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

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
    """
    event_bus = request.app.state.event_bus
    queue = event_bus.subscribe()
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
        logger.debug("SSE client disconnected")


@router.get("/events")
async def events(request: Request) -> StreamingResponse:
    """Open the live-update SSE stream (``run-updated`` / ``file-changed`` /
    ``graph-stale``)."""
    return StreamingResponse(
        _event_stream(request),
        media_type=_SSE_MEDIA_TYPE,
        headers=_SSE_HEADERS,
    )
