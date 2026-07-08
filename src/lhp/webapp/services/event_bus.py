"""In-process asyncio fan-out bus for webapp server-push events.

One :class:`EventBus` lives on ``app.state.event_bus``. Producers (the run
recorder today; a file watcher later) call :meth:`EventBus.publish` with an
event dict shaped ``{"event": "<type>", "data": {...}}``; each subscriber
(a later SSE endpoint) drains its own bounded ``asyncio.Queue``.

Delivery is best-effort per subscriber: ``publish`` never blocks and never
raises — a subscriber whose queue is full simply misses that event (SSE
consumers re-sync from ``/api/runs`` state, so drops are recoverable).

All bus methods are intended for the event-loop thread. Producers running on
worker threads hand off via :meth:`EventBus.publish_threadsafe` with the
loop captured while on the loop thread.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_QUEUE_SIZE = 256


class EventBus:
    """Bounded-queue publish/subscribe fan-out for event dicts."""

    def __init__(self, maxsize: int = _DEFAULT_QUEUE_SIZE) -> None:
        self._maxsize = maxsize
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()

    @property
    def subscriber_count(self) -> int:
        """Number of currently subscribed queues (used by tests/diagnostics)."""
        return len(self._subscribers)

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        """Register and return a new bounded subscriber queue."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._maxsize)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Deregister a subscriber queue (no-op if it is not subscribed)."""
        self._subscribers.discard(queue)

    def publish(self, event: dict[str, Any]) -> None:
        """Fan ``event`` out to every subscriber; drop per full (slow) queue."""
        for queue in tuple(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.debug(
                    f"event bus: dropped {event.get('event')!r} for a slow subscriber"
                )

    def publish_threadsafe(
        self, loop: asyncio.AbstractEventLoop, event: dict[str, Any]
    ) -> None:
        """Schedule :meth:`publish` onto ``loop`` from a non-loop thread.

        A closed / stopped loop is swallowed (debug-logged): the process is
        shutting down and there is no subscriber left to receive the event.
        """
        try:
            loop.call_soon_threadsafe(self.publish, event)
        except RuntimeError:
            logger.debug("event bus: publish dropped, event loop unavailable")
