"""Unit tests for the webapp asyncio event bus (fan-out, drops, threadsafe)."""

from __future__ import annotations

import asyncio
import threading

import pytest

from lhp.webapp.services.event_bus import EventBus

pytestmark = pytest.mark.webapp


def _event(name: str = "run-updated") -> dict[str, object]:
    return {"event": name, "data": {"run_id": "r1"}}


def test_subscriber_receives_published_event() -> None:
    bus = EventBus()
    queue = bus.subscribe()

    event = _event()
    bus.publish(event)

    assert queue.get_nowait() is event


def test_two_subscribers_both_receive() -> None:
    bus = EventBus()
    first = bus.subscribe()
    second = bus.subscribe()

    event = _event()
    bus.publish(event)

    assert first.get_nowait() is event
    assert second.get_nowait() is event


def test_unsubscribed_queue_stops_receiving() -> None:
    bus = EventBus()
    queue = bus.subscribe()
    bus.unsubscribe(queue)

    bus.publish(_event())

    assert queue.empty()


def test_unsubscribe_unknown_queue_is_noop() -> None:
    bus = EventBus()
    bus.unsubscribe(asyncio.Queue())


def test_full_queue_drops_without_raising() -> None:
    bus = EventBus(maxsize=2)
    slow = bus.subscribe()
    healthy = bus.subscribe()
    bus.publish(_event("one"))
    bus.publish(_event("two"))
    # Drain only the healthy subscriber so the slow one is left full.
    healthy.get_nowait()
    healthy.get_nowait()

    bus.publish(_event("three"))

    # The slow subscriber missed the third event; the healthy one got it.
    assert slow.qsize() == 2
    assert healthy.get_nowait()["event"] == "three"


def test_publish_with_no_subscribers_is_noop() -> None:
    EventBus().publish(_event())


def test_publish_threadsafe_from_worker_thread() -> None:
    async def scenario() -> None:
        bus = EventBus()
        queue = bus.subscribe()
        loop = asyncio.get_running_loop()

        worker = threading.Thread(
            target=bus.publish_threadsafe, args=(loop, _event("cross-thread"))
        )
        worker.start()
        worker.join()

        event = await asyncio.wait_for(queue.get(), timeout=5)
        assert event["event"] == "cross-thread"

    asyncio.run(scenario())
