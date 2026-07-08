"""Tests for ``GET /api/events`` — the live-update SSE channel.

Starlette's ``TestClient`` buffers a response's ENTIRE body before returning,
so an endless SSE stream can never be exercised through ``client.stream``
(the request would simply never complete). These tests therefore drive the
raw ASGI callable directly via :class:`_SSEConnection`: the full app —
middleware stack included — runs as an asyncio task on the test's own loop,
each ``http.response.body`` chunk is observed as it is produced, and the
request ends by delivering ``http.disconnect`` exactly as a closing browser
would. Running on the test loop also makes publishing deterministic: the
test coroutine calls ``event_bus.publish(...)`` directly, no thread handoff.

Determinism: every streaming test monkeypatches
:data:`lhp.webapp.routers.events.HEARTBEAT_SECONDS` down to 0.05s and reads
one heartbeat frame before publishing — the heartbeat proves the stream
generator has subscribed to the bus, so a subsequent publish cannot be lost
to the subscribe race. All queue reads are bounded by short timeouts.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app
from lhp.webapp.routers import events as events_module

from .conftest import LOOPBACK_BASE_URL

pytestmark = pytest.mark.webapp

_READ_TIMEOUT = 2.0
_FAST_HEARTBEAT = 0.05
_TEST_TOKEN = "sse-test-session-token-not-a-secret"


def _build_app(
    monkeypatch: pytest.MonkeyPatch, project_root: Path, token: str | None = None
) -> FastAPI:
    """Build ``create_app()`` against ``project_root`` with a clean env."""
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(project_root))
    for var in ("LHP_WEBAPP_PORT", "LHP_WEBAPP_LOG_LEVEL", "LHP_WEBAPP_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    if token is not None:
        monkeypatch.setenv("LHP_WEBAPP_TOKEN", token)
    return create_app()


class _SSEConnection:
    """Drive one streaming GET against the raw ASGI app, chunk by chunk."""

    def __init__(
        self, app: FastAPI, path: str = "/api/events", query: str = ""
    ) -> None:
        self._app = app
        self._scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query.encode(),
            "root_path": "",
            # Loopback Host so TrustedHostMiddleware admits the request.
            "headers": [(b"host", b"127.0.0.1")],
            "client": ("127.0.0.1", 50000),
            "server": ("127.0.0.1", 80),
            "state": {},
        }
        self._messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._disconnected = asyncio.Event()
        self._request_sent = False
        self._task: asyncio.Task[None] | None = None

    async def _receive(self) -> dict[str, Any]:
        if not self._request_sent:
            self._request_sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await self._disconnected.wait()
        return {"type": "http.disconnect"}

    async def _send(self, message: dict[str, Any]) -> None:
        await self._messages.put(message)

    async def __aenter__(self) -> "_SSEConnection":
        asgi = self._app  # FastAPI instance IS the ASGI callable
        self._task = asyncio.create_task(asgi(self._scope, self._receive, self._send))
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._disconnected.set()
        assert self._task is not None
        # The app task must terminate once the disconnect is delivered — a
        # hang here means the stream generator never observed the disconnect.
        await asyncio.wait_for(self._task, timeout=_READ_TIMEOUT)

    async def start(self) -> tuple[int, dict[str, str]]:
        """Await ``http.response.start``; return (status, lower-cased headers)."""
        message = await asyncio.wait_for(self._messages.get(), timeout=_READ_TIMEOUT)
        assert message["type"] == "http.response.start", message
        headers = {k.decode().lower(): v.decode() for k, v in message["headers"]}
        return message["status"], headers

    async def next_chunk(self) -> bytes:
        """Await the next ``http.response.body`` chunk's bytes."""
        message = await asyncio.wait_for(self._messages.get(), timeout=_READ_TIMEOUT)
        assert message["type"] == "http.response.body", message
        return message.get("body", b"")

    async def next_event_frame(self) -> bytes:
        """Await the next NON-heartbeat frame (skips ``: ping`` comments)."""
        while True:
            chunk = await self.next_chunk()
            if not chunk.startswith(b": ping"):
                return chunk


def _run(scenario: Callable[[], Awaitable[None]]) -> None:
    """Run an async test scenario on a fresh event loop (no pytest-asyncio)."""
    asyncio.run(scenario())


def test_stream_status_headers_and_media_type(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """200 + text/event-stream + no-cache + X-Accel-Buffering: no."""
    monkeypatch.setattr(events_module, "HEARTBEAT_SECONDS", _FAST_HEARTBEAT)
    app = _build_app(monkeypatch, e2e_project_path)

    async def scenario() -> None:
        async with _SSEConnection(app) as conn:
            status, headers = await conn.start()
            assert status == 200
            assert headers["content-type"].startswith("text/event-stream")
            assert headers["cache-control"] == "no-cache"
            assert headers["x-accel-buffering"] == "no"

    _run(scenario)


def test_heartbeat_comment_on_idle_stream(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With nothing published, the stream emits ``: ping`` comment frames."""
    monkeypatch.setattr(events_module, "HEARTBEAT_SECONDS", _FAST_HEARTBEAT)
    app = _build_app(monkeypatch, e2e_project_path)

    async def scenario() -> None:
        async with _SSEConnection(app) as conn:
            await conn.start()
            assert await conn.next_chunk() == b": ping\n\n"
            assert await conn.next_chunk() == b": ping\n\n"

    _run(scenario)


def test_file_changed_and_run_updated_framing(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Published bus events arrive as exact ``event:``/``data:`` SSE frames."""
    monkeypatch.setattr(events_module, "HEARTBEAT_SECONDS", _FAST_HEARTBEAT)
    app = _build_app(monkeypatch, e2e_project_path)
    bus = app.state.event_bus

    async def scenario() -> None:
        async with _SSEConnection(app) as conn:
            await conn.start()
            # First heartbeat proves the generator has subscribed; only then
            # is a publish guaranteed to reach this connection.
            assert await conn.next_chunk() == b": ping\n\n"

            bus.publish(
                {
                    "event": "file-changed",
                    "data": {"paths": ["pipelines/customer.yaml"]},
                }
            )
            assert await conn.next_event_frame() == (
                b'event: file-changed\ndata: {"paths":["pipelines/customer.yaml"]}\n\n'
            )

            bus.publish(
                {
                    "event": "run-updated",
                    "data": {
                        "run_id": "r-1",
                        "kind": "generate",
                        "status": "succeeded",
                    },
                }
            )
            assert await conn.next_event_frame() == (
                b"event: run-updated\n"
                b'data: {"run_id":"r-1","kind":"generate","status":"succeeded"}\n\n'
            )

    _run(scenario)


def test_disconnect_unsubscribes_from_bus(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Closing the stream returns the bus subscriber count to zero."""
    monkeypatch.setattr(events_module, "HEARTBEAT_SECONDS", _FAST_HEARTBEAT)
    app = _build_app(monkeypatch, e2e_project_path)
    bus = app.state.event_bus
    assert bus.subscriber_count == 0

    async def scenario() -> None:
        async with _SSEConnection(app) as conn:
            await conn.start()
            assert await conn.next_chunk() == b": ping\n\n"
            assert bus.subscriber_count == 1
        # __aexit__ awaited the app task, so the finally-unsubscribe has run.
        assert bus.subscriber_count == 0

    _run(scenario)
    assert bus.subscriber_count == 0


def test_events_requires_token_when_configured(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With a session token armed, a token-less /api/events is 401 (no stream)."""
    app = _build_app(monkeypatch, e2e_project_path, token=_TEST_TOKEN)
    client = TestClient(app, base_url=LOOPBACK_BASE_URL)
    # Rejected by TokenGuardMiddleware before any streaming starts, so the
    # buffered TestClient is safe to use here.
    response = client.get("/api/events")
    assert response.status_code == 401
    assert response.json() == {"detail": "invalid or missing session token"}


def test_events_accepts_query_token(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ``?token=`` query parameter opens the stream (EventSource has no headers)."""
    monkeypatch.setattr(events_module, "HEARTBEAT_SECONDS", _FAST_HEARTBEAT)
    app = _build_app(monkeypatch, e2e_project_path, token=_TEST_TOKEN)

    async def scenario() -> None:
        async with _SSEConnection(app, query=f"token={_TEST_TOKEN}") as conn:
            status, headers = await conn.start()
            assert status == 200
            assert headers["content-type"].startswith("text/event-stream")
            assert await conn.next_chunk() == b": ping\n\n"

    _run(scenario)
