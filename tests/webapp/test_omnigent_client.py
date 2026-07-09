"""Tests for the omnigent HTTP client (``services/omnigent_client.py``).

Self-sufficient: every test drives :class:`OmnigentClient` against an INLINE
FastAPI stub served over ``httpx.ASGITransport`` (no network, no respx, no
new dependencies). ``pytest-asyncio`` is NOT a project dependency, so tests
are plain ``def`` functions driving coroutines through ``asyncio.run`` —
same convention as ``test_stream_adapter.py``.

The multipart stub records the RAW request body and the tests parse it by
hand (``python-multipart`` is not a dependency either), asserting both part
names arrive and the bundle bytes survive intact.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, TypeVar

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from lhp.webapp.services.omnigent_client import (
    OmnigentClient,
    OmnigentUnavailable,
    get_omnigent_client,
)

pytestmark = pytest.mark.webapp

T = TypeVar("T")

_BASE_URL = "http://127.0.0.1:6767"

# Fake gzip tarball (magic bytes + filler). Deliberately free of trailing
# CR/LF so the hand-rolled multipart parser cannot clip legitimate bytes.
_TAR_BYTES = b"\x1f\x8b\x08\x00fake-bundle\x00config.yaml\x00payload-end"

# One SSE body exercising: comment lines, payload-carried type, fallback to
# the SSE ``event:`` field, multi-line data accumulation (SSE default event
# name), a dropped non-JSON frame, [DONE] termination, and a frame AFTER
# [DONE] that must never surface. The stream TERMINATES.
_SSE_BODY = (
    b": ping comment\n"
    b"event: response.delta\n"
    b'data: {"type": "response.output_text.delta", "delta": "Hello"}\n'
    b"\n"
    b"event: session.status\n"
    b'data: {"status": "idle"}\n'
    b"\n"
    b'data: {"multi":\n'
    b'data: "line"}\n'
    b"\n"
    b"data: not-json\n"
    b"\n"
    b"data: [DONE]\n"
    b"\n"
    b'data: {"type": "after.done"}\n'
    b"\n"
)


def _build_stub() -> tuple[FastAPI, dict[str, Any]]:
    """Inline omnigent 0.4.0 stub; ``recorded`` captures what it received."""
    recorded: dict[str, Any] = {}
    app = FastAPI()

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/info")
    async def info() -> dict[str, Any]:
        return {"server_version": "0.4.0", "needs_setup": False}

    @app.get("/v1/hosts")
    async def hosts() -> dict[str, Any]:
        return {
            "hosts": [
                {
                    "host_id": "h1",
                    "name": "mac",
                    "status": "online",
                    "configured_harnesses": {"claude_sdk": True},
                }
            ]
        }

    @app.post("/v1/sessions", status_code=201)
    async def create_session(request: Request) -> dict[str, str]:
        recorded["create_content_type"] = request.headers.get("content-type", "")
        recorded["create_body"] = await request.body()
        return {"session_id": "conv_1", "agent_id": "ag_1", "agent_name": "lhp"}

    @app.get("/v1/sessions")
    async def list_sessions(limit: int = 50) -> dict[str, Any]:
        recorded["list_limit"] = limit
        return {"sessions": [{"session_id": "conv_1"}, {"session_id": "conv_2"}]}

    @app.get("/v1/sessions/{session_id}")
    async def get_session(session_id: str, include_items: bool = False) -> Any:
        if session_id == "conv_missing":
            return JSONResponse(status_code=404, content={"detail": "not found"})
        recorded["snapshot_include_items"] = include_items
        return {"session_id": session_id, "status": "idle", "items": []}

    @app.delete("/v1/sessions/{session_id}")
    async def delete_session(session_id: str) -> dict[str, bool]:
        recorded["deleted"] = session_id
        return {"deleted": True}

    @app.post("/v1/sessions/{session_id}/events", status_code=202)
    async def post_event(session_id: str, request: Request) -> dict[str, Any]:
        payload = await request.json()
        recorded.setdefault("events", []).append((session_id, payload))
        if payload.get("type") == "interrupt":
            return {"queued": False}
        return {"queued": True, "item_id": "msg_001"}

    @app.post(
        "/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        status_code=202,
    )
    async def resolve(
        session_id: str, elicitation_id: str, request: Request
    ) -> dict[str, bool]:
        recorded.setdefault("resolves", []).append(
            (session_id, elicitation_id, await request.json())
        )
        return {"ok": True}

    @app.post("/v1/hosts/{host_id}/runners")
    async def launch_runner(host_id: str, request: Request) -> dict[str, str]:
        recorded["runner_host_id"] = host_id
        recorded["runner_body"] = await request.json()
        return {"runner_id": "run_1", "status": "launching"}

    @app.get("/v1/sessions/{session_id}/stream")
    async def stream(session_id: str) -> StreamingResponse:
        async def body() -> AsyncIterator[bytes]:
            yield _SSE_BODY

        return StreamingResponse(body(), media_type="text/event-stream")

    return app, recorded


def _client_over(app: FastAPI) -> OmnigentClient:
    http_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=_BASE_URL
    )
    return OmnigentClient(_BASE_URL, http_client=http_client)


def _run(scenario: Callable[[OmnigentClient], Awaitable[T]], app: FastAPI) -> T:
    """Run one async scenario against a stub-backed client, closing it after."""

    async def wrapper() -> T:
        client = _client_over(app)
        try:
            return await scenario(client)
        finally:
            await client.aclose()

    return asyncio.run(wrapper())


def _multipart_parts(body: bytes, content_type: str) -> dict[str, bytes]:
    """Hand-rolled multipart parser: part name -> raw payload bytes."""
    assert content_type.startswith("multipart/form-data"), content_type
    boundary = content_type.split("boundary=", 1)[1].encode("ascii")
    parts: dict[str, bytes] = {}
    for chunk in body.split(b"--" + boundary)[1:-1]:
        chunk = chunk.strip(b"\r\n")
        headers, _, payload = chunk.partition(b"\r\n\r\n")
        match = re.search(rb'name="([^"]+)"', headers)
        assert match is not None, f"multipart part without a name: {headers!r}"
        parts[match.group(1).decode("ascii")] = payload
    return parts


def test_health_info_hosts_parse_pinned_shapes() -> None:
    app, _ = _build_stub()

    async def scenario(client: OmnigentClient) -> None:
        assert await client.health() == {"status": "ok"}
        info = await client.info()
        assert info["server_version"] == "0.4.0"
        hosts = await client.hosts()
        assert isinstance(hosts, list)
        assert hosts[0]["host_id"] == "h1"
        assert hosts[0]["status"] == "online"

    _run(scenario, app)


def test_create_bundled_session_sends_metadata_and_bundle_parts() -> None:
    app, recorded = _build_stub()

    async def scenario(client: OmnigentClient) -> dict[str, Any]:
        return await client.create_bundled_session(
            _TAR_BYTES, title="LHP Assistant", workspace="/tmp/proj"
        )

    result = _run(scenario, app)
    assert result == {"session_id": "conv_1", "agent_id": "ag_1", "agent_name": "lhp"}

    body = recorded["create_body"]
    parts = _multipart_parts(body, recorded["create_content_type"])
    assert set(parts) == {"metadata", "bundle"}
    assert json.loads(parts["metadata"]) == {
        "title": "LHP Assistant",
        "workspace": "/tmp/proj",
    }
    # Config bytes intact — both in the parsed part and in the raw body.
    assert parts["bundle"] == _TAR_BYTES
    assert _TAR_BYTES in body


def test_launch_runner_payload_has_exactly_session_and_workspace() -> None:
    app, recorded = _build_stub()

    async def scenario(client: OmnigentClient) -> dict[str, Any]:
        return await client.launch_runner("h1", "conv_1", "/tmp/proj")

    result = _run(scenario, app)
    assert result == {"runner_id": "run_1", "status": "launching"}
    assert recorded["runner_host_id"] == "h1"
    # Pinned: exactly these two keys — in particular NO "git" key, ever.
    assert recorded["runner_body"] == {"session_id": "conv_1", "workspace": "/tmp/proj"}


def test_get_session_requests_items_and_session_alive_maps_404() -> None:
    app, recorded = _build_stub()

    async def scenario(client: OmnigentClient) -> None:
        snapshot = await client.get_session("conv_1")
        assert snapshot["session_id"] == "conv_1"
        assert recorded["snapshot_include_items"] is True
        await client.get_session("conv_1", include_items=False)
        assert recorded["snapshot_include_items"] is False
        assert await client.session_alive("conv_1") is True
        assert await client.session_alive("conv_missing") is False

    _run(scenario, app)


def test_delete_and_list_sessions() -> None:
    app, recorded = _build_stub()

    async def scenario(client: OmnigentClient) -> list[dict[str, Any]]:
        await client.delete_session("conv_1")
        return await client.list_sessions(limit=7)

    sessions = _run(scenario, app)
    assert recorded["deleted"] == "conv_1"
    assert recorded["list_limit"] == 7
    assert [s["session_id"] for s in sessions] == ["conv_1", "conv_2"]


def test_post_message_payload_shape() -> None:
    app, recorded = _build_stub()

    async def scenario(client: OmnigentClient) -> dict[str, Any]:
        return await client.post_message("conv_1", "hello agent")

    result = _run(scenario, app)
    assert result == {"queued": True, "item_id": "msg_001"}
    assert recorded["events"] == [
        (
            "conv_1",
            {
                "type": "message",
                "data": {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hello agent"}],
                },
            },
        )
    ]


def test_post_interrupt_payload_shape() -> None:
    app, recorded = _build_stub()

    async def scenario(client: OmnigentClient) -> dict[str, Any]:
        return await client.post_interrupt("conv_1")

    result = _run(scenario, app)
    assert result == {"queued": False}
    assert recorded["events"] == [("conv_1", {"type": "interrupt", "data": {}})]


def test_resolve_elicitation_payload_with_and_without_content() -> None:
    app, recorded = _build_stub()

    async def scenario(client: OmnigentClient) -> None:
        await client.resolve_elicitation("conv_1", "elc_1", "accept")
        await client.resolve_elicitation(
            "conv_1", "elc_2", "decline", content={"reason": "no"}
        )

    _run(scenario, app)
    assert recorded["resolves"] == [
        ("conv_1", "elc_1", {"action": "accept"}),
        ("conv_1", "elc_2", {"action": "decline", "content": {"reason": "no"}}),
    ]


def test_stream_session_yields_typed_frames_in_order_and_stops_at_done() -> None:
    app, _ = _build_stub()

    async def scenario(client: OmnigentClient) -> list[tuple[str, dict[str, Any]]]:
        frames = []
        async for frame in client.stream_session("conv_1"):
            frames.append(frame)
        return frames

    frames = _run(scenario, app)
    assert frames == [
        # Type from the payload's "type" key (beats the SSE event field).
        (
            "response.output_text.delta",
            {"type": "response.output_text.delta", "delta": "Hello"},
        ),
        # No "type" in the payload -> falls back to the SSE ``event:`` field.
        ("session.status", {"status": "idle"}),
        # Multi-line data joined per the SSE spec; no event field and no
        # payload type -> the SSE default "message". The non-JSON frame is
        # dropped and nothing after [DONE] surfaces.
        ("message", {"multi": "line"}),
    ]


@pytest.mark.parametrize(
    "exc_type", [httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout]
)
def test_transport_failures_wrap_in_omnigent_unavailable(
    exc_type: type[httpx.TransportError],
) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise exc_type("daemon down")

    async def scenario() -> None:
        client = OmnigentClient(
            _BASE_URL,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(refuse), base_url=_BASE_URL
            ),
        )
        try:
            with pytest.raises(OmnigentUnavailable) as excinfo:
                await client.health()
            assert isinstance(excinfo.value.__cause__, exc_type)
        finally:
            await client.aclose()

    asyncio.run(scenario())


def test_http_status_errors_propagate_untranslated() -> None:
    """5xx is NOT OmnigentUnavailable — callers translate status errors."""
    app = FastAPI()

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": "boom"})

    async def scenario(client: OmnigentClient) -> None:
        with pytest.raises(httpx.HTTPStatusError):
            await client.health()

    _run(scenario, app)


class _StubSettings:
    """Duck-typed stand-in for WebappSettings (only assistant_url is read)."""

    def __init__(self, assistant_url: Optional[str] = None) -> None:
        self.assistant_url = assistant_url


def test_get_omnigent_client_constructs_once_and_caches_on_app_state() -> None:
    app = FastAPI()
    app.state.settings = _StubSettings(assistant_url=None)

    client = get_omnigent_client(app)
    try:
        assert isinstance(client, OmnigentClient)
        assert get_omnigent_client(app) is client
        assert app.state.omnigent_client is client
    finally:
        asyncio.run(client.aclose())


def test_get_omnigent_client_returns_injected_instance() -> None:
    """app.state is the test-injection seam: a pre-set client short-circuits."""
    stub_app, _ = _build_stub()
    app = FastAPI()  # no settings on state — the cache hit must not need them
    injected = _client_over(stub_app)
    app.state.omnigent_client = injected
    try:
        assert get_omnigent_client(app) is injected
    finally:
        asyncio.run(injected.aclose())
