"""Scripted in-process stub of the omnigent 0.4.0 HTTP surface.

Backs the assistant provisioning / chat-relay tests: an inline FastAPI app
served to :class:`~lhp.webapp.services.omnigent_client.OmnigentClient` over
``httpx.ASGITransport`` (no network, no new dependencies). The multipart
``POST /v1/sessions`` body is parsed BY HAND — ``python-multipart`` is not a
project dependency (same approach as ``test_omnigent_client.py``).

Every handler records ``(method, path)`` into ``calls`` in arrival order, so
tests can assert protocol ordering (e.g. the stream subscription GET happens
BEFORE the message POST).

The SSE stream endpoint serves the NEXT script from ``stream_scripts``
(one list of ``(event_type, payload)`` tuples per expected stream GET) and
ALWAYS terminates with ``data: [DONE]`` unless the script says otherwise via
the sentinels below. NOTE: httpx's ``ASGITransport`` runs the whole ASGI app
body eagerly and buffers it, so scripts are fully pre-scripted — anything
that must block mid-turn blocks in the EVENTS handler (``event_gate``), not
in the stream body.

Script sentinels (first tuple element):

* ``RAISE`` — the body raises the given exception (a transport-level drop:
  raise an ``httpx.TransportError`` subclass so the client wraps it in
  ``OmnigentUnavailable``).
* ``DROP`` — the body ends immediately WITHOUT ``data: [DONE]`` (an aborted
  stream).
* ``BLOCK`` — the body sleeps for the given number of seconds BEFORE any
  event (a subscription that never confirms). The sleep is normally cut
  short by the client's first-event-timeout cancellation; the ceiling
  guarantees the script terminates even if cancellation never arrives.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, AsyncIterator, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from lhp.webapp.services.omnigent_client import OmnigentClient

BASE_URL = "http://127.0.0.1:6767"

RAISE = "__raise__"
DROP = "__drop__"
BLOCK = "__block__"

#: A scripted stream entry: ``(event_type, payload)`` or a sentinel tuple.
ScriptItem = tuple[Any, ...]


def parse_multipart(body: bytes, content_type: str) -> dict[str, bytes]:
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


class OmnigentStub:
    """Programmable omnigent daemon double; one instance per test.

    Attributes tests programme / assert on:

    * ``calls`` — ordered ``(method, path)`` of every request received.
    * ``stream_scripts`` — queue of scripts; each stream GET consumes one.
    * ``alive_sessions`` — session ids answering 200 on the snapshot GET
      (ids minted by the create endpoint are added automatically).
    * ``created`` — per create: parsed ``metadata`` dict, raw ``bundle``
      bytes, and the minted ``session_id``.
    * ``runners`` — ``(host_id, body)`` per runner launch.
    * ``events`` — ``(session_id, payload)`` per events POST.
    * ``resolves`` — ``(session_id, elicitation_id, body)`` per resolve.
    * ``message_posted`` — set when the first message-type event arrives.
    * ``event_gate`` — when not ``None``, the events handler BLOCKS on it
      after recording a message (the mid-turn hook for concurrency tests).
    """

    def __init__(self, host_id: str = "h1", host_status: str = "online") -> None:
        self.host_id = host_id
        self.host_status = host_status
        self.calls: list[tuple[str, str]] = []
        self.stream_scripts: list[list[ScriptItem]] = []
        self.alive_sessions: set[str] = set()
        self.created: list[dict[str, Any]] = []
        self.runners: list[tuple[str, dict[str, Any]]] = []
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.resolves: list[tuple[str, str, dict[str, Any]]] = []
        self.message_posted = asyncio.Event()
        self.event_gate: Optional[asyncio.Event] = None
        self._session_seq = 0
        self.app = self._build_app()

    def _record(self, method: str, path: str) -> None:
        self.calls.append((method, path))

    def _build_app(self) -> FastAPI:
        app = FastAPI()

        @app.get("/health")
        async def health() -> dict[str, str]:
            self._record("GET", "/health")
            return {"status": "ok"}

        @app.get("/v1/info")
        async def info() -> dict[str, Any]:
            self._record("GET", "/v1/info")
            return {"server_version": "0.4.0", "needs_setup": False}

        @app.get("/v1/hosts")
        async def hosts() -> dict[str, Any]:
            self._record("GET", "/v1/hosts")
            return {
                "hosts": [
                    {
                        "host_id": self.host_id,
                        "name": "stub",
                        "status": self.host_status,
                        "configured_harnesses": {"claude_sdk": True},
                    }
                ]
            }

        @app.post("/v1/sessions", status_code=201)
        async def create_session(request: Request) -> dict[str, str]:
            self._record("POST", "/v1/sessions")
            parts = parse_multipart(
                await request.body(), request.headers.get("content-type", "")
            )
            self._session_seq += 1
            session_id = f"conv_{self._session_seq}"
            self.created.append(
                {
                    "metadata": json.loads(parts["metadata"].decode("utf-8")),
                    "bundle": parts["bundle"],
                    "session_id": session_id,
                }
            )
            self.alive_sessions.add(session_id)
            return {
                "session_id": session_id,
                "agent_id": f"ag_{self._session_seq}",
                "agent_name": "lhp-stub",
            }

        @app.post("/v1/hosts/{host_id}/runners")
        async def launch_runner(host_id: str, request: Request) -> dict[str, str]:
            self._record("POST", f"/v1/hosts/{host_id}/runners")
            self.runners.append((host_id, await request.json()))
            return {"runner_id": f"run_{len(self.runners)}", "status": "launching"}

        @app.get("/v1/sessions/{session_id}")
        async def get_session(session_id: str) -> Any:
            self._record("GET", f"/v1/sessions/{session_id}")
            if session_id not in self.alive_sessions:
                return JSONResponse(status_code=404, content={"detail": "not found"})
            return {"session_id": session_id, "status": "idle", "items": []}

        @app.post("/v1/sessions/{session_id}/events", status_code=202)
        async def post_event(session_id: str, request: Request) -> dict[str, Any]:
            self._record("POST", f"/v1/sessions/{session_id}/events")
            payload = await request.json()
            self.events.append((session_id, payload))
            if payload.get("type") == "message":
                self.message_posted.set()
                if self.event_gate is not None:
                    await self.event_gate.wait()
                return {"queued": True, "item_id": f"msg_{len(self.events)}"}
            return {"queued": False}

        @app.post(
            "/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
            status_code=202,
        )
        async def resolve(
            session_id: str, elicitation_id: str, request: Request
        ) -> dict[str, bool]:
            self._record(
                "POST",
                f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
            )
            self.resolves.append((session_id, elicitation_id, await request.json()))
            return {"ok": True}

        @app.get("/v1/sessions/{session_id}/stream")
        async def stream(session_id: str) -> StreamingResponse:
            self._record("GET", f"/v1/sessions/{session_id}/stream")
            assert self.stream_scripts, (
                f"no stream script queued for session {session_id}"
            )
            script = self.stream_scripts.pop(0)

            async def body() -> AsyncIterator[bytes]:
                emit_done = True
                for item in script:
                    if item[0] == RAISE:
                        raise item[1]
                    if item[0] == DROP:
                        emit_done = False
                        break
                    if item[0] == BLOCK:
                        # Cut short by cancellation (first-event timeout);
                        # the ceiling guarantees termination regardless.
                        await asyncio.sleep(item[1])
                        emit_done = False
                        break
                    event_type, payload = item
                    frame = {"type": event_type, **payload}
                    data = json.dumps(frame, separators=(",", ":"))
                    yield f"data: {data}\n\n".encode("utf-8")
                if emit_done:
                    yield b"data: [DONE]\n\n"

            return StreamingResponse(body(), media_type="text/event-stream")

        return app


def make_client(stub: OmnigentStub) -> OmnigentClient:
    """OmnigentClient wired to the stub over an in-process ASGI transport."""
    http_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=stub.app), base_url=BASE_URL
    )
    return OmnigentClient(BASE_URL, http_client=http_client)
