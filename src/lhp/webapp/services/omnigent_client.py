"""Typed async HTTP client for the local Omnigent daemon (v0.4.0 API).

The assistant feature talks to a user-managed Omnigent daemon over loopback
HTTP. This module owns that wire protocol — one shared
:class:`httpx.AsyncClient` per app process, one thin typed coroutine per
endpoint, and SSE parsing for the live session stream. It holds no domain
logic and no approval/session semantics: callers interpret the payloads.

:class:`OmnigentClient` carries 14 public methods — one per pinned endpoint
below (§3.2 justification: the class is the single wire-protocol surface;
splitting it would scatter one protocol across modules without removing any
method).

Pinned request/response shapes (verified live against omnigent 0.4.0)
----------------------------------------------------------------------
* ``GET /health`` -> ``{"status": "ok"}``.
* ``GET /v1/info`` -> ``{"server_version", "needs_setup", ...}``.
* ``GET /v1/hosts`` -> ``{"hosts": [{"host_id", "name", "status",
  "configured_harnesses": {...}}]}``; a live host has ``status == "online"``.
* ``POST /v1/sessions`` — multipart with parts ``metadata`` (JSON string:
  ``{title?, workspace?, labels?}``) and ``bundle`` (gzip tarball with
  ``config.yaml`` at its root) -> ``201 {"session_id": "conv_...",
  "agent_id": "ag_...", "agent_name": ...}``. There is NO standalone agent
  registry: session + agent are created together in this one transaction.
* ``POST /v1/hosts/{host_id}/runners`` — JSON ``{"session_id", "workspace"}``
  -> ``200 {"runner_id", "status": "launching"}``. A ``git`` key must never
  appear anywhere in this payload.
* ``POST /v1/sessions/{id}/events`` — user message: ``{"type": "message",
  "data": {"role": "user", "content": [{"type": "input_text", "text":
  ...}]}}`` -> ``202 {"queued": true, "item_id": ...}``; interrupt:
  ``{"type": "interrupt", "data": {}}`` -> ``202 {"queued": false}``.
* ``POST /v1/sessions/{id}/elicitations/{elicitation_id}/resolve`` —
  ``{"action": "accept" | "decline" | "cancel", "content"?}`` -> ``202``.
* ``GET /v1/sessions/{id}?include_items=true`` -> rich snapshot incl.
  enveloped ``items`` (``{id, type, status, response_id, created_at,
  created_by, data: {...}}``), ``status``, ``host_online``,
  ``runner_online``, ``workspace``, ``title``, ``total_cost_usd``,
  ``pending_elicitations``, ``pending_inputs``.
* ``GET /v1/sessions?limit=N`` — list sessions; ``DELETE /v1/sessions/{id}``
  — delete one.
* ``GET /v1/sessions/{id}/stream`` — SSE (``event:`` / ``data:`` lines,
  dispatch on blank line, terminates with ``data: [DONE]``). Live tail only,
  NO replay of earlier items.

Error contract
--------------
Transport-level failures (connection refused, timeouts — any
:class:`httpx.TransportError`) are wrapped in :exc:`OmnigentUnavailable`
(``raise ... from e``). HTTP error statuses propagate as
:class:`httpx.HTTPStatusError`. Neither is an ``LHPError`` and neither may
cross the webapp boundary untranslated — callers translate to their own
error surface.

:stability: internal
"""

from __future__ import annotations

import json
import logging
from typing import Any, AsyncIterator, Literal, Optional, cast

import httpx
from fastapi import FastAPI

logger = logging.getLogger(__name__)

#: Non-stream requests: 5s to connect, 10s everywhere else.
_DEFAULT_TIMEOUT = httpx.Timeout(10.0, connect=5.0)

#: The live SSE tail has NO read deadline — omnigent's ``session.heartbeat``
#: frames keep the connection demonstrably alive between real events.
_STREAM_TIMEOUT = httpx.Timeout(10.0, connect=5.0, read=None)

#: SSE data sentinel that cleanly terminates the session stream.
_STREAM_DONE = "[DONE]"


class OmnigentUnavailable(Exception):
    """The omnigent daemon could not be reached (connect error or timeout).

    Module-internal by design: this is webapp-adapter transport plumbing, not
    a domain concept, so it does not live in ``lhp/errors`` and is NOT an
    ``LHPError`` (§2.2 governs domain exceptions only). It must never cross
    the webapp boundary untranslated — callers catch it and translate (e.g.
    to an HTTP 503 with daemon-setup guidance).
    """


def _json_object(response: httpx.Response) -> dict[str, Any]:
    """Parse a response body pinned by the protocol to be a JSON object.

    A ``cast``, not runtime validation: the shapes are pinned in the module
    docstring; drift is a server bug surfaced by callers, not silently
    coerced here.
    """
    return cast("dict[str, Any]", response.json())


def _sse_value(line: str, field: str) -> str:
    """Extract an SSE field value, stripping the single optional space."""
    value = line[len(field) + 1 :]
    return value[1:] if value.startswith(" ") else value


def _decode_frame(
    sse_event: Optional[str], data: str
) -> Optional[tuple[str, dict[str, Any]]]:
    """Decode one dispatched SSE frame into ``(event_type, payload)``.

    The event type comes from the payload's ``"type"`` key, falling back to
    the SSE ``event:`` field, then to the SSE default ``"message"``. Non-JSON
    or non-object data frames are dropped (``None``) with a warning — the
    relay swallows unknowns by design.
    """
    try:
        payload = json.loads(data)
    except json.JSONDecodeError:
        logger.warning(f"omnigent stream: dropping non-JSON SSE frame: {data[:120]!r}")
        return None
    if not isinstance(payload, dict):
        logger.warning(
            f"omnigent stream: dropping non-object SSE frame: {data[:120]!r}"
        )
        return None
    event_type = payload.get("type") or sse_event or "message"
    return (str(event_type), payload)


class OmnigentClient:
    """Async wrapper over the omnigent HTTP API on one shared connection pool.

    Canonical construction path (§4.5): :func:`get_omnigent_client`, which
    caches one instance on ``app.state.omnigent_client``. Direct construction
    is the test seam — pass ``http_client`` built over
    ``httpx.ASGITransport(app=stub_app)`` to drive an in-process stub server.

    Every method is a thin coroutine over one pinned endpoint (module
    docstring). Transport failures raise :exc:`OmnigentUnavailable`; HTTP
    error statuses raise :class:`httpx.HTTPStatusError`.
    """

    def __init__(
        self, base_url: str, *, http_client: Optional[httpx.AsyncClient] = None
    ) -> None:
        self._base_url = base_url
        self._client = (
            http_client
            if http_client is not None
            else httpx.AsyncClient(base_url=base_url, timeout=_DEFAULT_TIMEOUT)
        )

    async def aclose(self) -> None:
        """Close the underlying connection pool (app shutdown)."""
        await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue one request; wrap transport failures, raise on error status."""
        try:
            response = await self._client.request(method, path, **kwargs)
        except httpx.TransportError as exc:
            raise OmnigentUnavailable(
                f"omnigent daemon unreachable at {self._base_url}: {exc}"
            ) from exc
        response.raise_for_status()
        return response

    async def health(self) -> dict[str, Any]:
        """``GET /health`` -> ``{"status": "ok"}``."""
        return _json_object(await self._request("GET", "/health"))

    async def info(self) -> dict[str, Any]:
        """``GET /v1/info`` -> ``{"server_version", "needs_setup", ...}``."""
        return _json_object(await self._request("GET", "/v1/info"))

    async def hosts(self) -> list[dict[str, Any]]:
        """``GET /v1/hosts`` -> the parsed ``hosts`` list."""
        payload = _json_object(await self._request("GET", "/v1/hosts"))
        return list(payload.get("hosts", []))

    async def session_alive(self, session_id: str) -> bool:
        """Whether ``GET /v1/sessions/{id}`` answers 200 (404 -> ``False``)."""
        try:
            await self._request("GET", f"/v1/sessions/{session_id}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return False
            raise
        return True

    async def create_bundled_session(
        self, tar_bytes: bytes, *, title: str, workspace: str
    ) -> dict[str, Any]:
        """``POST /v1/sessions`` (multipart) -> session + agent in one call.

        ``metadata`` is a JSON-string form part; ``bundle`` is the gzip
        tarball with ``config.yaml`` at its root. Returns
        ``{"session_id", "agent_id", "agent_name"}`` — omnigent 0.4.0 has no
        standalone agent registry, so this one transaction creates both.
        """
        metadata = json.dumps({"title": title, "workspace": workspace})
        response = await self._request(
            "POST",
            "/v1/sessions",
            data={"metadata": metadata},
            files={"bundle": ("bundle.tar.gz", tar_bytes, "application/gzip")},
        )
        return _json_object(response)

    async def launch_runner(
        self, host_id: str, session_id: str, workspace: str
    ) -> dict[str, Any]:
        """``POST /v1/hosts/{host_id}/runners`` -> ``{"runner_id", "status"}``.

        The payload is EXACTLY ``{"session_id", "workspace"}`` — never add a
        ``git`` key (pinned by the live spike against 0.4.0).
        """
        response = await self._request(
            "POST",
            f"/v1/hosts/{host_id}/runners",
            json={"session_id": session_id, "workspace": workspace},
        )
        return _json_object(response)

    async def get_session(
        self, session_id: str, include_items: bool = True
    ) -> dict[str, Any]:
        """``GET /v1/sessions/{id}`` -> rich session snapshot.

        With ``include_items=True`` the snapshot carries the item history,
        each item ENVELOPED as ``{id, type, status, ..., data: {...}}`` —
        unlike the FLAT items on the live stream (renderers must unwrap
        ``data`` for snapshot items).
        """
        response = await self._request(
            "GET",
            f"/v1/sessions/{session_id}",
            params={"include_items": "true" if include_items else "false"},
        )
        return _json_object(response)

    async def delete_session(self, session_id: str) -> None:
        """``DELETE /v1/sessions/{id}``."""
        await self._request("DELETE", f"/v1/sessions/{session_id}")

    async def list_sessions(self, limit: int = 50) -> list[dict[str, Any]]:
        """``GET /v1/sessions?limit=N`` -> the parsed session list.

        The observed 0.4.0 build wraps the list under a ``"sessions"`` key; a
        bare JSON array is accepted defensively.
        """
        response = await self._request("GET", "/v1/sessions", params={"limit": limit})
        payload = response.json()
        if isinstance(payload, dict):
            return list(payload.get("sessions", []))
        return cast("list[dict[str, Any]]", payload)

    async def post_message(self, session_id: str, text: str) -> dict[str, Any]:
        """Queue a user message -> ``202 {"queued": true, "item_id": ...}``."""
        body = {
            "type": "message",
            "data": {
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
            },
        }
        response = await self._request(
            "POST", f"/v1/sessions/{session_id}/events", json=body
        )
        return _json_object(response)

    async def post_interrupt(self, session_id: str) -> dict[str, Any]:
        """Queue an interrupt -> ``202 {"queued": false}``."""
        response = await self._request(
            "POST",
            f"/v1/sessions/{session_id}/events",
            json={"type": "interrupt", "data": {}},
        )
        return _json_object(response)

    async def resolve_elicitation(
        self,
        session_id: str,
        elicitation_id: str,
        action: Literal["accept", "decline", "cancel"],
        content: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Resolve a pending approval -> ``202``.

        ``content`` is included in the payload only when provided.
        """
        body: dict[str, Any] = {"action": action}
        if content is not None:
            body["content"] = content
        response = await self._request(
            "POST",
            f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
            json=body,
        )
        return _json_object(response)

    async def stream_session(
        self, session_id: str
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """``GET /v1/sessions/{id}/stream`` -> live ``(event_type, payload)``.

        Parses the SSE wire format: ``event:`` / ``data:`` field lines
        accumulate until a blank line dispatches the frame (multi-``data:``
        lines are joined with newlines per the SSE spec); ``:`` comment lines
        and unknown fields are ignored. Stops cleanly at ``data: [DONE]``.
        The read timeout is disabled for the tail (heartbeats keep it live);
        connect failures and mid-stream transport drops raise
        :exc:`OmnigentUnavailable`.
        """
        try:
            async with self._client.stream(
                "GET", f"/v1/sessions/{session_id}/stream", timeout=_STREAM_TIMEOUT
            ) as response:
                response.raise_for_status()
                event_field: Optional[str] = None
                data_lines: list[str] = []
                async for line in response.aiter_lines():
                    if line.startswith(":"):
                        continue
                    if line:
                        if line.startswith("data:"):
                            data_lines.append(_sse_value(line, "data"))
                        elif line.startswith("event:"):
                            event_field = _sse_value(line, "event")
                        # id:/retry:/unknown fields are irrelevant here.
                        continue
                    # Blank line: dispatch the accumulated frame.
                    if not data_lines:
                        event_field = None
                        continue
                    data = "\n".join(data_lines)
                    data_lines = []
                    sse_event, event_field = event_field, None
                    if data == _STREAM_DONE:
                        return
                    frame = _decode_frame(sse_event, data)
                    if frame is not None:
                        yield frame
        except httpx.TransportError as exc:
            raise OmnigentUnavailable(
                f"omnigent stream dropped for session {session_id}: {exc}"
            ) from exc


def get_omnigent_client(app: FastAPI) -> OmnigentClient:
    """Return the app-wide shared client, constructing it on first use.

    Lazily caches one instance on ``app.state.omnigent_client`` — one
    connection pool per process, and the seam where tests inject an
    :class:`OmnigentClient` built over ``httpx.ASGITransport``. The base URL
    comes from :func:`~lhp.webapp.services.omnigent_lifecycle.resolve_base_url`
    (``settings.assistant_url`` or the loopback default). The cached client
    is closed via :meth:`OmnigentClient.aclose` at app shutdown.
    """
    cached: Optional[OmnigentClient] = getattr(app.state, "omnigent_client", None)
    if cached is not None:
        return cached
    # Deferred import: omnigent_lifecycle imports this module at module level
    # (OmnigentClient / OmnigentUnavailable), so the reverse dependency is
    # resolved lazily to keep the module-level import graph acyclic.
    from lhp.webapp.services.omnigent_lifecycle import resolve_base_url

    client = OmnigentClient(resolve_base_url(app.state.settings))
    app.state.omnigent_client = client
    return client
