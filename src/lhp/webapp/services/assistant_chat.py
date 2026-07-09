"""Relay one assistant chat turn as an NDJSON byte stream.

:func:`chat_turn` owns the full turn: ensure a live session
(:func:`~lhp.webapp.services.assistant_provision.ensure_session`),
SUBSCRIBE-FIRST to the omnigent SSE stream (there is NO replay — the
subscription must be live before the message is posted), post the user
message, then translate omnigent events into the pinned NDJSON frame
vocabulary below until a terminal event ends the turn. Frames use the same
compact JSON encoding as :mod:`~lhp.webapp.services.stream_adapter`.

Pinned frame vocabulary (the frontend types are built against EXACTLY this):

============================================= =====================================================
omnigent event                                NDJSON frame
============================================= =====================================================
response.output_text.delta                    ``{"type":"text.delta","delta":...}``
response.reasoning_text.delta /               ``{"type":"reasoning.delta","delta":...}``
response.reasoning_summary_text.delta
response.output_item.done                     ``{"type":"item.done","item":{...}}`` (flat
                                              passthrough of ``payload["item"]``)
response.elicitation_request                  ``{"type":"approval.request","elicitation_id":...,
                                              "params":{...}}``
response.created / .queued / .in_progress     ``{"type":"status","state":"running"}`` — coalesced:
                                              at most once per turn
response.completed                            ``{"type":"turn.completed"}`` then END
response.failed                               ``{"type":"turn.failed","reason":...}`` then END
response.incomplete                           reason ``user_interrupt`` ->
                                              ``{"type":"interrupted"}``; else
                                              ``{"type":"turn.failed","reason":...}``; then END
response.cancelled                            ``{"type":"interrupted"}`` then END (this omnigent
                                              build interrupts via cancelled + session.interrupted)
session.interrupted                           ``{"type":"interrupted"}`` (does NOT end; the
                                              ``response.*`` terminal ends the turn)
session.status with status == "failed"        ``{"type":"session.failed","detail":...,"hint":...}``
                                              then END — ``status`` / ``error`` are TOP-LEVEL
                                              payload keys (spike-verified); hint mapping is
                                              case-insensitive on ``error.message``: contains
                                              "omnigent setup" -> ``omnigent_setup``; contains
                                              "databricks" or "auth" -> ``databricks_auth``;
                                              else ``unknown``
response.heartbeat                            ``{"type":"heartbeat"}``
response.retry and ALL unknown events         swallowed (``logger.debug``)
OmnigentUnavailable / httpx error             the pinned ``LHP-GEN-902`` terminal error frame
                                              (same shape as stream_adapter's unexpected-error
                                              frame; code reused, never minted anew) then END
============================================= =====================================================

Before the table applies, every turn emits ``{"type":"status","state":
"preparing"}`` and then ``{"type":"session","session_id":...,
"created":bool}``.

Turn serialization: a module-level ``asyncio.Lock`` admits ONE turn at a
time for the whole turn; a second concurrent caller waits (single-user
local server, same posture as stream_adapter's run lock).

Client disconnect (``GeneratorExit``): the omnigent stream is closed and NO
interrupt is posted — the turn continues server-side and session
rehydration shows its result. The best-effort ``touch_session`` bookkeeping
is skipped on that path.

Approvals / interrupts: the router calls
:meth:`~lhp.webapp.services.omnigent_client.OmnigentClient.resolve_elicitation`
and :meth:`~lhp.webapp.services.omnigent_client.OmnigentClient.post_interrupt`
directly — the client surface already matches the wire protocol one-to-one,
so this module adds no pass-through glue.

:stability: internal
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, AsyncGenerator, AsyncIterator, cast

import httpx

from lhp.errors import ErrorFactory, codes
from lhp.webapp.services import assistant_store
from lhp.webapp.services.assistant_provision import ensure_session
from lhp.webapp.services.omnigent_client import OmnigentClient, OmnigentUnavailable
from lhp.webapp.services.stream_adapter import _error_frame

logger = logging.getLogger(__name__)

#: Whole-turn gate: one chat turn at a time; concurrent callers WAIT.
_turn_lock = asyncio.Lock()

#: How long to wait for the FIRST stream event confirming the subscription
#: is live before the message may be posted (no replay exists).
_FIRST_EVENT_TIMEOUT_S = 10.0

_REASONING_DELTA_EVENTS = frozenset(
    {"response.reasoning_text.delta", "response.reasoning_summary_text.delta"}
)
_RUNNING_EVENTS = frozenset(
    {"response.created", "response.queued", "response.in_progress"}
)


def _encode(frame: dict[str, Any]) -> bytes:
    """One NDJSON line — the same compact encoding stream_adapter emits."""
    return json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"


def _daemon_lost_frame() -> dict[str, Any]:
    """The pinned ``LHP-GEN-902`` terminal error frame for a dropped daemon.

    Reuses stream_adapter's error-frame builder so the shape is EXACTLY the
    frontend's existing error discriminator (R3: no new code minted). The
    copy is curated; the raw exception text stays in the server log only.
    """
    error = ErrorFactory.general_error(
        codes.GEN_902,
        title="Assistant daemon connection lost",
        details=(
            "The connection to the local omnigent daemon was lost during the "
            "turn. The turn may still be running; reopen the assistant to see "
            "its result."
        ),
        suggestions=[
            "Check that the omnigent server and host are still running",
            "Send the message again once the daemon is reachable",
        ],
        context={},
    )
    return _error_frame(error)


def _failure_reason(payload: dict[str, Any]) -> str:
    """Best error message carried by a ``response.failed`` / ``.incomplete``."""
    error = payload.get("error")
    if isinstance(error, dict) and error.get("message"):
        return str(error["message"])
    if payload.get("reason"):
        return str(payload["reason"])
    return "unknown"


def _session_error_message(payload: dict[str, Any]) -> str:
    """``error.message`` from a failed ``session.status`` (TOP-LEVEL keys)."""
    error = payload.get("error")
    if isinstance(error, dict) and error.get("message"):
        return str(error["message"])
    return ""


def _failure_hint(message: str) -> str:
    """Map a session-failure message to a UI hint (case-insensitive)."""
    lowered = message.lower()
    if "omnigent setup" in lowered:
        return "omnigent_setup"
    if "databricks" in lowered or "auth" in lowered:
        return "databricks_auth"
    return "unknown"


class _TurnState:
    """Mutable per-turn flags: coalesced running status + terminal marker."""

    __slots__ = ("running_emitted", "terminal")

    def __init__(self) -> None:
        self.running_emitted = False
        self.terminal = False


def _translate(
    event_type: str, payload: dict[str, Any], state: _TurnState
) -> list[dict[str, Any]]:
    """Map one omnigent event to its NDJSON frames per the pinned table.

    Returns zero or more frame dicts and sets ``state.terminal`` when the
    event ends the turn. Unknown events are swallowed with a debug log.
    """
    if event_type == "response.output_text.delta":
        return [{"type": "text.delta", "delta": payload.get("delta", "")}]
    if event_type in _REASONING_DELTA_EVENTS:
        return [{"type": "reasoning.delta", "delta": payload.get("delta", "")}]
    if event_type == "response.output_item.done":
        return [{"type": "item.done", "item": payload.get("item", {})}]
    if event_type == "response.elicitation_request":
        return [
            {
                "type": "approval.request",
                "elicitation_id": payload.get("elicitation_id"),
                "params": payload.get("params", {}),
            }
        ]
    if event_type in _RUNNING_EVENTS:
        if state.running_emitted:
            return []
        state.running_emitted = True
        return [{"type": "status", "state": "running"}]
    if event_type == "response.completed":
        state.terminal = True
        return [{"type": "turn.completed"}]
    if event_type == "response.failed":
        state.terminal = True
        return [{"type": "turn.failed", "reason": _failure_reason(payload)}]
    if event_type == "response.incomplete":
        state.terminal = True
        if payload.get("reason") == "user_interrupt":
            return [{"type": "interrupted"}]
        return [{"type": "turn.failed", "reason": _failure_reason(payload)}]
    if event_type == "response.cancelled":
        state.terminal = True
        return [{"type": "interrupted"}]
    if event_type == "session.interrupted":
        return [{"type": "interrupted"}]
    if event_type == "session.status":
        if payload.get("status") == "failed":
            state.terminal = True
            detail = _session_error_message(payload)
            return [
                {
                    "type": "session.failed",
                    "detail": detail,
                    "hint": _failure_hint(detail),
                }
            ]
        logger.debug(f"assistant chat: session.status {payload.get('status')!r}")
        return []
    if event_type == "response.heartbeat":
        return [{"type": "heartbeat"}]
    logger.debug(f"assistant chat: swallowing omnigent event {event_type!r}")
    return []


async def _touch_best_effort(project_root: Path, session_id: str) -> None:
    """Bump the session's ``last_used_at``; bookkeeping never fails the turn."""
    try:
        await asyncio.to_thread(assistant_store.touch_session, project_root, session_id)
    except Exception:
        logger.exception(f"assistant chat: touch_session failed for {session_id}")


async def chat_turn(
    client: OmnigentClient,
    project_root: Path,
    executor_cfg: dict[str, Any],
    host_id: str,
    text: str,
) -> AsyncIterator[bytes]:
    """Run one chat turn, yielding NDJSON frame lines (see module docstring).

    Sequence: ``status: preparing`` -> ensure_session -> ``session`` frame ->
    subscribe to the SSE stream and wait (10s) for its first event -> post
    the user message -> translate events until a terminal frame ends the
    turn. A daemon drop anywhere (including a stream that ends without a
    terminal event, or no first event within the timeout) yields the pinned
    ``LHP-GEN-902`` error frame as the terminal.

    Holds the module turn lock for the WHOLE turn; a concurrent caller's
    first frame appears only after this turn ends.
    """
    async with _turn_lock:
        session_id: str | None = None
        try:
            yield _encode({"type": "status", "state": "preparing"})
            session_id, created = await ensure_session(
                client, project_root, executor_cfg, host_id
            )
            yield _encode(
                {"type": "session", "session_id": session_id, "created": created}
            )

            # The client annotates the narrower AsyncIterator; the object is
            # an async generator whose aclose() is this turn's disconnect
            # contract, so name the real type for the finally below.
            stream = cast(
                "AsyncGenerator[tuple[str, dict[str, Any]], None]",
                client.stream_session(session_id),
            )
            try:
                state = _TurnState()
                try:
                    first = await asyncio.wait_for(
                        anext(stream), _FIRST_EVENT_TIMEOUT_S
                    )
                except StopAsyncIteration as exc:
                    raise OmnigentUnavailable(
                        f"omnigent stream for session {session_id} closed "
                        f"before its first event"
                    ) from exc
                except TimeoutError as exc:
                    raise OmnigentUnavailable(
                        f"omnigent stream for session {session_id} produced no "
                        f"event within {_FIRST_EVENT_TIMEOUT_S:.0f}s"
                    ) from exc

                for frame in _translate(first[0], first[1], state):
                    yield _encode(frame)
                turn_over = state.terminal
                if not turn_over:
                    # Subscription confirmed live — only now post the message.
                    await client.post_message(session_id, text)
                    async for event_type, payload in stream:
                        for frame in _translate(event_type, payload, state):
                            yield _encode(frame)
                        turn_over = state.terminal
                        if turn_over:
                            break
                if not turn_over:
                    raise OmnigentUnavailable(
                        f"omnigent stream for session {session_id} ended "
                        f"without a terminal event"
                    )
            finally:
                # Runs on success, on error, and on GeneratorExit (client
                # disconnect) — the omnigent stream never outlives the turn.
                # No interrupt is posted: the turn continues server-side.
                await stream.aclose()
        except (OmnigentUnavailable, httpx.HTTPError):
            logger.exception(
                f"assistant chat: omnigent connection lost "
                f"(session {session_id or 'not yet created'})"
            )
            yield _encode(_daemon_lost_frame())

        # Skipped automatically on GeneratorExit (it propagates past here).
        if session_id is not None:
            await _touch_best_effort(project_root, session_id)
