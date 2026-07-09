"""Tests for the assistant chat relay (``assistant_chat.py``).

Drives :func:`~lhp.webapp.services.assistant_chat.chat_turn` directly,
collecting decoded NDJSON frames, against the scripted omnigent stub over
``httpx.ASGITransport`` (fully buffered: scripts are pre-scripted; mid-turn
blocking happens in the stub's EVENTS handler). ``asyncio.run`` per test —
repo convention, no pytest-asyncio.

``ensure_session`` is faked (provisioning has its own test module) and the
store's ``touch_session`` is replaced with a recorder, so ``project_root``
can be a bare tmp dir. Every stub script terminates.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
import pytest

from lhp.webapp.services import assistant_chat
from lhp.webapp.services.assistant_chat import chat_turn
from tests.webapp._omnigent_stub import BLOCK, DROP, RAISE, OmnigentStub, make_client

pytestmark = pytest.mark.webapp

_CFG: dict[str, Any] = {"mode": "omnigent_defaults"}
_HOST = "h1"

_PREPARING = {"type": "status", "state": "preparing"}
_SESSION = {"type": "session", "session_id": "conv_1", "created": False}
_RUNNING = {"type": "status", "state": "running"}

#: The pinned LHP-GEN-902 terminal frame shape (stream_adapter's builder).
_ERROR_FRAME_KEYS = {
    "type",
    "code",
    "title",
    "details",
    "suggestions",
    "context",
    "doc_link",
}


def _fake_ensure_session(
    monkeypatch: pytest.MonkeyPatch,
    stub: OmnigentStub,
    session_id: str = "conv_1",
) -> None:
    """Short-circuit provisioning: the session exists and is reused."""

    async def fake(
        client: Any, project_root: Path, executor_cfg: dict[str, Any], host_id: str
    ) -> tuple[str, bool]:
        stub.alive_sessions.add(session_id)
        return session_id, False

    monkeypatch.setattr(assistant_chat, "ensure_session", fake)


def _record_touches(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Path, str]]:
    touches: list[tuple[Path, str]] = []

    def fake_touch(project_root: Path, session_id: str) -> None:
        touches.append((project_root, session_id))

    monkeypatch.setattr(assistant_chat.assistant_store, "touch_session", fake_touch)
    return touches


async def _collect(gen: AsyncIterator[bytes]) -> list[dict[str, Any]]:
    return [json.loads(raw) async for raw in gen]


def _drive(
    stub: OmnigentStub, project: Path, text: str = "hello"
) -> list[dict[str, Any]]:
    """Run one full chat turn against the stub; return the decoded frames."""

    async def scenario() -> list[dict[str, Any]]:
        client = make_client(stub)
        try:
            return await _collect(chat_turn(client, project, _CFG, _HOST, text))
        finally:
            await client.aclose()

    return asyncio.run(scenario())


def test_happy_path_frame_order_and_subscribe_before_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    touches = _record_touches(monkeypatch)
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            ("response.created", {}),
            ("response.in_progress", {}),  # coalesced with created
            ("session.usage", {"total_cost_usd": 0.5}),  # unknown -> swallowed
            ("response.retry", {}),  # pinned swallow
            ("response.output_text.delta", {"delta": "Hel"}),
            ("response.output_text.delta", {"delta": "lo"}),
            ("response.completed", {}),
        ]
    )

    frames = _drive(stub, tmp_path)

    assert frames == [
        _PREPARING,
        _SESSION,
        _RUNNING,
        {"type": "text.delta", "delta": "Hel"},
        {"type": "text.delta", "delta": "lo"},
        {"type": "turn.completed"},
    ]
    # SUBSCRIBE-FIRST: the stream GET precedes the message POST.
    assert stub.calls.index(("GET", "/v1/sessions/conv_1/stream")) < stub.calls.index(
        ("POST", "/v1/sessions/conv_1/events")
    )
    assert [payload["type"] for _, payload in stub.events] == ["message"]
    assert stub.events[0][1]["data"]["content"][0]["text"] == "hello"
    assert touches == [(tmp_path, "conv_1")]


def test_reasoning_deltas_and_heartbeat_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            ("response.reasoning_text.delta", {"delta": "th"}),
            ("response.reasoning_summary_text.delta", {"delta": "ink"}),
            ("response.heartbeat", {}),
            ("response.completed", {}),
        ]
    )

    frames = _drive(stub, tmp_path)

    assert frames == [
        _PREPARING,
        _SESSION,
        {"type": "reasoning.delta", "delta": "th"},
        {"type": "reasoning.delta", "delta": "ink"},
        {"type": "heartbeat"},
        {"type": "turn.completed"},
    ]


def test_item_done_flat_passthrough(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    item = {
        "id": "itm_1",
        "type": "function_call",
        "status": "completed",
        "name": "sys_os_read",
        "arguments": "{}",
    }
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            ("response.output_item.done", {"item": item}),
            ("response.completed", {}),
        ]
    )

    frames = _drive(stub, tmp_path)
    assert frames[2] == {"type": "item.done", "item": item}


def test_approval_request_mapping_and_turn_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            (
                "response.elicitation_request",
                {"elicitation_id": "elc_1", "params": {"tool_name": "sys_os_edit"}},
            ),
            ("response.completed", {}),
        ]
    )

    frames = _drive(stub, tmp_path)
    assert frames[2] == {
        "type": "approval.request",
        "elicitation_id": "elc_1",
        "params": {"tool_name": "sys_os_edit"},
    }
    # An approval request is NOT terminal: the turn still completes.
    assert frames[-1] == {"type": "turn.completed"}


@pytest.mark.parametrize(
    ("message", "hint"),
    [
        ("Run omnigent setup to configure credentials", "omnigent_setup"),
        ("Databricks profile 'x' not found", "databricks_auth"),
        ("Claude SDK AUTH token expired", "databricks_auth"),
        ("disk exploded", "unknown"),
    ],
)
def test_session_status_failed_maps_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, message: str, hint: str
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    # Spike-verified: status and error are TOP-LEVEL keys on session.status.
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            (
                "session.status",
                {
                    "status": "failed",
                    "error": {"code": "runner_error", "message": message},
                },
            ),
        ]
    )

    frames = _drive(stub, tmp_path)
    assert frames[-1] == {"type": "session.failed", "detail": message, "hint": hint}


def test_interrupt_pair_cancelled_ends_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # This omnigent build interrupts via session.interrupted (non-terminal)
    # plus response.cancelled (terminal).
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            ("response.created", {}),
            ("session.interrupted", {"data": {"requested_at": "now"}}),
            ("response.cancelled", {}),
        ]
    )

    frames = _drive(stub, tmp_path)
    assert frames == [
        _PREPARING,
        _SESSION,
        _RUNNING,
        {"type": "interrupted"},
        {"type": "interrupted"},
    ]


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("user_interrupt", {"type": "interrupted"}),
        ("max_tokens", {"type": "turn.failed", "reason": "max_tokens"}),
    ],
)
def test_response_incomplete_by_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    expected: dict[str, Any],
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    stub.stream_scripts.append(
        [("session.presence", {}), ("response.incomplete", {"reason": reason})]
    )

    frames = _drive(stub, tmp_path)
    assert frames[-1] == expected


def test_response_failed_carries_error_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            ("response.failed", {"error": {"message": "model exploded"}}),
        ]
    )

    frames = _drive(stub, tmp_path)
    assert frames[-1] == {"type": "turn.failed", "reason": "model exploded"}


def _assert_gen902(frame: dict[str, Any]) -> None:
    assert set(frame) == _ERROR_FRAME_KEYS
    assert frame["type"] == "error"
    assert frame["code"] == "LHP-GEN-902"
    assert frame["title"] == "Assistant daemon connection lost"
    assert frame["suggestions"]
    assert frame["context"] == {}


def test_transport_drop_yields_pinned_gen902_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    touches = _record_touches(monkeypatch)
    stub.stream_scripts.append([(RAISE, httpx.ReadError("connection dropped"))])

    frames = _drive(stub, tmp_path)

    assert frames[:2] == [_PREPARING, _SESSION]
    _assert_gen902(frames[-1])
    assert len(frames) == 3
    # Best-effort bookkeeping still runs after the error terminal.
    assert touches == [(tmp_path, "conv_1")]


def test_first_event_timeout_yields_gen902_and_never_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No first event within the timeout -> the pinned 902 terminal.

    SUBSCRIBE-FIRST corollary: because the subscription was never confirmed
    live, the user message must NOT have been posted.
    """
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    touches = _record_touches(monkeypatch)
    monkeypatch.setattr(assistant_chat, "_FIRST_EVENT_TIMEOUT_S", 0.05)
    # Ceiling bounds the stub body even if cancellation were missed.
    stub.stream_scripts.append([(BLOCK, 30.0)])

    frames = _drive(stub, tmp_path)

    assert frames[:2] == [_PREPARING, _SESSION]
    _assert_gen902(frames[-1])
    assert len(frames) == 3
    assert stub.events == []  # nothing posted: subscription never went live
    # Best-effort bookkeeping still runs after the error terminal.
    assert touches == [(tmp_path, "conv_1")]


def test_stream_abort_without_terminal_yields_gen902(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            ("response.output_text.delta", {"delta": "Hi"}),
            (DROP,),  # body ends with no [DONE] and no terminal event
        ]
    )

    frames = _drive(stub, tmp_path)
    assert frames[2] == {"type": "text.delta", "delta": "Hi"}
    _assert_gen902(frames[-1])


def test_single_flight_second_turn_waits_for_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    _record_touches(monkeypatch)
    script = [
        ("session.presence", {}),
        ("response.output_text.delta", {"delta": "x"}),
        ("response.completed", {}),
    ]
    stub.stream_scripts.extend([list(script), list(script)])
    # Turn 1 blocks INSIDE its message POST until the gate opens.
    stub.event_gate = asyncio.Event()

    async def scenario() -> list[tuple[str, dict[str, Any]]]:
        journal: list[tuple[str, dict[str, Any]]] = []

        async def run(tag: str, client: Any, text: str) -> None:
            async for raw in chat_turn(client, tmp_path, _CFG, _HOST, text):
                journal.append((tag, json.loads(raw)))

        client_one = make_client(stub)
        client_two = make_client(stub)
        try:
            turn_one = asyncio.create_task(run("t1", client_one, "first"))
            await asyncio.wait_for(stub.message_posted.wait(), timeout=5.0)
            # Turn 1 now provably holds the lock (mid-POST, mid-turn).
            turn_two = asyncio.create_task(run("t2", client_two, "second"))
            for _ in range(50):
                await asyncio.sleep(0)
            assert not [entry for entry in journal if entry[0] == "t2"], (
                "second turn emitted frames while the first held the lock"
            )
            assert stub.event_gate is not None
            stub.event_gate.set()
            await asyncio.wait_for(asyncio.gather(turn_one, turn_two), timeout=5.0)
        finally:
            await client_one.aclose()
            await client_two.aclose()
        return journal

    journal = asyncio.run(scenario())

    tags = [tag for tag, _ in journal]
    # Strict serialization: every t1 frame precedes every t2 frame.
    assert tags == ["t1"] * tags.count("t1") + ["t2"] * tags.count("t2")
    assert [frame for tag, frame in journal if tag == "t1"][-1] == {
        "type": "turn.completed"
    }
    assert [frame for tag, frame in journal if tag == "t2"][-1] == {
        "type": "turn.completed"
    }


def test_generator_exit_closes_stream_without_interrupt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub = OmnigentStub()
    _fake_ensure_session(monkeypatch, stub)
    touches = _record_touches(monkeypatch)
    stub.stream_scripts.append(
        [
            ("session.presence", {}),
            ("response.created", {}),
            ("response.output_text.delta", {"delta": "partial"}),
            ("response.output_text.delta", {"delta": "rest"}),
            ("response.completed", {}),
        ]
    )

    async def scenario() -> list[dict[str, Any]]:
        client = make_client(stub)
        try:
            gen = chat_turn(client, tmp_path, _CFG, _HOST, "hello")
            seen: list[dict[str, Any]] = []
            while True:
                frame = json.loads(await anext(gen))
                seen.append(frame)
                if frame["type"] == "text.delta":
                    break
            await gen.aclose()  # client disconnected mid-relay
            with pytest.raises(StopAsyncIteration):
                await anext(gen)
            return seen
        finally:
            await client.aclose()

    seen = asyncio.run(scenario())

    assert seen[-1] == {"type": "text.delta", "delta": "partial"}
    # No interrupt was posted: the only session event is the user message,
    # and the turn's bookkeeping touch is skipped on the disconnect path.
    assert [payload["type"] for _, payload in stub.events] == ["message"]
    assert touches == []
