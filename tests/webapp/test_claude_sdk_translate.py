"""Unit tests for the Claude SDK -> NDJSON frame/envelope translator.

Messages are built from the REAL claude_agent_sdk dataclasses (the SDK is a
webapp dependency), so constructor drift in an SDK upgrade fails here first.
"""

from __future__ import annotations

import json

import pytest
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from lhp.webapp.services import claude_sdk_translate as tr

pytestmark = pytest.mark.webapp


def _stream_event(delta: dict, parent: str | None = None) -> StreamEvent:
    return StreamEvent(
        uuid="u1",
        session_id="sdk-1",
        event={"type": "content_block_delta", "index": 0, "delta": delta},
        parent_tool_use_id=parent,
    )


def _result(
    subtype: str = "success", is_error: bool = False, **kwargs
) -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=10,
        duration_api_ms=8,
        is_error=is_error,
        num_turns=1,
        session_id="sdk-2",
        **kwargs,
    )


def _drain(messages, state: tr.TranslationState):
    """Translate a message sequence; return (frames, items)."""
    frames: list[dict] = []
    items: list[dict] = []
    for message in messages:
        out = tr.translate(message, state)
        frames.extend(out.frames)
        items.extend(out.items)
    return frames, items


def test_happy_turn_frame_sequence_and_running_once() -> None:
    state = tr.TranslationState()
    frames, items = _drain(
        [
            SystemMessage(subtype="init", data={"session_id": "sdk-1"}),
            _stream_event({"type": "text_delta", "text": "Hel"}),
            _stream_event({"type": "text_delta", "text": "lo"}),
            AssistantMessage(content=[TextBlock(text="Hello")], model="m"),
            _result(),
        ],
        state,
    )

    assert frames == [
        {"type": "status", "state": "running"},
        {"type": "text.delta", "delta": "Hel"},
        {"type": "text.delta", "delta": "lo"},
        {"type": "turn.completed"},
    ]
    # The full text arrived as partials, so no duplicate whole-block frame —
    # but the message envelope is still persisted.
    assert [item["type"] for item in items] == ["message"]
    assert items[0]["data"]["content"] == [{"type": "output_text", "text": "Hello"}]
    assert state.terminal == "completed"
    assert state.sdk_session_id == "sdk-2"  # refreshed by the ResultMessage


def test_system_init_captures_sdk_session_id() -> None:
    state = tr.TranslationState()
    tr.translate(SystemMessage(subtype="init", data={"session_id": "sdk-9"}), state)
    assert state.sdk_session_id == "sdk-9"


def test_reasoning_deltas_and_envelope() -> None:
    state = tr.TranslationState()
    frames, items = _drain(
        [
            _stream_event({"type": "thinking_delta", "thinking": "hmm"}),
            AssistantMessage(
                content=[ThinkingBlock(thinking="hmm", signature="s")], model="m"
            ),
        ],
        state,
    )
    assert {"type": "reasoning.delta", "delta": "hmm"} in frames
    # Partials were seen, so exactly one reasoning.delta frame (no fallback).
    assert sum(f["type"] == "reasoning.delta" for f in frames) == 1
    assert [item["type"] for item in items] == ["reasoning"]
    assert items[0]["data"]["summary"] == [{"type": "summary_text", "text": "hmm"}]


def test_whole_block_fallback_when_no_partials_seen() -> None:
    state = tr.TranslationState()
    out = tr.translate(
        AssistantMessage(
            content=[ThinkingBlock(thinking="t", signature="s"), TextBlock(text="x")],
            model="m",
        ),
        state,
    )
    assert {"type": "reasoning.delta", "delta": "t"} in out.frames
    assert {"type": "text.delta", "delta": "x"} in out.frames


def test_fallback_flags_reset_per_assistant_message() -> None:
    state = tr.TranslationState()
    _drain(
        [
            _stream_event({"type": "text_delta", "text": "a"}),
            AssistantMessage(content=[TextBlock(text="a")], model="m"),
        ],
        state,
    )
    # Second assistant message with NO partials must fall back again.
    out = tr.translate(
        AssistantMessage(content=[TextBlock(text="b")], model="m"), state
    )
    assert {"type": "text.delta", "delta": "b"} in out.frames


def test_input_json_delta_and_bookkeeping_events_swallowed() -> None:
    state = tr.TranslationState()
    frames, items = _drain(
        [
            _stream_event({"type": "input_json_delta", "partial_json": '{"a"'}),
            StreamEvent(uuid="u", session_id="s", event={"type": "message_start"}),
            StreamEvent(uuid="u", session_id="s", event={"type": "content_block_stop"}),
        ],
        state,
    )
    assert frames == [{"type": "status", "state": "running"}]
    assert items == []


def test_tool_use_result_join_completed() -> None:
    state = tr.TranslationState()
    frames, items = _drain(
        [
            AssistantMessage(
                content=[ToolUseBlock(id="tu_1", name="Read", input={"path": "x"})],
                model="m",
            ),
            UserMessage(
                content=[ToolResultBlock(tool_use_id="tu_1", content="file body")]
            ),
        ],
        state,
    )
    done = [f for f in frames if f["type"] == "item.done"]
    assert len(done) == 1
    item = done[0]["item"]
    assert item["id"] == "tu_1"
    assert item["type"] == "tool_call"
    assert item["name"] == "Read"
    assert item["status"] == "completed"
    assert json.loads(item["arguments"]) == {"path": "x"}
    assert item["output_preview"] == "file body"
    # ToolUseBlock alone produced no frame and no envelope.
    assert [i["type"] for i in items] == ["tool_call"]
    assert items[0]["data"] == item
    assert state.pending_tool_uses == {}


def test_tool_result_error_status_and_preview_clip() -> None:
    state = tr.TranslationState()
    long = "y" * (tr.PREVIEW_MAX_CHARS + 500)
    frames, _ = _drain(
        [
            AssistantMessage(
                content=[ToolUseBlock(id="tu_2", name="Bash", input={})], model="m"
            ),
            UserMessage(
                content=[
                    ToolResultBlock(
                        tool_use_id="tu_2",
                        content=[{"type": "text", "text": long}],
                        is_error=True,
                    )
                ]
            ),
        ],
        state,
    )
    item = next(f for f in frames if f["type"] == "item.done")["item"]
    assert item["status"] == "failed"
    assert len(item["output_preview"]) == tr.PREVIEW_MAX_CHARS


def test_unmatched_tool_result_uses_unknown_name() -> None:
    state = tr.TranslationState()
    out = tr.translate(
        UserMessage(content=[ToolResultBlock(tool_use_id="ghost", content=None)]),
        state,
    )
    item = next(f for f in out.frames if f["type"] == "item.done")["item"]
    assert item["name"] == "unknown"
    assert item["output_preview"] == ""


def test_subagent_traffic_is_swallowed() -> None:
    state = tr.TranslationState()
    frames, items = _drain(
        [
            _stream_event({"type": "text_delta", "text": "sub"}, parent="tu_task"),
            AssistantMessage(
                content=[TextBlock(text="sub answer")],
                model="m",
                parent_tool_use_id="tu_task",
            ),
            UserMessage(
                content=[ToolResultBlock(tool_use_id="x", content="r")],
                parent_tool_use_id="tu_task",
            ),
        ],
        state,
    )
    assert frames == [{"type": "status", "state": "running"}]
    assert items == []


def test_result_error_subtype_maps_to_turn_failed() -> None:
    state = tr.TranslationState()
    out = tr.translate(_result(subtype="error_max_turns", is_error=True), state)
    assert {"type": "turn.failed", "reason": "error_max_turns"} in out.frames
    assert state.terminal == "failed"


def test_result_errors_list_beats_subtype_in_reason() -> None:
    state = tr.TranslationState()
    out = tr.translate(
        _result(subtype="error_during_execution", is_error=True, errors=["boom"]),
        state,
    )
    assert {"type": "turn.failed", "reason": "boom"} in out.frames


def test_result_success_with_is_error_fails() -> None:
    state = tr.TranslationState()
    out = tr.translate(_result(is_error=True, api_error_status=529), state)
    assert out.frames[-1]["type"] == "turn.failed"
    assert "529" in out.frames[-1]["reason"]


def test_interrupt_requested_renders_any_terminal_as_interrupted() -> None:
    state = tr.TranslationState()
    state.interrupt_requested = True
    out = tr.translate(_result(subtype="success"), state)
    assert {"type": "interrupted"} in out.frames
    assert state.terminal == "interrupted"


def test_first_terminal_wins() -> None:
    state = tr.TranslationState()
    tr.translate(_result(), state)
    out = tr.translate(_result(subtype="error_during_execution", is_error=True), state)
    assert out.frames == []
    assert state.terminal == "completed"


def test_envelope_and_user_message_envelope_shapes() -> None:
    env = tr.user_message_envelope("hi there")
    assert set(env) == {
        "id",
        "type",
        "status",
        "response_id",
        "created_at",
        "created_by",
        "data",
    }
    assert env["type"] == "message"
    assert env["created_by"] == "user"
    assert env["data"] == {
        "role": "user",
        "content": [{"type": "input_text", "text": "hi there"}],
    }


def test_session_failed_frame_shape() -> None:
    assert tr.session_failed_frame("no creds", "claude_auth") == {
        "type": "session.failed",
        "detail": "no creds",
        "hint": "claude_auth",
    }
