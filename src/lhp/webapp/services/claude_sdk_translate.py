"""Translate Claude Agent SDK messages into the pinned NDJSON frame vocabulary.

Pure functions: :func:`translate` maps one SDK message onto zero or more
NDJSON frame dicts (the EXACT vocabulary of
:mod:`~lhp.webapp.services.assistant_chat` — the chat UI is shared between
providers) plus zero or more persistence envelopes for ``assistant_items``
(the Omnigent snapshot shape ``{id, type, status, response_id, created_at,
created_by, data}`` that the frontend's ``normalizeSnapshotItem`` unwraps).

Pinned mapping (SDK message -> frames / envelopes):

=============================================== ==============================================
SDK message                                     effect
=============================================== ==============================================
any first message of the turn                   ``{"type":"status","state":"running"}`` once
StreamEvent ``text_delta``                      ``{"type":"text.delta","delta":...}``
StreamEvent ``thinking_delta``                  ``{"type":"reasoning.delta","delta":...}``
StreamEvent ``input_json_delta`` / block        swallowed
start-stop / message_* bookkeeping
SystemMessage ``init``                          captures the SDK session id; no frame
AssistantMessage TextBlock / ThinkingBlock      envelope (``message`` / ``reasoning``); the
                                                text is re-emitted as ONE delta frame only
                                                when no partial deltas were seen (fallback
                                                for ``include_partial_messages`` gaps)
AssistantMessage ToolUseBlock                   recorded for the result join; no frame
UserMessage ToolResultBlock                     joined with its ToolUseBlock ->
                                                ``{"type":"item.done","item":{id, type:
                                                "tool_call", name, status, arguments,
                                                output_preview}}`` + envelope
ResultMessage (first terminal wins)             interrupt requested -> ``{"type":
                                                "interrupted"}``; ``success`` and not
                                                ``is_error`` -> ``{"type":"turn.completed"}``;
                                                else ``{"type":"turn.failed","reason":...}``.
                                                Also refreshes the SDK session id (resume
                                                handles may be re-minted per resume).
subagent traffic (``parent_tool_use_id`` set)   swallowed — the parent ``Task`` tool's own
                                                result renders the aggregate
RateLimitEvent and ALL unknown messages         swallowed (``logger.debug``)
=============================================== ==============================================

:stability: internal
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

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

from lhp.webapp.services.sqlite_store import utc_now_iso

logger = logging.getLogger(__name__)

#: Tool output / approval content previews are clipped to this many chars.
PREVIEW_MAX_CHARS = 2000


@dataclass
class Translated:
    """One message's translation: NDJSON frames + persistence envelopes."""

    frames: list[dict[str, Any]] = field(default_factory=list)
    items: list[dict[str, Any]] = field(default_factory=list)


class TranslationState:
    """Mutable per-turn accumulator threaded through :func:`translate`."""

    __slots__ = (
        "interrupt_requested",
        "pending_tool_uses",
        "running_emitted",
        "saw_text_delta",
        "saw_thinking_delta",
        "sdk_session_id",
        "terminal",
    )

    def __init__(self) -> None:
        self.running_emitted = False
        #: ``None`` until a terminal frame was produced, then one of
        #: ``completed`` / ``failed`` / ``interrupted`` (first terminal wins).
        self.terminal: Optional[str] = None
        #: Synced from the turn handle by the engine before each translate
        #: call; renders any terminal (or stream end) as ``interrupted``.
        self.interrupt_requested = False
        #: SDK session id captured from ``SystemMessage init`` and refreshed
        #: by every ``ResultMessage`` — the next turn's resume handle.
        self.sdk_session_id: Optional[str] = None
        #: ``ToolUseBlock``s awaiting their ``ToolResultBlock`` join,
        #: ``id -> (name, input)``.
        self.pending_tool_uses: dict[str, tuple[str, dict[str, Any]]] = {}
        #: Per-assistant-message flags: partial deltas were already streamed,
        #: so the whole-block fallback must not re-emit the text.
        self.saw_text_delta = False
        self.saw_thinking_delta = False


def _mint_id() -> str:
    return f"item_{uuid.uuid4().hex}"


def envelope(
    item_id: str,
    item_type: str,
    status: str,
    data: dict[str, Any],
    created_by: str = "assistant",
) -> dict[str, Any]:
    """One ``assistant_items`` persistence envelope (Omnigent snapshot shape)."""
    return {
        "id": item_id,
        "type": item_type,
        "status": status,
        "response_id": None,
        "created_at": utc_now_iso(),
        "created_by": created_by,
        "data": data,
    }


def user_message_envelope(text: str) -> dict[str, Any]:
    """The turn-opening user-message envelope (written by the engine)."""
    return envelope(
        _mint_id(),
        "message",
        "completed",
        {"role": "user", "content": [{"type": "input_text", "text": text}]},
        created_by="user",
    )


def session_failed_frame(detail: str, hint: str) -> dict[str, Any]:
    """A ``session.failed`` frame (hints: ``claude_setup`` / ``claude_auth``
    / ``databricks_auth`` / ``unknown``)."""
    return {"type": "session.failed", "detail": detail, "hint": hint}


def _preview(content: Any) -> str:
    """Flatten a tool result's content to a clipped text preview."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content[:PREVIEW_MAX_CHARS]
    parts: list[str] = []
    for entry in content:
        if isinstance(entry, dict):
            text = entry.get("text")
            parts.append(text if isinstance(text, str) else json.dumps(entry))
        else:
            parts.append(str(entry))
    return "".join(parts)[:PREVIEW_MAX_CHARS]


def _tool_call_item(
    tool_use_id: str, name: str, tool_input: dict[str, Any], result: ToolResultBlock
) -> dict[str, Any]:
    """The flat ``tool_call`` item carried by ``item.done`` and its envelope."""
    return {
        "id": tool_use_id,
        "type": "tool_call",
        "name": name,
        "status": "failed" if result.is_error else "completed",
        "arguments": json.dumps(tool_input, separators=(",", ":"), default=str),
        "output_preview": _preview(result.content),
    }


def _failure_reason(message: ResultMessage) -> str:
    if message.errors:
        return "; ".join(str(error) for error in message.errors)
    if message.result:
        return str(message.result)
    if message.subtype and message.subtype != "success":
        return message.subtype
    if message.api_error_status is not None:
        return f"API error (HTTP {message.api_error_status})"
    return "unknown"


def _translate_stream_event(
    out: Translated, message: StreamEvent, state: TranslationState
) -> None:
    event = message.event
    if event.get("type") != "content_block_delta":
        return
    delta = event.get("delta") or {}
    delta_type = delta.get("type")
    if delta_type == "text_delta":
        state.saw_text_delta = True
        out.frames.append({"type": "text.delta", "delta": delta.get("text", "")})
    elif delta_type == "thinking_delta":
        state.saw_thinking_delta = True
        out.frames.append(
            {"type": "reasoning.delta", "delta": delta.get("thinking", "")}
        )
    # input_json_delta (tool arguments) is deliberately not streamed.


def _translate_assistant(
    out: Translated, message: AssistantMessage, state: TranslationState
) -> None:
    text_parts: list[str] = []
    thinking_parts: list[str] = []
    for block in message.content:
        if isinstance(block, TextBlock):
            text_parts.append(block.text)
        elif isinstance(block, ThinkingBlock):
            thinking_parts.append(block.thinking)
        elif isinstance(block, ToolUseBlock):
            state.pending_tool_uses[block.id] = (block.name, block.input)

    thinking = "".join(thinking_parts)
    if thinking:
        if not state.saw_thinking_delta:
            out.frames.append({"type": "reasoning.delta", "delta": thinking})
        out.items.append(
            envelope(
                _mint_id(),
                "reasoning",
                "completed",
                {"summary": [{"type": "summary_text", "text": thinking}]},
            )
        )
    text = "".join(text_parts)
    if text:
        if not state.saw_text_delta:
            out.frames.append({"type": "text.delta", "delta": text})
        out.items.append(
            envelope(
                message.message_id or message.uuid or _mint_id(),
                "message",
                "completed",
                {
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": text}],
                },
            )
        )
    if message.error is not None:
        logger.debug(f"assistant chat: AssistantMessage error {message.error!r}")
    # The whole-block fallback is per assistant message: reset for the next.
    state.saw_text_delta = False
    state.saw_thinking_delta = False


def _translate_user(
    out: Translated, message: UserMessage, state: TranslationState
) -> None:
    if not isinstance(message.content, list):
        return
    for block in message.content:
        if not isinstance(block, ToolResultBlock):
            continue
        name, tool_input = state.pending_tool_uses.pop(
            block.tool_use_id, ("unknown", {})
        )
        item = _tool_call_item(block.tool_use_id, name, tool_input, block)
        out.frames.append({"type": "item.done", "item": item})
        out.items.append(envelope(item["id"], "tool_call", item["status"], item))


def _translate_result(
    out: Translated, message: ResultMessage, state: TranslationState
) -> None:
    if message.session_id:
        state.sdk_session_id = message.session_id
    if state.terminal is not None:
        logger.debug(
            f"assistant chat: swallowing post-terminal ResultMessage "
            f"{message.subtype!r}"
        )
        return
    if state.interrupt_requested:
        state.terminal = "interrupted"
        out.frames.append({"type": "interrupted"})
    elif message.subtype == "success" and not message.is_error:
        state.terminal = "completed"
        out.frames.append({"type": "turn.completed"})
    else:
        state.terminal = "failed"
        out.frames.append({"type": "turn.failed", "reason": _failure_reason(message)})


def translate(message: Any, state: TranslationState) -> Translated:
    """Map one SDK message to NDJSON frames + persistence envelopes.

    Mutates ``state`` (running coalescing, tool-use joins, terminal marker,
    SDK session id). Unknown message types are swallowed with a debug log.
    """
    out = Translated()
    if not state.running_emitted:
        state.running_emitted = True
        out.frames.append({"type": "status", "state": "running"})

    parent = getattr(message, "parent_tool_use_id", None)
    if parent is not None:
        # Subagent traffic: the parent Task tool's result carries the outcome.
        return out

    if isinstance(message, StreamEvent):
        _translate_stream_event(out, message, state)
    elif isinstance(message, SystemMessage):
        if message.subtype == "init":
            session_id = message.data.get("session_id")
            if session_id:
                state.sdk_session_id = str(session_id)
        else:
            logger.debug(
                f"assistant chat: swallowing SystemMessage {message.subtype!r}"
            )
    elif isinstance(message, AssistantMessage):
        _translate_assistant(out, message, state)
    elif isinstance(message, UserMessage):
        _translate_user(out, message, state)
    elif isinstance(message, ResultMessage):
        _translate_result(out, message, state)
    else:
        logger.debug(f"assistant chat: swallowing SDK message {type(message).__name__}")
    return out
