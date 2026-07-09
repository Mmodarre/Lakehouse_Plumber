"""Integration tests for the Claude SDK turn engine (stubbed client).

Everything drives :func:`claude_sdk_chat.chat_turn` end-to-end through the
``client_factory`` seam with :mod:`tests.webapp._claude_sdk_stub` — no SDK
subprocess. ``SkillFacade`` is monkeypatched (as in the sessions tests).
Async paths run under ``asyncio.run`` (no pytest-asyncio).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Optional

import pytest
from claude_agent_sdk import (
    AssistantMessage,
    CLINotFoundError,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    TextBlock,
    ToolUseBlock,
)

from lhp.webapp.services import (
    assistant_store,
    claude_sdk_chat,
    claude_sdk_sessions,
    sqlite_store,
)
from lhp.webapp.services.claude_sdk_auth import ClaudeAuthError
from lhp.webapp.services.claude_sdk_bridge import ClaudeTurnRegistry

from ._claude_sdk_stub import (
    HANG,
    RAISE,
    Approval,
    FakeClaudeClient,
    Sleep,
    make_factory,
)

pytestmark = pytest.mark.webapp

_CFG = {"provider": "claude_sdk", "mode": "claude_subscription"}

#: Every test turn must finish well inside this (protects against deadlocks).
_TEST_TIMEOUT_S = 10.0


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    sqlite_store.run_migrations(tmp_path)

    class _Skill:
        def __init__(self, root: Path) -> None:
            self._root = root

        def install_project_skill(self, force: bool = False):
            marker = self._root / ".claude" / "skills" / "lhp"
            marker.mkdir(parents=True, exist_ok=True)
            (marker / ".lhp_skill_version").write_text("1.0.0\n", encoding="utf-8")
            return SimpleNamespace(skill_version="1.0.0")

    monkeypatch.setattr(claude_sdk_sessions, "SkillFacade", _Skill)
    return tmp_path


def _init(session_id: str = "sdk-1") -> SystemMessage:
    return SystemMessage(subtype="init", data={"session_id": session_id})


def _text_delta(text: str) -> StreamEvent:
    return StreamEvent(
        uuid="u",
        session_id="sdk-1",
        event={
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": text},
        },
    )


def _result(
    session_id: str = "sdk-2", subtype: str = "success", is_error: bool = False
) -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=5,
        duration_api_ms=4,
        is_error=is_error,
        num_turns=1,
        session_id=session_id,
    )


_HAPPY_SCRIPT = [
    _init(),
    _text_delta("Hel"),
    _text_delta("lo"),
    AssistantMessage(content=[TextBlock(text="Hello")], model="m"),
    _result(),
]

OnFrame = Optional[Callable[[dict], Awaitable[None]]]


async def _collect(
    project: Path,
    script: list[Any],
    registry: ClaudeTurnRegistry,
    holder: list[FakeClaudeClient],
    cfg: dict[str, Any] = _CFG,
    text: str = "hi",
    on_frame: OnFrame = None,
    connect_error: Exception | None = None,
    permission_mode: str = "default",
) -> tuple[list[dict], list[bytes]]:
    turn = claude_sdk_chat.chat_turn(
        project,
        cfg,
        text,
        registry,
        permission_mode=permission_mode,
        client_factory=make_factory(script, holder, connect_error=connect_error),
    )
    frames: list[dict] = []
    raw: list[bytes] = []

    async def run() -> None:
        async for line in turn:
            raw.append(line)
            frame = json.loads(line)
            frames.append(frame)
            if on_frame is not None:
                await on_frame(frame)

    await asyncio.wait_for(run(), _TEST_TIMEOUT_S)
    return frames, raw


def test_happy_turn_exact_frame_sequence_and_encoding(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        frames, raw = await _collect(project, list(_HAPPY_SCRIPT), registry, holder)

        assert [f["type"] for f in frames] == [
            "status",  # preparing
            "session",
            "status",  # running (exactly once)
            "text.delta",
            "text.delta",
            "turn.completed",
        ]
        assert frames[0] == {"type": "status", "state": "preparing"}
        assert frames[1]["created"] is True
        assert frames[1]["session_id"].startswith("claude_")
        assert frames[2] == {"type": "status", "state": "running"}
        assert sum(f == {"type": "status", "state": "running"} for f in frames) == 1
        # Byte-compact NDJSON encoding, one frame per line.
        for line, frame in zip(raw, frames, strict=False):
            assert line == json.dumps(frame, separators=(",", ":")).encode() + b"\n"

        (client,) = holder
        assert client.connected and client.disconnected
        assert client.queries == ["hi"]
        # Registry cleaned up after the turn.
        assert registry.get(frames[1]["session_id"]) is None

    asyncio.run(run())


def test_options_pinned_for_subscription_turn(project: Path) -> None:
    async def run() -> None:
        holder: list[FakeClaudeClient] = []
        await _collect(project, list(_HAPPY_SCRIPT), ClaudeTurnRegistry(), holder)
        options = holder[0].options

        assert options.cwd == str(project)
        assert options.env == {}
        assert options.resume is None
        assert options.include_partial_messages is True
        assert options.setting_sources == ["project"]
        assert options.permission_mode == "default"
        assert options.max_turns == claude_sdk_chat._MAX_TURNS
        assert options.system_prompt["type"] == "preset"
        assert options.system_prompt["preset"] == "claude_code"
        assert "Lakehouse Plumber" in options.system_prompt["append"]
        assert options.can_use_tool is not None

    asyncio.run(run())


def test_databricks_turn_env_names_only(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FakeConfig:
        def __init__(self, **kwargs) -> None:
            self.host = "https://ws.example.com"

        def authenticate(self) -> dict[str, str]:
            return {"Authorization": "Bearer tok-1"}

    monkeypatch.setattr("databricks.sdk.core.Config", _FakeConfig)

    async def run() -> None:
        holder: list[FakeClaudeClient] = []
        cfg = {"provider": "claude_sdk", "mode": "databricks", "profile": "DEFAULT"}
        await _collect(
            project, list(_HAPPY_SCRIPT), ClaudeTurnRegistry(), holder, cfg=cfg
        )
        options = holder[0].options
        # Assert env NAMES, never values.
        assert set(options.env) == {
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_AUTH_TOKEN",
            "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS",
        }
        assert options.model == "databricks-claude-opus-4-8"

    asyncio.run(run())


def test_user_and_assistant_items_persisted_for_rehydration(project: Path) -> None:
    async def run() -> None:
        frames, _ = await _collect(
            project, list(_HAPPY_SCRIPT), ClaudeTurnRegistry(), []
        )
        session_id = frames[1]["session_id"]
        items = assistant_store.list_items(project, session_id)

        assert [i["type"] for i in items] == ["message", "message"]
        assert [i["created_by"] for i in items] == ["user", "assistant"]
        # Snapshot-envelope shape the frontend normalizer unwraps.
        for item in items:
            assert {"id", "type", "status", "data"} <= set(item)
        assert items[0]["data"]["content"] == [{"type": "input_text", "text": "hi"}]
        assert items[1]["data"]["content"] == [{"type": "output_text", "text": "Hello"}]

    asyncio.run(run())


def test_resume_handle_stored_and_passed_on_next_turn(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        frames, _ = await _collect(
            project, [_init(), _result(session_id="sdk-A")], registry, holder
        )
        session_id = frames[1]["session_id"]
        active = assistant_store.get_active_session(project)
        assert active is not None
        assert active["runtime_session_id"] == "sdk-A"

        frames2, _ = await _collect(
            project, [_init(), _result(session_id="sdk-B")], registry, holder
        )
        assert frames2[1]["session_id"] == session_id
        assert frames2[1]["created"] is False
        assert holder[1].options.resume == "sdk-A"
        active = assistant_store.get_active_session(project)
        assert active is not None
        assert active["runtime_session_id"] == "sdk-B"

    asyncio.run(run())


def test_dead_resume_is_cleared_after_result_less_stream(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        await _collect(project, [_init(), _result(session_id="sdk-A")], registry, [])
        # Second turn resumes sdk-A but the runtime dies without a result.
        frames, _ = await _collect(project, [RAISE], registry, [])
        assert frames[-1]["code"] == "LHP-GEN-902"
        active = assistant_store.get_active_session(project)
        assert active is not None
        assert active["runtime_session_id"] is None

    asyncio.run(run())


def test_approval_accept_roundtrip(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        approvals: list[dict] = []
        session_ids: list[str] = []

        async def on_frame(frame: dict) -> None:
            if frame["type"] == "session":
                session_ids.append(frame["session_id"])
            if frame["type"] == "approval.request":
                approvals.append(frame)
                assert registry.resolve_approval(
                    session_ids[0], frame["elicitation_id"], "accept"
                )

        frames, _ = await _collect(
            project,
            [_init(), Approval("Bash", {"command": "ls"}), _result()],
            registry,
            holder,
            on_frame=on_frame,
        )

        (approval,) = approvals
        assert approval["elicitation_id"].startswith("elic_")
        params = approval["params"]
        assert params["tool_name"] == "Bash"
        assert params["phase"] == "tool_use"
        assert params["policy_name"] == "Bash"
        assert json.loads(params["content_preview"]) == {"command": "ls"}
        assert "message" in params

        (client,) = holder
        assert isinstance(client.permission_results[0], PermissionResultAllow)
        assert frames[-1] == {"type": "turn.completed"}

    asyncio.run(run())


def test_approval_decline_turn_continues(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        session_ids: list[str] = []

        async def on_frame(frame: dict) -> None:
            if frame["type"] == "session":
                session_ids.append(frame["session_id"])
            if frame["type"] == "approval.request":
                registry.resolve_approval(
                    session_ids[0], frame["elicitation_id"], "decline"
                )

        frames, _ = await _collect(
            project,
            [_init(), Approval("Write", {"file_path": "x"}), _result()],
            registry,
            holder,
            on_frame=on_frame,
        )

        (client,) = holder
        result = client.permission_results[0]
        assert isinstance(result, PermissionResultDeny)
        assert client.interrupt_calls == 0
        # The model re-plans; the turn still completes normally.
        assert frames[-1] == {"type": "turn.completed"}

    asyncio.run(run())


def test_approval_cancel_denies_and_interrupts(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        session_ids: list[str] = []

        async def on_frame(frame: dict) -> None:
            if frame["type"] == "session":
                session_ids.append(frame["session_id"])
            if frame["type"] == "approval.request":
                registry.resolve_approval(
                    session_ids[0], frame["elicitation_id"], "cancel"
                )

        frames, _ = await _collect(
            project,
            # HANG parks until the cancel-triggered interrupt lands, then the
            # (interrupt-flagged) result renders as `interrupted`.
            [_init(), Approval("Bash", {"command": "rm"}), HANG, _result()],
            registry,
            holder,
            on_frame=on_frame,
        )

        (client,) = holder
        assert isinstance(client.permission_results[0], PermissionResultDeny)
        assert client.interrupt_calls == 1
        assert frames[-1] == {"type": "interrupted"}
        assert sum(f["type"] == "interrupted" for f in frames) == 1

    asyncio.run(run())


def test_approval_timeout_denies_and_clears_registry(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(claude_sdk_chat, "_APPROVAL_TIMEOUT_S", 0.05)

    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        frames, _ = await _collect(
            project,
            [_init(), Approval("Bash", {"command": "ls"}), _result()],
            registry,
            holder,
        )
        (client,) = holder
        result = client.permission_results[0]
        assert isinstance(result, PermissionResultDeny)
        assert "timed out" in result.message
        assert frames[-1] == {"type": "turn.completed"}
        # No stray approval survives the turn.
        assert registry._turns == {}

    asyncio.run(run())


def test_auto_allowed_tool_never_asks(project: Path) -> None:
    async def run() -> None:
        holder: list[FakeClaudeClient] = []
        frames, _ = await _collect(
            project,
            [_init(), Approval("Read", {"file_path": "x"}), _result()],
            ClaudeTurnRegistry(),
            holder,
        )
        (client,) = holder
        assert isinstance(client.permission_results[0], PermissionResultAllow)
        assert all(f["type"] != "approval.request" for f in frames)

    asyncio.run(run())


def test_accept_edits_mode_auto_allows_edit_tools(project: Path) -> None:
    async def run() -> None:
        holder: list[FakeClaudeClient] = []
        frames, _ = await _collect(
            project,
            [
                _init(),
                Approval("Edit", {"file_path": "x"}),
                Approval("Write", {"file_path": "y"}),
                _result(),
            ],
            ClaudeTurnRegistry(),
            holder,
            permission_mode="acceptEdits",
        )
        (client,) = holder
        assert all(
            isinstance(r, PermissionResultAllow) for r in client.permission_results
        )
        assert all(f["type"] != "approval.request" for f in frames)
        assert frames[-1] == {"type": "turn.completed"}

    asyncio.run(run())


def test_accept_edits_mode_still_asks_for_bash(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        approvals: list[dict] = []
        session_ids: list[str] = []

        async def on_frame(frame: dict) -> None:
            if frame["type"] == "session":
                session_ids.append(frame["session_id"])
            if frame["type"] == "approval.request":
                approvals.append(frame)
                registry.resolve_approval(
                    session_ids[0], frame["elicitation_id"], "decline"
                )

        frames, _ = await _collect(
            project,
            [_init(), Approval("Bash", {"command": "ls"}), _result()],
            registry,
            holder,
            on_frame=on_frame,
            permission_mode="acceptEdits",
        )

        assert len(approvals) == 1
        (client,) = holder
        assert isinstance(client.permission_results[0], PermissionResultDeny)
        assert frames[-1] == {"type": "turn.completed"}

    asyncio.run(run())


def test_bypass_permissions_mode_auto_allows_everything(project: Path) -> None:
    async def run() -> None:
        holder: list[FakeClaudeClient] = []
        frames, _ = await _collect(
            project,
            [
                _init(),
                Approval("Bash", {"command": "ls"}),
                Approval("WebFetch", {"url": "https://example.com"}),
                _result(),
            ],
            ClaudeTurnRegistry(),
            holder,
            permission_mode="bypassPermissions",
        )
        (client,) = holder
        assert all(
            isinstance(r, PermissionResultAllow) for r in client.permission_results
        )
        assert all(f["type"] != "approval.request" for f in frames)
        assert frames[-1] == {"type": "turn.completed"}

    asyncio.run(run())


def test_unknown_permission_mode_degrades_to_ask(project: Path) -> None:
    # The router's Literal already rejects unknown modes; the engine's own
    # fallback must fail toward ASKING, never toward allowing.
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        approvals: list[dict] = []
        session_ids: list[str] = []

        async def on_frame(frame: dict) -> None:
            if frame["type"] == "session":
                session_ids.append(frame["session_id"])
            if frame["type"] == "approval.request":
                approvals.append(frame)
                registry.resolve_approval(
                    session_ids[0], frame["elicitation_id"], "decline"
                )

        await _collect(
            project,
            [_init(), Approval("Edit", {"file_path": "x"}), _result()],
            registry,
            holder,
            on_frame=on_frame,
            permission_mode="trustMeBro",
        )
        assert len(approvals) == 1
        (client,) = holder
        assert isinstance(client.permission_results[0], PermissionResultDeny)

    asyncio.run(run())


def test_interrupt_mid_turn_single_interrupted_terminal(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        session_ids: list[str] = []
        fired: list[bool] = []

        async def on_frame(frame: dict) -> None:
            if frame["type"] == "session":
                session_ids.append(frame["session_id"])
            if (
                frame["type"] == "status"
                and frame.get("state") == "running"
                and not fired
            ):
                fired.append(True)
                assert await registry.request_interrupt(session_ids[0])

        # No ResultMessage after HANG: stream end + requested interrupt must
        # still render exactly one `interrupted` terminal (never GEN-902).
        frames, _ = await _collect(
            project, [_init(), HANG], registry, holder, on_frame=on_frame
        )

        (client,) = holder
        assert client.interrupt_calls == 1
        assert frames[-1] == {"type": "interrupted"}
        assert sum(f["type"] == "interrupted" for f in frames) == 1
        assert all(f.get("code") != "LHP-GEN-902" for f in frames)

    asyncio.run(run())


def test_interrupt_while_approval_pending_unblocks_callback(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        session_ids: list[str] = []

        async def on_frame(frame: dict) -> None:
            if frame["type"] == "session":
                session_ids.append(frame["session_id"])
            if frame["type"] == "approval.request":
                assert await registry.request_interrupt(session_ids[0])

        frames, _ = await _collect(
            project,
            [_init(), Approval("Bash", {"command": "x"}), _result()],
            registry,
            holder,
            on_frame=on_frame,
        )

        (client,) = holder
        assert isinstance(client.permission_results[0], PermissionResultDeny)
        assert client.interrupt_calls == 1
        assert frames[-1] == {"type": "interrupted"}

    asyncio.run(run())


def test_auth_failure_unset_token_env_fails_pre_session(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MISSING_TOKEN_VAR", raising=False)

    async def run() -> None:
        cfg = {
            "provider": "claude_sdk",
            "mode": "claude_subscription",
            "oauth_token_env": "MISSING_TOKEN_VAR",
        }
        frames, _ = await _collect(project, [], ClaudeTurnRegistry(), [], cfg=cfg)
        assert [f["type"] for f in frames] == ["status", "session.failed"]
        assert frames[1]["hint"] == "claude_auth"

    asyncio.run(run())


def test_auth_failure_databricks_hint(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(cfg: dict[str, Any]):
        raise ClaudeAuthError(detail="no workspace", hint="databricks_auth")

    monkeypatch.setattr(claude_sdk_chat, "build_turn_env", boom)

    async def run() -> None:
        frames, _ = await _collect(project, [], ClaudeTurnRegistry(), [])
        assert frames[-1] == {
            "type": "session.failed",
            "detail": "no workspace",
            "hint": "databricks_auth",
        }

    asyncio.run(run())


def test_missing_cli_binary_fails_with_claude_setup_hint(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        frames, _ = await _collect(
            project,
            [],
            registry,
            [],
            connect_error=CLINotFoundError("claude not found"),
        )
        assert frames[-1]["type"] == "session.failed"
        assert frames[-1]["hint"] == "claude_setup"
        # Registry cleaned up even on the connect-failure path.
        assert registry._turns == {}

    asyncio.run(run())


def test_stream_end_without_result_yields_pinned_gen_902(project: Path) -> None:
    async def run() -> None:
        frames, _ = await _collect(
            project, [_init(), _text_delta("x"), RAISE], ClaudeTurnRegistry(), []
        )
        terminal = frames[-1]
        assert terminal["type"] == "error"
        assert terminal["code"] == "LHP-GEN-902"
        assert set(terminal) == {
            "type",
            "code",
            "title",
            "details",
            "suggestions",
            "context",
            "doc_link",
        }

    asyncio.run(run())


def test_tool_use_result_join_emits_item_done(project: Path) -> None:
    from claude_agent_sdk import ToolResultBlock, UserMessage

    async def run() -> None:
        script = [
            _init(),
            AssistantMessage(
                content=[ToolUseBlock(id="tu_1", name="Read", input={"path": "a"})],
                model="m",
            ),
            UserMessage(content=[ToolResultBlock(tool_use_id="tu_1", content="body")]),
            _result(),
        ]
        frames, _ = await _collect(project, script, ClaudeTurnRegistry(), [])
        done = next(f for f in frames if f["type"] == "item.done")
        assert done["item"]["name"] == "Read"
        assert done["item"]["status"] == "completed"

    asyncio.run(run())


def test_heartbeat_on_queue_silence(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(claude_sdk_chat, "_HEARTBEAT_INTERVAL_S", 0.05)

    async def run() -> None:
        frames, _ = await _collect(
            project, [_init(), Sleep(0.3), _result()], ClaudeTurnRegistry(), []
        )
        assert any(f == {"type": "heartbeat"} for f in frames)
        assert frames[-1] == {"type": "turn.completed"}

    asyncio.run(run())


def test_turn_lock_serializes_concurrent_turns(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        order: list[tuple[str, str]] = []

        async def drive(tag: str) -> None:
            turn = claude_sdk_chat.chat_turn(
                project,
                _CFG,
                tag,
                registry,
                client_factory=make_factory(list(_HAPPY_SCRIPT), []),
            )
            async for line in turn:
                order.append((tag, json.loads(line)["type"]))

        await asyncio.wait_for(
            asyncio.gather(drive("t1"), drive("t2")), _TEST_TIMEOUT_S
        )

        tags = [tag for tag, _ in order]
        # No interleaving: whichever turn won the lock emits ALL its frames
        # before the other's first frame (gather does not promise start order).
        winner = tags[0]
        boundary = tags.index(next(t for t in tags if t != winner))
        assert set(tags[:boundary]) == {winner}
        assert winner not in tags[boundary:]

    asyncio.run(run())
