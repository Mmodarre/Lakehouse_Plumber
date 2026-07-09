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
    claude_sdk_policy,
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
    Wait,
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
    session_id: str = "sdk-2",
    subtype: str = "success",
    is_error: bool = False,
    **kwargs: Any,
) -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=5,
        duration_api_ms=4,
        is_error=is_error,
        num_turns=1,
        session_id=session_id,
        **kwargs,
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
    session_id: Optional[str] = None,
) -> tuple[list[dict], list[bytes]]:
    turn = claude_sdk_chat.chat_turn(
        project,
        cfg,
        text,
        registry,
        session_id=session_id,
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
            [_init(), Approval("Bash", {"command": "npm run build"}), _result()],
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
        assert json.loads(params["content_preview"]) == {"command": "npm run build"}
        assert "message" in params
        # Server-derived offer: first two shlex tokens of the command.
        assert params["always_allow_offer"] == {
            "tool": "Bash",
            "prefix": "npm run",
            "label": 'Always allow "npm run"',
        }

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
    monkeypatch.setattr(claude_sdk_policy, "_APPROVAL_TIMEOUT_S", 0.05)

    async def run() -> None:
        registry = ClaudeTurnRegistry()
        holder: list[FakeClaudeClient] = []
        frames, _ = await _collect(
            project,
            [_init(), Approval("Bash", {"command": "npm test"}), _result()],
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


@pytest.mark.parametrize("mode", ["default", "acceptEdits"])
def test_read_only_bash_auto_allowed_without_prompt(project: Path, mode: str) -> None:
    async def run() -> None:
        holder: list[FakeClaudeClient] = []
        frames, _ = await _collect(
            project,
            [
                _init(),
                Approval("Bash", {"command": "git status"}),
                Approval("Bash", {"command": "grep a | head -3"}),
                _result(),
            ],
            ClaudeTurnRegistry(),
            holder,
            permission_mode=mode,
        )
        (client,) = holder
        assert all(
            isinstance(r, PermissionResultAllow) for r in client.permission_results
        )
        assert all(f["type"] != "approval.request" for f in frames)
        assert frames[-1]["type"] == "turn.completed"

    asyncio.run(run())


def test_non_read_only_bash_still_prompts_in_default_mode(project: Path) -> None:
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
            [_init(), Approval("Bash", {"command": "rm -rf build"}), _result()],
            registry,
            holder,
            on_frame=on_frame,
        )
        assert len(approvals) == 1
        (client,) = holder
        assert isinstance(client.permission_results[0], PermissionResultDeny)

    asyncio.run(run())


def test_persisted_tool_rule_skips_prompt(project: Path) -> None:
    assistant_store.put_config(
        project, "permissions", {"always_allow": [{"tool": "WebFetch", "prefix": None}]}
    )

    async def run() -> None:
        holder: list[FakeClaudeClient] = []
        frames, _ = await _collect(
            project,
            [_init(), Approval("WebFetch", {"url": "https://x"}), _result()],
            ClaudeTurnRegistry(),
            holder,
        )
        (client,) = holder
        assert isinstance(client.permission_results[0], PermissionResultAllow)
        assert all(f["type"] != "approval.request" for f in frames)

    asyncio.run(run())


def test_bash_prefix_rule_matches_word_boundary_only(project: Path) -> None:
    assistant_store.put_config(
        project,
        "permissions",
        {"always_allow": [{"tool": "Bash", "prefix": "npm test"}]},
    )

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
            [
                _init(),
                Approval("Bash", {"command": "npm test -- --run"}),  # boundary hit
                Approval("Bash", {"command": "npm test"}),  # exact hit
                Approval("Bash", {"command": "npm testx"}),  # NOT a substring match
                _result(),
            ],
            registry,
            holder,
            on_frame=on_frame,
        )
        # Only the substring-lookalike prompts; the rule covers the other two.
        assert len(approvals) == 1
        assert json.loads(approvals[0]["params"]["content_preview"]) == {
            "command": "npm testx"
        }
        (client,) = holder
        results = client.permission_results
        assert isinstance(results[0], PermissionResultAllow)
        assert isinstance(results[1], PermissionResultAllow)
        assert isinstance(results[2], PermissionResultDeny)

    asyncio.run(run())


def test_rule_persisted_mid_turn_covers_the_next_call(project: Path) -> None:
    from lhp.webapp.services.claude_sdk_policy import record_always_allow_rule

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
                # Mirror the /approval endpoint's always-allow flow: persist
                # the rule from the REGISTRY-recorded call, then resolve.
                entry = registry.peek_approval(session_ids[0], frame["elicitation_id"])
                assert entry is not None
                assert record_always_allow_rule(
                    project, entry.tool_name, entry.tool_input
                )
                registry.resolve_approval(
                    session_ids[0], frame["elicitation_id"], "accept"
                )

        await _collect(
            project,
            [
                _init(),
                Approval("Bash", {"command": "npm test"}),
                Approval("Bash", {"command": "npm test"}),
                _result(),
            ],
            registry,
            holder,
            on_frame=on_frame,
        )
        # The second identical call sails through on the freshly stored rule.
        assert len(approvals) == 1
        (client,) = holder
        assert all(
            isinstance(r, PermissionResultAllow) for r in client.permission_results
        )
        stored = assistant_store.get_config(project, "permissions")
        assert stored == {"always_allow": [{"tool": "Bash", "prefix": "npm test"}]}

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
            [_init(), Approval("Bash", {"command": "npm test"}), _result()],
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
        started = next(f for f in frames if f["type"] == "item.started")
        assert started["item"]["id"] == done["item"]["id"]
        assert frames.index(started) < frames.index(done)

    asyncio.run(run())


def test_started_only_tool_call_never_persisted(project: Path) -> None:
    async def run() -> None:
        script = [
            _init(),
            AssistantMessage(
                content=[ToolUseBlock(id="tu_1", name="Bash", input={"command": "ls"})],
                model="m",
            ),
            _result(),
        ]
        frames, _ = await _collect(project, script, ClaudeTurnRegistry(), [])
        assert any(f["type"] == "item.started" for f in frames)
        # The result never arrived: the snapshot must contain no tool item.
        session_id = frames[1]["session_id"]
        items = assistant_store.list_items(project, session_id)
        assert [i["type"] for i in items] == ["message"]  # the user envelope only

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


_TURN_USAGE = {
    "input_tokens": 100,
    "output_tokens": 50,
    "cache_read_input_tokens": 1000,
    "cache_creation_input_tokens": 200,
}

_TURN_MODEL_USAGE = {"claude-sonnet-5": {"inputTokens": 100, "outputTokens": 50}}


def _usage_result() -> ResultMessage:
    return _result(
        usage=dict(_TURN_USAGE),
        total_cost_usd=0.42,
        model_usage=_TURN_MODEL_USAGE,
    )


def test_completed_turn_persists_usage_row_and_enriches_terminal(
    project: Path,
) -> None:
    assistant_store.put_config(
        project,
        "pricing",
        {
            "models": {
                "claude-sonnet-": {"input_per_mtok": 3.0, "output_per_mtok": 15.0}
            }
        },
    )

    async def run() -> None:
        frames, _ = await _collect(
            project, [_init(), _usage_result()], ClaudeTurnRegistry(), []
        )
        session_id = frames[1]["session_id"]
        terminal = frames[-1]
        assert terminal["type"] == "turn.completed"
        assert terminal["usage"] == _TURN_USAGE
        assert terminal["total_cost_usd"] == 0.42
        expected_cost = (100 * 3.0 + 50 * 15.0) / 1_000_000
        assert terminal["configured_cost_usd"] == pytest.approx(expected_cost)
        totals = terminal["session_totals"]
        assert totals["input_tokens"] == 100
        assert totals["output_tokens"] == 50
        assert totals["sdk_cost_usd"] == pytest.approx(0.42)
        assert totals["configured_cost_usd"] == pytest.approx(expected_cost)

        stored = assistant_store.usage_totals(project, session_id)
        assert stored == totals

    asyncio.run(run())


def test_usage_terminal_without_pricing_omits_configured_cost(project: Path) -> None:
    async def run() -> None:
        frames, _ = await _collect(
            project, [_init(), _usage_result()], ClaudeTurnRegistry(), []
        )
        terminal = frames[-1]
        assert terminal["type"] == "turn.completed"
        assert "configured_cost_usd" not in terminal
        # The row still persists (totals present) for later repricing.
        assert terminal["session_totals"]["configured_cost_usd"] is None

    asyncio.run(run())


def test_session_totals_accumulate_across_turns(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        await _collect(project, [_init(), _usage_result()], registry, [])
        frames, _ = await _collect(project, [_init(), _usage_result()], registry, [])
        totals = frames[-1]["session_totals"]
        assert totals["input_tokens"] == 200
        assert totals["sdk_cost_usd"] == pytest.approx(0.84)

    asyncio.run(run())


def test_usage_free_turn_terminal_stays_bare(project: Path) -> None:
    async def run() -> None:
        frames, _ = await _collect(
            project, list(_HAPPY_SCRIPT), ClaudeTurnRegistry(), []
        )
        assert frames[-1] == {"type": "turn.completed"}
        session_id = frames[1]["session_id"]
        assert assistant_store.usage_totals(project, session_id) is None

    asyncio.run(run())


def test_usage_persist_failure_never_breaks_the_stream(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*args: Any, **kwargs: Any) -> int:
        raise RuntimeError("disk full")

    monkeypatch.setattr(claude_sdk_chat.assistant_store, "insert_turn_usage", boom)

    async def run() -> None:
        frames, _ = await _collect(
            project, [_init(), _usage_result()], ClaudeTurnRegistry(), []
        )
        terminal = frames[-1]
        # The turn still terminates cleanly; enrichment is skipped, the
        # translator-attached keys survive.
        assert terminal["type"] == "turn.completed"
        assert terminal["usage"] == _TURN_USAGE
        assert "session_totals" not in terminal
        assert "configured_cost_usd" not in terminal

    asyncio.run(run())


def test_same_session_turns_serialize(project: Path) -> None:
    # ONE live turn per session: while t1 holds the session lock (parked on
    # its Wait gate), t2 emits only its pre-lock frames (preparing + session)
    # and its pumped frames all land after t1's terminal.
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        frames, _ = await _collect(project, list(_HAPPY_SCRIPT), registry, [])
        sid = frames[1]["session_id"]

        gate = asyncio.Event()
        t1_running = asyncio.Event()
        t2_session = asyncio.Event()
        timeline: list[tuple[str, str, Any]] = []
        holders: dict[str, list[FakeClaudeClient]] = {"t1": [], "t2": []}

        async def drive(tag: str, script: list[Any]) -> None:
            turn = claude_sdk_chat.chat_turn(
                project,
                _CFG,
                tag,
                registry,
                session_id=sid,
                client_factory=make_factory(script, holders[tag]),
            )
            async for line in turn:
                frame = json.loads(line)
                timeline.append((tag, frame["type"], frame.get("state")))
                if tag == "t1" and frame.get("state") == "running":
                    t1_running.set()
                if tag == "t2" and frame["type"] == "session":
                    t2_session.set()

        task1 = asyncio.create_task(
            drive("t1", [_init(), Wait(gate), _result(session_id="sdk-T1")])
        )
        await asyncio.wait_for(t1_running.wait(), _TEST_TIMEOUT_S)
        task2 = asyncio.create_task(drive("t2", [_init(), _result()]))
        # t2 emits its pre-lock frames, then parks on the session lock —
        # nothing further can arrive while t1 holds it.
        await asyncio.wait_for(t2_session.wait(), _TEST_TIMEOUT_S)

        t2_so_far = [(t, s) for tag, t, s in timeline if tag == "t2"]
        assert t2_so_far == [("status", "preparing"), ("session", None)]

        gate.set()
        await asyncio.wait_for(asyncio.gather(task1, task2), _TEST_TIMEOUT_S)

        t1_terminal = timeline.index(("t1", "turn.completed", None))
        t2_running = timeline.index(("t2", "status", "running"))
        assert t1_terminal < t2_running
        assert ("t2", "turn.completed", None) in timeline
        # t2 read its resume handle UNDER the lock: it must see the handle t1
        # stored, not the pre-lock snapshot from before t1 finished.
        assert holders["t2"][0].options.resume == "sdk-T1"

    asyncio.run(run())


def test_turns_on_different_sessions_run_concurrently(project: Path) -> None:
    # Multi-tab: while session A's turn is parked mid-stream (lock A held),
    # a full turn on session B starts AND completes. A global lock would
    # deadlock this (the wait_for timeouts fail the test).
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        frames_a, _ = await _collect(project, list(_HAPPY_SCRIPT), registry, [])
        sid_a = frames_a[1]["session_id"]
        frames_b, _ = await _collect(
            project, list(_HAPPY_SCRIPT), registry, [], session_id="draft:1"
        )
        sid_b = frames_b[1]["session_id"]
        assert sid_b != sid_a

        gate = asyncio.Event()
        a_running = asyncio.Event()
        a_frames: list[dict] = []

        async def drive_a() -> None:
            turn = claude_sdk_chat.chat_turn(
                project,
                _CFG,
                "a2",
                registry,
                session_id=sid_a,
                client_factory=make_factory([_init(), Wait(gate), _result()], []),
            )
            async for line in turn:
                frame = json.loads(line)
                a_frames.append(frame)
                if frame.get("state") == "running":
                    a_running.set()

        task_a = asyncio.create_task(drive_a())
        await asyncio.wait_for(a_running.wait(), _TEST_TIMEOUT_S)

        frames_b2, _ = await _collect(
            project, list(_HAPPY_SCRIPT), registry, [], session_id=sid_b
        )
        assert frames_b2[-1] == {"type": "turn.completed"}
        assert frames_b2[1]["session_id"] == sid_b
        assert not task_a.done()  # A is still parked mid-turn

        gate.set()
        await asyncio.wait_for(task_a, _TEST_TIMEOUT_S)
        assert a_frames[-1] == {"type": "turn.completed"}

    asyncio.run(run())


def test_chat_turn_explicit_session_id_reaches_that_session(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        frames, _ = await _collect(project, list(_HAPPY_SCRIPT), registry, [])
        sid_a = frames[1]["session_id"]
        frames_b, _ = await _collect(
            project, list(_HAPPY_SCRIPT), registry, [], session_id="draft:1"
        )
        sid_b = frames_b[1]["session_id"]

        # Target the first (non-MRU) session explicitly: the turn binds to it
        # and its transcript, not the MRU one, gains the new envelopes.
        before = len(assistant_store.list_items(project, sid_a))
        frames_a2, _ = await _collect(
            project, list(_HAPPY_SCRIPT), registry, [], text="again", session_id=sid_a
        )
        assert frames_a2[1]["session_id"] == sid_a
        assert frames_a2[1]["created"] is False
        assert len(assistant_store.list_items(project, sid_a)) == before + 2

    asyncio.run(run())


def test_first_user_message_claims_the_tab_title_once(project: Path) -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        long_text = "Please generate the bronze layer " + "x" * 100
        frames, _ = await _collect(
            project, list(_HAPPY_SCRIPT), registry, [], text=long_text
        )
        sid = frames[1]["session_id"]
        row = assistant_store.get_session(project, sid)
        assert row is not None
        assert row["title"] == long_text.strip()[:60]

        await _collect(
            project, list(_HAPPY_SCRIPT), registry, [], text="second", session_id=sid
        )
        row = assistant_store.get_session(project, sid)
        assert row is not None
        assert row["title"] == long_text.strip()[:60]  # unchanged

    asyncio.run(run())
