"""Run one Claude SDK assistant chat turn as an NDJSON byte stream.

:func:`chat_turn` is the Claude-provider counterpart of
:func:`~lhp.webapp.services.assistant_chat.chat_turn` and emits the SAME
pinned frame vocabulary (byte-identical encoding); the chat UI cannot tell
the providers apart. Per turn: mint auth env
(:mod:`~lhp.webapp.services.claude_sdk_auth`), ensure a session
(:mod:`~lhp.webapp.services.claude_sdk_sessions`), spawn a per-turn
``ClaudeSDKClient``, and pump SDK messages through
:func:`~lhp.webapp.services.claude_sdk_translate.translate` onto the stream.

Turn serialization: a module-level ``asyncio.Lock`` admits ONE turn at a time
(same single-user posture as the Omnigent relay). A silent stream emits a
``heartbeat`` frame every 15s — approvals can park the SDK for minutes and
the HTTP response must stay alive.

Approvals: the SDK's ``can_use_tool`` callback is the policy source of truth
(``ClaudeAgentOptions.permission_mode`` stays ``"default"`` — the SDK's own
modes would resolve permissions BEFORE the callback and bypass the interrupt
/deny machinery). The UI's per-turn ``permission_mode`` is implemented
inside the callback instead: ``default`` auto-allows only the read-only
tools in :data:`AUTO_ALLOWED_TOOLS`; ``acceptEdits`` additionally
auto-allows the local file-edit tools in :data:`EDIT_TOOLS`;
``bypassPermissions`` auto-allows everything. Anything not auto-allowed
emits an ``approval.request`` frame and parks on a Future resolved by
``POST /assistant/approval`` (timeout 300s -> deny). ``accept`` allows;
``decline`` denies and the model re-plans; ``cancel`` denies AND interrupts
the turn (matching Omnigent elicitation semantics).

Interrupts: ``POST /assistant/interrupt`` goes through
:meth:`~lhp.webapp.services.claude_sdk_bridge.ClaudeTurnRegistry.request_interrupt`.
The pump keeps draining after an interrupt (the SDK requires the stream be
consumed) under a 30s ceiling; a requested interrupt renders any terminal —
or a stream end without one — as ``interrupted``, never ``LHP-GEN-902``.

Terminal guarantees: a stream that ends without a ``ResultMessage`` yields
the pinned ``LHP-GEN-902`` error frame (code reused from stream_adapter,
never minted anew). The ``finally`` path never yields (GeneratorExit-safe):
it cancels the pump, ends the registry turn, disconnects the client — a
client disconnect therefore interrupts the SDK subprocess, a deliberate
divergence from Omnigent's survive-disconnect daemon (an in-process turn has
no supervisor to hand off to) — and stores the latest SDK resume handle.

Test seam: :class:`ClaudeClientProtocol` (structural typing — the sanctioned
§4.12 plugin boundary; the production object is Anthropic's
``ClaudeSDKClient``) plus the injectable ``client_factory``. Tests never
spawn a subprocess.

:stability: internal
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Optional, Protocol

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ClaudeSDKError,
    CLINotFoundError,
    PermissionResultAllow,
    PermissionResultDeny,
)
from claude_agent_sdk.types import PermissionResult, ToolPermissionContext

from lhp.errors import ErrorFactory, codes
from lhp.webapp.services import assistant_store
from lhp.webapp.services.assistant_provision import ASSISTANT_PROMPT
from lhp.webapp.services.claude_sdk_auth import ClaudeAuthError, build_turn_env
from lhp.webapp.services.claude_sdk_bridge import ClaudeTurnRegistry, TurnHandle
from lhp.webapp.services.claude_sdk_sessions import ensure_claude_session
from lhp.webapp.services.claude_sdk_translate import (
    PREVIEW_MAX_CHARS,
    TranslationState,
    session_failed_frame,
    translate,
    user_message_envelope,
)
from lhp.webapp.services.stream_adapter import _error_frame

logger = logging.getLogger(__name__)

#: Whole-turn gate: one chat turn at a time; concurrent callers WAIT.
_turn_lock = asyncio.Lock()

#: Ceiling on auth-env minting (``external-browser`` can block on a human).
_AUTH_TIMEOUT_S = 120.0
#: How long an ``approval.request`` may park before it is denied.
_APPROVAL_TIMEOUT_S = 300.0
#: Silence threshold after which a ``heartbeat`` frame keeps the stream alive.
_HEARTBEAT_INTERVAL_S = 15.0
#: Post-interrupt drain ceiling: the SDK stream must be consumed after an
#: interrupt, but a wedged subprocess must not hang the response forever.
_INTERRUPT_DRAIN_CEILING_S = 30.0
#: Agentic-loop bound handed to the SDK.
_MAX_TURNS = 50

#: Read-only local tools that never need approval. Everything else —
#: Edit/Write/Bash/WebSearch/WebFetch/mcp__* — goes through the approval
#: bridge. The callback is the policy source of truth, independent of
#: whether ``allowed_tools`` would short-circuit it.
AUTO_ALLOWED_TOOLS = frozenset(
    {"Read", "Glob", "Grep", "NotebookRead", "TodoWrite", "Task"}
)

#: Local file-edit tools additionally auto-allowed in ``acceptEdits`` mode.
#: Bash is deliberately NOT here — a shell command can edit files, but
#: ``acceptEdits`` means "trust edits, still ask for commands", matching
#: Claude Code's own mode of the same name.
EDIT_TOOLS = frozenset({"Edit", "Write", "MultiEdit", "NotebookEdit"})

#: Queue sentinel: the pump finished (terminal seen, stream end, or error).
_STREAM_END = object()


class ClaudeClientProtocol(Protocol):
    """Structural contract of the SDK client surface the engine consumes.

    The §4.12-sanctioned plugin boundary: the production implementation is
    Anthropic's ``ClaudeSDKClient`` (external code that cannot inherit an
    LHP ABC); tests inject a scripted double via ``client_factory``.
    """

    async def connect(self) -> None: ...

    async def query(self, prompt: str) -> None: ...

    def receive_messages(self) -> AsyncIterator[Any]: ...

    async def interrupt(self) -> None: ...

    async def disconnect(self) -> None: ...


#: Builds the per-turn client from the assembled options (test seam).
ClientFactory = Callable[[ClaudeAgentOptions], ClaudeClientProtocol]


def _encode(frame: dict[str, Any]) -> bytes:
    """One NDJSON line — the same compact encoding stream_adapter emits."""
    return json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"


def _runtime_lost_frame() -> dict[str, Any]:
    """The pinned ``LHP-GEN-902`` terminal error frame for a dead SDK run.

    Reuses stream_adapter's error-frame builder so the shape is EXACTLY the
    frontend's existing error discriminator. The copy is curated; raw
    exception text stays in the server log only.
    """
    error = ErrorFactory.general_error(
        codes.GEN_902,
        title="Assistant runtime connection lost",
        details=(
            "The Claude runtime ended unexpectedly during the turn, before "
            "reporting a result."
        ),
        suggestions=[
            "Send the message again to start a fresh turn",
            "Check the `lhp web` server log for the underlying error",
        ],
        context={},
    )
    return _error_frame(error)


def _build_options(
    project_root: Path,
    model: Optional[str],
    env: dict[str, str],
    resume: Optional[str],
    can_use_tool: Any,
) -> ClaudeAgentOptions:
    return ClaudeAgentOptions(
        cwd=str(project_root),
        # Preset + append keeps Claude Code's own tool guidance; a plain
        # string prompt would drop it.
        system_prompt={
            "type": "preset",
            "preset": "claude_code",
            "append": ASSISTANT_PROMPT,
        },
        model=model,
        env=env,
        resume=resume,
        include_partial_messages=True,
        setting_sources=["project"],
        permission_mode="default",
        max_turns=_MAX_TURNS,
        can_use_tool=can_use_tool,
    )


def _approval_params(
    tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
) -> dict[str, Any]:
    """The ``approval.request`` params the frontend approval card renders."""
    preview = json.dumps(tool_input, separators=(",", ":"), default=str)
    return {
        "message": context.title or f"Claude wants to use {tool_name}",
        "phase": "tool_use",
        "policy_name": context.display_name or tool_name,
        "content_preview": preview[:PREVIEW_MAX_CHARS],
        "tool_name": tool_name,
    }


async def _persist_item(
    project_root: Path, session_id: str, item: dict[str, Any]
) -> None:
    try:
        await asyncio.to_thread(
            assistant_store.insert_item, project_root, session_id, item
        )
    except Exception:
        # Best-effort bookkeeping: a failed transcript insert must never kill
        # a live stream; rehydration just misses this item.
        logger.exception(f"assistant chat: insert_item failed for {session_id}")


def _make_can_use_tool(
    session_id: str,
    registry: ClaudeTurnRegistry,
    handle: TurnHandle,
    queue: "asyncio.Queue[Any]",
    interrupt_tasks: set["asyncio.Task[Any]"],
    permission_mode: str = "default",
) -> Any:
    """Build the per-turn ``can_use_tool`` approval-policy callback.

    ``permission_mode`` widens the silent-allow policy (see module
    docstring); an unknown value degrades to ``default`` — ask for
    everything non-read-only — never to more permissive.
    """
    bypass = permission_mode == "bypassPermissions"
    auto_allowed = AUTO_ALLOWED_TOOLS
    if permission_mode == "acceptEdits":
        auto_allowed = AUTO_ALLOWED_TOOLS | EDIT_TOOLS

    async def can_use_tool(
        tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
    ) -> PermissionResult:
        if bypass or tool_name in auto_allowed:
            return PermissionResultAllow()
        if handle.interrupt_requested:
            return PermissionResultDeny(message="Turn was interrupted")
        elicitation_id, future = registry.create_approval(session_id)
        await queue.put(
            {
                "type": "approval.request",
                "elicitation_id": elicitation_id,
                "params": _approval_params(tool_name, tool_input, context),
            }
        )
        try:
            action = await asyncio.wait_for(future, _APPROVAL_TIMEOUT_S)
        except TimeoutError:
            handle.approvals.pop(elicitation_id, None)
            return PermissionResultDeny(message="Approval request timed out")
        except asyncio.CancelledError:
            if future.cancelled():
                # end_turn cancelled the Future (turn is over): deny quietly.
                return PermissionResultDeny(message="Turn ended")
            raise
        if action == "accept":
            return PermissionResultAllow()
        if action == "cancel":
            # Deny AND stop the turn (Omnigent elicitation semantics). The
            # interrupt runs as its own task — awaiting it here would
            # deadlock the SDK control loop this callback replies on.
            task = asyncio.get_running_loop().create_task(
                registry.request_interrupt(session_id)
            )
            interrupt_tasks.add(task)
            task.add_done_callback(interrupt_tasks.discard)
            return PermissionResultDeny(message="User cancelled the turn")
        # "decline" or the interrupt-deny sentinel: the turn continues (the
        # model re-plans) unless an interrupt is already in flight.
        return PermissionResultDeny(message="User declined")

    return can_use_tool


async def _pump(
    client: ClaudeClientProtocol,
    state: TranslationState,
    handle: TurnHandle,
    queue: "asyncio.Queue[Any]",
    project_root: Path,
    session_id: str,
) -> None:
    """Translate SDK messages onto the frame queue until terminal/stream end."""
    try:
        async for message in client.receive_messages():
            state.interrupt_requested = handle.interrupt_requested
            out = translate(message, state)
            for item in out.items:
                await _persist_item(project_root, session_id, item)
            for frame in out.frames:
                await queue.put(frame)
            if state.terminal is not None:
                break
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            f"assistant chat: SDK message stream failed (session {session_id})"
        )
    finally:
        queue.put_nowait(_STREAM_END)


async def chat_turn(
    project_root: Path,
    executor_cfg: dict[str, Any],
    text: str,
    registry: ClaudeTurnRegistry,
    *,
    permission_mode: str = "default",
    client_factory: Optional[ClientFactory] = None,
) -> AsyncIterator[bytes]:
    """Run one Claude SDK chat turn, yielding NDJSON frame lines.

    Sequence: ``status: preparing`` -> auth mint (failure -> ``session.failed``
    pre-session) -> ensure session -> ``session`` frame -> persist the user
    envelope -> spawn the per-turn client -> pump frames (heartbeats on
    silence) until a terminal frame, an interrupt, or ``LHP-GEN-902``.

    ``permission_mode`` is the caller-chosen approval policy for THIS turn
    (see module docstring); it is deliberately per-turn, not stored config —
    the user cycles it in the chat UI like Claude Code's own modes.

    Holds the module turn lock for the WHOLE turn.
    """
    async with _turn_lock:
        yield _encode({"type": "status", "state": "preparing"})
        try:
            auth = await asyncio.wait_for(
                asyncio.to_thread(build_turn_env, executor_cfg), _AUTH_TIMEOUT_S
            )
        except ClaudeAuthError as exc:
            yield _encode(session_failed_frame(exc.detail, exc.hint))
            return
        except TimeoutError:
            mode = executor_cfg.get("mode", "claude_subscription")
            hint = "databricks_auth" if mode == "databricks" else "claude_auth"
            yield _encode(
                session_failed_frame(
                    f"Authentication did not complete within {_AUTH_TIMEOUT_S:.0f}s.",
                    hint,
                )
            )
            return

        session_id, created, resume = await ensure_claude_session(
            project_root, executor_cfg
        )
        yield _encode({"type": "session", "session_id": session_id, "created": created})
        await _persist_item(project_root, session_id, user_message_envelope(text))

        handle = registry.begin_turn(session_id)
        state = TranslationState()
        queue: asyncio.Queue[Any] = asyncio.Queue()
        interrupt_tasks: set[asyncio.Task[Any]] = set()
        factory: ClientFactory = client_factory or (
            lambda options: ClaudeSDKClient(options=options)
        )
        options = _build_options(
            project_root,
            auth.model,
            auth.env,
            resume,
            _make_can_use_tool(
                session_id,
                registry,
                handle,
                queue,
                interrupt_tasks,
                permission_mode=permission_mode,
            ),
        )
        client: Optional[ClaudeClientProtocol] = None
        pump_task: Optional["asyncio.Task[None]"] = None
        try:
            client = factory(options)
            try:
                await client.connect()
                handle.interrupt = client.interrupt
                await client.query(text)
            except CLINotFoundError:
                logger.exception("assistant chat: bundled Claude runtime not found")
                yield _encode(
                    session_failed_frame(
                        "The Claude runtime bundled with claude-agent-sdk was "
                        "not found. Reinstall with `pip install "
                        "'lakehouse-plumber[webapp]'`.",
                        "claude_setup",
                    )
                )
                return
            except ClaudeSDKError:
                # E.g. the subprocess died on startup (spike-verified: a dead
                # resume handle exits the CLI before any message). The finally
                # clears the stored resume so the next turn starts clean.
                logger.exception("assistant chat: SDK client failed to start")
                yield _encode(_runtime_lost_frame())
                return
            pump_task = asyncio.create_task(
                _pump(client, state, handle, queue, project_root, session_id)
            )

            loop = asyncio.get_running_loop()
            interrupt_deadline: Optional[float] = None
            while True:
                if handle.interrupt_requested and interrupt_deadline is None:
                    interrupt_deadline = loop.time() + _INTERRUPT_DRAIN_CEILING_S
                if (
                    interrupt_deadline is not None
                    and loop.time() > interrupt_deadline
                    and state.terminal is None
                ):
                    state.terminal = "interrupted"
                    yield _encode({"type": "interrupted"})
                    break
                try:
                    entry = await asyncio.wait_for(queue.get(), _HEARTBEAT_INTERVAL_S)
                except TimeoutError:
                    yield _encode({"type": "heartbeat"})
                    continue
                if entry is _STREAM_END:
                    break
                yield _encode(entry)

            if state.terminal is None:
                if handle.interrupt_requested:
                    state.terminal = "interrupted"
                    yield _encode({"type": "interrupted"})
                else:
                    logger.error(
                        f"assistant chat: stream ended without a result "
                        f"(session {session_id})"
                    )
                    yield _encode(_runtime_lost_frame())
        finally:
            # GeneratorExit-safe teardown: awaits are fine here, yields are
            # not. Disconnect kills the per-turn subprocess — on client
            # disconnect this interrupts the turn (see module docstring).
            if pump_task is not None:
                pump_task.cancel()
                with contextlib.suppress(BaseException):
                    await pump_task
            for task in list(interrupt_tasks):
                task.cancel()
            registry.end_turn(session_id)
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.disconnect()
            try:
                if state.sdk_session_id:
                    await asyncio.to_thread(
                        assistant_store.set_runtime_session_id,
                        project_root,
                        session_id,
                        state.sdk_session_id,
                    )
                elif resume is not None and state.terminal in (None, "failed"):
                    # The stored resume handle produced no run (likely a dead
                    # transcript): clear it so the next turn starts clean.
                    await asyncio.to_thread(
                        assistant_store.set_runtime_session_id,
                        project_root,
                        session_id,
                        None,
                    )
                await asyncio.to_thread(
                    assistant_store.touch_session, project_root, session_id
                )
            except Exception:
                # Best-effort bookkeeping: never mask the turn's outcome.
                logger.exception(
                    f"assistant chat: post-turn bookkeeping failed "
                    f"(session {session_id})"
                )
