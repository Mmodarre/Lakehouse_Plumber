"""Tool-permission policy for the Claude SDK assistant provider.

Builds the per-turn ``can_use_tool`` callback that
:mod:`~lhp.webapp.services.claude_sdk_chat` hands to the SDK: which tools
are silently allowed per permission mode, which Bash commands are provably
read-only (:mod:`~lhp.webapp.services.claude_sdk_bash_policy`), which calls
match the project's persisted always-allow rules, and how everything else
parks on the approval bridge.

Always-allow rules live in the ``assistant_config`` store under the
``"permissions"`` key as ``{"always_allow": [{"tool": ..., "prefix": ...}]}``
(see :class:`~lhp.webapp.schemas.assistant.PermissionsConfig`). The rule a
prompt OFFERS and the rule the approval endpoint PERSISTS are both derived
server-side by :func:`derive_always_allow_rule` — the client echo is never
trusted.

:stability: internal
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
from pathlib import Path
from typing import Any, Optional

from claude_agent_sdk import PermissionResultAllow, PermissionResultDeny
from claude_agent_sdk.types import PermissionResult, ToolPermissionContext

from lhp.webapp.services import assistant_store
from lhp.webapp.services.claude_sdk_bash_policy import is_read_only_command
from lhp.webapp.services.claude_sdk_bridge import ClaudeTurnRegistry, TurnHandle
from lhp.webapp.services.claude_sdk_translate import PREVIEW_MAX_CHARS

logger = logging.getLogger(__name__)

#: How long an ``approval.request`` may park before it is denied.
_APPROVAL_TIMEOUT_S = 300.0

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

#: Permission modes in which the read-only-Bash fast path and the persisted
#: always-allow rules apply. An UNKNOWN mode is deliberately excluded: it
#: degrades to prompt-for-everything, never toward allowing.
_PROMPTING_MODES = frozenset({"default", "acceptEdits"})


def derive_always_allow_rule(
    tool_name: str, tool_input: dict[str, Any]
) -> Optional[dict[str, Optional[str]]]:
    """The ``{tool, prefix}`` always-allow rule for one tool call, or ``None``.

    Single source of truth for BOTH the offer attached to an
    ``approval.request`` and the rule the approval endpoint persists —
    deriving each from the same recorded call keeps them from drifting.
    Non-Bash tools yield a whole-tool rule (``prefix`` ``None``). Bash yields
    a prefix rule from the command's first two shlex tokens; a command with
    fewer than two tokens (or unparseable quoting) yields ``None`` — never a
    bare allow-all-Bash rule.
    """
    if tool_name != "Bash":
        return {"tool": tool_name, "prefix": None}
    command = tool_input.get("command")
    if not isinstance(command, str):
        return None
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    if len(tokens) < 2:
        return None
    return {"tool": "Bash", "prefix": f"{tokens[0]} {tokens[1]}"}


def _offer_label(rule: dict[str, Optional[str]]) -> str:
    prefix = rule.get("prefix")
    if prefix is not None:
        return f'Always allow "{prefix}"'
    return f"Always allow {rule['tool']}"


def _rule_allows(
    rule: dict[str, Any], tool_name: str, tool_input: dict[str, Any]
) -> bool:
    """Whether one stored always-allow rule covers this tool call.

    Bash rules require a non-empty ``prefix`` and match word-boundary
    guarded (``command == prefix`` or ``command.startswith(prefix + " ")``)
    — never by substring. A hand-written bare-Bash rule (no prefix) is
    ignored, fail-closed. Non-Bash rules match the whole tool and must not
    carry a prefix.
    """
    if rule.get("tool") != tool_name:
        return False
    prefix = rule.get("prefix")
    if tool_name == "Bash":
        if not isinstance(prefix, str) or not prefix:
            return False
        command = tool_input.get("command")
        if not isinstance(command, str):
            return False
        return command == prefix or command.startswith(prefix + " ")
    return prefix is None


def _stored_rules(project_root: Path) -> list[dict[str, Any]]:
    """The persisted always-allow rules (malformed entries dropped)."""
    stored = assistant_store.get_config(project_root, "permissions")
    if not stored:
        return []
    rules = stored.get("always_allow")
    if not isinstance(rules, list):
        return []
    return [rule for rule in rules if isinstance(rule, dict)]


def record_always_allow_rule(
    project_root: Path, tool_name: str, tool_input: dict[str, Any]
) -> bool:
    """Persist the server-derived always-allow rule for a recorded tool call.

    Synchronous (callers bridge via ``asyncio.to_thread``). Re-derives the
    rule from the recorded ``tool_name``/``tool_input`` with
    :func:`derive_always_allow_rule`; returns ``False`` (persisting nothing)
    when no valid rule derives. Already-present rules are not duplicated.
    """
    rule = derive_always_allow_rule(tool_name, tool_input)
    if rule is None:
        return False
    stored = assistant_store.get_config(project_root, "permissions") or {}
    rules = [r for r in stored.get("always_allow", []) if isinstance(r, dict)]
    if any(
        r.get("tool") == rule["tool"] and r.get("prefix") == rule["prefix"]
        for r in rules
    ):
        return True
    rules.append(rule)
    stored["always_allow"] = rules
    assistant_store.put_config(project_root, "permissions", stored)
    logger.debug(f"Persisted always-allow rule {rule}")
    return True


def _approval_params(
    tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
) -> dict[str, Any]:
    """The ``approval.request`` params the frontend approval card renders.

    ``always_allow_offer`` is the SERVER-derived rule the card may offer as
    a third action; when no valid rule derives (e.g. a one-token Bash
    command) the key is omitted and the card shows no such button.
    """
    preview = json.dumps(tool_input, separators=(",", ":"), default=str)
    params: dict[str, Any] = {
        "message": context.title or f"Claude wants to use {tool_name}",
        "phase": "tool_use",
        "policy_name": context.display_name or tool_name,
        "content_preview": preview[:PREVIEW_MAX_CHARS],
        "tool_name": tool_name,
    }
    rule = derive_always_allow_rule(tool_name, tool_input)
    if rule is not None:
        params["always_allow_offer"] = {**rule, "label": _offer_label(rule)}
    return params


def make_can_use_tool(
    project_root: Path,
    session_id: str,
    registry: ClaudeTurnRegistry,
    handle: TurnHandle,
    queue: "asyncio.Queue[Any]",
    interrupt_tasks: set["asyncio.Task[Any]"],
    permission_mode: str = "default",
) -> Any:
    """Build the per-turn ``can_use_tool`` approval-policy callback.

    ``permission_mode`` widens the silent-allow policy (see the engine's
    module docstring); an unknown value degrades to ``default`` — ask for
    everything non-read-only — never to more permissive.

    Decision order per call: bypass / auto-allowed set -> interrupted-turn
    deny -> read-only Bash classifier -> persisted always-allow rules
    (read FRESH from the store on every call, so a rule saved mid-turn
    covers the very next call) -> approval prompt.
    """
    bypass = permission_mode == "bypassPermissions"
    auto_allowed = AUTO_ALLOWED_TOOLS
    if permission_mode == "acceptEdits":
        auto_allowed = AUTO_ALLOWED_TOOLS | EDIT_TOOLS
    rules_apply = permission_mode in _PROMPTING_MODES

    async def _matches_stored_rule(tool_name: str, tool_input: dict[str, Any]) -> bool:
        try:
            rules = await asyncio.to_thread(_stored_rules, project_root)
        except Exception:
            # Fail-closed: an unreadable store means no rule matches.
            logger.exception("Failed to read the assistant permissions config")
            return False
        return any(_rule_allows(rule, tool_name, tool_input) for rule in rules)

    async def can_use_tool(
        tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
    ) -> PermissionResult:
        if bypass or tool_name in auto_allowed:
            return PermissionResultAllow()
        if handle.interrupt_requested:
            return PermissionResultDeny(message="Turn was interrupted")
        if rules_apply:
            if tool_name == "Bash":
                command = tool_input.get("command")
                if isinstance(command, str) and is_read_only_command(command):
                    logger.debug(f"Auto-allowing read-only Bash command: {command!r}")
                    return PermissionResultAllow()
            if await _matches_stored_rule(tool_name, tool_input):
                logger.debug(
                    f"Auto-allowing {tool_name} via a persisted always-allow rule"
                )
                return PermissionResultAllow()
        elicitation_id, future = registry.create_approval(
            session_id, tool_name, tool_input
        )
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
