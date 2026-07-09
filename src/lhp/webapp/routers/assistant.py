"""Assistant panel router — status, provisioning, chat, approvals, interrupts.

HTTP surface over the assistant services, dispatching per stored executor
``provider``: the default in-process Claude SDK provider
(:mod:`~lhp.webapp.services.claude_sdk_chat` and friends) or the
user-managed Omnigent daemon (:mod:`lhp.webapp.services.omnigent_client` /
:mod:`~lhp.webapp.services.omnigent_lifecycle` /
:mod:`~lhp.webapp.services.assistant_provision` /
:mod:`~lhp.webapp.services.assistant_chat`); this module only wires them to
routes. The config-ish endpoints (``/config``, ``/databricks-profiles``,
``/skill``) live in :mod:`lhp.webapp.routers.assistant_config` and the
session-lifecycle endpoints (``/session``, ``/sessions``, ``/session/new``,
``/session/archive``) in :mod:`lhp.webapp.routers.assistant_sessions`
(mechanical size splits; same prefix). Per the ``webapp-uses-public-api``
contract this
module imports ONLY :mod:`lhp.api` from the ``lhp`` package (``LHPError``
translation happens in the app-level exception handler, so ``lhp.errors``
is not needed here).

Error surface conventions:

* Absent resources (no stored config, no active session) -> plain 404
  ``HTTPException``, like every other router.
* The chat gates (executor unconfigured / skill not installed / host
  offline) -> 409 carrying the repo's ``ErrorDetail`` envelope with the
  webapp-scoped gate codes below. The panel pre-gates on ``/status``; these
  are the backstop.
* :exc:`OmnigentUnavailable` NEVER leaks: daemon-down is a 503 with setup
  guidance; omnigent HTTP error statuses translate to 404/502.
* ``LHPError`` (e.g. ``LHP-CFG-011`` from the skill installer) propagates to
  the app-level handler, which renders the standard error envelope.

Concurrency: ``/approval`` and ``/interrupt`` talk to the omnigent client /
the Claude turn registry directly and NEVER touch a chat-turn lock, so both
stay callable while a chat stream is open on another request.

Store-backed endpoints 409 in ``no_project`` state (the lifespan only runs
DB migrations for a real project — same posture as the runs router).

ROUTER CONVENTION: routes carry their sub-path under ``/assistant``; the app
mounts this router with ``prefix="/api"`` (TokenGuard / OriginGuard /
TrustedHost apply to all ``/api/*`` routes automatically).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from lhp.webapp.dependencies import get_project_root
from lhp.webapp.routers._guards import assert_project_loaded
from lhp.webapp.schemas.assistant import (
    ActiveSessionInfo,
    ApprovalRequest,
    AssistantStatus,
    ChatRequest,
    DaemonStartResponse,
    InterruptRequest,
)
from lhp.webapp.schemas.common import ErrorDetail, ErrorResponse, SuccessResponse
from lhp.webapp.services import assistant_store, claude_sdk_auth, omnigent_lifecycle
from lhp.webapp.services.assistant_chat import chat_turn
from lhp.webapp.services.assistant_provision import installed_skill_version
from lhp.webapp.services.claude_sdk_bridge import get_claude_turns
from lhp.webapp.services.claude_sdk_chat import chat_turn as claude_chat_turn
from lhp.webapp.services.claude_sdk_policy import record_always_allow_rule
from lhp.webapp.services.omnigent_client import (
    OmnigentUnavailable,
    get_omnigent_client,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/assistant", tags=["assistant"])

_NDJSON_MEDIA_TYPE = "application/x-ndjson"

#: Webapp-scoped HTTP gate codes for the chat-precondition 409s. These are
#: NOT ``lhp.errors`` registry codes (the gates are HTTP adapter state, not
#: domain failures); the ``LHP-WEB-`` namespace cannot collide with the
#: domain categories (CF/VAL/IO/CFG/DEP/ACT/GEN).
_CODE_EXECUTOR_UNCONFIGURED = "LHP-WEB-001"
_CODE_SKILL_NOT_INSTALLED = "LHP-WEB-002"
_CODE_HOST_OFFLINE = "LHP-WEB-003"


def _gate_409(
    code: str, message: str, details: str, suggestions: list[str]
) -> JSONResponse:
    """A chat-gate 409 carrying the repo's ``ErrorDetail`` envelope."""
    envelope = ErrorResponse(
        error=ErrorDetail(
            code=code,
            # Nearest existing ErrorCategory value — no WEB category exists
            # by design (LHP-WEB-* codes are HTTP-adapter discriminators,
            # not domain-registry codes).
            category="CFG",
            message=message,
            details=details,
            suggestions=suggestions,
            context={},
            http_status=409,
        )
    )
    return JSONResponse(status_code=409, content=envelope.model_dump())


def _daemon_down_error() -> HTTPException:
    """503 translation for :exc:`OmnigentUnavailable` (which never leaks)."""
    return HTTPException(
        503,
        "The omnigent daemon is not reachable. Start it from the assistant "
        "panel (or run 'omnigent server start' and 'omnigent host') and retry.",
    )


def _omnigent_http_error(exc: httpx.HTTPStatusError) -> HTTPException:
    """Translate an omnigent HTTP error status; a raw 500 never surfaces."""
    if exc.response.status_code == 404:
        return HTTPException(404, "Assistant session not found on the omnigent daemon")
    return HTTPException(
        502, f"omnigent daemon rejected the request (HTTP {exc.response.status_code})"
    )


async def _resolve_session(
    project_root: Path, session_id: str | None
) -> dict[str, Any]:
    """The targeted session row: explicit id, or the MRU active fallback.

    Explicit unknown ids and an empty active set both raise the 404-shaped
    absence.
    """
    if session_id is not None:
        row = await asyncio.to_thread(
            assistant_store.get_session, project_root, session_id
        )
        if row is None:
            raise HTTPException(404, "No assistant session with that id")
        return row
    active = await asyncio.to_thread(assistant_store.get_active_session, project_root)
    if active is None:
        raise HTTPException(404, "No active assistant session")
    return active


def _provider_of(config: dict[str, Any] | None) -> str:
    """Provider discriminator; configs stored pre-provider read as omnigent."""
    if config is None:
        return "omnigent"
    return str(config.get("provider", "omnigent"))


@router.get("/status", response_model=AssistantStatus)
async def get_status(
    request: Request, project_root: Path = Depends(get_project_root)
) -> AssistantStatus:
    """The assistant panel's single source of truth.

    Omnigent provider: a daemon that is down reports as falsy ladder fields
    (never a 500) — the detection ladder swallows connection failures by
    design, and the client getter is deferred so no client is even built when
    the binary is missing. Claude provider: the same ladder fields report the
    in-process SDK's availability (no daemon probe runs at all).
    """
    assert_project_loaded(request, "the assistant is unavailable")
    skill_version = await asyncio.to_thread(installed_skill_version, project_root)
    executor = await asyncio.to_thread(
        assistant_store.get_config, project_root, "executor"
    )
    active_row = await asyncio.to_thread(
        assistant_store.get_active_session, project_root
    )
    active = (
        ActiveSessionInfo(
            session_id=str(active_row["session_id"]),
            title=active_row["title"],
            status=str(active_row["status"]),
            created_at=str(active_row["created_at"]),
            last_used_at=str(active_row["last_used_at"]),
        )
        if active_row is not None
        else None
    )
    provider = _provider_of(executor) if executor is not None else None
    if provider == "claude_sdk":
        sdk_ok = await asyncio.to_thread(claude_sdk_auth.sdk_available)
        return AssistantStatus(
            binary_found=sdk_ok,
            server_ok=sdk_ok,
            host_online=sdk_ok,
            host_id="local" if sdk_ok else None,
            server_url="",  # in-process: there is no daemon URL
            skill_installed=skill_version is not None,
            skill_version=skill_version,
            executor_configured=True,
            active_session=active,
            provider=provider,
        )
    daemon = await omnigent_lifecycle.detect(
        lambda: get_omnigent_client(request.app), request.app.state.settings
    )
    return AssistantStatus(
        binary_found=daemon.binary_found,
        server_ok=daemon.server_ok,
        host_online=daemon.host_online,
        host_id=daemon.host_id,
        server_url=daemon.base_url,
        skill_installed=skill_version is not None,
        skill_version=skill_version,
        executor_configured=executor is not None,
        active_session=active,
        provider=provider,
    )


@router.post("/daemon/start", response_model=DaemonStartResponse)
async def start_daemon(
    request: Request, project_root: Path = Depends(get_project_root)
) -> DaemonStartResponse:
    """Spawn the detached omnigent daemon processes (fire-and-forget)."""
    assert_project_loaded(request, "the assistant is unavailable")
    try:
        await asyncio.to_thread(omnigent_lifecycle.start_daemon, project_root)
    except OSError:
        logger.exception("Failed to spawn the omnigent daemon processes")
        return DaemonStartResponse(
            started=False,
            detail="Could not spawn 'omnigent' — is it installed and on PATH?",
        )
    return DaemonStartResponse(started=True, detail=None)


@router.post("/chat", response_model=None)
async def chat(
    body: ChatRequest,
    request: Request,
    project_root: Path = Depends(get_project_root),
) -> Response:
    """Run one assistant chat turn as an NDJSON stream.

    Backstop gates (the panel pre-gates on ``/status``): executor
    unconfigured and skill not installed 409 for BOTH providers; the
    daemon-online gate applies to the omnigent provider only (the Claude
    provider has no daemon). Past the gates, the whole turn — session
    provisioning included — is relayed by the provider's ``chat_turn``,
    whose frame protocol is pinned in its module docstring.
    """
    assert_project_loaded(request, "the assistant is unavailable")
    executor_cfg: dict[str, Any] | None = await asyncio.to_thread(
        assistant_store.get_config, project_root, "executor"
    )
    if executor_cfg is None:
        return _gate_409(
            _CODE_EXECUTOR_UNCONFIGURED,
            "Assistant executor is not configured",
            "Chat requires an executor configuration before the first turn.",
            ["Configure the executor in the assistant panel settings"],
        )
    if await asyncio.to_thread(installed_skill_version, project_root) is None:
        return _gate_409(
            _CODE_SKILL_NOT_INSTALLED,
            "LHP skill is not installed in this project",
            "Chat requires the packaged LHP skill in .claude/skills/lhp/.",
            ["Install the skill from the assistant panel (POST /api/assistant/skill)"],
        )
    if _provider_of(executor_cfg) == "claude_sdk":
        claude_frames = claude_chat_turn(
            project_root,
            executor_cfg,
            body.message,
            get_claude_turns(request.app),
            session_id=body.session_id,
            permission_mode=body.permission_mode,
        )
        return StreamingResponse(claude_frames, media_type=_NDJSON_MEDIA_TYPE)
    daemon = await omnigent_lifecycle.detect(
        lambda: get_omnigent_client(request.app), request.app.state.settings
    )
    if not daemon.host_online or daemon.host_id is None:
        return _gate_409(
            _CODE_HOST_OFFLINE,
            "No omnigent host is online",
            daemon.detail or "The omnigent daemon has no online host.",
            ["Start the daemon from the assistant panel and retry"],
        )
    frames = chat_turn(
        get_omnigent_client(request.app),
        project_root,
        executor_cfg,
        daemon.host_id,
        body.message,
    )
    return StreamingResponse(frames, media_type=_NDJSON_MEDIA_TYPE)


@router.post("/approval", response_model=SuccessResponse)
async def resolve_approval(
    body: ApprovalRequest,
    request: Request,
    project_root: Path = Depends(get_project_root),
) -> SuccessResponse:
    """Resolve a pending elicitation on one session's live turn.

    ``body.session_id`` targets a tab's session; absent, the MRU active
    session is assumed (pre-multi-tab clients). Never touches a chat-turn
    lock, so approvals work while a chat stream is open on another request.
    Claude provider: resolves the in-process turn registry (unknown /
    already-resolved elicitations 404). ``accept`` with ``always_allow``
    persists the rule RE-DERIVED from the registry-recorded tool call BEFORE
    resolving; the client echo is never trusted. Omnigent: talks to the
    daemon client directly.
    """
    assert_project_loaded(request, "the assistant is unavailable")
    active = await _resolve_session(project_root, body.session_id)
    session_id = str(active["session_id"])
    if active.get("provider") == "claude_sdk":
        registry = get_claude_turns(request.app)
        if body.action == "accept" and body.always_allow:
            entry = registry.peek_approval(session_id, body.elicitation_id)
            if entry is not None:
                await asyncio.to_thread(
                    record_always_allow_rule,
                    project_root,
                    entry.tool_name,
                    entry.tool_input,
                )
        resolved = registry.resolve_approval(
            session_id, body.elicitation_id, body.action
        )
        if not resolved:
            raise HTTPException(404, "No pending approval with that elicitation id")
        return SuccessResponse(
            message="Approval resolved",
            details={"session_id": session_id, "action": body.action},
        )
    client = get_omnigent_client(request.app)
    try:
        await client.resolve_elicitation(
            session_id, body.elicitation_id, body.action, body.content
        )
    except OmnigentUnavailable as exc:
        raise _daemon_down_error() from exc
    except httpx.HTTPStatusError as exc:
        raise _omnigent_http_error(exc) from exc
    return SuccessResponse(
        message="Approval resolved",
        details={"session_id": session_id, "action": body.action},
    )


@router.post("/interrupt", response_model=SuccessResponse)
async def interrupt(
    request: Request,
    body: InterruptRequest | None = None,
    project_root: Path = Depends(get_project_root),
) -> SuccessResponse:
    """Interrupt one session's running turn (no chat-turn lock).

    The body is optional: ``session_id`` targets a tab's turn; no body (or
    ``null``) falls back to the MRU active session — legacy clients POST
    nothing. Claude provider: interrupting with no live turn is a harmless
    no-op (``delivered: false``) — the stop button can race a turn that just
    finished.
    """
    assert_project_loaded(request, "the assistant is unavailable")
    active = await _resolve_session(
        project_root, body.session_id if body is not None else None
    )
    session_id = str(active["session_id"])
    if active.get("provider") == "claude_sdk":
        delivered = await get_claude_turns(request.app).request_interrupt(session_id)
        return SuccessResponse(
            message="Interrupt requested",
            details={"session_id": session_id, "delivered": delivered},
        )
    client = get_omnigent_client(request.app)
    try:
        await client.post_interrupt(session_id)
    except OmnigentUnavailable as exc:
        raise _daemon_down_error() from exc
    except httpx.HTTPStatusError as exc:
        raise _omnigent_http_error(exc) from exc
    return SuccessResponse(
        message="Interrupt queued", details={"session_id": session_id}
    )
