"""Assistant panel router — daemon status, executor config, provisioning, chat.

HTTP surface over the P5 assistant services. All omnigent protocol knowledge
lives in :mod:`lhp.webapp.services.omnigent_client` /
:mod:`~lhp.webapp.services.omnigent_lifecycle` /
:mod:`~lhp.webapp.services.assistant_provision` /
:mod:`~lhp.webapp.services.assistant_chat`; this module only wires them to
routes. Per the ``webapp-uses-public-api`` contract it imports ONLY
:mod:`lhp.api` from the ``lhp`` package (``LHPError`` translation happens in
the app-level exception handler, so ``lhp.errors`` is not needed here).

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

Concurrency: ``/approval`` and ``/interrupt`` call the omnigent client
directly and NEVER touch :mod:`assistant_chat`'s turn lock, so both stay
callable while a chat stream is open on another request.

Store-backed endpoints 409 in ``no_project`` state (the lifespan only runs
DB migrations for a real project — same posture as the runs router).
``/databricks-profiles`` (reads ``~/.databrickscfg`` only) and ``/skill``
(the facade's ``LHP-CFG-011`` already reports a non-project root) are exempt.

ROUTER CONVENTION: routes carry their sub-path under ``/assistant``; the app
mounts this router with ``prefix="/api"`` (TokenGuard / OriginGuard /
TrustedHost apply to all ``/api/*`` routes automatically).
"""

from __future__ import annotations

import asyncio
import configparser
import logging
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from lhp.api import SkillFacade, SkillInstallResult
from lhp.webapp.dependencies import get_project_root
from lhp.webapp.schemas.assistant import (
    ActiveSessionInfo,
    ApprovalRequest,
    AssistantStatus,
    ChatRequest,
    DaemonStartResponse,
    DatabricksProfilesResponse,
    ExecutorConfig,
    ExecutorConfigUpdate,
    SessionListItem,
    SessionListResponse,
    SessionSnapshot,
    SkillInstallResponse,
)
from lhp.webapp.schemas.common import ErrorDetail, ErrorResponse, SuccessResponse
from lhp.webapp.services import assistant_store, omnigent_lifecycle
from lhp.webapp.services.assistant_chat import chat_turn
from lhp.webapp.services.assistant_provision import installed_skill_version
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


def _assert_project_loaded(request: Request) -> None:
    """Reject store-backed assistant access in ``no_project`` state."""
    if getattr(request.app.state, "project_state", "ok") != "ok":
        raise HTTPException(409, "No LHP project loaded; the assistant is unavailable")


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


async def _require_active_session(project_root: Path) -> str:
    """Return the active session id, or raise the 404-shaped absence."""
    active = await asyncio.to_thread(assistant_store.get_active_session, project_root)
    if active is None:
        raise HTTPException(404, "No active assistant session")
    return str(active["session_id"])


@router.get("/status", response_model=AssistantStatus)
async def get_status(
    request: Request, project_root: Path = Depends(get_project_root)
) -> AssistantStatus:
    """The assistant panel's single source of truth.

    A daemon that is down reports as falsy ladder fields (never a 500): the
    detection ladder swallows connection failures by design, and the client
    getter is deferred so no client is even built when the binary is missing.
    """
    _assert_project_loaded(request)
    daemon = await omnigent_lifecycle.detect(
        lambda: get_omnigent_client(request.app), request.app.state.settings
    )
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
    )


@router.get("/config", response_model=ExecutorConfig)
async def get_executor_config(
    request: Request, project_root: Path = Depends(get_project_root)
) -> ExecutorConfig:
    """Return the stored executor config; 404 when none has been set."""
    _assert_project_loaded(request)
    stored = await asyncio.to_thread(
        assistant_store.get_config, project_root, "executor"
    )
    if stored is None:
        raise HTTPException(404, "Executor config is not set")
    return ExecutorConfig(**stored)


@router.put("/config", response_model=ExecutorConfig)
async def put_executor_config(
    body: ExecutorConfigUpdate,
    request: Request,
    project_root: Path = Depends(get_project_root),
) -> ExecutorConfig:
    """Store the executor config; echo back EXACTLY what was stored.

    Switching executors marks the active session stale so the next chat
    turn reprovisions against the new config (bundle-hash drift would catch
    it anyway; the stale mark makes the drift explicit and immediate).
    """
    _assert_project_loaded(request)
    stored = body.model_dump()
    previous = await asyncio.to_thread(
        assistant_store.get_config, project_root, "executor"
    )
    await asyncio.to_thread(
        assistant_store.put_config, project_root, "executor", stored
    )
    if previous != stored:
        active = await asyncio.to_thread(
            assistant_store.get_active_session, project_root
        )
        if active is not None:
            await asyncio.to_thread(
                assistant_store.mark_stale, project_root, str(active["session_id"])
            )
            logger.info(
                f"Executor config changed; marked assistant session "
                f"{active['session_id']} stale"
            )
    return ExecutorConfig(**stored)


@router.get("/databricks-profiles", response_model=DatabricksProfilesResponse)
def databricks_profiles() -> DatabricksProfilesResponse:
    """Section names from ``~/.databrickscfg`` — names only, never values.

    ``DEFAULT`` is included only when it has content (configparser keeps it
    out of ``sections()``; ``defaults()`` exposes its keys). A missing or
    unparseable file yields an empty list.
    """
    path = Path.home() / ".databrickscfg"
    if not path.is_file():
        return DatabricksProfilesResponse(profiles=[])
    parser = configparser.ConfigParser()
    try:
        parser.read(path, encoding="utf-8")
    except (configparser.Error, UnicodeDecodeError):
        logger.warning(f"Could not parse {path}; reporting no profiles")
        return DatabricksProfilesResponse(profiles=[])
    profiles = list(parser.sections())
    if parser.defaults():
        profiles.insert(0, "DEFAULT")
    return DatabricksProfilesResponse(profiles=profiles)


def _install_skill(project_root: Path) -> SkillInstallResult:
    """Force-install the packaged LHP skill (thread-bridged by the route)."""
    return SkillFacade(project_root).install_project_skill(force=True)


@router.post("/skill", response_model=SkillInstallResponse)
async def install_skill(
    project_root: Path = Depends(get_project_root),
) -> SkillInstallResponse:
    """Install (force-refresh) the LHP skill into the project.

    ``LHPError`` (``LHP-CFG-011`` for a non-project root) propagates to the
    app-level handler, which renders the repo's standard error envelope.
    """
    result = await asyncio.to_thread(_install_skill, project_root)
    return SkillInstallResponse(
        install_dir=str(result.install_dir),
        skill_version=result.skill_version,
        action=result.action,
    )


@router.post("/daemon/start", response_model=DaemonStartResponse)
async def start_daemon(
    request: Request, project_root: Path = Depends(get_project_root)
) -> DaemonStartResponse:
    """Spawn the detached omnigent daemon processes (fire-and-forget)."""
    _assert_project_loaded(request)
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
    unconfigured, skill not installed, or no online omnigent host each 409
    with the ``ErrorDetail`` envelope. Past the gates, the whole turn —
    session provisioning included — is relayed by
    :func:`~lhp.webapp.services.assistant_chat.chat_turn`, whose frame
    protocol is pinned in its module docstring.
    """
    _assert_project_loaded(request)
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
    """Resolve a pending elicitation on the active session.

    Talks to the omnigent client directly (no chat-turn lock), so approvals
    work while a chat stream is open on another request.
    """
    _assert_project_loaded(request)
    session_id = await _require_active_session(project_root)
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
    request: Request, project_root: Path = Depends(get_project_root)
) -> SuccessResponse:
    """Interrupt the active session's running turn (no chat-turn lock)."""
    _assert_project_loaded(request)
    session_id = await _require_active_session(project_root)
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


@router.get("/session", response_model=SessionSnapshot)
async def get_session(
    request: Request, project_root: Path = Depends(get_project_root)
) -> SessionSnapshot:
    """Snapshot of the active session for panel rehydration.

    ``items`` pass through UNMODIFIED in omnigent's snapshot envelope shape
    (spike S8); the client-side renderer unwraps each item's ``data``.
    """
    _assert_project_loaded(request)
    session_id = await _require_active_session(project_root)
    client = get_omnigent_client(request.app)
    try:
        snapshot = await client.get_session(session_id, include_items=True)
    except OmnigentUnavailable as exc:
        raise _daemon_down_error() from exc
    except httpx.HTTPStatusError as exc:
        raise _omnigent_http_error(exc) from exc
    return SessionSnapshot(
        session_id=str(snapshot.get("session_id", session_id)),
        title=snapshot.get("title"),
        status=str(snapshot.get("status", "")),
        items=list(snapshot.get("items", [])),
    )


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(
    request: Request, project_root: Path = Depends(get_project_root)
) -> SessionListResponse:
    """List locally-tracked assistant sessions, most recently used first."""
    _assert_project_loaded(request)
    rows = await asyncio.to_thread(assistant_store.list_sessions, project_root)
    sessions = [
        SessionListItem(
            session_id=str(row["session_id"]),
            title=row["title"],
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            last_used_at=str(row["last_used_at"]),
        )
        for row in rows
    ]
    return SessionListResponse(sessions=sessions, total=len(sessions))


@router.post("/session/new", response_model=SuccessResponse)
async def new_session(
    request: Request, project_root: Path = Depends(get_project_root)
) -> SuccessResponse:
    """Archive the active session so the next chat turn starts a fresh one.

    Nothing is created here — provisioning happens lazily on the next chat
    turn (idempotent when no session is active).
    """
    _assert_project_loaded(request)
    archived = await asyncio.to_thread(assistant_store.archive_active, project_root)
    return SuccessResponse(
        message=(
            "Active session archived" if archived else "No active session to archive"
        ),
        details={"archived": bool(archived)},
    )
