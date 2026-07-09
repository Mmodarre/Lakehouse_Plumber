"""Assistant session-lifecycle router — snapshot, list, new, archive.

Mechanical split from :mod:`lhp.webapp.routers.assistant` (which was nearing
the §3.3 size limit), same pattern as :mod:`~lhp.webapp.routers.assistant_config`:
the session-lifecycle endpoints live here under the SAME ``/assistant``
prefix. Chat / approval / interrupt / status / daemon stay in the assistant
router; error-surface conventions are documented there.

Multi-tab semantics (Claude provider): many sessions can be ``active`` at
once (one per open tab). ``GET /session`` takes an optional ``session_id``
query param — an explicit id is served regardless of status (history views
need archived transcripts); without it the MRU active session is served, as
before. ``POST /session/archive`` closes one tab; ``POST /session/new``
keeps its pre-multi-tab semantics (archive everything active) for the
omnigent provider's single-session flow.

ROUTER CONVENTION: routes carry their sub-path under ``/assistant``; the app
mounts this router with ``prefix="/api"``.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from lhp.webapp.dependencies import get_project_root
from lhp.webapp.routers._guards import assert_project_loaded
from lhp.webapp.routers.assistant import _daemon_down_error, _omnigent_http_error
from lhp.webapp.schemas.assistant import (
    ArchiveSessionRequest,
    SessionListItem,
    SessionListResponse,
    SessionSnapshot,
    UsageTotals,
)
from lhp.webapp.schemas.common import SuccessResponse
from lhp.webapp.services import assistant_store
from lhp.webapp.services.claude_sdk_sessions import snapshot_items
from lhp.webapp.services.omnigent_client import (
    OmnigentUnavailable,
    get_omnigent_client,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/assistant", tags=["assistant"])


@router.get("/session", response_model=SessionSnapshot)
async def get_session(
    request: Request,
    session_id: Optional[str] = None,
    project_root: Path = Depends(get_project_root),
) -> SessionSnapshot:
    """Snapshot of one session for panel (re)hydration.

    Explicit ``session_id``: that row is served whatever its status —
    archived transcripts back the history view. Absent: the MRU active
    session (the pre-multi-tab behavior). ``items`` pass through UNMODIFIED
    in the snapshot envelope shape (spike S8; the Claude provider persists
    the SAME envelope shape in ``assistant_items``); the client-side
    renderer unwraps each item's ``data``. ``resumable`` reports whether a
    stored SDK resume handle exists (Claude rows; omnigent is always
    ``True`` — resume is daemon-managed).
    """
    assert_project_loaded(request, "the assistant is unavailable")
    if session_id is not None:
        row = await asyncio.to_thread(
            assistant_store.get_session, project_root, session_id
        )
        if row is None:
            raise HTTPException(404, "No assistant session with that id")
    else:
        row = await asyncio.to_thread(assistant_store.get_active_session, project_root)
        if row is None:
            raise HTTPException(404, "No active assistant session")
    resolved_id = str(row["session_id"])
    totals = await asyncio.to_thread(
        assistant_store.usage_totals, project_root, resolved_id
    )
    usage = UsageTotals(**totals) if totals is not None else None
    if row.get("provider") == "claude_sdk":
        items = await asyncio.to_thread(snapshot_items, project_root, resolved_id)
        return SessionSnapshot(
            session_id=resolved_id,
            title=row.get("title"),
            status=str(row["status"]),
            items=items,
            usage_totals=usage,
            resumable=bool(row.get("runtime_session_id")),
        )
    client = get_omnigent_client(request.app)
    try:
        snapshot = await client.get_session(resolved_id, include_items=True)
    except OmnigentUnavailable as exc:
        raise _daemon_down_error() from exc
    except httpx.HTTPStatusError as exc:
        raise _omnigent_http_error(exc) from exc
    return SessionSnapshot(
        session_id=str(snapshot.get("session_id", resolved_id)),
        title=snapshot.get("title"),
        status=str(snapshot.get("status", "")),
        items=list(snapshot.get("items", [])),
        usage_totals=usage,
        resumable=True,
    )


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(
    request: Request, project_root: Path = Depends(get_project_root)
) -> SessionListResponse:
    """List locally-tracked assistant sessions, most recently used first.

    All statuses and both providers are returned — the frontend splits
    ``active`` Claude rows into tabs and ``archived`` ones into history.
    Usage totals come from ONE grouped query over all listed ids (no N+1).
    """
    assert_project_loaded(request, "the assistant is unavailable")
    rows = await asyncio.to_thread(assistant_store.list_sessions, project_root)
    totals_by_id = await asyncio.to_thread(
        assistant_store.usage_totals_by_session,
        project_root,
        [str(row["session_id"]) for row in rows],
    )
    sessions = [
        SessionListItem(
            session_id=str(row["session_id"]),
            title=row["title"],
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            last_used_at=str(row["last_used_at"]),
            provider=str(row["provider"]),
            usage_totals=(
                UsageTotals(**totals_by_id[str(row["session_id"])])
                if str(row["session_id"]) in totals_by_id
                else None
            ),
        )
        for row in rows
    ]
    return SessionListResponse(sessions=sessions, total=len(sessions))


@router.post("/session/new", response_model=SuccessResponse)
async def new_session(
    request: Request, project_root: Path = Depends(get_project_root)
) -> SuccessResponse:
    """Archive every active session so the next chat turn starts a fresh one.

    Pre-multi-tab semantics, kept for the omnigent provider's single-session
    flow (the Claude tabs UI archives per-session via ``/session/archive``).
    Nothing is created here — provisioning happens lazily on the next chat
    turn (idempotent when no session is active).
    """
    assert_project_loaded(request, "the assistant is unavailable")
    archived = await asyncio.to_thread(assistant_store.archive_active, project_root)
    return SuccessResponse(
        message=(
            "Active session archived" if archived else "No active session to archive"
        ),
        details={"archived": bool(archived)},
    )


@router.post("/session/archive", response_model=SuccessResponse)
async def archive_session(
    body: ArchiveSessionRequest,
    request: Request,
    project_root: Path = Depends(get_project_root),
) -> SuccessResponse:
    """Archive ONE session by id (a tab was closed).

    Unknown ids 404; archiving a session that is not active (already
    archived, or stale) is a harmless no-op reported via ``archived``.
    """
    assert_project_loaded(request, "the assistant is unavailable")
    row: Optional[dict[str, Any]] = await asyncio.to_thread(
        assistant_store.get_session, project_root, body.session_id
    )
    if row is None:
        raise HTTPException(404, "No assistant session with that id")
    archived = await asyncio.to_thread(
        assistant_store.archive_session, project_root, body.session_id
    )
    return SuccessResponse(
        message="Session archived" if archived else "Session was not active",
        details={"session_id": body.session_id, "archived": bool(archived)},
    )
