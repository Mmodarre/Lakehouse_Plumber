"""AI assistant router — exposes OpenCode status and session management.

The frontend uses these endpoints to:
1. Discover whether the AI assistant is available (``GET /api/ai/status``)
2. Create a session scoped to the correct project directory (``POST /api/ai/session``)
3. Clean up sessions (``DELETE /api/ai/session/{session_id}``)

The critical security boundary is in ``POST /api/ai/session``: the backend
resolves the project directory using the same auth + workspace logic as all
other endpoints (``get_project_root_adaptive``), so the frontend never chooses
which directory the AI can access.
"""

import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from lhp.api.config import APISettings
from lhp.api.dependencies import get_project_root_adaptive, get_settings
from lhp.api.schemas.ai import (
    AISessionDeleteResponse,
    AISessionResponse,
    AIStatusResponse,
)
from lhp.api.services.opencode_manager import OpenCodeManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ai", tags=["ai"])


def _get_opencode_manager(request: Request) -> Optional[OpenCodeManager]:
    """Retrieve the OpenCodeManager from app state, or None if AI is disabled."""
    return getattr(request.app.state, "opencode_manager", None)


@router.get("/status", response_model=AIStatusResponse)
async def ai_status(
    request: Request,
    settings: APISettings = Depends(get_settings),
) -> AIStatusResponse:
    """Check whether the AI assistant is available.

    The frontend polls this on startup to decide whether to show the
    chat panel.  Returns quickly regardless of OpenCode server state.
    """
    if not settings.ai_enabled:
        return AIStatusResponse(available=False)

    mgr = _get_opencode_manager(request)
    if mgr is None:
        return AIStatusResponse(available=False)

    return AIStatusResponse(
        available=mgr.available,
        url=mgr.url if mgr.available else None,
        auth_required=mgr.auth_required,
    )


@router.post("/session", response_model=AISessionResponse)
async def create_session(
    request: Request,
    project_root: Path = Depends(get_project_root_adaptive),
    settings: APISettings = Depends(get_settings),
) -> AISessionResponse:
    """Create an OpenCode session scoped to the user's project directory.

    In dev mode, the session is scoped to ``settings.project_root``.
    In production, it's scoped to the user's workspace clone directory.
    The frontend never chooses the directory — the backend resolves it
    using the same auth + workspace logic as all other endpoints.

    In production mode, this also deploys OpenCode config files
    (``.opencode/`` agent + instructions) into the workspace if they
    don't already exist.
    """
    mgr = _get_opencode_manager(request)
    if mgr is None:
        raise HTTPException(
            status_code=503,
            detail="AI assistant is not available",
        )

    # Ensure OpenCode is running with cwd=project_root.
    # In dev mode, this is a no-op (already started with correct cwd).
    # In production mode, this deploys config and (re)starts the subprocess
    # so the AI operates on the user's workspace files.
    await mgr.ensure_workspace(project_root)

    if not mgr.available:
        raise HTTPException(
            status_code=503,
            detail="AI assistant failed to start for this workspace",
        )

    return AISessionResponse(
        session_id="pending",  # Created on frontend via OpenCode API
        project_root=str(project_root),
    )


@router.delete("/session/{session_id}", response_model=AISessionDeleteResponse)
async def delete_session(
    session_id: str,
    request: Request,
) -> AISessionDeleteResponse:
    """Clean up an OpenCode session.

    Called when the user closes a chat session or the workspace is destroyed.
    """
    mgr = _get_opencode_manager(request)
    if mgr is None or not mgr.available:
        # If OpenCode is down, the session is already gone
        return AISessionDeleteResponse(deleted=True)

    # Session cleanup will be handled via OpenCode SDK when available.
    # For now, this is a no-op acknowledgement.
    logger.debug(f"Session {session_id} cleanup requested")
    return AISessionDeleteResponse(deleted=True)
