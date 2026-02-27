"""AI assistant router — proxy endpoints for OpenCode integration.

All AI traffic routes through these endpoints.  The frontend talks only
to ``/api/ai/*``; the backend forwards requests to the user's OpenCode
process.  This maintains the security boundary: the frontend never
connects to OpenCode directly.

Endpoints:
- ``GET  /api/ai/status``              — pool status + config summary
- ``GET  /api/ai/config``              — allowed providers/models
- ``POST /api/ai/config``              — update provider/model selection
- ``GET  /api/ai/sessions``            — list sessions (proxy)
- ``POST /api/ai/session``             — create session (proxy, accepts mode)
- ``DELETE /api/ai/session/{id}``      — delete session (proxy)
- ``GET  /api/ai/session/{id}/messages``  — get messages (proxy)
- ``POST /api/ai/session/{id}/message``   — send message (proxy)
- ``GET  /api/ai/events``                 — user-scoped SSE (all sessions)
- ``GET  /api/ai/session/{id}/events``    — session-scoped SSE passthrough
- ``POST /api/ai/session/{id}/cancel``    — cancel generation (proxy)
- ``POST /api/ai/session/{id}/question/{rid}/reply``  — reply to question (proxy)
- ``POST /api/ai/session/{id}/question/{rid}/reject`` — dismiss question (proxy)
"""

import asyncio
import logging
import os
from typing import AsyncGenerator

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from lhp.api.config import APISettings
from lhp.api.dependencies import (
    get_ai_config,
    get_opencode_pool,
    get_settings,
    get_user_process,
)
from lhp.api.schemas.ai import (
    AIConfigSummary,
    AIConfigUpdateRequest,
    AIGenerateTitleRequest,
    AIMessageRequest,
    AISessionCreateRequest,
    AISessionDeleteResponse,
    AISessionResponse,
    AIStatusResponse,
    SessionMode,
)
from lhp.api.services.ai_config import AIConfig
from lhp.api.services.opencode_manager import OpenCodeProcess, OpenCodeProcessPool

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ai", tags=["ai"])


# ── Proxy helper ─────────────────────────────────────────────


async def _proxy_request(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    json: dict | None = None,
    timeout: float = 15.0,
    expected_statuses: tuple[int, ...] = (200,),
) -> httpx.Response:
    """Forward a request to the user's OpenCode process.

    Catches transport-level errors and translates them into
    appropriate HTTP status codes so the frontend never sees
    raw 500s from unhandled exceptions.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, headers=headers, json=json)
            if resp.status_code not in expected_statuses:
                raise HTTPException(resp.status_code, resp.text)
            return resp
    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.warning(f"Proxy timeout ({timeout}s) for {method} {url}")
        raise HTTPException(504, f"AI backend timed out after {timeout}s")
    except httpx.ConnectError:
        logger.warning(f"Proxy connect error for {method} {url}")
        raise HTTPException(502, "Cannot reach AI backend process")
    except httpx.HTTPError as exc:
        logger.warning(f"Proxy HTTP error for {method} {url}: {exc}")
        raise HTTPException(502, "AI backend communication error")


# ── Title generation helper ───────────────────────────────────


async def _generate_title_via_llm(
    user_message: str,
    ai_config: AIConfig,
) -> str | None:
    """Call the LLM to generate a short session title.

    Best-effort: returns ``None`` on any failure (timeout, missing
    config, bad response, etc.).
    """
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "")
    if not base_url:
        return None

    # Determine auth token (same precedence as ai_config.to_opencode_json)
    auth_token = os.environ.get("ANTHROPIC_AUTH_TOKEN") or os.environ.get(
        "ANTHROPIC_API_KEY", ""
    )
    if not auth_token:
        return None

    # Append /v1 if missing (same logic as to_opencode_json)
    if not base_url.endswith("/v1"):
        base_url = base_url.rstrip("/") + "/v1"

    # Strip provider prefix from model name (e.g. "anthropic/databricks-claude-sonnet-4-6" → "databricks-claude-sonnet-4-6")
    model = ai_config.model
    if "/" in model:
        model = model.split("/", 1)[1]

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{base_url}/messages",
                headers={
                    "Authorization": f"Bearer {auth_token}",
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": model,
                    "max_tokens": 30,
                    "system": (
                        "You are a title generator. Your ONLY job is to output "
                        "a short, descriptive title (3-6 words) that summarizes "
                        "the topic of the user's message. Rules:\n"
                        "- Output ONLY the title, no other text\n"
                        "- No markdown, no quotes, no punctuation at the end\n"
                        "- No prefixes like 'Title:' or 'Sure'\n"
                        "- Do NOT answer or respond to the message\n"
                        "- Maximum 40 characters"
                    ),
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                f"Generate a title for this message:\n\n"
                                f"<message>{user_message[:500]}</message>"
                            ),
                        },
                    ],
                },
            )
            if resp.status_code != 200:
                logger.debug(f"Title LLM returned status {resp.status_code}")
                return None

            data = resp.json()
            content = data.get("content", [])
            if content and isinstance(content, list):
                title = content[0].get("text", "").strip()
                # Strip markdown formatting the LLM may include
                title = title.strip("#").strip().strip('"').strip("'")
                return title[:50] if title else None
            return None
    except Exception:
        logger.debug("Title generation LLM call failed", exc_info=True)
        return None


# ── Status & config ──────────────────────────────────────────


@router.get("/status", response_model=AIStatusResponse)
async def ai_status(
    request: Request,
    settings: APISettings = Depends(get_settings),
) -> AIStatusResponse:
    """Check whether the AI assistant is available.

    The frontend polls this on startup to decide whether to show the
    chat panel.  Includes config summary for the settings modal.
    """
    if not settings.ai_enabled:
        return AIStatusResponse(available=False)

    pool: OpenCodeProcessPool | None = getattr(request.app.state, "opencode_pool", None)
    if pool is None:
        return AIStatusResponse(available=False)

    status = pool.get_status()
    ai_config: AIConfig | None = getattr(request.app.state, "ai_config", None)

    config_summary = None
    if ai_config:
        config_summary = {
            "provider": ai_config.provider,
            "model": ai_config.model,
            "allowed_models": ai_config.get_allowed_models(),
        }

    return AIStatusResponse(
        available=status["available"],
        url=status.get("url"),
        auth_required=pool._password is not None,
        config=config_summary,
    )


@router.get("/config", response_model=AIConfigSummary)
async def get_config(
    ai_config: AIConfig = Depends(get_ai_config),
) -> AIConfigSummary:
    """Return allowed providers/models for the frontend settings modal."""
    return AIConfigSummary(
        provider=ai_config.provider,
        model=ai_config.model,
        allowed_models=ai_config.get_allowed_models(),
    )


@router.post("/config", response_model=AIConfigSummary)
async def update_config(
    body: AIConfigUpdateRequest,
    request: Request,
    ai_config: AIConfig = Depends(get_ai_config),
) -> AIConfigSummary:
    """Update the provider/model selection.

    Validates the selection against the allowed list and updates the
    in-memory config.  The next OpenCode process spawn (or config deploy)
    will use the new selection.
    """
    if not ai_config.validate_selection(body.provider, body.model):
        raise HTTPException(
            400,
            f"Invalid selection: {body.provider}/{body.model}. "
            f"Allowed: {ai_config.get_allowed_models()}",
        )

    # Update in-memory config
    ai_config.provider = body.provider
    ai_config.model = body.model
    logger.info(f"AI config updated: provider={body.provider}, model={body.model}")

    return AIConfigSummary(
        provider=ai_config.provider,
        model=ai_config.model,
        allowed_models=ai_config.get_allowed_models(),
    )


# ── Session management (proxied to OpenCode) ─────────────────


@router.get("/sessions")
async def list_sessions(
    process: OpenCodeProcess = Depends(get_user_process),
):
    """List all sessions for the current user (proxy to OpenCode).

    Post-processes the response to:
    - Filter out child/subagent sessions (those with ``parentID``)
    - Sort by creation time descending (newest first)
    - Cap at 10 most recent sessions
    """
    process.touch()
    resp = await _proxy_request(
        "GET",
        f"{process.url}/session",
        headers=process.auth_headers(),
        timeout=15.0,
    )
    raw_sessions = resp.json()
    if isinstance(raw_sessions, list):
        top_level = [s for s in raw_sessions if not s.get("parentID")]
        top_level.sort(
            key=lambda s: (s.get("time") or {}).get("created", 0),
            reverse=True,
        )
        return top_level[:10]
    return raw_sessions


@router.post("/session", response_model=AISessionResponse)
async def create_session(
    request: Request,
    body: AISessionCreateRequest | None = None,
    process: OpenCodeProcess = Depends(get_user_process),
) -> AISessionResponse:
    """Create a new OpenCode session (proxy).

    The backend resolves the user's workspace via auth, then creates a
    session on their OpenCode process.  Returns a real session ID.

    Accepts an optional body with ``mode``:
    - ``agent`` (default): full access — AI can read/write files, run commands
    - ``chat``: read-only — edit and bash permissions are denied
    """
    process.touch()

    if not process.available:
        raise HTTPException(503, "AI assistant process is not ready")

    mode = (body.mode if body else SessionMode.agent)

    # Build the OpenCode session payload with permission rules
    session_payload: dict = {}
    if mode == SessionMode.chat:
        session_payload["permission"] = [
            {"permission": "edit", "pattern": "*", "action": "deny"},
            {"permission": "bash", "pattern": "*", "action": "deny"},
        ]

    resp = await _proxy_request(
        "POST",
        f"{process.url}/session",
        headers=process.auth_headers(),
        json=session_payload,
        timeout=30.0,
        expected_statuses=(200, 201),
    )

    data = resp.json()
    session_id = data.get("id", data.get("session_id", "unknown"))

    return AISessionResponse(
        session_id=session_id,
        project_root=str(process.workspace_path),
        mode=mode.value,
    )


@router.delete("/session/{session_id}", response_model=AISessionDeleteResponse)
async def delete_session(
    session_id: str,
    process: OpenCodeProcess = Depends(get_user_process),
) -> AISessionDeleteResponse:
    """Delete an OpenCode session (proxy)."""
    process.touch()
    try:
        await _proxy_request(
            "DELETE",
            f"{process.url}/session/{session_id}",
            headers=process.auth_headers(),
            timeout=15.0,
            expected_statuses=(200, 204, 404),
        )
    except HTTPException:
        logger.warning(f"Failed to delete session {session_id}", exc_info=True)

    return AISessionDeleteResponse(deleted=True)


@router.get("/session/{session_id}/messages")
async def get_messages(
    session_id: str,
    process: OpenCodeProcess = Depends(get_user_process),
):
    """Get messages for a session (for session restore on page refresh)."""
    process.touch()
    resp = await _proxy_request(
        "GET",
        f"{process.url}/session/{session_id}/message",
        headers=process.auth_headers(),
        timeout=15.0,
    )
    return resp.json()


# ── Message sending + SSE streaming ──────────────────────────


@router.post("/session/{session_id}/message")
async def send_message(
    session_id: str,
    body: AIMessageRequest,
    process: OpenCodeProcess = Depends(get_user_process),
):
    """Send a message to an OpenCode session (proxy).

    Returns immediately with ``{ok: true}`` — the response streams
    back via the SSE ``/events`` endpoint.
    """
    process.touch()

    if not process.available:
        raise HTTPException(503, "AI assistant process is not ready")

    await _proxy_request(
        "POST",
        f"{process.url}/session/{session_id}/message",
        headers=process.auth_headers(),
        json={"parts": [p.model_dump() for p in body.parts]},
        timeout=30.0,
        expected_statuses=(200, 201, 202),
    )

    return {"ok": True}


@router.get("/session/{session_id}/events")
async def stream_events(
    session_id: str,
    request: Request,
    process: OpenCodeProcess = Depends(get_user_process),
):
    """SSE passthrough — streams events from the user's OpenCode process.

    Each user has their own OpenCode process, so all events on their
    ``/event`` stream are their own.  The frontend filters by session ID
    for multi-session support.
    """
    process.touch()

    async def event_generator() -> AsyncGenerator[str, None]:
        async with httpx.AsyncClient(timeout=None) as client:
            try:
                async with client.stream(
                    "GET",
                    f"{process.url}/event",
                    headers=process.auth_headers(),
                ) as resp:
                    line_iter = resp.aiter_lines().__aiter__()
                    while True:
                        if await request.is_disconnected():
                            break
                        try:
                            line = await asyncio.wait_for(
                                line_iter.__anext__(), timeout=15.0
                            )
                            yield f"{line}\n"
                        except asyncio.TimeoutError:
                            # No data for 15s — send SSE comment as keepalive
                            yield ": keepalive\n\n"
                        except StopAsyncIteration:
                            break
            except httpx.HTTPError:
                logger.warning("SSE stream disconnected from OpenCode", exc_info=True)
            except asyncio.CancelledError:
                pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/events")
async def stream_user_events(
    request: Request,
    process: OpenCodeProcess = Depends(get_user_process),
):
    """User-scoped SSE passthrough — streams all events from the user's OpenCode process.

    Unlike the session-scoped ``/session/{id}/events``, this endpoint does
    not filter by session.  The frontend dispatches events to the correct
    session tab using the ``sessionID`` field in each event payload.
    """
    process.touch()

    async def event_generator() -> AsyncGenerator[str, None]:
        async with httpx.AsyncClient(timeout=None) as client:
            try:
                async with client.stream(
                    "GET",
                    f"{process.url}/event",
                    headers=process.auth_headers(),
                ) as resp:
                    line_iter = resp.aiter_lines().__aiter__()
                    while True:
                        if await request.is_disconnected():
                            break
                        try:
                            line = await asyncio.wait_for(
                                line_iter.__anext__(), timeout=15.0
                            )
                            yield f"{line}\n"
                        except asyncio.TimeoutError:
                            yield ": keepalive\n\n"
                        except StopAsyncIteration:
                            break
            except httpx.HTTPError:
                logger.warning(
                    "User-scoped SSE stream disconnected from OpenCode",
                    exc_info=True,
                )
            except asyncio.CancelledError:
                pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/session/{session_id}/cancel")
async def cancel_generation(
    session_id: str,
    process: OpenCodeProcess = Depends(get_user_process),
):
    """Cancel an in-progress generation (proxy)."""
    process.touch()
    try:
        await _proxy_request(
            "POST",
            f"{process.url}/session/{session_id}/cancel",
            headers=process.auth_headers(),
            timeout=5.0,
            expected_statuses=(200, 204),
        )
        return {"ok": True}
    except HTTPException:
        return {"ok": False}


# ── Question tool response (proxied to OpenCode) ─────────────


@router.get("/questions")
async def list_pending_questions(
    process: OpenCodeProcess = Depends(get_user_process),
):
    """List pending question requests (proxy to OpenCode).

    Used on SSE connect/reconnect to discover question requests
    whose ``question.asked`` event was missed.
    """
    process.touch()
    resp = await _proxy_request(
        "GET",
        f"{process.url}/question",
        headers=process.auth_headers(),
        timeout=10.0,
    )
    return resp.json()


@router.post("/session/{session_id}/question/{request_id}/reply")
async def reply_to_question(
    session_id: str,
    request_id: str,
    request: Request,
    process: OpenCodeProcess = Depends(get_user_process),
):
    """Reply to a question tool call (proxy to OpenCode).

    OpenCode's question tool blocks until the user replies.  This
    forwards the structured answer to ``POST /question/{id}/reply``
    so the LLM can continue processing.
    """
    process.touch()
    body = await request.json()
    await _proxy_request(
        "POST",
        f"{process.url}/question/{request_id}/reply",
        headers=process.auth_headers(),
        json=body,
        timeout=10.0,
        expected_statuses=(200, 204),
    )
    return {"ok": True}


@router.post("/session/{session_id}/question/{request_id}/reject")
async def reject_question(
    session_id: str,
    request_id: str,
    process: OpenCodeProcess = Depends(get_user_process),
):
    """Dismiss a question without answering (proxy to OpenCode).

    The LLM will see a graceful error and can decide how to proceed.
    """
    process.touch()
    await _proxy_request(
        "POST",
        f"{process.url}/question/{request_id}/reject",
        headers=process.auth_headers(),
        json={},
        timeout=10.0,
        expected_statuses=(200, 204),
    )
    return {"ok": True}


# ── Session title generation ──────────────────────────────────


@router.post("/session/{session_id}/generate-title")
async def generate_title(
    session_id: str,
    body: AIGenerateTitleRequest,
    process: OpenCodeProcess = Depends(get_user_process),
    ai_config: AIConfig = Depends(get_ai_config),
):
    """Generate a short LLM title for a session and PATCH it to OpenCode.

    Best-effort: returns ``{ok: false}`` on any failure.  The PATCH
    triggers OpenCode's ``session.updated`` SSE event, which the
    frontend's existing handler picks up to update the tab title.
    """
    process.touch()

    # Extract text from message parts
    text = " ".join(p.text for p in body.parts if p.text).strip()
    if not text:
        return {"ok": False}

    title = await _generate_title_via_llm(text, ai_config)
    if not title:
        return {"ok": False}

    # PATCH the title to OpenCode so it persists and triggers SSE
    try:
        await _proxy_request(
            "PATCH",
            f"{process.url}/session/{session_id}",
            headers=process.auth_headers(),
            json={"title": title},
            timeout=10.0,
            expected_statuses=(200, 204),
        )
    except HTTPException:
        logger.debug(f"Failed to PATCH title for session {session_id}", exc_info=True)
        return {"ok": False}

    return {"ok": True, "title": title}
