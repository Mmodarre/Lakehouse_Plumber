"""Schemas for AI assistant endpoints."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel


class SessionMode(str, Enum):
    """Session permission mode."""

    agent = "agent"
    chat = "chat"


class AIStatusResponse(BaseModel):
    """Response for GET /api/ai/status."""

    available: bool
    url: Optional[str] = None
    auth_required: bool = False
    config: Optional[dict[str, Any]] = None


class AISessionCreateRequest(BaseModel):
    """Request body for POST /api/ai/session."""

    mode: SessionMode = SessionMode.agent


class AISessionResponse(BaseModel):
    """Response for POST /api/ai/session."""

    session_id: str
    project_root: str
    mode: str = "agent"


class AISessionDeleteResponse(BaseModel):
    """Response for DELETE /api/ai/session/{session_id}."""

    deleted: bool


class AISessionSummary(BaseModel):
    """Summary of one OpenCode session."""

    id: str
    title: Optional[str] = None
    created_at: Optional[str] = None


class AISessionListResponse(BaseModel):
    """Response for GET /api/ai/sessions."""

    sessions: list[AISessionSummary]


class AIMessagePart(BaseModel):
    """A single part in a message sent to OpenCode."""

    type: str = "text"
    text: str = ""


class AIMessageRequest(BaseModel):
    """Request body for POST /api/ai/session/{id}/message."""

    parts: list[AIMessagePart]


class AIGenerateTitleRequest(BaseModel):
    """Request body for POST /api/ai/session/{id}/generate-title."""

    parts: list[AIMessagePart]


class AIConfigSummary(BaseModel):
    """Response for GET /api/ai/config — allowed providers/models."""

    provider: str
    model: str
    allowed_models: dict[str, list[str]]


class AIConfigUpdateRequest(BaseModel):
    """Request body for POST /api/ai/config."""

    provider: str
    model: str
