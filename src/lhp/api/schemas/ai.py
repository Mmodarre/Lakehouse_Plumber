"""Schemas for AI assistant endpoints."""

from typing import Optional

from pydantic import BaseModel


class AIStatusResponse(BaseModel):
    """Response for GET /api/ai/status."""

    available: bool
    url: Optional[str] = None
    auth_required: bool = False


class AISessionResponse(BaseModel):
    """Response for POST /api/ai/session."""

    session_id: str
    project_root: str


class AISessionDeleteResponse(BaseModel):
    """Response for DELETE /api/ai/session/{session_id}."""

    deleted: bool
