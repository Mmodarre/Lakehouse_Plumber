"""Assistant panel request/response schemas for the ``/api/assistant`` endpoints.

Webapp-owned HTTP contract models (NOT ``lhp.api`` DTOs), following the A3
rule: every Pydantic model used by :mod:`lhp.webapp.routers.assistant` lives
here, none inline in the router.

Security invariant: no model in this module ever carries a secret VALUE. The
``api_key_env`` field holds the NAME of an environment variable (validated
against the POSIX identifier pattern) that the omnigent server expands from
ITS process env — a value containing whitespace or key-material characters
(``-``, ``.`` etc.) fails validation.
"""

from __future__ import annotations

import re
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, model_validator

#: POSIX-style environment-variable NAME. Anything else — whitespace, ``-``,
#: ``.`` — is rejected, which also rejects pasted key material like
#: ``sk-abc123``.
_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ActiveSessionInfo(BaseModel):
    """The locally-tracked active assistant session (assistant_sessions row)."""

    session_id: str  # omnigent conversation id ("conv_...")
    title: Optional[str]
    status: str  # always "active" for the row surfaced here
    created_at: str  # ISO-8601 UTC
    last_used_at: str  # ISO-8601 UTC


class AssistantStatus(BaseModel):
    """Single source of truth for the assistant panel's readiness display.

    The first four fields mirror the daemon-detection ladder
    (:class:`~lhp.webapp.services.omnigent_lifecycle.DaemonStatus`): a daemon
    that is down shows as falsy ladder fields, never as an error response.
    """

    binary_found: bool
    server_ok: bool
    host_online: bool
    host_id: Optional[str]
    server_url: str
    skill_installed: bool
    skill_version: Optional[str]
    executor_configured: bool
    active_session: Optional[ActiveSessionInfo]


class ExecutorConfig(BaseModel):
    """Stored executor configuration, echoed back exactly as stored.

    ``api_key_env`` is an environment-variable NAME (see module docstring);
    no field ever carries a key value.
    """

    mode: Literal["omnigent_defaults", "databricks", "api_key_env"]
    profile: Optional[str] = None  # databricks CLI profile name
    model: Optional[str] = None  # executor model override
    api_key_env: Optional[str] = None  # env var NAME, never a key value


class ExecutorConfigUpdate(ExecutorConfig):
    """``PUT /assistant/config`` body: :class:`ExecutorConfig` + mode rules."""

    @model_validator(mode="after")
    def _require_mode_fields(self) -> ExecutorConfigUpdate:
        """Enforce the per-mode required fields and the env-var-name shape."""
        if self.mode == "databricks" and not self.profile:
            raise ValueError("mode 'databricks' requires 'profile'")
        if self.mode == "api_key_env":
            if not self.api_key_env:
                raise ValueError("mode 'api_key_env' requires 'api_key_env'")
            if not _ENV_VAR_NAME_RE.fullmatch(self.api_key_env):
                raise ValueError(
                    "api_key_env must be an environment variable NAME "
                    "(letters, digits and underscores, not starting with a "
                    "digit) — never an API key value"
                )
        return self


class DatabricksProfilesResponse(BaseModel):
    """Section names from ``~/.databrickscfg`` — names only, no values."""

    profiles: list[str]


class ChatRequest(BaseModel):
    """``POST /assistant/chat`` body."""

    message: str = Field(..., min_length=1, description="User message text.")


class ApprovalRequest(BaseModel):
    """``POST /assistant/approval`` body: resolve one pending elicitation."""

    elicitation_id: str = Field(..., min_length=1)
    action: Literal["accept", "decline", "cancel"]
    content: Optional[dict[str, Any]] = None  # optional MCP resolve payload


class SessionSnapshot(BaseModel):
    """Active session snapshot for panel rehydration.

    ``items`` are the omnigent snapshot items passed through UNMODIFIED —
    each is enveloped as ``{id, type, status, response_id, created_at,
    created_by, data: {...}}`` (spike S8); the shared renderer unwraps
    ``data`` client-side.
    """

    session_id: str
    title: Optional[str] = None
    status: str
    items: list[dict[str, Any]]


class SessionListItem(BaseModel):
    """One locally-tracked assistant session (newest-used first in lists)."""

    session_id: str
    title: Optional[str]
    status: str  # "active" | "archived" | "stale"
    created_at: str
    last_used_at: str


class SessionListResponse(BaseModel):
    """``GET /assistant/sessions`` response."""

    sessions: list[SessionListItem]
    total: int


class DaemonStartResponse(BaseModel):
    """``POST /assistant/daemon/start`` outcome (spawn attempt, not readiness)."""

    started: bool
    detail: Optional[str] = None


class SkillInstallResponse(BaseModel):
    """``POST /assistant/skill`` install summary."""

    install_dir: str
    skill_version: str
    action: Literal["installed", "updated"]
