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

    For the ``omnigent`` provider the first four fields mirror the
    daemon-detection ladder
    (:class:`~lhp.webapp.services.omnigent_lifecycle.DaemonStatus`): a daemon
    that is down shows as falsy ladder fields, never as an error response.
    For the ``claude_sdk`` provider the same ladder fields report the
    in-process SDK's availability (bundled binary present) so the existing
    frontend gate logic keeps working; ``host_id`` is ``"local"``.
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
    #: Provider of the STORED executor config (``omnigent`` / ``claude_sdk``);
    #: ``None`` until an executor has been configured.
    provider: Optional[str] = None


class ExecutorConfig(BaseModel):
    """Stored executor configuration, echoed back exactly as stored.

    ``provider`` selects the assistant backend. The Pydantic default is
    ``omnigent`` purely for back-compat parsing of configs stored before the
    field existed; the PRODUCT default is the Claude provider (the setup UI
    preselects ``claude_sdk``).

    ``api_key_env`` / ``oauth_token_env`` are environment-variable NAMES
    (see module docstring); no field ever carries a key value.
    """

    provider: Literal["omnigent", "claude_sdk"] = "omnigent"
    mode: Literal[
        "omnigent_defaults", "databricks", "api_key_env", "claude_subscription"
    ]
    profile: Optional[str] = None  # databricks profile name (~/.databrickscfg)
    host: Optional[str] = None  # databricks workspace host (claude_sdk alternative)
    model: Optional[str] = None  # executor model override
    api_key_env: Optional[str] = None  # env var NAME, never a key value
    oauth_token_env: Optional[str] = None  # env var NAME, never a token value


#: provider -> the modes it accepts.
_PROVIDER_MODES = {
    "omnigent": ("omnigent_defaults", "databricks", "api_key_env"),
    "claude_sdk": ("claude_subscription", "databricks"),
}


class ExecutorConfigUpdate(ExecutorConfig):
    """``PUT /assistant/config`` body: :class:`ExecutorConfig` + mode rules."""

    @model_validator(mode="after")
    def _require_mode_fields(self) -> ExecutorConfigUpdate:
        """Enforce provider/mode pairing, required fields, env-var-name shape."""
        allowed = _PROVIDER_MODES[self.provider]
        if self.mode not in allowed:
            raise ValueError(
                f"provider {self.provider!r} does not support mode "
                f"{self.mode!r} (expected one of {', '.join(allowed)})"
            )
        if self.mode == "databricks":
            if self.provider == "omnigent" and not self.profile:
                raise ValueError("mode 'databricks' requires 'profile'")
            if self.provider == "claude_sdk" and not (self.profile or self.host):
                raise ValueError("mode 'databricks' requires 'profile' or 'host'")
        if self.mode == "api_key_env":
            if not self.api_key_env:
                raise ValueError("mode 'api_key_env' requires 'api_key_env'")
            if not _ENV_VAR_NAME_RE.fullmatch(self.api_key_env):
                raise ValueError(
                    "api_key_env must be an environment variable NAME "
                    "(letters, digits and underscores, not starting with a "
                    "digit) — never an API key value"
                )
        if self.oauth_token_env and not _ENV_VAR_NAME_RE.fullmatch(
            self.oauth_token_env
        ):
            raise ValueError(
                "oauth_token_env must be an environment variable NAME "
                "(letters, digits and underscores, not starting with a "
                "digit) — never a token value"
            )
        return self


class DatabricksProfilesResponse(BaseModel):
    """Section names from ``~/.databrickscfg`` — names only, no values."""

    profiles: list[str]


class ChatRequest(BaseModel):
    """``POST /assistant/chat`` body."""

    message: str = Field(..., min_length=1, description="User message text.")
    #: Per-turn approval policy, honored by the ``claude_sdk`` provider only
    #: (the omnigent provider has its own elicitation flow). Vocabulary
    #: matches Claude Code's permission modes: ``default`` asks for every
    #: non-read-only tool, ``acceptEdits`` additionally auto-allows local
    #: file edits, ``bypassPermissions`` auto-allows everything.
    permission_mode: Literal["default", "acceptEdits", "bypassPermissions"] = "default"


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
