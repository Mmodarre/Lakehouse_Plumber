"""Developer-sandbox read response schema."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SandboxResponse(BaseModel):
    """Sandbox profile and its resolved pipeline scope for one environment.

    Mirrors the public :class:`lhp.api.SandboxScopeResult`. ``profile_exists``
    is ``false`` when no ``.lhp/profile.yaml`` is present (the normal
    not-opted-in state, ``error`` null). ``error`` carries a surfaced
    sandbox-specific failure message (malformed profile, zero-match scope, or
    an env the team policy does not enable) so the panel can always render.
    """

    profile_exists: bool
    namespace: str | None = None
    patterns: list[str] = Field(default_factory=list)
    resolved_pipelines: list[str] = Field(default_factory=list)
    allowed_envs: list[str] | None = None
    error: str | None = None
