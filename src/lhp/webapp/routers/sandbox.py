"""Developer-sandbox read endpoint for the ``lhp web`` local IDE backend.

``GET /api/sandbox`` (optional ``?env=<env>``) reports the personal
``.lhp/profile.yaml`` selection and the concrete pipeline scope a ``--sandbox``
run would cover — the SAME resolution the CLI performs
(:func:`lhp.core.sandbox.resolve_sandbox_run`, monitoring pipeline excluded),
via the public :meth:`lhp.api.SandboxFacade.describe_scope`. Sandbox-specific
failures (malformed profile, zero-match scope, env not sandbox-enabled) are
surfaced on the response's ``error`` field rather than as an HTTP error, so
the frontend panel always renders; only project-level failures raise (and are
normalized to 422 by the app's ``LHPError`` handler).

Profile WRITES are not handled here — the profile is an ordinary writable
project file the frontend edits through the files API.

Per the ``webapp-uses-public-api`` import contract, this module may import
only :mod:`lhp.api` / :mod:`lhp.errors` from the ``lhp`` package (plus FastAPI
and the standard library).
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Depends, Query

from lhp.api import SandboxFacade
from lhp.webapp.dependencies import get_sandbox
from lhp.webapp.schemas.sandbox import SandboxResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sandbox", tags=["sandbox"])


@router.get("", response_model=SandboxResponse)
async def get_sandbox_scope(
    env: str | None = Query(
        default=None,
        description=(
            "Optional substitution environment. When given, the team-policy "
            "allowed_envs gate (LHP-CFG-065) is applied and surfaced on "
            "'error'; omit for the env-independent scope."
        ),
    ),
    sandbox: SandboxFacade = Depends(get_sandbox),
) -> SandboxResponse:
    """Sandbox profile + resolved pipeline scope (optionally scoped to ``env``)."""
    result = await asyncio.to_thread(sandbox.describe_scope, env=env)
    return SandboxResponse(
        profile_exists=result.profile_exists,
        namespace=result.namespace,
        patterns=list(result.patterns),
        resolved_pipelines=list(result.resolved_pipelines),
        allowed_envs=(
            list(result.allowed_envs) if result.allowed_envs is not None else None
        ),
        error=result.error,
    )
