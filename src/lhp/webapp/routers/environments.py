"""Environment read endpoints for the LHP web IDE backend.

``GET /api/environments`` enumerates the project's substitution files
(``substitutions/*.yaml``) and returns one entry per file (name = stem).
This is webapp-owned byte I/O: the substitution layout is a filesystem
convention the IDE reads directly, not a surface exposed by the inspection
facade.

``GET /api/environments/{env}/resolved`` returns the fully resolved
substitution context for one environment via the public inspection facade
(:meth:`InspectionFacade.build_substitution_view`). An environment with no
``substitutions/<env>.yaml`` file responds 404 — the facade itself would
fall back to an empty manager, but for the IDE an unknown environment is a
missing resource, not an empty one.

Environment create/update/delete are out of scope for the local IDE in this
revision.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api import InspectionFacade
from lhp.webapp.dependencies import get_inspection, get_project_root
from lhp.webapp.schemas.project import (
    SecretReferenceSummary,
    SubstitutionResolvedResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/environments", tags=["environments"])


@router.get("")
def list_environments(
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(50, ge=1, le=200, description="Max items to return"),
    project_root: Path = Depends(get_project_root),
) -> dict[str, Any]:
    """List environment names from ``substitutions/*.yaml`` files.

    The React environment selector consumes ``{"environments": [...],
    "total": N}``; ``total`` reflects the full count before pagination.
    """
    subs_dir = project_root / "substitutions"

    if subs_dir.is_dir():
        env_names = sorted(p.stem for p in subs_dir.glob("*.yaml"))
    else:
        env_names = []

    total = len(env_names)
    page = env_names[offset : offset + limit]

    return {"environments": page, "total": total}


@router.get("/{env}/resolved", response_model=SubstitutionResolvedResponse)
async def get_resolved_substitutions(
    env: str,
    inspection: InspectionFacade = Depends(get_inspection),
    project_root: Path = Depends(get_project_root),
) -> SubstitutionResolvedResponse:
    """Resolved substitution tokens for one environment.

    404 when ``substitutions/<env>.yaml`` does not exist (same filesystem
    convention the list endpoint enumerates). ``secret_references`` is
    typically empty here: the facade populates it only after substitution
    has processed a payload, which a fresh resolution has not.
    """
    # ``{env}`` is a single path segment, but guard against encoded
    # separators / dot-dot so the name can never escape substitutions/.
    if "/" in env or "\\" in env or ".." in env:
        raise HTTPException(404, f"Environment '{env}' not found")
    if not (project_root / "substitutions" / f"{env}.yaml").is_file():
        raise HTTPException(404, f"Environment '{env}' not found")

    view = await asyncio.to_thread(inspection.build_substitution_view, env)

    return SubstitutionResolvedResponse(
        env=view.env,
        tokens=dict(view.tokens),
        raw_mappings=dict(view.raw_mappings),
        default_secret_scope=view.default_secret_scope,
        secret_references=[
            SecretReferenceSummary(scope=ref.scope, key=ref.key)
            for ref in view.secret_references
        ],
    )
