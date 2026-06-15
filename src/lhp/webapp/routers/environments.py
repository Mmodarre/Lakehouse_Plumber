"""Environment read endpoint for the LHP web IDE backend.

``GET /api/environments`` enumerates the project's substitution files
(``substitutions/*.yaml``) and returns one entry per file (name = stem).
This is webapp-owned byte I/O: the substitution layout is a filesystem
convention the IDE reads directly, not a surface exposed by the inspection
facade.

Only the list endpoint is exposed. Per-environment token read, secret scan,
and create/update/delete are out of scope for the read-only local IDE in this
revision.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Query

from lhp.webapp.dependencies import get_project_root

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/environments", tags=["environments"])


@router.get("")
async def list_environments(
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
