"""Blueprint read endpoint for the LHP web IDE backend.

Read-only: ``GET /api/blueprints`` lists every blueprint declared in the
project via the public inspection facade
(:meth:`InspectionFacade.list_blueprints`). With ``include_instances=true``
each blueprint additionally carries its instance files with resolved
flowgroup counts and pipeline names.

Blueprint create/update/delete are intentionally not exposed — the local IDE
edits YAML through the file-write surface, not a dedicated blueprint CUD API.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Query

from lhp.api import BlueprintView, InspectionFacade
from lhp.webapp.dependencies import get_inspection, get_project_root
from lhp.webapp.schemas.blueprint import (
    BlueprintInstanceSummary,
    BlueprintListResponse,
    BlueprintSummary,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/blueprints", tags=["blueprints"])


@router.get("", response_model=BlueprintListResponse)
async def list_blueprints(
    include_instances: bool = Query(
        False,
        description="Include per-instance flowgroup counts and pipeline names",
    ),
    inspection: InspectionFacade = Depends(get_inspection),
    project_root: Path = Depends(get_project_root),
) -> BlueprintListResponse:
    """List all blueprints declared in the project."""
    views = await asyncio.to_thread(
        inspection.list_blueprints, include_instances=include_instances
    )
    summaries = [_view_to_summary(v, project_root) for v in views]
    return BlueprintListResponse(blueprints=summaries, total=len(summaries))


def _relative_source(file_path: Path, project_root: Path) -> str:
    """Return ``file_path`` relative to ``project_root``, or as-is if outside."""
    try:
        return str(file_path.relative_to(project_root))
    except ValueError:
        return str(file_path)


def _view_to_summary(view: BlueprintView, project_root: Path) -> BlueprintSummary:
    """Project a frozen :class:`BlueprintView` to the response schema."""
    instances = [
        BlueprintInstanceSummary(
            instance_file_path=_relative_source(i.instance_file_path, project_root),
            flowgroup_count=i.flowgroup_count,
            pipelines=list(i.pipelines),
        )
        for i in view.instances
    ]
    return BlueprintSummary(
        name=view.name,
        version=view.version,
        description=view.description,
        parameter_count=view.parameter_count,
        flowgroup_count=view.flowgroup_count,
        instance_count=view.instance_count,
        instances=instances,
    )
