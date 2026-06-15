"""Project info / stats router for the ``lhp web`` local IDE backend.

Two read-only endpoints over the public :class:`InspectionFacade`:

- ``GET /api/project`` — project overview (name / version / description /
  author) plus resource counts (pipelines, flowgroups, presets, templates,
  environments).
- ``GET /api/project/stats`` — aggregate pipeline / flowgroup / action
  statistics with a per-action-type breakdown and per-pipeline rows.

There is no ``/api/project/config`` GET or PUT: the local IDE edits
``lhp.yaml`` through the file-editing surface, not a structured config PUT.

Per the ``webapp-uses-public-api`` import contract this module imports only
``lhp.api`` (via the webapp DI helpers) — no ``lhp`` internals.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends

from lhp.api import InspectionFacade
from lhp.webapp.dependencies import get_inspection, get_project_root
from lhp.webapp.schemas.project import (
    ProjectInfoResponse,
    ProjectStatsResponse,
    ResourceCounts,
)

router = APIRouter(prefix="/project", tags=["project"])


def _count_environments(project_root: Path) -> int:
    """Count ``substitutions/*.yaml`` files — one per declared environment."""
    sub_dir = project_root / "substitutions"
    if not sub_dir.is_dir():
        return 0
    return len(list(sub_dir.glob("*.yaml")) + list(sub_dir.glob("*.yml")))


@router.get("", response_model=ProjectInfoResponse)
async def get_project_info(
    inspection: InspectionFacade = Depends(get_inspection),
    project_root: Path = Depends(get_project_root),
) -> ProjectInfoResponse:
    """Project overview with resource counts."""
    config = inspection.get_project_config()
    flowgroups = inspection.list_flowgroups()
    pipelines = {fg.pipeline for fg in flowgroups}
    presets = inspection.list_presets()
    templates = inspection.list_templates()

    return ProjectInfoResponse(
        name=config.name,
        version=config.version,
        description=config.description,
        author=config.author,
        resource_counts=ResourceCounts(
            pipelines=len(pipelines),
            flowgroups=len(flowgroups),
            presets=len(presets),
            templates=len(templates),
            environments=_count_environments(project_root),
        ),
    )


@router.get("/stats", response_model=ProjectStatsResponse)
async def get_project_stats(
    inspection: InspectionFacade = Depends(get_inspection),
) -> ProjectStatsResponse:
    """Pipeline statistics and complexity metrics."""
    stats = inspection.compute_stats()

    pipelines = [
        {
            "name": row.pipeline_name,
            "flowgroup_count": row.flowgroup_count,
            "action_count": row.total_actions,
        }
        for row in stats.pipeline_breakdown
    ]

    return ProjectStatsResponse(
        total_pipelines=stats.pipeline_count,
        total_flowgroups=stats.flowgroup_count,
        total_actions=stats.total_actions,
        actions_by_type=dict(stats.action_counts_by_type),
        pipelines=pipelines,
    )
