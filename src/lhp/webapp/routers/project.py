"""Project info / stats / init router for the ``lhp web`` local IDE backend.

Two read-only endpoints over the public :class:`InspectionFacade`:

- ``GET /api/project`` — project overview (name / version / description /
  author) plus resource counts (pipelines, flowgroups, presets, templates,
  environments).
- ``GET /api/project/stats`` — aggregate pipeline / flowgroup / action
  statistics with a per-action-type breakdown and per-pipeline rows.

Plus one lifecycle endpoint over the public bootstrap:

- ``POST /api/project/init`` — scaffold a new project into the configured
  project root. Only valid while the server runs in ``no_project`` state;
  otherwise it responds 409.

There is no ``/api/project/config`` GET or PUT: the local IDE edits
``lhp.yaml`` through the file-editing surface, not a structured config PUT.

Per the ``webapp-uses-public-api`` import contract this module imports only
``lhp.api`` (via the webapp DI helpers) — no ``lhp`` internals.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request

from lhp.api import InspectionFacade, LakehousePlumberBootstrap
from lhp.webapp.dependencies import get_inspection, get_project_root, invalidate_facade
from lhp.webapp.schemas.project import (
    InitProjectRequest,
    InitProjectResponse,
    ProjectInfoResponse,
    ProjectStatsResponse,
    ResourceCounts,
)
from lhp.webapp.services import sqlite_store

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/project", tags=["project"])


def _count_environments(project_root: Path) -> int:
    """Count ``substitutions/*.yaml`` files — one per declared environment."""
    sub_dir = project_root / "substitutions"
    if not sub_dir.is_dir():
        return 0
    return len(list(sub_dir.glob("*.yaml")) + list(sub_dir.glob("*.yml")))


@router.get("", response_model=ProjectInfoResponse)
def get_project_info(
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
def get_project_stats(
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


def _relative_path(path: Path, project_root: Path) -> str:
    """Return ``path`` relative to ``project_root``, or as-is if outside.

    Always POSIX separators: the frontend and file_io key files by
    forward-slash paths regardless of the host OS.
    """
    try:
        return path.relative_to(project_root).as_posix()
    except ValueError:
        return path.as_posix()


@router.post("/init", response_model=InitProjectResponse)
async def init_project(
    request: Request,
    body: InitProjectRequest,
    project_root: Path = Depends(get_project_root),
) -> InitProjectResponse:
    """Scaffold a new LHP project into the configured project root.

    Only valid while the server runs in ``no_project`` state (no ``lhp.yaml``
    at the project root) — otherwise responds 409. The public bootstrap never
    raises: a scaffolding failure (e.g. non-empty directory, ``LHP-IO-007``)
    is returned as ``success=False`` with the error code and message.

    On success the handler performs the startup work the lifespan skipped in
    ``no_project`` state: flips ``app.state.project_state`` to ``"ok"``, runs
    the run-history DB migrations and crash recovery, and invalidates the
    cached facade. The file watcher is NOT started here — the lifespan owns
    its task handle (a local it cancels on shutdown), so a router-started
    watcher could never be cancelled cleanly. External-edit detection begins
    on the next server restart; edits made through the IDE's own file-write
    surface invalidate the facade explicitly and need no watcher.
    """
    if getattr(request.app.state, "project_state", "ok") != "no_project":
        raise HTTPException(409, "project already initialized")

    bootstrap = LakehousePlumberBootstrap()
    result = await asyncio.to_thread(
        bootstrap.init_project,
        project_root,
        bundle=body.bundle,
        project_name=body.project_name,
        initialize_git=False,
    )

    if result.is_successful():
        logger.info(f"Project initialized at {project_root}")
        request.app.state.project_state = "ok"
        await asyncio.to_thread(sqlite_store.run_migrations, project_root)
        await asyncio.to_thread(sqlite_store.mark_orphaned_runs_failed, project_root)
        invalidate_facade(request.app)
    else:
        logger.warning(
            f"Project init failed at {project_root}: "
            f"{result.error_code} {result.error_message}"
        )

    return InitProjectResponse(
        success=result.success,
        created_files=[_relative_path(p, project_root) for p in result.created_files],
        created_dirs=[_relative_path(p, project_root) for p in result.created_dirs],
        bundle_enabled=result.bundle_enabled,
        error_message=result.error_message,
        error_code=result.error_code,
    )
