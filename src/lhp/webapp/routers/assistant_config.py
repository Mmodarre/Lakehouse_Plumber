"""Assistant configuration router — executor config, profiles, skill install.

Mechanical split from :mod:`lhp.webapp.routers.assistant` (which was nearing
the §3.3 size limit): the four config-ish endpoints moved here VERBATIM —
same ``/assistant`` prefix, same paths, zero behavior change. The chat /
approval / interrupt / session surface stays in the assistant router.

Error surface and store-backed 409 conventions are the assistant router's;
see its module docstring.

ROUTER CONVENTION: routes carry their sub-path under ``/assistant``; the app
mounts this router with ``prefix="/api"``.
"""

from __future__ import annotations

import asyncio
import configparser
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request

from lhp.api import SkillFacade, SkillInstallResult
from lhp.webapp.dependencies import get_project_root
from lhp.webapp.routers._guards import assert_project_loaded
from lhp.webapp.schemas.assistant import (
    DatabricksProfilesResponse,
    ExecutorConfig,
    ExecutorConfigUpdate,
    PermissionsConfig,
    PricingConfig,
    SkillInstallResponse,
)
from lhp.webapp.services import assistant_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/assistant", tags=["assistant"])


@router.get("/config", response_model=ExecutorConfig)
async def get_executor_config(
    request: Request, project_root: Path = Depends(get_project_root)
) -> ExecutorConfig:
    """Return the stored executor config; 404 when none has been set."""
    assert_project_loaded(request, "the assistant is unavailable")
    stored = await asyncio.to_thread(
        assistant_store.get_config, project_root, "executor"
    )
    if stored is None:
        raise HTTPException(404, "Executor config is not set")
    return ExecutorConfig(**stored)


@router.put("/config", response_model=ExecutorConfig)
async def put_executor_config(
    body: ExecutorConfigUpdate,
    request: Request,
    project_root: Path = Depends(get_project_root),
) -> ExecutorConfig:
    """Store the executor config; echo back EXACTLY what was stored.

    Switching executors — including switching provider — marks the active
    session stale so the next chat turn reprovisions against the new config
    (bundle-hash drift would catch it anyway; the stale mark makes the drift
    explicit and immediate).
    """
    assert_project_loaded(request, "the assistant is unavailable")
    stored = body.model_dump()
    previous = await asyncio.to_thread(
        assistant_store.get_config, project_root, "executor"
    )
    await asyncio.to_thread(
        assistant_store.put_config, project_root, "executor", stored
    )
    if previous != stored:
        active = await asyncio.to_thread(
            assistant_store.get_active_session, project_root
        )
        if active is not None:
            await asyncio.to_thread(
                assistant_store.mark_stale, project_root, str(active["session_id"])
            )
            logger.info(
                f"Executor config changed; marked assistant session "
                f"{active['session_id']} stale"
            )
    return ExecutorConfig(**stored)


@router.get("/pricing", response_model=PricingConfig)
async def get_pricing(
    request: Request, project_root: Path = Depends(get_project_root)
) -> PricingConfig:
    """Return the stored model pricing; empty (no models) when never set."""
    assert_project_loaded(request, "the assistant is unavailable")
    stored = await asyncio.to_thread(
        assistant_store.get_config, project_root, "pricing"
    )
    return PricingConfig(**stored) if stored is not None else PricingConfig()


@router.put("/pricing", response_model=PricingConfig)
async def put_pricing(
    body: PricingConfig,
    request: Request,
    project_root: Path = Depends(get_project_root),
) -> PricingConfig:
    """Store the model pricing; echo back exactly what was stored.

    Deliberately NEVER touches session staleness: pricing lives under its
    own ``assistant_config`` key (``"pricing"``) and only affects cost
    labeling — the stale-marking in :func:`put_executor_config` is specific
    to the ``executor`` key, whose changes alter how turns run.
    """
    assert_project_loaded(request, "the assistant is unavailable")
    stored = body.model_dump()
    await asyncio.to_thread(assistant_store.put_config, project_root, "pricing", stored)
    return PricingConfig(**stored)


@router.get("/permissions", response_model=PermissionsConfig)
async def get_permissions(
    request: Request, project_root: Path = Depends(get_project_root)
) -> PermissionsConfig:
    """Return the stored always-allow rules; empty (no rules) when never set."""
    assert_project_loaded(request, "the assistant is unavailable")
    stored = await asyncio.to_thread(
        assistant_store.get_config, project_root, "permissions"
    )
    return PermissionsConfig(**stored) if stored is not None else PermissionsConfig()


@router.put("/permissions", response_model=PermissionsConfig)
async def put_permissions(
    body: PermissionsConfig,
    request: Request,
    project_root: Path = Depends(get_project_root),
) -> PermissionsConfig:
    """Store the always-allow rules; echo back exactly what was stored.

    Same posture as :func:`put_pricing`: its own ``assistant_config`` key
    (``"permissions"``), and deliberately NEVER touches session staleness —
    rules only widen the silent-allow set for future tool calls.
    """
    assert_project_loaded(request, "the assistant is unavailable")
    stored = body.model_dump()
    await asyncio.to_thread(
        assistant_store.put_config, project_root, "permissions", stored
    )
    return PermissionsConfig(**stored)


@router.get("/databricks-profiles", response_model=DatabricksProfilesResponse)
def databricks_profiles() -> DatabricksProfilesResponse:
    """Section names from ``~/.databrickscfg`` — names only, never values.

    ``DEFAULT`` is included only when it has content (configparser keeps it
    out of ``sections()``; ``defaults()`` exposes its keys). A missing or
    unparseable file yields an empty list.
    """
    path = Path.home() / ".databrickscfg"
    if not path.is_file():
        return DatabricksProfilesResponse(profiles=[])
    parser = configparser.ConfigParser()
    try:
        parser.read(path, encoding="utf-8")
    except (configparser.Error, UnicodeDecodeError):
        logger.warning(f"Could not parse {path}; reporting no profiles")
        return DatabricksProfilesResponse(profiles=[])
    profiles = list(parser.sections())
    if parser.defaults():
        profiles.insert(0, "DEFAULT")
    return DatabricksProfilesResponse(profiles=profiles)


def _install_skill(project_root: Path) -> SkillInstallResult:
    """Force-install the packaged LHP skill (thread-bridged by the route)."""
    return SkillFacade(project_root).install_project_skill(force=True)


@router.post("/skill", response_model=SkillInstallResponse)
async def install_skill(
    project_root: Path = Depends(get_project_root),
) -> SkillInstallResponse:
    """Install (force-refresh) the LHP skill into the project.

    ``LHPError`` (``LHP-CFG-011`` for a non-project root) propagates to the
    app-level handler, which renders the repo's standard error envelope.
    """
    result = await asyncio.to_thread(_install_skill, project_root)
    return SkillInstallResponse(
        install_dir=str(result.install_dir),
        skill_version=result.skill_version,
        action=result.action,
    )
