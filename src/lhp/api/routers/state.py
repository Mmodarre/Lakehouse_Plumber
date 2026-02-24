import asyncio
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Query

from lhp.api.dependencies import get_discoverer, get_facade, get_state_manager
from lhp.api.schemas.state import (
    CleanupResponse,
    EnvironmentStateResponse,
    FileStateResponse,
    StalenessResponse,
    StateOverviewResponse,
)
from lhp.core.layers import (
    LakehousePlumberApplicationFacade,
    StalenessAnalysisRequest,
)
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer
from lhp.core.state_manager import StateManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/state", tags=["state"])


def _file_state_to_response(fs) -> FileStateResponse:
    """Convert dataclass FileState to Pydantic response."""
    return FileStateResponse(
        source_yaml=fs.source_yaml,
        generated_path=fs.generated_path,
        checksum=fs.checksum,
        source_yaml_checksum=fs.source_yaml_checksum,
        timestamp=fs.timestamp,
        environment=fs.environment,
        pipeline=fs.pipeline,
        flowgroup=fs.flowgroup,
    )


@router.get("", response_model=StateOverviewResponse)
async def get_state(
    state_mgr: StateManager = Depends(get_state_manager),
) -> StateOverviewResponse:
    """#63: Full state overview across all environments."""
    await asyncio.to_thread(state_mgr.load_state)
    state = state_mgr.state

    if state is None:
        return StateOverviewResponse(
            version="",
            last_updated="",
            environments={},
            total_files=0,
        )

    envs = {}
    total = 0
    for env_name, files in state.environments.items():
        envs[env_name] = {
            path: _file_state_to_response(fs) for path, fs in files.items()
        }
        total += len(files)

    return StateOverviewResponse(
        version=state.version,
        last_updated=state.last_updated,
        environments=envs,
        total_files=total,
    )


# ── Route ordering constraint ──────────────────────────────────────────
# /staleness and /cleanup MUST be registered BEFORE /{env} because
# FastAPI matches routes in declaration order and {env} would capture
# "staleness" or "cleanup" as an environment name otherwise.
#
# If this ordering ever becomes fragile (e.g., many more fixed sub-paths),
# consider restructuring to /state/env/{env} for the parameterised route
# or adding a regex constraint:  env: str = Path(..., pattern=r"^(?!staleness$|cleanup$)")
# ───────────────────────────────────────────────────────────────────────

@router.get("/staleness", response_model=StalenessResponse)
async def get_staleness(
    env: str = Query("dev", description="Environment to analyze"),
    include_tests: bool = Query(False, description="Include test actions"),
    facade: LakehousePlumberApplicationFacade = Depends(get_facade),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> StalenessResponse:
    """#66: Staleness analysis — which files need regeneration."""
    # Discover all pipeline names for the request DTO
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    pipeline_names = list({fg.pipeline for fg in flowgroups})

    request = StalenessAnalysisRequest(
        pipeline_names=pipeline_names,
        environment=env,
        include_tests=include_tests,
    )
    response = await asyncio.to_thread(facade.analyze_staleness, request)

    return StalenessResponse(
        has_work_to_do=response.has_work_to_do(),
        pipelines_needing_generation=response.pipelines_needing_generation,
        pipelines_up_to_date=response.pipelines_up_to_date,
        has_global_changes=response.has_global_changes,
        global_changes=response.global_changes,
        total_new=response.total_new_files,
        total_stale=response.total_stale_files,
        total_up_to_date=response.total_up_to_date_files,
    )


@router.post("/cleanup", response_model=CleanupResponse)
async def cleanup_orphaned_files(
    env: str = Query("dev", description="Environment to clean up"),
    state_mgr: StateManager = Depends(get_state_manager),
) -> CleanupResponse:
    """#65: Clean up orphaned generated files.

    Actual signature: StateManager.find_orphaned_files(environment: str) -> List[FileState]
                      StateManager.cleanup_orphaned_files(environment: str, dry_run: bool = False) -> List[str]
    """
    cleaned = await asyncio.to_thread(state_mgr.cleanup_orphaned_files, env)

    return CleanupResponse(
        cleaned_files=cleaned if cleaned else [],
        total_cleaned=len(cleaned) if cleaned else 0,
    )


@router.get("/{env}", response_model=EnvironmentStateResponse)
async def get_state_by_env(
    env: str,
    state_mgr: StateManager = Depends(get_state_manager),
) -> EnvironmentStateResponse:
    """#64: State for a specific environment."""
    await asyncio.to_thread(state_mgr.load_state)
    state = state_mgr.state
    env_files = state.environments.get(env, {}) if state else {}

    return EnvironmentStateResponse(
        environment=env,
        files={path: _file_state_to_response(fs) for path, fs in env_files.items()},
        total_files=len(env_files),
    )
