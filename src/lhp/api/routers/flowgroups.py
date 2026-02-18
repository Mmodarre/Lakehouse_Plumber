import asyncio
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api.dependencies import (
    get_discoverer,
    get_orchestrator,
    get_project_root,
)
from lhp.api.schemas.flowgroup import (
    FlowgroupDetailResponse,
    FlowgroupListResponse,
    FlowgroupSummary,
    ResolvedFlowgroupResponse,
)
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/flowgroups", tags=["flowgroups"])

# NOTE: The /resolved endpoint needs to create a substitution manager.
# See orchestrator.validate_pipeline_by_field() for the same pattern.


@router.get("", response_model=FlowgroupListResponse)
async def list_flowgroups(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline name"),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> FlowgroupListResponse:
    """#34: List all flowgroups, optionally filtered by pipeline."""
    if pipeline:
        flowgroups = await asyncio.to_thread(
            discoverer.discover_flowgroups_by_pipeline_field, pipeline
        )
    else:
        flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)

    # Try to get source file paths
    try:
        fg_with_paths = await asyncio.to_thread(
            discoverer.discover_all_flowgroups_with_paths
        )
        path_map = {fg.flowgroup: str(path) for fg, path in fg_with_paths}
    except Exception:
        path_map = {}

    summaries = [
        FlowgroupSummary(
            name=fg.flowgroup,
            pipeline=fg.pipeline,
            action_count=len(fg.actions),
            action_types=list({a.type.value for a in fg.actions}),
            source_file=path_map.get(fg.flowgroup, ""),
            presets=fg.presets,
            template=fg.use_template,
        )
        for fg in flowgroups
    ]

    return FlowgroupListResponse(flowgroups=summaries, total=len(summaries))


@router.get("/{name}", response_model=FlowgroupDetailResponse)
async def get_flowgroup(
    name: str,
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> FlowgroupDetailResponse:
    """#35: Get raw flowgroup config (before template/preset/substitution processing)."""
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    match = next((fg for fg in flowgroups if fg.flowgroup == name), None)

    if not match:
        raise HTTPException(404, f"Flowgroup '{name}' not found")

    # Get source file path
    source_file = ""
    try:
        source_path = await asyncio.to_thread(
            discoverer.find_source_yaml_for_flowgroup, name
        )
        if source_path:
            source_file = str(source_path)
    except Exception:
        pass

    return FlowgroupDetailResponse(flowgroup=match, source_file=source_file)


@router.get("/{name}/resolved", response_model=ResolvedFlowgroupResponse)
async def get_resolved_flowgroup(
    name: str,
    env: str = Query("dev", description="Environment for substitution resolution"),
    orchestrator: ActionOrchestrator = Depends(get_orchestrator),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    project_root: Path = Depends(get_project_root),
) -> ResolvedFlowgroupResponse:
    """#36: Get flowgroup after template expansion, preset merging, and substitution.

    This calls the same processing pipeline as `lhp show <flowgroup> --env <env>`.

    Actual signature: ActionOrchestrator.process_flowgroup(
        flowgroup: FlowGroup, substitution_mgr: EnhancedSubstitutionManager
    ) -> FlowGroup
    """
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    match = next((fg for fg in flowgroups if fg.flowgroup == name), None)

    if not match:
        raise HTTPException(404, f"Flowgroup '{name}' not found")

    # Create substitution manager for the environment
    # (same pattern used by orchestrator.validate_pipeline_by_field)
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    substitution_mgr = orchestrator.dependencies.create_substitution_manager(
        substitution_file, env
    )

    # Process flowgroup through orchestrator (template + preset + substitution)
    processed = await asyncio.to_thread(
        orchestrator.process_flowgroup, match, substitution_mgr
    )

    return ResolvedFlowgroupResponse(
        flowgroup=processed,
        environment=env,
        applied_presets=match.presets,
        applied_template=match.use_template,
    )
