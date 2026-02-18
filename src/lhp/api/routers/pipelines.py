import asyncio
import logging
from pathlib import Path
from typing import Any, Dict

import yaml
from fastapi import APIRouter, Depends, HTTPException

from lhp.api.dependencies import (
    get_discoverer,
    get_pipeline_config_loader,
    get_project_root,
)
from lhp.api.schemas.pipeline import (
    PipelineConfigResponse,
    PipelineDetailResponse,
    PipelineListResponse,
    PipelineSummary,
)
from lhp.api.schemas.flowgroup import FlowgroupSummary, PipelineFlowgroupsResponse
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer
from lhp.core.services.pipeline_config_loader import PipelineConfigLoader  # type for DI

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("", response_model=PipelineListResponse)
async def list_pipelines(
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> PipelineListResponse:
    """#29: List all pipelines with flowgroup counts."""
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)

    pipeline_map: Dict[str, list] = {}
    for fg in flowgroups:
        pipeline_map.setdefault(fg.pipeline, []).append(fg)

    summaries = [
        PipelineSummary(
            name=name,
            flowgroup_count=len(fgs),
            action_count=sum(len(fg.actions) for fg in fgs),
        )
        for name, fgs in sorted(pipeline_map.items())
    ]

    return PipelineListResponse(pipelines=summaries, total=len(summaries))


@router.get("/{name}", response_model=PipelineDetailResponse)
async def get_pipeline(
    name: str,
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    config_loader: PipelineConfigLoader = Depends(get_pipeline_config_loader),
) -> PipelineDetailResponse:
    """#30: Pipeline detail with flowgroup list and merged config."""
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    pipeline_fgs = [fg for fg in flowgroups if fg.pipeline == name]

    if not pipeline_fgs:
        raise HTTPException(404, f"Pipeline '{name}' not found")

    config = await asyncio.to_thread(config_loader.get_pipeline_config, name)

    return PipelineDetailResponse(
        name=name,
        flowgroup_count=len(pipeline_fgs),
        flowgroups=[fg.flowgroup for fg in pipeline_fgs],
        config=config,
    )


@router.get("/{name}/config", response_model=PipelineConfigResponse)
async def get_pipeline_config(
    name: str,
    config_loader: PipelineConfigLoader = Depends(get_pipeline_config_loader),
) -> PipelineConfigResponse:
    """#31: Pipeline-specific config (serverless, edition, channel, etc.)."""
    config = await asyncio.to_thread(config_loader.get_pipeline_config, name)
    return PipelineConfigResponse(pipeline=name, config=config)


@router.put("/{name}/config", response_model=PipelineConfigResponse)
async def update_pipeline_config(
    name: str,
    config: Dict[str, Any],
    project_root: Path = Depends(get_project_root),
) -> PipelineConfigResponse:
    """#32: Update pipeline-specific config.

    Note: This writes to the pipeline_config.yaml file. The implementation
    must handle multi-document YAML carefully to update only the target pipeline.
    """
    # Implementation detail: read existing pipeline_config.yaml,
    # find or create the document for this pipeline, update it, write back.
    # For Phase 1, this is a best-effort implementation.
    # Full YAML round-trip preservation will use ruamel.yaml.
    raise HTTPException(501, "Pipeline config update not yet implemented — coming in Phase 2")


@router.get("/{name}/flowgroups", response_model=PipelineFlowgroupsResponse)
async def get_pipeline_flowgroups(
    name: str,
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> PipelineFlowgroupsResponse:
    """#33: List flowgroups belonging to a specific pipeline."""
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    pipeline_fgs = [fg for fg in flowgroups if fg.pipeline == name]

    if not pipeline_fgs:
        raise HTTPException(404, f"Pipeline '{name}' not found")

    summaries = [
        FlowgroupSummary(
            name=fg.flowgroup,
            pipeline=fg.pipeline,
            action_count=len(fg.actions),
            action_types=list({a.type.value for a in fg.actions}),
            source_file="",  # Populated by discover_all_flowgroups_with_paths
            presets=fg.presets,
            template=fg.use_template,
        )
        for fg in pipeline_fgs
    ]

    return PipelineFlowgroupsResponse(flowgroups=summaries, total=len(summaries))
