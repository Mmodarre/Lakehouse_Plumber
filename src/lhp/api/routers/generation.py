import asyncio
import logging
from pathlib import Path

from fastapi import APIRouter, Depends

from lhp.api.dependencies import get_discoverer, get_facade, get_project_root
from lhp.api.schemas.generation import (
    GenerateRequest,
    GenerateResponse,
    PreviewResponse,
)
from lhp.core.layers import (
    LakehousePlumberApplicationFacade,
    PipelineGenerationRequest,
)
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/generate", tags=["generation"])


@router.post("", response_model=GenerateResponse)
async def generate(
    req: GenerateRequest,
    project_root: Path = Depends(get_project_root),
    facade: LakehousePlumberApplicationFacade = Depends(get_facade),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> GenerateResponse:
    """#53: Generate pipeline code.

    Wraps LakehousePlumberApplicationFacade.generate_pipeline().
    If no pipeline is specified, discovers all pipelines and generates each.
    """
    output_dir = project_root / f"generated/{req.environment}"

    if req.pipeline:
        pipelines = [req.pipeline]
    else:
        # Use cached discoverer for consistency with other routers
        flowgroups = await asyncio.to_thread(
            discoverer.discover_all_flowgroups
        )
        pipelines = list({fg.pipeline for fg in flowgroups})

    all_files = {}
    total_written = 0
    total_flowgroups = 0

    for pipeline_id in pipelines:
        request = PipelineGenerationRequest(
            pipeline_identifier=pipeline_id,
            environment=req.environment,
            include_tests=req.include_tests,
            force_all=req.force,
            output_directory=output_dir,
            dry_run=False,
            pipeline_config_path=req.pipeline_config,
        )
        response = await asyncio.to_thread(facade.generate_pipeline, request)
        all_files.update(response.generated_files)
        total_written += response.files_written
        total_flowgroups += response.total_flowgroups

    return GenerateResponse(
        success=total_written > 0 or not pipelines,
        files_written=total_written,
        total_flowgroups=total_flowgroups,
        generated_files=all_files,
        output_location=str(output_dir),
    )


@router.post("/preview", response_model=PreviewResponse)
async def generate_preview(
    req: GenerateRequest,
    project_root: Path = Depends(get_project_root),
    facade: LakehousePlumberApplicationFacade = Depends(get_facade),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> PreviewResponse:
    """#54: Dry-run generation — shows what WOULD be generated without writing files."""
    if req.pipeline:
        pipelines = [req.pipeline]
    else:
        flowgroups = await asyncio.to_thread(
            discoverer.discover_all_flowgroups
        )
        pipelines = list({fg.pipeline for fg in flowgroups})

    would_generate = {}
    total_flowgroups = 0

    for pipeline_id in pipelines:
        request = PipelineGenerationRequest(
            pipeline_identifier=pipeline_id,
            environment=req.environment,
            include_tests=req.include_tests,
            force_all=req.force,
            dry_run=True,
            pipeline_config_path=req.pipeline_config,
        )
        response = await asyncio.to_thread(facade.generate_pipeline, request)
        would_generate.update(response.generated_files)
        total_flowgroups += response.total_flowgroups

    return PreviewResponse(
        success=True,
        would_generate=would_generate,
        total_flowgroups=total_flowgroups,
        generation_mode="force" if req.force else "smart",
    )
