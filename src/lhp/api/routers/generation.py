import asyncio
import logging
from pathlib import Path
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from lhp.api.dependencies import (
    get_workspace_discoverer,
    get_workspace_facade,
    get_workspace_orchestrator,
    get_workspace_project_root,
)
from lhp.api.schemas.common import PaginationParams, get_pagination
from lhp.api.schemas.generation import (
    GenerateRequest,
    GenerateResponse,
    PreviewResponse,
)
from lhp.core.layers import (
    LakehousePlumberApplicationFacade,
    PipelineGenerationRequest,
)
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer

# ---------------------------------------------------------------------------
# Phase 1 endpoints below are now Phase 2: all generation endpoints use
# get_workspace_project_root so that reads, writes, and state tracking all
# operate inside the user's isolated workspace clone rather than the shared
# source project root.
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/generate", tags=["generation"])

# Second router for /generated/* endpoints (different URL prefix)
generated_router = APIRouter(prefix="/generated", tags=["generation"])


# ---------------------------------------------------------------------------
# Phase 1 endpoints
# ---------------------------------------------------------------------------


@router.post("", response_model=GenerateResponse)
async def generate(
    req: GenerateRequest,
    project_root: Path = Depends(get_workspace_project_root),
    facade: LakehousePlumberApplicationFacade = Depends(get_workspace_facade),
    discoverer: FlowgroupDiscoverer = Depends(get_workspace_discoverer),
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
    facade: LakehousePlumberApplicationFacade = Depends(get_workspace_facade),
    discoverer: FlowgroupDiscoverer = Depends(get_workspace_discoverer),
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


# ---------------------------------------------------------------------------
# Phase 2 endpoints — generation additions
# ---------------------------------------------------------------------------


@router.get("/plan")
async def get_generation_plan(
    env: str = Query(...),
    force: bool = Query(False),
    include_tests: bool = Query(False),
    orchestrator: ActionOrchestrator = Depends(get_workspace_orchestrator),
    discoverer: FlowgroupDiscoverer = Depends(get_workspace_discoverer),
) -> Dict:
    """#55: Get generation plan analysis (what would be generated and why).

    Returns an analysis of what would be generated without actually writing
    anything: has_work_to_do, generation_mode, to_generate, to_skip, etc.
    """
    # Discover all flowgroups and extract unique pipeline names
    all_flowgroups = await asyncio.to_thread(
        discoverer.discover_all_flowgroups
    )
    pipelines = list({fg.pipeline for fg in all_flowgroups})

    # Merge plans across all pipelines
    merged_to_generate: List[str] = []
    merged_to_skip: List[str] = []
    merged_staleness: Dict[str, int] = {"new": 0, "stale": 0, "up_to_date": 0}
    merged_context_changes: Dict[str, str] = {}
    generation_mode = "smart"

    for pipeline in pipelines:
        plan = await asyncio.to_thread(
            orchestrator.planning_service.create_generation_plan,
            env,
            pipeline,
            include_tests,
            force,
        )

        merged_to_generate.extend(
            fg.flowgroup for fg in plan.flowgroups_to_generate
        )
        merged_to_skip.extend(
            fg.flowgroup for fg in plan.flowgroups_to_skip
        )

        # Merge staleness summary counts
        for key in merged_staleness:
            merged_staleness[key] += plan.staleness_summary.get(key, 0)

        merged_context_changes.update(plan.generation_context_changes)

        # Use the mode from the last plan (they should all be consistent)
        generation_mode = plan.generation_mode

    return {
        "has_work_to_do": len(merged_to_generate) > 0,
        "generation_mode": generation_mode,
        "to_generate": merged_to_generate,
        "to_skip": merged_to_skip,
        "staleness_summary": merged_staleness,
        "context_changes": merged_context_changes,
    }


class SingleFlowgroupRequest(BaseModel):
    """Request body for POST /generate/flowgroup/{name}."""

    environment: str
    force: bool = False
    include_tests: bool = False


@router.post("/flowgroup/{name}")
async def generate_single_flowgroup(
    name: str,
    req: SingleFlowgroupRequest,
    project_root: Path = Depends(get_workspace_project_root),
    facade: LakehousePlumberApplicationFacade = Depends(get_workspace_facade),
    discoverer: FlowgroupDiscoverer = Depends(get_workspace_discoverer),
) -> Dict:
    """#58: Generate code for a single flowgroup.

    Discovers the pipeline containing the named flowgroup, then generates
    only that flowgroup within the pipeline.
    """
    # Discover all flowgroups and find the one matching the name
    all_flowgroups = await asyncio.to_thread(
        discoverer.discover_all_flowgroups
    )

    target_fg = None
    for fg in all_flowgroups:
        if fg.flowgroup == name:
            target_fg = fg
            break

    if target_fg is None:
        raise HTTPException(
            status_code=404,
            detail=f"Flowgroup '{name}' not found",
        )

    output_dir = project_root / f"generated/{req.environment}"

    request = PipelineGenerationRequest(
        pipeline_identifier=target_fg.pipeline,
        environment=req.environment,
        include_tests=req.include_tests,
        force_all=req.force,
        specific_flowgroups=[name],
        output_directory=output_dir,
        dry_run=False,
    )

    response = await asyncio.to_thread(facade.generate_pipeline, request)

    # Return files relevant to the requested flowgroup
    files = []
    for filename, content in response.generated_files.items():
        files.append({"path": filename, "content": content})

    return {
        "success": response.success,
        "files": files,
        "generated_files": response.generated_files,
    }


# ---------------------------------------------------------------------------
# /generated/* endpoints (separate router prefix)
# ---------------------------------------------------------------------------


@generated_router.get("/{env}/{pipeline}/{flowgroup}")
async def get_generated_code(
    env: str,
    pipeline: str,
    flowgroup: str,
    project_root: Path = Depends(get_workspace_project_root),
) -> Dict:
    """#56: Get generated Python code for a specific flowgroup."""
    file_path = project_root / "generated" / env / pipeline / f"{flowgroup}.py"
    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Generated file not found: {pipeline}/{flowgroup}.py",
        )
    content = await asyncio.to_thread(file_path.read_text)
    return {
        "path": str(file_path.relative_to(project_root)),
        "content": content,
    }


@generated_router.get("/{env}")
async def list_generated_files(
    env: str,
    project_root: Path = Depends(get_workspace_project_root),
    pagination: PaginationParams = Depends(get_pagination),
) -> Dict:
    """#57: List all generated files for an environment.

    Supports pagination via offset and limit query parameters.
    """
    gen_dir = project_root / "generated" / env
    if not gen_dir.exists():
        return {"files": [], "total": 0}

    # Collect all .py files and parse pipeline/flowgroup from path
    all_entries = []
    for f in sorted(gen_dir.rglob("*.py")):
        rel = f.relative_to(project_root / "generated" / env)
        parts = rel.parts
        if len(parts) >= 2:
            pipeline_name = parts[0]
            flowgroup_name = f.stem
        else:
            pipeline_name = ""
            flowgroup_name = f.stem

        all_entries.append(
            {
                "path": str(f.relative_to(project_root)),
                "pipeline": pipeline_name,
                "flowgroup": flowgroup_name,
            }
        )

    total = len(all_entries)
    page = all_entries[pagination.offset : pagination.offset + pagination.limit]

    return {"files": page, "total": total}
