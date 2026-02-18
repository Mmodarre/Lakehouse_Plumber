import asyncio
import logging
from pathlib import Path
from typing import Any, Dict

import yaml
from fastapi import APIRouter, Depends, HTTPException

from lhp.api.dependencies import (
    get_discoverer,
    get_preset_manager,
    get_project_config_loader,
    get_project_root,
    get_template_engine,
)
from lhp.api.schemas.common import SuccessResponse
from lhp.api.schemas.project import (
    ProjectConfigResponse,
    ProjectInfoResponse,
    ProjectStatsResponse,
    ResourceCounts,
)
from lhp.core.project_config_loader import ProjectConfigLoader
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer
from lhp.presets.preset_manager import PresetManager
from lhp.core.template_engine import TemplateEngine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/project", tags=["project"])


@router.get("", response_model=ProjectInfoResponse)
async def get_project_info(
    project_root: Path = Depends(get_project_root),
    config_loader: ProjectConfigLoader = Depends(get_project_config_loader),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    preset_mgr: PresetManager = Depends(get_preset_manager),
    template_eng: TemplateEngine = Depends(get_template_engine),
) -> ProjectInfoResponse:
    """Project overview with resource counts."""
    config = await asyncio.to_thread(config_loader.load_project_config)
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    pipelines = {fg.pipeline for fg in flowgroups}

    # Count presets and templates
    presets = await asyncio.to_thread(preset_mgr.list_presets)
    templates = await asyncio.to_thread(template_eng.list_templates)

    # Count environments
    sub_dir = project_root / "substitutions"
    envs = list(sub_dir.glob("*.yaml")) if sub_dir.exists() else []

    return ProjectInfoResponse(
        name=config.name if config else "Unknown",
        version=config.version if config else "1.0",
        description=config.description if config else None,
        author=config.author if config else None,
        resource_counts=ResourceCounts(
            pipelines=len(pipelines),
            flowgroups=len(flowgroups),
            presets=len(presets),
            templates=len(templates),
            environments=len(envs),
        ),
    )


@router.get("/config", response_model=ProjectConfigResponse)
async def get_project_config(
    project_root: Path = Depends(get_project_root),
) -> ProjectConfigResponse:
    """Return lhp.yaml content as structured JSON."""
    config_path = project_root / "lhp.yaml"
    raw = await asyncio.to_thread(config_path.read_text)
    parsed = yaml.safe_load(raw)
    return ProjectConfigResponse(config=parsed or {})


@router.put("/config", response_model=SuccessResponse)
async def update_project_config(
    config: Dict[str, Any],
    project_root: Path = Depends(get_project_root),
    config_loader: ProjectConfigLoader = Depends(get_project_config_loader),
) -> SuccessResponse:
    """Update lhp.yaml with new configuration.

    Safety measures:
    1. Quick structural check (must be a dict with 'name')
    2. Backup the original file
    3. Write the new content, then validate through ProjectConfigLoader
    4. Rollback if ProjectConfigLoader rejects the config
    """
    import shutil

    config_path = project_root / "lhp.yaml"
    backup_path = config_path.with_suffix(".yaml.bak")

    # Step 1: Quick structural validation before touching disk
    content = yaml.dump(config, default_flow_style=False, sort_keys=False)
    try:
        reparsed = yaml.safe_load(content)
        if not isinstance(reparsed, dict):
            raise HTTPException(422, "Config must be a YAML mapping")
        if "name" not in reparsed:
            raise HTTPException(422, "Config must include 'name' field")
    except yaml.YAMLError as e:
        raise HTTPException(422, f"Invalid YAML: {e}")

    # Step 2: Backup existing config
    if config_path.exists():
        await asyncio.to_thread(shutil.copy2, config_path, backup_path)

    # Step 3: Write new config and validate through ProjectConfigLoader
    try:
        await asyncio.to_thread(config_path.write_text, content)
        # ProjectConfigLoader.load_project_config() reads lhp.yaml from disk
        # and runs _parse_project_config, _validate_preset_references, etc.
        # If any check fails it raises LHPError, which triggers rollback.
        await asyncio.to_thread(config_loader.load_project_config)
    except Exception:
        # Restore backup on any failure (write error OR validation rejection)
        if backup_path.exists():
            await asyncio.to_thread(shutil.copy2, backup_path, config_path)
        raise

    return SuccessResponse(
        message="Project config updated",
        details={"backup": str(backup_path)},
    )


@router.get("/stats", response_model=ProjectStatsResponse)
async def get_project_stats(
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> ProjectStatsResponse:
    """Pipeline statistics and complexity metrics."""
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)

    # Group by pipeline and count by action type
    pipeline_map: Dict[str, list] = {}
    actions_by_type: Dict[str, int] = {}
    total_actions = 0

    for fg in flowgroups:
        pipeline_map.setdefault(fg.pipeline, []).append(fg)
        for action in fg.actions:
            action_type = action.type.value if hasattr(action.type, "value") else str(action.type)
            actions_by_type[action_type] = actions_by_type.get(action_type, 0) + 1
            total_actions += 1

    pipelines = []
    for name, fgs in pipeline_map.items():
        pipelines.append({
            "name": name,
            "flowgroup_count": len(fgs),
            "action_count": sum(len(fg.actions) for fg in fgs),
        })

    return ProjectStatsResponse(
        total_pipelines=len(pipeline_map),
        total_flowgroups=len(flowgroups),
        total_actions=total_actions,
        actions_by_type=actions_by_type,
        pipelines=pipelines,
    )
