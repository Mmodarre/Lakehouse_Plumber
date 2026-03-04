import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml as pyyaml
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel

from lhp.api.auth import UserContext, get_current_user
from lhp.api.dependencies import (
    check_etag,
    compute_etag,
    get_auto_commit_service,
    get_discoverer,
    get_orchestrator,
    get_project_root_adaptive,
    get_template_engine,
    get_workspace_project_root,
    get_yaml_editor,
)
from lhp.api.schemas.flowgroup import (
    FlowgroupCreateRequest,
    FlowgroupDetailResponse,
    FlowgroupListResponse,
    FlowgroupMutationResponse,
    FlowgroupRelatedFilesResponse,
    FlowgroupSummary,
    FlowgroupUpdateRequest,
    RelatedFileInfo,
    ResolvedFlowgroupResponse,
    build_flowgroup_summary,
)
from lhp.api.services.related_files_extractor import extract_related_files
from lhp.api.services.auto_commit_service import AutoCommitService
from lhp.api.services.yaml_editor import YAMLEditor
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer
from lhp.core.template_engine import TemplateEngine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/flowgroups", tags=["flowgroups"])

# NOTE: The /resolved endpoint needs to create a substitution manager.
# See orchestrator.validate_pipeline_by_field() for the same pattern.

# Read endpoints use get_project_root_adaptive (dev -> local path, prod -> workspace).
# CUD endpoints use get_workspace_project_root (always workspace, requires auth).


def _user_hash(user: UserContext) -> str:
    """Compute short hash of user ID for auto-commit tracking."""
    return hashlib.sha256(user.user_id.encode()).hexdigest()[:16]


@router.get("", response_model=FlowgroupListResponse)
async def list_flowgroups(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline name"),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    template_engine: TemplateEngine = Depends(get_template_engine),
    project_root: Path = Depends(get_project_root_adaptive),
) -> FlowgroupListResponse:
    """#34: List all flowgroups, optionally filtered by pipeline."""
    if pipeline:
        flowgroups = await asyncio.to_thread(
            discoverer.discover_flowgroups_by_pipeline_field, pipeline
        )
    else:
        flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)

    # Try to get source file paths (relative to project root for the files API)
    try:
        fg_with_paths = await asyncio.to_thread(
            discoverer.discover_all_flowgroups_with_paths
        )
        path_map = {
            fg.flowgroup: str(path.relative_to(project_root))
            for fg, path in fg_with_paths
        }
    except Exception:
        path_map = {}

    summaries = [
        build_flowgroup_summary(fg, template_engine, path_map.get(fg.flowgroup, ""))
        for fg in flowgroups
    ]

    return FlowgroupListResponse(flowgroups=summaries, total=len(summaries))


@router.get("/{name}", response_model=FlowgroupDetailResponse, response_model_exclude_none=True)
async def get_flowgroup(
    name: str,
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    project_root: Path = Depends(get_project_root_adaptive),
) -> FlowgroupDetailResponse:
    """#35: Get raw flowgroup config (before template/preset/substitution processing)."""
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    match = next((fg for fg in flowgroups if fg.flowgroup == name), None)

    if not match:
        raise HTTPException(404, f"Flowgroup '{name}' not found")

    # Get source file path (relative to project root for the files API)
    source_file = ""
    try:
        source_path = await asyncio.to_thread(
            discoverer.find_source_yaml_for_flowgroup, match
        )
        if source_path:
            source_file = str(source_path.relative_to(project_root))
    except Exception:
        pass

    return FlowgroupDetailResponse(flowgroup=match, source_file=source_file)


@router.get("/{name}/resolved", response_model=ResolvedFlowgroupResponse, response_model_exclude_none=True)
async def get_resolved_flowgroup(
    name: str,
    env: str = Query("dev", description="Environment for substitution resolution"),
    orchestrator: ActionOrchestrator = Depends(get_orchestrator),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    project_root: Path = Depends(get_project_root_adaptive),
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


@router.get(
    "/{name}/related-files", response_model=FlowgroupRelatedFilesResponse
)
async def get_related_files(
    name: str,
    env: str = Query("dev", description="Environment for substitution resolution"),
    orchestrator: ActionOrchestrator = Depends(get_orchestrator),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    project_root: Path = Depends(get_project_root_adaptive),
) -> FlowgroupRelatedFilesResponse:
    """Get all files referenced by a flowgroup (SQL, Python, schema, expectations).

    Returns the source YAML file plus all external file references found in
    the resolved flowgroup's actions. Used by the multi-tab editor UI.
    """
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    match = next((fg for fg in flowgroups if fg.flowgroup == name), None)

    if not match:
        raise HTTPException(404, f"Flowgroup '{name}' not found")

    # Get source file path
    source_path = await asyncio.to_thread(
        discoverer.find_source_yaml_for_flowgroup, match
    )
    source_file_rel = ""
    if source_path:
        source_file_rel = str(source_path.relative_to(project_root))

    # Resolve the flowgroup (template + preset + substitution) to see all file refs
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    substitution_mgr = orchestrator.dependencies.create_substitution_manager(
        substitution_file, env
    )
    processed = await asyncio.to_thread(
        orchestrator.process_flowgroup, match, substitution_mgr
    )

    # Extract related files from the resolved flowgroup
    related = await asyncio.to_thread(
        extract_related_files, processed, project_root
    )

    source_file_info = RelatedFileInfo(
        path=source_file_rel,
        category="yaml",
        action_name="",
        field="source_file",
        exists=bool(source_path and source_path.exists()),
    )

    related_infos = [
        RelatedFileInfo(
            path=rf.path,
            category=rf.category,
            action_name=rf.action_name,
            field=rf.field,
            exists=rf.exists,
        )
        for rf in related
    ]

    return FlowgroupRelatedFilesResponse(
        flowgroup=name,
        source_file=source_file_info,
        related_files=related_infos,
        environment=env,
    )


# ---------------------------------------------------------------------------
# YAML Preview (pure compute, no I/O)
# ---------------------------------------------------------------------------


class YAMLPreviewRequest(BaseModel):
    """Request body for YAML preview generation."""

    pipeline: str
    flowgroup: str
    config: Dict[str, Any] = {}


class YAMLPreviewResponse(BaseModel):
    """Generated YAML preview string."""

    yaml_content: str


@router.post("/preview-yaml", response_model=YAMLPreviewResponse)
async def preview_yaml(body: YAMLPreviewRequest) -> YAMLPreviewResponse:
    """Generate a YAML preview from a flowgroup configuration.

    Pure compute endpoint — accepts pipeline/flowgroup/config, serializes
    to a YAML string, and returns without writing any file.
    """
    doc: Dict[str, Any] = {
        "pipeline": body.pipeline,
        "flowgroup": body.flowgroup,
    }
    doc.update(body.config)

    yaml_str = pyyaml.dump(doc, default_flow_style=False, sort_keys=False)
    return YAMLPreviewResponse(yaml_content=yaml_str)


# ---------------------------------------------------------------------------
# Phase 2: CUD endpoints (create, update, delete) + source retrieval
# ---------------------------------------------------------------------------


@router.post("", status_code=201, response_model=FlowgroupMutationResponse)
async def create_flowgroup(
    body: FlowgroupCreateRequest,
    project_root: Path = Depends(get_workspace_project_root),
    editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> FlowgroupMutationResponse:
    """#37: Create a new flowgroup YAML file."""
    config = {"pipeline": body.pipeline, "flowgroup": body.flowgroup, **body.config}
    path = await asyncio.to_thread(
        editor.create_flowgroup, project_root, body.pipeline, body.flowgroup, config
    )

    etag = compute_etag(path.read_bytes())

    await auto_commit_service.notify_change(_user_hash(user), str(path))

    return FlowgroupMutationResponse(
        success=True,
        path=str(path.relative_to(project_root)),
        etag=etag,
    )


@router.get("/{name}/source")
async def get_flowgroup_source(
    name: str,
    response: Response,
    project_root: Path = Depends(get_workspace_project_root),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> Dict:
    """#41: Source YAML path + raw content. Returns ETag for concurrency."""
    # find_source_yaml_for_flowgroup expects a FlowGroup object, not a string.
    # Discover all flowgroups first and find the matching one by name.
    all_flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    target_fg = None
    for fg in all_flowgroups:
        if fg.flowgroup == name:
            target_fg = fg
            break

    if target_fg is None:
        raise HTTPException(404, f"Flowgroup '{name}' not found")

    source_path = await asyncio.to_thread(
        discoverer.find_source_yaml_for_flowgroup, target_fg
    )
    if not source_path:
        raise HTTPException(404, f"Source file for flowgroup '{name}' not found")

    content = await asyncio.to_thread(source_path.read_text)
    etag = compute_etag(source_path.read_bytes())
    response.headers["ETag"] = etag

    return {
        "flowgroup": name,
        "path": str(source_path.relative_to(project_root)),
        "content": content,
        "etag": etag,
    }


@router.put("/{name}", response_model=FlowgroupMutationResponse)
async def update_flowgroup(
    name: str,
    body: FlowgroupUpdateRequest,
    if_match: Optional[str] = Header(None),
    project_root: Path = Depends(get_workspace_project_root),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> FlowgroupMutationResponse:
    """#38: Update flowgroup YAML file.

    Handles single-flowgroup, multi-document, and array-syntax files.
    For multi-flowgroup files, only the matching document/entry is updated.

    Supports optimistic concurrency via If-Match header. If provided,
    the update is rejected with 412 if the file was modified since the
    client last read it. If omitted, last-write-wins applies.
    """
    # find_source_yaml_for_flowgroup expects a FlowGroup object, not a string.
    all_flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    target_fg = next((fg for fg in all_flowgroups if fg.flowgroup == name), None)
    if target_fg is None:
        raise HTTPException(404, f"Flowgroup '{name}' not found")

    source_path = await asyncio.to_thread(
        discoverer.find_source_yaml_for_flowgroup, target_fg
    )
    if not source_path:
        raise HTTPException(404, f"Source file for flowgroup '{name}' not found")

    # Optimistic concurrency check
    if if_match:
        check_etag(source_path, if_match)

    result = await asyncio.to_thread(
        editor.update_flowgroup, project_root, source_path, name, body.config
    )

    new_etag = compute_etag(source_path.read_bytes())

    await auto_commit_service.notify_change(_user_hash(user), str(source_path))

    return FlowgroupMutationResponse(
        success=True,
        path=str(source_path.relative_to(project_root)),
        multi_flowgroup_file=result.is_multi_flowgroup_file,
        etag=new_etag,
    )


@router.delete("/{name}")
async def delete_flowgroup(
    name: str,
    if_match: Optional[str] = Header(None),
    project_root: Path = Depends(get_workspace_project_root),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
    editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> Dict:
    """#39: Delete flowgroup YAML file.

    For multi-flowgroup files, only removes the matching document.
    Other flowgroups in the same file are preserved. If the last
    flowgroup in a multi-doc file is removed, the file is deleted.

    Supports optimistic concurrency via If-Match header.
    """
    # find_source_yaml_for_flowgroup expects a FlowGroup object, not a string.
    all_flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    target_fg = next((fg for fg in all_flowgroups if fg.flowgroup == name), None)
    if target_fg is None:
        raise HTTPException(404, f"Flowgroup '{name}' not found")

    source_path = await asyncio.to_thread(
        discoverer.find_source_yaml_for_flowgroup, target_fg
    )
    if not source_path:
        raise HTTPException(404, f"Source file for flowgroup '{name}' not found")

    # Optimistic concurrency check
    if if_match:
        check_etag(source_path, if_match)

    result = await asyncio.to_thread(
        editor.delete_flowgroup, source_path, name, project_root
    )

    await auto_commit_service.notify_change(_user_hash(user), str(source_path))

    return {
        "success": True,
        "name": name,
        "file_deleted": not source_path.exists(),
        "was_multi_flowgroup_file": result.is_multi_flowgroup_file,
    }
