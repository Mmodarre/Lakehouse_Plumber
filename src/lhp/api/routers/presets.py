import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Response

from lhp.api.auth import UserContext, get_current_user
from lhp.api.dependencies import (
    check_etag,
    compute_etag,
    get_auto_commit_service,
    get_preset_manager,
    get_workspace_project_root,
    get_yaml_editor,
)
from lhp.api.schemas.preset import PresetDetailResponse, PresetListResponse
from lhp.api.services.auto_commit_service import AutoCommitService
from lhp.api.services.yaml_editor import YAMLEditor
from lhp.presets.preset_manager import PresetManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/presets", tags=["presets"])


def _user_hash(user: UserContext) -> str:
    """Compute short hash of user ID for auto-commit tracking."""
    return hashlib.sha256(user.user_id.encode()).hexdigest()[:16]


@router.get("", response_model=PresetListResponse)
async def list_presets(
    preset_mgr: PresetManager = Depends(get_preset_manager),
) -> PresetListResponse:
    """#43: List all available presets."""
    presets = await asyncio.to_thread(preset_mgr.list_presets)
    return PresetListResponse(presets=presets, total=len(presets))


@router.get("/{name}", response_model=PresetDetailResponse, response_model_exclude_none=True)
async def get_preset(
    name: str,
    response: Response,
    preset_mgr: PresetManager = Depends(get_preset_manager),
    project_root: Path = Depends(get_workspace_project_root),
) -> PresetDetailResponse:
    """#44: Get preset details including inheritance chain resolution."""
    preset = await asyncio.to_thread(preset_mgr.get_preset, name)
    if preset is None:
        raise HTTPException(404, f"Preset '{name}' not found")

    # Resolve full inheritance chain
    resolved = await asyncio.to_thread(preset_mgr.resolve_preset_chain, [name])

    # Set ETag header if the file exists
    file_path = project_root / "presets" / f"{name}.yaml"
    if file_path.exists():
        etag = compute_etag(file_path.read_bytes())
        response.headers["ETag"] = etag

    return PresetDetailResponse(
        name=name,
        raw=preset.model_dump(),
        resolved=resolved,
    )


# ---------------------------------------------------------------------------
# Phase 2: CUD endpoints (create, update, delete)
# ---------------------------------------------------------------------------


@router.post("", status_code=201)
async def create_preset(
    body: Dict[str, Any],
    project_root: Path = Depends(get_workspace_project_root),
    editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> Dict[str, Any]:
    """#45: Create a new preset YAML file."""
    name = body.get("name")
    if not name:
        raise HTTPException(400, "Preset body must include a 'name' field")

    file_path = project_root / "presets" / f"{name}.yaml"
    if file_path.exists():
        raise HTTPException(409, f"Preset '{name}' already exists")

    path = await asyncio.to_thread(editor.create_preset, project_root, name, body)

    await auto_commit_service.notify_change(_user_hash(user), str(path))

    return {"success": True, "path": str(path.relative_to(project_root))}


@router.put("/{name}")
async def update_preset(
    name: str,
    body: Dict[str, Any],
    if_match: Optional[str] = Header(None),
    project_root: Path = Depends(get_workspace_project_root),
    editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> Dict[str, Any]:
    """#46: Update an existing preset YAML file.

    Supports optimistic concurrency via If-Match header. If provided,
    the update is rejected with 412 if the file was modified since the
    client last read it. If omitted, last-write-wins applies.
    """
    file_path = project_root / "presets" / f"{name}.yaml"
    if not file_path.exists():
        raise HTTPException(404, f"Preset '{name}' not found")

    # Optimistic concurrency check
    if if_match:
        check_etag(file_path, if_match)

    await asyncio.to_thread(editor.write, file_path, body)

    await auto_commit_service.notify_change(_user_hash(user), str(file_path))

    return {"success": True, "path": str(file_path.relative_to(project_root))}


@router.delete("/{name}")
async def delete_preset(
    name: str,
    project_root: Path = Depends(get_workspace_project_root),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> Dict[str, Any]:
    """#47: Delete a preset YAML file."""
    file_path = project_root / "presets" / f"{name}.yaml"
    if not file_path.exists():
        raise HTTPException(404, f"Preset '{name}' not found")

    file_path.unlink()
    logger.info(f"Deleted preset: {file_path}")

    await auto_commit_service.notify_change(_user_hash(user), str(file_path))

    return {"success": True, "name": name}
