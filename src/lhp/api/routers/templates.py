import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response

from lhp.api.auth import UserContext, get_current_user
from lhp.api.dependencies import (
    check_etag,
    compute_etag,
    get_auto_commit_service,
    get_template_engine,
    get_workspace_project_root,
    get_yaml_editor,
)
from lhp.api.schemas.template import (
    TemplateDetailResponse,
    TemplateInfoResponse,
    TemplateListDetailResponse,
    TemplateListResponse,
    TemplateSummary,
)
from lhp.api.services.auto_commit_service import AutoCommitService
from lhp.api.services.yaml_editor import YAMLEditor
from lhp.core.template_engine import TemplateEngine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/templates", tags=["templates"])


def _user_hash(user: UserContext) -> str:
    """Compute short hash of user ID for auto-commit tracking."""
    return hashlib.sha256(user.user_id.encode()).hexdigest()[:16]


@router.get("")
async def list_templates(
    detail: bool = Query(False, description="Include summary metadata per template"),
    engine: TemplateEngine = Depends(get_template_engine),
) -> TemplateListResponse | TemplateListDetailResponse:
    """#48: List all available templates.

    When detail=false (default), returns a simple list of names (backward compatible).
    When detail=true, returns summaries with description, parameter/action counts.
    """
    templates = await asyncio.to_thread(engine.list_templates)

    if not detail:
        return TemplateListResponse(templates=templates, total=len(templates))

    summaries: list[TemplateSummary] = []
    for name in templates:
        info = await asyncio.to_thread(engine.get_template_info, name)
        if info:
            # Extract action types from template actions
            action_types: list[str] = []
            template_obj = engine.get_template(name)
            if template_obj:
                for a in template_obj.actions:
                    if isinstance(a, dict):
                        t = a.get("type", "")
                    else:
                        t = a.type.value if hasattr(a.type, "value") else str(a.type)
                    if t and t not in action_types:
                        action_types.append(t)

            summaries.append(
                TemplateSummary(
                    name=name,
                    description=info.get("description"),
                    parameter_count=len(info.get("parameters", [])),
                    action_count=info.get("action_count", 0),
                    action_types=action_types,
                )
            )

    return TemplateListDetailResponse(templates=summaries, total=len(summaries))


@router.get("/{name}", response_model=TemplateDetailResponse, response_model_exclude_none=True)
async def get_template(
    name: str,
    response: Response,
    engine: TemplateEngine = Depends(get_template_engine),
    project_root: Path = Depends(get_workspace_project_root),
) -> TemplateDetailResponse:
    """#49: Get template details including parameters."""
    info = await asyncio.to_thread(engine.get_template_info, name)
    if not info:
        raise HTTPException(404, f"Template '{name}' not found")

    # Set ETag header if the file exists
    file_path = project_root / "templates" / f"{name}.yaml"
    if file_path.exists():
        etag = compute_etag(file_path.read_bytes())
        response.headers["ETag"] = etag

    return TemplateDetailResponse(
        name=name,
        template=TemplateInfoResponse(**info),
    )


# ---------------------------------------------------------------------------
# Phase 2: CUD endpoints (create, update, delete)
# ---------------------------------------------------------------------------


@router.post("", status_code=201)
async def create_template(
    body: Dict[str, Any],
    project_root: Path = Depends(get_workspace_project_root),
    editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> Dict[str, Any]:
    """#50: Create a new template YAML file."""
    name = body.get("name")
    if not name:
        raise HTTPException(400, "Template body must include a 'name' field")

    content = body.get("content")
    if content is None:
        raise HTTPException(400, "Template body must include a 'content' field")

    file_path = project_root / "templates" / f"{name}.yaml"
    if file_path.exists():
        raise HTTPException(409, f"Template '{name}' already exists")

    path = await asyncio.to_thread(
        editor.create_template, project_root, name, content
    )

    await auto_commit_service.notify_change(_user_hash(user), str(path))

    return {"success": True, "path": str(path.relative_to(project_root))}


@router.put("/{name}")
async def update_template(
    name: str,
    body: Dict[str, Any],
    if_match: Optional[str] = Header(None),
    project_root: Path = Depends(get_workspace_project_root),
    editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> Dict[str, Any]:
    """#51: Update an existing template YAML file.

    Supports optimistic concurrency via If-Match header. If provided,
    the update is rejected with 412 if the file was modified since the
    client last read it. If omitted, last-write-wins applies.
    """
    file_path = project_root / "templates" / f"{name}.yaml"
    if not file_path.exists():
        raise HTTPException(404, f"Template '{name}' not found")

    content = body.get("content")
    if content is None:
        raise HTTPException(400, "Template body must include a 'content' field")

    # Optimistic concurrency check
    if if_match:
        check_etag(file_path, if_match)

    await asyncio.to_thread(editor.write, file_path, content)

    await auto_commit_service.notify_change(_user_hash(user), str(file_path))

    return {"success": True, "path": str(file_path.relative_to(project_root))}


@router.delete("/{name}")
async def delete_template(
    name: str,
    project_root: Path = Depends(get_workspace_project_root),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    user: UserContext = Depends(get_current_user),
) -> Dict[str, Any]:
    """#52: Delete a template YAML file."""
    file_path = project_root / "templates" / f"{name}.yaml"
    if not file_path.exists():
        raise HTTPException(404, f"Template '{name}' not found")

    file_path.unlink()
    logger.info(f"Deleted template: {file_path}")

    await auto_commit_service.notify_change(_user_hash(user), str(file_path))

    return {"success": True, "name": name}
