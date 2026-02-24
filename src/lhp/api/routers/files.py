import asyncio
import hashlib
from pathlib import Path
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from lhp.api.auth import UserContext, get_current_user
from lhp.api.dependencies import (
    get_auto_commit_service,
    get_project_root_adaptive,
    get_workspace_project_root,
)
from lhp.api.schemas.common import PaginationParams, get_pagination
from lhp.api.services.auto_commit_service import AutoCommitService


class FileWriteRequest(BaseModel):
    """Typed request body for file write operations."""
    content: str = Field(..., description="File content to write")
    encoding: str = "utf-8"


router = APIRouter(prefix="/files", tags=["files"])


# Paths that must not be written or deleted via the file browser API.
# These are critical project files that could corrupt the project state.
WRITE_PROTECTED_PREFIXES = (".git/", "generated/")
WRITE_PROTECTED_FILES = (".lhp_state.json",)


def _validate_path(
    project_root: Path, relative_path: str, check_write_protection: bool = False
) -> Path:
    """Resolve and validate that path is within project root.

    Uses Path.is_relative_to() (Python 3.9+) to prevent prefix-matching
    attacks. Resolves symlinks to prevent symlink-based escapes.

    If check_write_protection is True, also rejects writes to critical
    project files (.git/, .lhp_state.json, generated/).
    """
    root_resolved = project_root.resolve()
    target_resolved = (project_root / relative_path).resolve()

    if not target_resolved.is_relative_to(root_resolved):
        raise HTTPException(
            status_code=403,
            detail="Path traversal not allowed",
        )

    if check_write_protection:
        if any(relative_path.startswith(prefix) for prefix in WRITE_PROTECTED_PREFIXES):
            raise HTTPException(
                status_code=403,
                detail=f"Write-protected path: {relative_path}",
            )
        if relative_path in WRITE_PROTECTED_FILES:
            raise HTTPException(
                status_code=403,
                detail=f"Write-protected file: {relative_path}",
            )

    return target_resolved


@router.get("")
async def list_files(
    project_root: Path = Depends(get_project_root_adaptive),
    pagination: PaginationParams = Depends(get_pagination),
) -> Dict:
    """#75: Project file tree (non-recursive first level + key subdirectories).

    Supports pagination via offset and limit query parameters.
    """
    items = []
    for item in sorted(project_root.iterdir()):
        if item.name.startswith(".") and item.name not in (".lhp_state.json",):
            continue
        items.append(
            {
                "name": item.name,
                "type": "directory" if item.is_dir() else "file",
                "size": item.stat().st_size if item.is_file() else None,
            }
        )
    total = len(items)
    page = items[pagination.offset : pagination.offset + pagination.limit]
    return {
        "items": page,
        "total": total,
        "offset": pagination.offset,
        "limit": pagination.limit,
        "project_root": str(project_root),
    }


MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB


@router.get("/{path:path}")
async def read_file(
    path: str,
    project_root: Path = Depends(get_project_root_adaptive),
) -> Dict:
    """#76: Read file content.

    Includes size limit (5MB) and binary file detection for safety.
    """
    file_path = _validate_path(project_root, path)
    if not file_path.exists():
        raise HTTPException(404, f"File not found: {path}")
    if file_path.is_dir():
        # Return directory listing
        items = [
            {"name": i.name, "type": "directory" if i.is_dir() else "file"}
            for i in sorted(file_path.iterdir())
            if not i.name.startswith(".")
        ]
        return {"type": "directory", "path": path, "items": items}

    # Size limit check
    file_size = file_path.stat().st_size
    if file_size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({file_size} bytes). Maximum is {MAX_FILE_SIZE} bytes.",
        )

    # Binary file detection: check for null bytes in the first 8KB
    sample = await asyncio.to_thread(lambda: file_path.read_bytes()[:8192])
    if b"\x00" in sample:
        return {
            "type": "binary",
            "path": path,
            "size": file_size,
            "message": "Binary file detected. Content not returned.",
        }

    content = await asyncio.to_thread(file_path.read_text)
    return {"type": "file", "path": path, "content": content}


@router.put("/{path:path}")
async def write_file(
    path: str,
    body: FileWriteRequest,
    user: UserContext = Depends(get_current_user),
    project_root: Path = Depends(get_workspace_project_root),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
) -> Dict:
    """#77: Write file content + trigger auto-commit.

    Cache invalidation is handled by CacheInvalidationMiddleware.
    Auto-commit is triggered explicitly via auto_commit_service.
    """
    file_path = _validate_path(project_root, path, check_write_protection=True)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(file_path.write_text, body.content, encoding=body.encoding)

    # Notify auto-commit of the changed file
    user_hash = hashlib.sha256(user.user_id.encode()).hexdigest()[:16]
    await auto_commit_service.notify_change(user_hash, path)

    return {"written": True, "path": path}


@router.delete("/{path:path}")
async def delete_file(
    path: str,
    user: UserContext = Depends(get_current_user),
    project_root: Path = Depends(get_workspace_project_root),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
) -> Dict:
    """#78: Delete file + trigger auto-commit.

    Cache invalidation is handled by CacheInvalidationMiddleware.
    Auto-commit is triggered explicitly via auto_commit_service.
    """
    file_path = _validate_path(project_root, path, check_write_protection=True)
    if not file_path.exists():
        raise HTTPException(404, f"File not found: {path}")
    await asyncio.to_thread(file_path.unlink)

    # Notify auto-commit of the deleted file
    user_hash = hashlib.sha256(user.user_id.encode()).hexdigest()[:16]
    await auto_commit_service.notify_change(user_hash, path)

    return {"deleted": True, "path": path}
