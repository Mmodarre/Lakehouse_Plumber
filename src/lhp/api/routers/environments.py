"""Environments router -- CRUD for substitution files.

Endpoints #23-28: list, get, create, update, delete environments,
and secret reference scanning.
"""

import hashlib
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from lhp.api.auth import UserContext, get_current_user
from lhp.api.dependencies import (
    check_etag,
    compute_etag,
    get_auto_commit_service,
    get_project_root_adaptive,
    get_workspace_project_root,
    get_yaml_editor,
    require_not_dev_mode,
)
from lhp.api.schemas.common import PaginationParams, get_pagination
from lhp.api.services.auto_commit_service import AutoCommitService
from lhp.api.services.yaml_editor import YAMLEditor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/environments", tags=["environments"])

# Regex for secret references: ${secret:scope/key}
_SECRET_PATTERN = re.compile(r"\$\{secret:([^}]+)\}")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class CreateEnvironmentRequest(BaseModel):
    """Body for POST /environments."""

    name: str
    tokens: Dict[str, Any]


class UpdateEnvironmentRequest(BaseModel):
    """Body for PUT /environments/{env}."""

    tokens: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _substitutions_dir(project_root: Path) -> Path:
    """Return the substitutions directory path."""
    return project_root / "substitutions"


def _env_file(project_root: Path, env_name: str) -> Path:
    """Return the path for a specific environment's substitution file."""
    return _substitutions_dir(project_root) / f"{env_name}.yaml"


def _extract_tokens(data: Any, env_name: str) -> Dict[str, Any]:
    """Extract tokens from loaded YAML data.

    Handles two formats:
    - Wrapped: ``{env_name: {tokens...}, secrets: ...}`` (standard LHP layout)
    - Flat: ``{token_key: value, ...}`` (created via the API)

    For the wrapped format, returns the dict nested under the env name key.
    For the flat format, returns the whole dict.
    """
    if isinstance(data, dict) and env_name in data:
        tokens = data[env_name]
        if isinstance(tokens, dict):
            return dict(tokens)
    # Flat format or unexpected shape -- return as-is
    if isinstance(data, dict):
        return dict(data)
    return {}


def _user_hash(user: UserContext) -> str:
    """Compute short hash of user ID for auto-commit tracking."""
    return hashlib.sha256(user.user_id.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# #23 — GET /environments
# ---------------------------------------------------------------------------


@router.get("")
async def list_environments(
    pagination: PaginationParams = Depends(get_pagination),
    project_root: Path = Depends(get_project_root_adaptive),
) -> Dict[str, Any]:
    """List environment names from substitutions/*.yaml files."""
    subs_dir = _substitutions_dir(project_root)

    if subs_dir.is_dir():
        env_names = sorted(p.stem for p in subs_dir.glob("*.yaml"))
    else:
        env_names = []

    total = len(env_names)

    # Apply pagination
    page = env_names[pagination.offset : pagination.offset + pagination.limit]

    return {"environments": page, "total": total}


# ---------------------------------------------------------------------------
# #24 — GET /environments/{env}
# ---------------------------------------------------------------------------


@router.get("/{env}")
async def get_environment(
    env: str,
    project_root: Path = Depends(get_project_root_adaptive),
    yaml_editor: YAMLEditor = Depends(get_yaml_editor),
) -> JSONResponse:
    """Return substitution tokens for the given environment.

    Returns an ``ETag`` response header for optimistic concurrency.
    """
    file_path = _env_file(project_root, env)

    if not file_path.exists():
        raise HTTPException(404, f"Environment not found: {env}")

    data = yaml_editor.read(file_path)
    tokens = _extract_tokens(data, env)

    etag_value = compute_etag(file_path.read_bytes())

    return JSONResponse(
        content={"tokens": tokens},
        headers={"etag": etag_value},
    )


# ---------------------------------------------------------------------------
# #26 — POST /environments
# ---------------------------------------------------------------------------


@router.post("", status_code=201)
async def create_environment(
    body: CreateEnvironmentRequest,
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    project_root: Path = Depends(get_workspace_project_root),
    yaml_editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
) -> Dict[str, Any]:
    """Create a new environment substitution file."""
    file_path = _env_file(project_root, body.name)

    if file_path.exists():
        raise HTTPException(409, f"Environment already exists: {body.name}")

    yaml_editor.create_environment(project_root, body.name, body.tokens)
    logger.info(f"Created environment '{body.name}' at {file_path}")

    await auto_commit_service.notify_change(_user_hash(user), str(file_path))

    return {"name": body.name, "created": True}


# ---------------------------------------------------------------------------
# #25 — PUT /environments/{env}
# ---------------------------------------------------------------------------


@router.put("/{env}")
async def update_environment(
    env: str,
    body: UpdateEnvironmentRequest,
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    project_root: Path = Depends(get_workspace_project_root),
    yaml_editor: YAMLEditor = Depends(get_yaml_editor),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
    if_match: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """Update an environment's substitution tokens.

    Supports optional ``If-Match`` header for optimistic concurrency.
    If the header is absent, last-write-wins semantics apply.
    """
    file_path = _env_file(project_root, env)

    if not file_path.exists():
        raise HTTPException(404, f"Environment not found: {env}")

    # Optimistic concurrency check
    check_etag(file_path, if_match)

    yaml_editor.write(file_path, body.tokens)
    logger.info(f"Updated environment '{env}' at {file_path}")

    await auto_commit_service.notify_change(_user_hash(user), str(file_path))

    return {"name": env, "updated": True}


# ---------------------------------------------------------------------------
# #27 — DELETE /environments/{env}
# ---------------------------------------------------------------------------


@router.delete("/{env}")
async def delete_environment(
    env: str,
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    project_root: Path = Depends(get_workspace_project_root),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
) -> Dict[str, Any]:
    """Delete an environment substitution file."""
    file_path = _env_file(project_root, env)

    if not file_path.exists():
        raise HTTPException(404, f"Environment not found: {env}")

    file_path.unlink()
    logger.info(f"Deleted environment '{env}' at {file_path}")

    await auto_commit_service.notify_change(_user_hash(user), str(file_path))

    return {"name": env, "deleted": True}


# ---------------------------------------------------------------------------
# #28 — GET /environments/{env}/secrets
# ---------------------------------------------------------------------------


@router.get("/{env}/secrets")
async def get_environment_secrets(
    env: str,
    project_root: Path = Depends(get_project_root_adaptive),
) -> Dict[str, List[str]]:
    """Scan the environment's substitution file for ``${secret:scope/key}`` references."""
    file_path = _env_file(project_root, env)

    if not file_path.exists():
        raise HTTPException(404, f"Environment not found: {env}")

    content = file_path.read_text()
    secrets = _SECRET_PATTERN.findall(content)

    return {"secrets": secrets}
