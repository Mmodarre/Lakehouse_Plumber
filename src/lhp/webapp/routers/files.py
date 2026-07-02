"""File-browser router: the webapp-owned byte-I/O HTTP surface.

Exposes the four file operations the local IDE needs — tree listing, read,
write/create, delete — under ``/api/files`` (the router carries the ``/files``
sub-prefix; :func:`lhp.webapp.app.create_app` mounts it under ``/api``).

All filesystem work is delegated to :class:`lhp.webapp.services.file_io.FileIOService`,
which is the security boundary: it enforces the path-traversal guard on every
operation and the write/delete-protection list on mutations. This router only
maps the service's outcomes onto HTTP:

* :class:`~lhp.webapp.services.file_io.PathTraversalError` → ``403``
  (resolved outside the project root).
* :class:`~lhp.webapp.services.file_io.WriteProtectedError` → ``403`` with a
  distinct detail message (path is inside the root but under a read-only
  prefix such as ``.git/`` or ``generated/``).
* ``FileNotFoundError`` on read/delete → ``404``.
* ``IsADirectoryError`` on read/delete (the target path is a directory, not a
  file) → ``400`` with a ``"Not a file"`` detail.

Reads are unrestricted (only traversal-guarded), so the UI can display
generated code under ``generated/``.

PINNED DESIGN DECISION — bad YAML on PUT returns HTTP 200, not 400.
``PUT /api/files/{path}`` writes the bytes first and the write *persists* even
when the content is syntactically broken YAML (the user is deliberately saving
mid-edit). For ``*.yaml`` / ``*.yml`` targets the service re-parses the content
and, on failure, returns a positioned (1-based) diagnostic. That diagnostic is
surfaced on the SUCCESS path as the optional ``yaml_error`` field of the 200
response body — the frontend's Monaco editor consumes the structured field. A
4xx is therefore NOT returned for a YAML syntax error; only genuine
policy/security rejections (traversal, write-protection) produce a 4xx.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from lhp.webapp.dependencies import get_project_root
from lhp.webapp.services.file_io import (
    FileIOService,
    PathTraversalError,
    WriteProtectedError,
)

router = APIRouter(prefix="/files", tags=["files"])


def get_file_io_service(
    project_root: Path = Depends(get_project_root),
) -> FileIOService:
    """Build a :class:`FileIOService` bound to the configured project root."""
    return FileIOService(project_root)


class FileWriteRequest(BaseModel):
    """Request body for ``PUT /api/files/{path}``.

    Mirrors what the frontend ``writeFile`` helper sends: a single ``content``
    field (``JSON.stringify({ content })``).
    """

    content: str = Field(..., description="Full file content to write")


class YamlErrorBody(BaseModel):
    """Structured YAML syntax diagnostic (1-based line/column) for Monaco."""

    line: int
    column: int
    message: str


class FileWriteResponse(BaseModel):
    """Response body for a successful ``PUT``.

    ``yaml_error`` is the structured diagnostic, ``null`` unless a ``*.yaml`` /
    ``*.yml`` target failed to re-parse (the write still persisted).
    """

    written: bool = True
    path: str
    yaml_error: Optional[YamlErrorBody] = None


class FileDeleteResponse(BaseModel):
    """Response body for a successful ``DELETE``."""

    deleted: bool = True
    path: str


@router.get("")
def list_files(
    service: FileIOService = Depends(get_file_io_service),
) -> dict[str, Any]:
    """Return the full recursive project file tree.

    The body is the service's tree node shape — each node is
    ``{"name", "path", "type", "children"?}`` (``children`` present only on
    directory nodes) — matching the front-end ``FileBrowser`` contract.
    Excluded noise directories (``.git``, ``__pycache__``, ``.venv`` …) are
    omitted.
    """
    return service.list_tree()


@router.get("/{path:path}", response_class=PlainTextResponse)
def read_file(
    path: str,
    service: FileIOService = Depends(get_file_io_service),
) -> str:
    """Return the text content of a file.

    Reads are unrestricted except for the traversal guard (generated code under
    ``generated/`` is readable). Maps traversal to ``403``, a missing file to
    ``404``, and a directory target to ``400`` ("Not a file").
    """
    try:
        return service.read_file(path)
    except PathTraversalError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"File not found: {path}") from exc
    except IsADirectoryError as exc:
        raise HTTPException(status_code=400, detail=f"Not a file: {path}") from exc


@router.put("/{path:path}")
def write_file(
    path: str,
    body: FileWriteRequest,
    service: FileIOService = Depends(get_file_io_service),
) -> FileWriteResponse:
    """Write (creating parent dirs / the file) ``body.content`` to ``path``.

    Returns ``200`` with ``{"written": true, "path": ..., "yaml_error": ...}``.
    Bad YAML still returns ``200`` with ``yaml_error`` populated (the write
    persisted) — see the module docstring. Maps traversal and write-protection
    to ``403`` with distinct detail messages.
    """
    try:
        result = service.write_file(path, body.content)
    except PathTraversalError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except WriteProtectedError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    yaml_error: Optional[YamlErrorBody] = None
    if result.yaml_error is not None:
        yaml_error = YamlErrorBody(
            line=result.yaml_error.line,
            column=result.yaml_error.column,
            message=result.yaml_error.message,
        )
    return FileWriteResponse(written=True, path=result.path, yaml_error=yaml_error)


@router.delete("/{path:path}")
def delete_file(
    path: str,
    service: FileIOService = Depends(get_file_io_service),
) -> FileDeleteResponse:
    """Delete the file at ``path``.

    Maps traversal and write-protection to ``403`` (distinct detail messages),
    a missing file to ``404``, and a directory target to ``400`` ("Not a file").
    """
    try:
        service.delete_file(path)
    except PathTraversalError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except WriteProtectedError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"File not found: {path}") from exc
    except IsADirectoryError as exc:
        raise HTTPException(status_code=400, detail=f"Not a file: {path}") from exc

    return FileDeleteResponse(deleted=True, path=path)
