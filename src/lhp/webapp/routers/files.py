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
* ``UnicodeDecodeError`` on read (the target is not UTF-8 text) → ``415``.

Reads are unrestricted (only traversal-guarded), so the UI can display
generated code under ``generated/``.

OPTIMISTIC CONCURRENCY — every GET returns a strong ``ETag`` (quoted
``sha256(content)[:16]``) of the current bytes, and PUT / DELETE accept an
optional ``If-Match`` header. When ``If-Match`` is present and the target still
exists, its ETag must match the current on-disk content or the mutation is
rejected with ``412`` and no write happens. A missing target (PUT-create) or an
absent ``If-Match`` proceeds unconditionally, so a plain ``curl`` still works.
ETags are computed from RAW DISK BYTES everywhere — the GET body is decoded
from those same bytes with no newline translation (CRLF is preserved; Monaco
handles CRLF), and the write path persists exactly ``content.encode("utf-8")``
— so the validator a client holds never drifts from what the enforcement side
hashes. Mutations are serialized process-wide by a module-level lock, so an
If-Match check and the write/delete it guards are atomic with respect to
concurrent mutations.

FACADE INVALIDATION — a successful write or delete outside the generated /
internal trees (``generated/`` / ``.lhp/`` / ``.git/``) invalidates the cached
application facade so the edit is immediately visible to browse / validate /
generate without a server restart. This fires even when ``yaml_error`` is set,
because the bytes persisted.

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

import threading
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from lhp.webapp.dependencies import compute_etag, get_project_root, invalidate_facade
from lhp.webapp.services.file_io import (
    FileIOService,
    PathTraversalError,
    WriteProtectedError,
    is_generated_or_internal,
)

router = APIRouter(prefix="/files", tags=["files"])

# Serializes If-Match enforcement together with the mutation it guards, in both
# PUT and DELETE. Without it, two conditional mutations carrying the same valid
# If-Match could both pass the check under the threadpool and both write
# (check-then-mutate TOCTOU). Serializing file mutations is cheap for a
# single-user local IDE; mirrors ``_facade_lock`` in ``lhp.webapp.dependencies``.
_mutation_lock = threading.Lock()


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
    ``*.yml`` target failed to re-parse (the write still persisted). ``etag`` is
    the strong validator of the bytes just written — the frontend echoes it back
    as the next ``If-Match`` for optimistic concurrency.
    """

    written: bool = True
    path: str
    yaml_error: Optional[YamlErrorBody] = None
    etag: Optional[str] = None


class FileDeleteResponse(BaseModel):
    """Response body for a successful ``DELETE``."""

    deleted: bool = True
    path: str


def _parse_if_match(if_match: Optional[str]) -> Optional[str]:
    """Normalize an ``If-Match`` header value to a bare ETag for comparison.

    Strips an optional weak-validator ``W/`` prefix and the surrounding double
    quotes so the value compares equal to :func:`compute_etag`'s bare digest.
    Returns ``None`` when the header is absent.

    Deliberate RFC 9110 deviations: ``*`` is treated as a literal etag value
    (the match-anything form is unsupported), and comma-separated etag lists
    are unsupported — the only intended client is the bundled SPA, which
    always echoes back a single quoted validator.
    """
    if if_match is None:
        return None
    value = if_match.strip()
    if value.startswith("W/"):
        value = value[2:]
    return value.strip('"')


def _enforce_if_match(
    service: FileIOService, path: str, if_match: Optional[str]
) -> None:
    """Reject a stale conditional mutation with ``412`` before it writes.

    When ``If-Match`` is present and the target currently exists on disk, its
    ETag must equal the current content's ETag; a mismatch means the client's
    view is stale and the mutation would clobber a concurrent change. A
    non-existent target (create) or an absent header proceeds unconditionally.
    Callers hold ``_mutation_lock`` across this check and the mutation it
    guards so the pair is atomic.
    """
    expected = _parse_if_match(if_match)
    if expected is None:
        return
    current = service.read_bytes_if_exists(path)
    if current is None:
        return
    if compute_etag(current) != expected:
        raise HTTPException(
            status_code=412,
            detail="File was modified since you last read it. "
            "Re-fetch the current version and retry.",
        )


@router.get("")
def list_files(
    service: FileIOService = Depends(get_file_io_service),
) -> dict[str, Any]:
    """Return the full recursive project file tree.

    The body is the service's tree node shape — each node is
    ``{"name", "path", "type", "children"?}`` (``children`` present only on
    directory nodes; an unreadable directory also carries an ``"error"``
    marker) — matching the front-end ``FileBrowser`` contract. Excluded noise
    directories (``.git``, ``__pycache__``, ``.venv`` …) are omitted.
    """
    return service.list_tree()


@router.get("/{path:path}", response_class=PlainTextResponse)
def read_file(
    path: str,
    service: FileIOService = Depends(get_file_io_service),
) -> PlainTextResponse:
    """Return the text content of a file, tagged with a strong ``ETag``.

    Reads are unrestricted except for the traversal guard (generated code under
    ``generated/`` is readable). Maps traversal to ``403``, a missing file to
    ``404``, a directory target to ``400`` ("Not a file"), and non-UTF-8
    content to ``415``. The file is read ONCE as raw bytes: the ``ETag`` header
    is the quoted hash of those exact bytes (so it is always a valid
    ``If-Match`` for the next mutation) and the body is their UTF-8 decoding
    with no newline translation (CRLF is preserved).
    """
    try:
        raw = service.read_file_bytes(path)
    except PathTraversalError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"File not found: {path}") from exc
    except IsADirectoryError as exc:
        raise HTTPException(status_code=400, detail=f"Not a file: {path}") from exc

    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=415, detail="Not a UTF-8 text file") from exc

    etag = compute_etag(raw)
    return PlainTextResponse(content, headers={"ETag": f'"{etag}"'})


@router.put("/{path:path}")
def write_file(
    path: str,
    body: FileWriteRequest,
    request: Request,
    response: Response,
    service: FileIOService = Depends(get_file_io_service),
    if_match: Optional[str] = Header(None, alias="If-Match"),
) -> FileWriteResponse:
    """Write (creating parent dirs / the file) ``body.content`` to ``path``.

    Returns ``200`` with ``{"written": true, "path": ..., "yaml_error": ...,
    "etag": ...}`` and an ``ETag`` response header for the new content. Bad YAML
    still returns ``200`` with ``yaml_error`` populated (the write persisted) —
    see the module docstring. A stale ``If-Match`` yields ``412`` with no write;
    the check and the write are atomic under the process-wide mutation lock.
    Maps traversal and write-protection to ``403`` with distinct details. A
    successful mutation outside the generated/internal trees invalidates the
    cached facade.
    """
    try:
        with _mutation_lock:
            _enforce_if_match(service, path, if_match)
            result = service.write_file(path, body.content)
    except PathTraversalError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except WriteProtectedError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    if not is_generated_or_internal(path):
        invalidate_facade(request.app)

    new_etag = compute_etag(body.content.encode("utf-8"))
    response.headers["ETag"] = f'"{new_etag}"'

    yaml_error: Optional[YamlErrorBody] = None
    if result.yaml_error is not None:
        yaml_error = YamlErrorBody(
            line=result.yaml_error.line,
            column=result.yaml_error.column,
            message=result.yaml_error.message,
        )
    return FileWriteResponse(
        written=True, path=result.path, yaml_error=yaml_error, etag=new_etag
    )


@router.delete("/{path:path}")
def delete_file(
    path: str,
    request: Request,
    service: FileIOService = Depends(get_file_io_service),
    if_match: Optional[str] = Header(None, alias="If-Match"),
) -> FileDeleteResponse:
    """Delete the file at ``path``.

    A stale ``If-Match`` yields ``412`` with no deletion (the file still
    exists); the check and the unlink are atomic under the process-wide
    mutation lock. Maps traversal and write-protection to ``403`` (distinct
    detail messages), a missing file to ``404``, and a directory target to
    ``400`` ("Not a file"). A successful deletion outside the
    generated/internal trees invalidates the cached facade.
    """
    try:
        with _mutation_lock:
            _enforce_if_match(service, path, if_match)
            service.delete_file(path)
    except PathTraversalError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except WriteProtectedError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"File not found: {path}") from exc
    except IsADirectoryError as exc:
        raise HTTPException(status_code=400, detail=f"Not a file: {path}") from exc

    if not is_generated_or_internal(path):
        invalidate_facade(request.app)

    return FileDeleteResponse(deleted=True, path=path)
