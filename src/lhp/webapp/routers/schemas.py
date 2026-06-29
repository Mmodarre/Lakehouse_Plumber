"""Canonical JSON-Schema router for the LHP web IDE backend.

Serves the JSON Schema documents that ship as package data inside the
``lhp.schemas`` package (``<kind>.schema.json``). The frontend's monaco-yaml
wiring consumes the response body *directly* as an inline JSON Schema, so the
raw parsed document is returned with no envelope (no ``{"schema": ...}`` wrapper).

``lhp.schemas`` is accessed as package DATA via ``importlib.resources`` on the
package-name string — never imported as a module — so this router stays within
the ``lhp.webapp`` boundary contract (it imports only ``lhp.errors`` and stdlib).
"""

from __future__ import annotations

import json
import re
from importlib.resources import files
from typing import Any

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/schemas", tags=["schemas"])

# A schema ``kind`` is a bare lowercase identifier. Validating against this
# pattern BEFORE building any path blocks traversal (``..``, ``/``, ``%2F``,
# etc.) — only ``[a-z_]`` characters can ever reach the filesystem lookup.
_KIND_PATTERN = re.compile(r"^[a-z_]+$")

# Package whose data files (``<kind>.schema.json``) are served. Passed as a
# STRING to importlib.resources — NOT imported as a module (boundary §5.3).
_SCHEMAS_PACKAGE = "lhp.schemas"


@router.get("/{kind}")
def get_schema(kind: str) -> Any:
    """Return the raw parsed ``<kind>.schema.json`` JSON Schema document.

    The body is the schema document itself (no envelope), suitable for direct
    use as an inline JSON Schema by the frontend.

    Raises:
        HTTPException: 404 if ``kind`` is malformed or the schema does not exist.
    """
    if not _KIND_PATTERN.fullmatch(kind):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown schema kind: {kind!r}",
        )

    resource = files(_SCHEMAS_PACKAGE) / f"{kind}.schema.json"
    if not resource.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"Unknown schema kind: {kind!r}",
        )

    return json.loads(resource.read_text(encoding="utf-8"))
