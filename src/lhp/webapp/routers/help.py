"""Packaged contextual field guidance, available without an external docs service."""

from __future__ import annotations

import json
import re
from importlib.resources import files
from typing import Any

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/help", tags=["help"])
_KIND = re.compile(r"^[a-z_]+$")


@router.get("/{kind}")
def get_field_help(kind: str) -> Any:
    """Read one small category of reviewed field guidance from package data."""
    if not _KIND.fullmatch(kind):
        raise HTTPException(404, "Unknown help category")
    resource = files("lhp.schemas") / "help" / f"{kind}.json"
    if not resource.is_file():
        raise HTTPException(404, "Unknown help category")
    return json.loads(resource.read_text(encoding="utf-8"))
