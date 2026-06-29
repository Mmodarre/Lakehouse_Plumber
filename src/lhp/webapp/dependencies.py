"""FastAPI dependency functions for the ``lhp web`` local IDE backend.

The webapp is a single-user local server: one project, one facade. The
:class:`~lhp.api.LakehousePlumberApplicationFacade` is expensive to build
(it composes the full service graph), so it is created lazily on first use
and cached on ``request.app.state`` for the lifetime of the process.

Per the ``webapp-uses-public-api`` import contract, this module may import
only :mod:`lhp.api` / :mod:`lhp.errors` from the ``lhp`` package (plus
FastAPI and the standard library).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

from fastapi import Header, HTTPException, Request

from lhp.api import InspectionFacade, LakehousePlumberApplicationFacade
from lhp.webapp.settings import WebappSettings
from lhp.webapp.settings import get_settings as _get_settings


def get_settings() -> WebappSettings:
    """Return the env-var-driven webapp settings."""
    return _get_settings()


def get_project_root() -> Path:
    """Return the configured project root from settings."""
    return get_settings().project_root


def get_facade(request: Request) -> LakehousePlumberApplicationFacade:
    """Return the one cached application facade for this process.

    Built lazily on first request via
    :meth:`LakehousePlumberApplicationFacade.for_project` and cached on
    ``request.app.state.facade`` so every subsequent request reuses the
    same fully-wired service graph.
    """
    facade: Optional[LakehousePlumberApplicationFacade] = getattr(
        request.app.state, "facade", None
    )
    if facade is None:
        facade = LakehousePlumberApplicationFacade.for_project(get_project_root())
        request.app.state.facade = facade
    return facade


def get_inspection(request: Request) -> InspectionFacade:
    """Return the read-only inspection facade from the cached application facade."""
    return get_facade(request).inspection


def compute_etag(content: bytes) -> str:
    """Compute an ETag from file content."""
    return hashlib.sha256(content).hexdigest()[:16]


def check_etag(
    file_path: Path,
    if_match: Optional[str] = Header(None),
) -> None:
    """Validate If-Match header against current file content."""
    if if_match is None:
        return
    if not file_path.exists():
        raise HTTPException(404, f"File not found: {file_path.name}")
    current_etag = compute_etag(file_path.read_bytes())
    if current_etag != if_match:
        raise HTTPException(
            status_code=412,
            detail="File was modified since you last read it. "
            "Re-fetch the current version and retry.",
        )
