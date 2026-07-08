"""FastAPI dependency functions for the ``lhp web`` local IDE backend.

The webapp is a single-user local server: one project, one facade. The
:class:`~lhp.api.LakehousePlumberApplicationFacade` is expensive to build
(it composes the full service graph) and caches discovered flowgroups, so it
is created lazily on first use and cached on ``request.app.state`` for the
lifetime of the process.

Because that cache would otherwise never see edits made through the file-write
surface, :func:`invalidate_facade` drops the cached instance so the next
request rebuilds a fresh service graph that re-discovers flowgroups from disk.
Both the build and the invalidation are serialised by ``_facade_lock`` so a
concurrent request cannot observe or race a half-swapped ``app.state.facade``.

Per the ``webapp-uses-public-api`` import contract, this module may import
only :mod:`lhp.api` / :mod:`lhp.errors` from the ``lhp`` package (plus
FastAPI and the standard library).
"""

from __future__ import annotations

import hashlib
import threading
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request

from lhp.api import InspectionFacade, LakehousePlumberApplicationFacade
from lhp.webapp.settings import WebappSettings
from lhp.webapp.settings import get_settings as _get_settings

# Serialises the lazy build in :func:`get_facade` against
# :func:`invalidate_facade` so a mutation-driven invalidation and a concurrent
# read never race the ``app.state.facade`` swap.
_facade_lock = threading.Lock()


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
    ``request.app.state.facade`` so every subsequent request reuses the same
    fully-wired service graph. Uses double-checked locking so the expensive
    build happens once even under concurrent first requests, and so a rebuild
    triggered by :func:`invalidate_facade` is observed atomically.
    """
    facade: Optional[LakehousePlumberApplicationFacade] = getattr(
        request.app.state, "facade", None
    )
    if facade is not None:
        return facade
    with _facade_lock:
        facade = getattr(request.app.state, "facade", None)
        if facade is None:
            facade = LakehousePlumberApplicationFacade.for_project(get_project_root())
            request.app.state.facade = facade
    return facade


def invalidate_facade(app: FastAPI) -> None:
    """Drop the cached facade so the next request rebuilds it from disk.

    Called after a successful file mutation outside the generated/internal
    trees so the newly-written (or deleted) flowgroup YAML becomes visible to
    browse / validate / generate without a server restart.
    """
    with _facade_lock:
        app.state.facade = None


def get_inspection(request: Request) -> InspectionFacade:
    """Return the read-only inspection facade from the cached application facade."""
    return get_facade(request).inspection


def compute_etag(content: bytes) -> str:
    """Compute an ETag from file content."""
    return hashlib.sha256(content).hexdigest()[:16]
