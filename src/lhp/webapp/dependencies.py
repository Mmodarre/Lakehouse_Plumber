"""FastAPI dependency functions for the ``lhp web`` local IDE backend.

The webapp is a single-user local server over one project. The
:class:`~lhp.api.LakehousePlumberApplicationFacade` is expensive to build
(it composes the full service graph) and caches discovered flowgroups, so
instances are created lazily on first use and cached on
``request.app.state.facades`` for the lifetime of the process — a dict keyed
by the resolved absolute pipeline-config path the facade was built with
(``None`` for the default, config-less facade; the streaming router keys runs
by the request's ``pipeline_config``, mirroring the CLI's ``--pipeline-config``
flag).

Because that cache would otherwise never see edits made through the file-write
surface, :func:`invalidate_facade` drops every cached instance so the next
request rebuilds a fresh service graph that re-discovers flowgroups from disk.
Both the build and the invalidation are serialised by ``_facade_lock`` so a
concurrent request cannot observe or race a half-swapped
``app.state.facades``.

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

from lhp.api import (
    DependencyFacade,
    InspectionFacade,
    LakehousePlumberApplicationFacade,
    SandboxFacade,
)
from lhp.webapp.settings import WebappSettings
from lhp.webapp.settings import get_settings as _get_settings

# Serialises the lazy builds in :func:`get_facade_for` against
# :func:`invalidate_facade` so a mutation-driven invalidation and a concurrent
# read never race the ``app.state.facades`` swap.
_facade_lock = threading.Lock()


def get_settings() -> WebappSettings:
    """Return the env-var-driven webapp settings."""
    return _get_settings()


def get_project_root() -> Path:
    """Return the configured project root from settings."""
    return get_settings().project_root


def _cache_key(pipeline_config: Optional[str]) -> Optional[str]:
    """Canonicalise a pipeline-config path into its facade-cache key.

    Resolved against the PROJECT ROOT (never the process cwd — the server may
    have been launched from anywhere), so the key and the path handed to
    ``for_project`` are stable and absolute. An already-absolute input passes
    through ``Path.__truediv__`` unchanged. ``None`` keys the default facade.
    """
    if pipeline_config is None:
        return None
    return str((get_project_root() / pipeline_config).resolve())


def get_facade_for(
    request: Request, pipeline_config: Optional[str]
) -> LakehousePlumberApplicationFacade:
    """Return the cached application facade keyed by ``pipeline_config``.

    ``pipeline_config`` is a pipeline-config YAML path (project-relative or
    absolute) or ``None`` for the default facade. Built lazily via
    :meth:`LakehousePlumberApplicationFacade.for_project` — with the resolved
    absolute path as ``pipeline_config_path`` — and cached in the
    ``request.app.state.facades`` dict so subsequent requests for the same
    config reuse the same fully-wired service graph. Uses double-checked
    locking so the expensive build happens once even under concurrent first
    requests, and so a rebuild triggered by :func:`invalidate_facade` is
    observed atomically.
    """
    key = _cache_key(pipeline_config)
    cache: Optional[dict[Optional[str], LakehousePlumberApplicationFacade]] = getattr(
        request.app.state, "facades", None
    )
    if cache is not None:
        facade = cache.get(key)
        if facade is not None:
            return facade
    with _facade_lock:
        cache = getattr(request.app.state, "facades", None)
        if cache is None:
            cache = {}
            request.app.state.facades = cache
        facade = cache.get(key)
        if facade is None:
            facade = LakehousePlumberApplicationFacade.for_project(
                get_project_root(), pipeline_config_path=key
            )
            cache[key] = facade
    return facade


def get_facade(request: Request) -> LakehousePlumberApplicationFacade:
    """Return the default (config-less) cached application facade.

    Equivalent to ``get_facade_for(request, None)`` — the facade every DI user
    outside the streaming router depends on.
    """
    return get_facade_for(request, None)


def invalidate_facade(app: FastAPI) -> None:
    """Drop every cached facade so the next request rebuilds from disk.

    Called after a successful file mutation outside the generated/internal
    trees so the newly-written (or deleted) YAML becomes visible to browse /
    validate / generate without a server restart. Clears ALL keys: any config
    edit may affect every facade regardless of which pipeline-config file it
    was built with.
    """
    with _facade_lock:
        app.state.facades = {}


def invalidate_discovery_caches(app: FastAPI) -> None:
    """Refresh every cached facade's flowgroup discovery WITHOUT dropping it.

    The targeted counterpart to :func:`invalidate_facade`, called by the file
    watcher on a graph-relevant edit made outside the write API. Dropping the
    whole facade would also nuke the in-process dependency-graph memo and
    defeat serve-stale, so this clears ONLY each facade's discovery memo (via
    :meth:`~lhp.api.InspectionFacade.invalidate_discovery_cache`): the
    inspection reads (``/api/flowgroups``, ``/api/project``, stats, tables, …)
    re-read the edited project from disk on their next call, while the
    dependency-graph endpoints keep serving the last-good graph from the
    untouched memo until an explicit ``POST /api/dependencies/refresh``.
    Serialised by ``_facade_lock`` against the lazy builds in
    :func:`get_facade_for`.
    """
    with _facade_lock:
        cache: Optional[dict[Optional[str], LakehousePlumberApplicationFacade]] = (
            getattr(app.state, "facades", None)
        )
        if not cache:
            return
        for facade in cache.values():
            facade.inspection.invalidate_discovery_cache()


def get_inspection(request: Request) -> InspectionFacade:
    """Return the read-only inspection facade from the cached application facade."""
    return get_facade(request).inspection


def get_dependency(request: Request) -> DependencyFacade:
    """Return the dependency-analysis facade from the cached application facade."""
    return get_facade(request).dependency


def get_sandbox(request: Request) -> SandboxFacade:
    """Return the sandbox scope facade from the cached application facade."""
    return get_facade(request).sandbox


def compute_etag(content: bytes) -> str:
    """Compute an ETag from file content."""
    return hashlib.sha256(content).hexdigest()[:16]
