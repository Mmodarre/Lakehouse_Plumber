"""FastAPI application factory for the LHP local web IDE backend.

``create_app`` is the zero-argument uvicorn factory target
(``lhp.webapp.app:create_app``); all configuration flows from the
``LHP_WEBAPP_*`` environment via :func:`lhp.webapp.settings.get_settings`.

The router registry is deliberately *tolerant*: it iterates a pinned list of
router module names and skips any that are not yet present. Static SPA serving
lives in :mod:`lhp.webapp.static_app`.

This app is same-origin only (the SPA and API are served from one process), so
there is no CORS middleware. There is no workspace/state init and no cache
middleware — those belonged to the multi-tenant hosted variant, not the local
IDE.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from fastapi import FastAPI
from starlette.middleware.trustedhost import TrustedHostMiddleware

from lhp import telemetry
from lhp.errors import LHPError
from lhp.webapp import static_app
from lhp.webapp.middleware.error_handler import (
    generic_error_handler,
    lhp_error_handler,
)
from lhp.webapp.middleware.origin_guard import OriginGuardMiddleware
from lhp.webapp.middleware.request_logging import RequestLoggingMiddleware
from lhp.webapp.middleware.telemetry_session import TelemetrySessionMiddleware
from lhp.webapp.middleware.token_guard import TokenGuardMiddleware
from lhp.webapp.services import dataset_index, file_watcher, sqlite_store
from lhp.webapp.services.event_bus import EventBus
from lhp.webapp.services.telemetry_sessions import WebSessionRegistry
from lhp.webapp.settings import get_settings
from lhp.webapp.static_app import _API_PREFIX

# DNS-rebinding defense: a rebinding attack changes the resolved IP but not the
# Host header, so restricting Host to loopback names cuts it off. Compared
# without the port by Starlette's TrustedHostMiddleware.
_ALLOWED_HOSTS = ["127.0.0.1", "localhost", "::1"]

logger = logging.getLogger(__name__)

# Pinned router registry. Each name maps to ``lhp.webapp.routers.<name>`` whose
# ``router`` attribute is mounted under ``/api`` (the routers carry their own
# sub-prefix, e.g. ``/pipelines``, so the final path is ``/api/pipelines``).
# Missing modules are skipped.
_ROUTER_MODULES: tuple[str, ...] = (
    "health",
    "project",
    "pipelines",
    "flowgroups",
    "tables",
    "lineage",
    "presets",
    "templates",
    "blueprints",
    "environments",
    "metadata",
    "dependencies",
    "sandbox",
    "schemas",
    "config_templates",
    "files",
    "streaming",
    "runs",
    "events",
    "telemetry",
    "assistant",
    "assistant_config",
    "assistant_sessions",
)


def _get_version() -> str:
    """Resolve the installed package version, or ``"unknown"`` if unavailable."""
    try:
        return version("lakehouse-plumber")
    except PackageNotFoundError:
        return "unknown"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Lifespan handler: log startup, resolve the project state, drain on exit.

    Fail-closed project root: when ``project_root`` holds no ``lhp.yaml`` the
    server keeps running (a later init wizard needs it up) but
    ``app.state.project_state`` is set to ``"no_project"`` so ``/api/health``
    can tell the SPA to render guidance instead of a broken IDE. Shutdown
    stops the watcher, emits every open telemetry session and closes the
    assistant client, in that order.
    """
    settings = app.state.settings
    logger.info(
        f"LHP web IDE starting up: version={_get_version()}, "
        f"project_root={settings.project_root}"
    )
    watcher_task: asyncio.Task[None] | None = None
    if _holds_project(settings.project_root):
        app.state.project_state = "ok"
        # Run-history DB init only for a REAL project: migrations bring
        # .lhp/webapp.db to the current schema, then crash recovery closes out
        # runs a previous process left "running". In "no_project" state this is
        # skipped entirely so a non-project directory never grows a .lhp/.
        await asyncio.to_thread(sqlite_store.run_migrations, settings.project_root)
        await asyncio.to_thread(
            sqlite_store.mark_orphaned_runs_failed, settings.project_root
        )
        # Live updates only for a real project: the watcher polls the tree,
        # marks the dependency graph stale (serve-stale) or invalidates the
        # cached facade depending on what changed, and publishes file-changed /
        # graph-stale bus events for the SSE endpoint.
        watcher_task = asyncio.create_task(
            file_watcher.watch(app), name="lhp-file-watcher"
        )
    else:
        logger.error(
            f"No lhp.yaml found at {settings.project_root} — "
            "serving in 'no_project' state"
        )
        app.state.project_state = "no_project"
    yield
    if watcher_task is not None:
        watcher_task.cancel()
        with suppress(asyncio.CancelledError):
            await watcher_task
    await _shutdown_telemetry(app)
    # The lazily-cached omnigent client (see get_omnigent_client) owns a
    # connection pool; close it if this process ever built one.
    omnigent_client = getattr(app.state, "omnigent_client", None)
    if omnigent_client is not None:
        await omnigent_client.aclose()
    logger.info("LHP web IDE shut down")


def _holds_project(project_root: Path) -> bool:
    """Whether ``project_root`` is a real LHP project rather than a bare directory."""
    return (project_root / "lhp.yaml").is_file()


async def _shutdown_telemetry(app: FastAPI) -> None:
    """End every live web session as ``shutdown`` and let the batch leave.

    Pending SSE grace tasks are cancelled and awaited first so none can race
    ``emit_all`` with a ``disconnect`` of its own; the flush then waits at
    most one second for the sender thread. Telemetry must never keep the
    server from shutting down, so every failure is logged and dropped.
    """
    if not getattr(app.state, "telemetry_enabled", False):
        return
    registry: WebSessionRegistry | None = getattr(app.state, "web_sessions", None)
    if registry is None:
        return
    try:
        pending = registry.pending_grace_tasks()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.to_thread(registry.emit_all, "shutdown")
        await asyncio.to_thread(telemetry.flush, 1.0)
    except Exception:  # shutdown must never fail because of telemetry
        logger.debug("telemetry: web session shutdown emission failed", exc_info=True)


def _register_routers(app: FastAPI) -> None:
    """Mount each available router module under ``/api`` (tolerant registry)."""
    for name in _ROUTER_MODULES:
        try:
            module = importlib.import_module(f"lhp.webapp.routers.{name}")
        except ModuleNotFoundError:
            logger.debug(f"Router module not present, skipping: {name}")
            continue
        app.include_router(module.router, prefix=_API_PREFIX)


def create_app() -> FastAPI:
    """Build and configure the FastAPI application.

    Zero-argument: this is the uvicorn factory target
    (``lhp.webapp.app:create_app``). All configuration flows from the
    ``LHP_WEBAPP_*`` environment via :func:`get_settings`.
    """
    settings = get_settings()

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = FastAPI(
        title="Lakehouse Plumber Web IDE",
        description=(
            "Local web IDE for Lakehouse Plumber — YAML-driven Databricks "
            "pipeline generation."
        ),
        version=_get_version(),
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    app.state.settings = settings
    # Server-push fan-out (run-updated today; file-watcher events later). One
    # bus per app instance, shared by the run recorder and a future SSE endpoint.
    app.state.event_bus = EventBus()
    # Serve-stale dependency graph: the watcher sets this True on a
    # graph-relevant edit; POST /api/dependencies/refresh clears it. Seeded
    # False so GET /api/dependencies/staleness is O(1) and never falls through
    # to the facade metadata in normal operation.
    app.state.graph_stale = False
    # Per-env dataset lineage index cache (GET /api/lineage). Dropped by the
    # file watcher on a graph-relevant edit and by POST /api/dependencies/refresh,
    # mirroring the dependency graph's serve-stale invalidation.
    app.state.dataset_index_cache = dataset_index.DatasetIndexCache()
    # Consent is evaluated once per process: the uvicorn worker inherits the
    # shell environment, so every off switch applies, and --reload re-evaluates
    # it in each respawned worker. Every telemetry hook gates on this flag.
    app.state.telemetry_enabled = telemetry.effective_state().enabled
    # The registry always exists so hooks can read it unconditionally; its
    # project_root feeds every web.session's project id and is therefore the
    # served directory only when it really holds a project, None otherwise.
    app.state.web_sessions = WebSessionRegistry(
        project_root=(
            settings.project_root if _holds_project(settings.project_root) else None
        )
    )

    # Middleware (Starlette: last added = outermost). Effective request order:
    # TrustedHost -> OriginGuard -> TokenGuard -> RequestLogging ->
    # TelemetrySession -> routes. Telemetry is innermost so a guard rejection
    # is never counted and the matched route is already in the scope.
    # Same-origin only: no CORS.
    app.add_middleware(TelemetrySessionMiddleware)
    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(TokenGuardMiddleware)
    app.add_middleware(OriginGuardMiddleware)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=_ALLOWED_HOSTS)

    # Exception handlers.
    app.add_exception_handler(LHPError, lhp_error_handler)  # type: ignore[arg-type]  # handler registry typed against Exception base; LHPError signature is intentionally narrower
    app.add_exception_handler(Exception, generic_error_handler)

    # Routers first, then static mount so /api wins.
    _register_routers(app)
    static_app.mount_spa(app)

    logger.info(f"LHP web IDE initialized: project_root={settings.project_root}")
    return app
