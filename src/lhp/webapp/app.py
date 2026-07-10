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

from fastapi import FastAPI
from starlette.middleware.trustedhost import TrustedHostMiddleware

from lhp.errors import LHPError
from lhp.webapp import static_app
from lhp.webapp.middleware.error_handler import (
    generic_error_handler,
    lhp_error_handler,
)
from lhp.webapp.middleware.origin_guard import OriginGuardMiddleware
from lhp.webapp.middleware.request_logging import RequestLoggingMiddleware
from lhp.webapp.middleware.token_guard import TokenGuardMiddleware
from lhp.webapp.services import file_watcher, sqlite_store
from lhp.webapp.services.event_bus import EventBus
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
    "presets",
    "templates",
    "blueprints",
    "environments",
    "dependencies",
    "schemas",
    "config_templates",
    "files",
    "streaming",
    "runs",
    "events",
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
    """Lifespan handler: log startup and resolve the project state.

    Fail-closed project root: when ``project_root`` holds no ``lhp.yaml`` the
    server keeps running (a later init wizard needs it up) but
    ``app.state.project_state`` is set to ``"no_project"`` so ``/api/health``
    can tell the SPA to render guidance instead of a broken IDE.
    """
    settings = app.state.settings
    logger.info(
        f"LHP web IDE starting up: version={_get_version()}, "
        f"project_root={settings.project_root}"
    )
    watcher_task: asyncio.Task[None] | None = None
    if (settings.project_root / "lhp.yaml").is_file():
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
        # invalidates the cached facade on change, and publishes file-changed
        # bus events for the SSE endpoint.
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
    # The lazily-cached omnigent client (see get_omnigent_client) owns a
    # connection pool; close it if this process ever built one.
    omnigent_client = getattr(app.state, "omnigent_client", None)
    if omnigent_client is not None:
        await omnigent_client.aclose()
    logger.info("LHP web IDE shut down")


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

    # Middleware (Starlette: last added = outermost). Effective request order:
    # TrustedHost -> OriginGuard -> TokenGuard -> RequestLogging -> routes.
    # Same-origin only: no CORS.
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
