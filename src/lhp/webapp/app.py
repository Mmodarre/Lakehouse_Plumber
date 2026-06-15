"""FastAPI application factory for the LHP local web IDE backend.

``create_app`` is the zero-argument uvicorn factory target
(``lhp.webapp.app:create_app``); all configuration flows from the
``LHP_WEBAPP_*`` environment via :func:`lhp.webapp.settings.get_settings`.

The router registry is deliberately *tolerant*: it iterates a pinned list of
router module names and skips any that are not yet present.

This app is same-origin only (the SPA and API are served from one process), so
there is no CORS middleware. There is no workspace/state init and no cache
middleware — those belonged to the multi-tenant hosted variant, not the local
IDE.
"""

from __future__ import annotations

import importlib
import importlib.resources
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from starlette.responses import FileResponse, Response
from starlette.staticfiles import StaticFiles

from lhp.errors import LHPError
from lhp.webapp.middleware.error_handler import (
    generic_error_handler,
    lhp_error_handler,
)
from lhp.webapp.middleware.request_logging import RequestLoggingMiddleware
from lhp.webapp.settings import get_settings

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
    "environments",
    "dependencies",
    "schemas",
    "files",
    "streaming",
)

_API_PREFIX = "/api"

_SPA_NOT_BUILT_MESSAGE = (
    "Lakehouse Plumber web IDE\n"
    "=========================\n\n"
    "The single-page app has not been built, so only the JSON API is being\n"
    "served (try /api/health).\n\n"
    "To build the frontend assets, run:\n\n"
    "    scripts/build_webapp.sh\n\n"
    "then restart the server.\n"
)


def _get_version() -> str:
    """Resolve the installed package version, or ``"unknown"`` if unavailable."""
    try:
        return version("lakehouse-plumber")
    except PackageNotFoundError:
        return "unknown"


class SPAStaticFiles(StaticFiles):
    """StaticFiles subclass with SPA fallback.

    Serves ``index.html`` for any path that does not match a real static file,
    enabling client-side routing (React Router). Mounted after the API routers,
    so ``/api/*`` paths never reach this handler.
    """

    async def get_response(self, path: str, scope: Any) -> Response:
        try:
            return await super().get_response(path, scope)
        except Exception:
            # Path doesn't match a static file — serve index.html so the SPA
            # can handle client-side routing. ``directory`` is always set when
            # this subclass is constructed in ``_mount_static``.
            assert self.directory is not None
            return FileResponse(
                Path(self.directory) / "index.html",
                media_type="text/html",
            )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Lifespan handler: logging only — no workspace/state initialization."""
    settings = get_settings()
    logger.info(
        f"LHP web IDE starting up: version={_get_version()}, "
        f"project_root={settings.project_root}"
    )
    yield
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


def _mount_static(app: FastAPI) -> None:
    """Mount the SPA at ``/`` if built; otherwise add a plain-text fallback.

    Mounted AFTER the routers so ``/api`` matches first. In a dev tree the
    static assets are gitignored, so the API still serves and ``GET /`` returns
    a 200 plain-text page explaining how to build the SPA.
    """
    static_dir = importlib.resources.files("lhp.webapp") / "static"
    index_html = static_dir / "index.html"

    if static_dir.is_dir() and index_html.is_file():
        app.mount(
            "/",
            SPAStaticFiles(directory=str(static_dir), html=True),
            name="static",
        )
        logger.info(f"Serving SPA from {static_dir}")
        return

    logger.warning(
        "SPA static assets not found — serving API only. "
        "Run scripts/build_webapp.sh to build the frontend."
    )

    @app.get("/", response_class=PlainTextResponse)
    async def spa_not_built() -> str:
        return _SPA_NOT_BUILT_MESSAGE


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

    # Middleware (last added = first executed). Same-origin only: no CORS.
    app.add_middleware(RequestLoggingMiddleware)

    # Exception handlers.
    app.add_exception_handler(LHPError, lhp_error_handler)  # type: ignore[arg-type]  # handler registry typed against Exception base; LHPError signature is intentionally narrower
    app.add_exception_handler(Exception, generic_error_handler)

    # Routers first, then static mount so /api wins.
    _register_routers(app)
    _mount_static(app)

    logger.info(f"LHP web IDE initialized: project_root={settings.project_root}")
    return app
