import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from lhp.api.config import APISettings
from lhp.api.middleware.error_handler import generic_error_handler, lhp_error_handler
from lhp.api.middleware.request_logging import RequestLoggingMiddleware
from lhp.utils.error_formatter import LHPError

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan handler for startup/shutdown."""
    logger.info("LHP API starting up")
    yield
    logger.info("LHP API shut down")


def create_app(settings: Optional[APISettings] = None) -> FastAPI:
    """Application factory.

    Creates and configures the FastAPI application with all middleware,
    exception handlers, and routers. Uses the lifespan pattern (not
    deprecated on_startup/on_shutdown events) for resource management.

    Args:
        settings: Optional settings override (uses env vars if not provided)
    """
    settings = settings or APISettings()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = FastAPI(
        title="Lakehouse Plumber API",
        description="API layer for Lakehouse Plumber — YAML-driven Databricks pipeline generation",
        version=_get_version(),
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )

    # Store settings on app state for dependency injection
    app.state.settings = settings

    # --- Middleware (last added = first executed) ---
    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=settings.cors_allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # --- Exception handlers ---
    app.add_exception_handler(LHPError, lhp_error_handler)
    app.add_exception_handler(Exception, generic_error_handler)

    # --- Register routers ---
    # All routes under /api prefix for Databricks Apps OAuth compatibility
    from lhp.api.routers import (
        dependencies,
        flowgroups,
        generation,
        health,
        pipelines,
        presets,
        project,
        state,
        templates,
        validation,
    )

    api_prefix = "/api"
    app.include_router(health.router, prefix=api_prefix)
    app.include_router(project.router, prefix=api_prefix)
    app.include_router(pipelines.router, prefix=api_prefix)
    app.include_router(flowgroups.router, prefix=api_prefix)
    app.include_router(presets.router, prefix=api_prefix)
    app.include_router(templates.router, prefix=api_prefix)
    app.include_router(generation.router, prefix=api_prefix)
    app.include_router(validation.router, prefix=api_prefix)
    app.include_router(state.router, prefix=api_prefix)
    app.include_router(dependencies.router, prefix=api_prefix)

    logger.info(
        f"LHP API initialized: dev_mode={settings.dev_mode}, "
        f"project_root={settings.project_root}"
    )

    return app


def _get_version() -> str:
    """Get LHP package version."""
    try:
        from importlib.metadata import version
        return version("lakehouse-plumber")
    except Exception:
        return "unknown"
