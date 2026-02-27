import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from lhp.api.config import APISettings
from lhp.api.middleware.cache_invalidation import CacheInvalidationMiddleware
from lhp.api.middleware.error_handler import generic_error_handler, lhp_error_handler
from lhp.api.middleware.request_logging import RequestLoggingMiddleware
from lhp.api.services.workspace_manager import WorkspaceManager
from lhp.utils.error_formatter import LHPError

logger = logging.getLogger(__name__)


async def _periodic_cleanup(workspace_mgr: WorkspaceManager) -> None:
    """Run workspace cleanup every hour."""
    while True:
        await asyncio.sleep(3600)
        removed = await asyncio.to_thread(workspace_mgr.cleanup_expired)
        if removed:
            logger.info(f"Periodic cleanup removed {len(removed)} workspace(s)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan handler for startup/shutdown."""
    logger.info("LHP API starting up")

    # --- Phase 2 startup ---
    from lhp.api.services.auto_commit_service import AutoCommitService

    settings = app.state.settings

    # Initialize workspace manager (per-app instance, not global singleton)
    source_repo = settings.source_repo or str(settings.project_root)
    workspace_mgr = WorkspaceManager(
        workspace_root=settings.workspace_root,
        source_repo=source_repo,
        max_workspaces=settings.workspace_max_count,
        idle_ttl_hours=settings.workspace_idle_ttl_hours,
        stopped_ttl_hours=settings.workspace_stopped_ttl_hours,
    )
    app.state.workspace_manager = workspace_mgr

    # Initialize auto-commit service (per-app instance)
    auto_commit_svc = AutoCommitService()
    app.state.auto_commit_service = auto_commit_svc

    # Wire auto-commit resolver to workspace manager
    auto_commit_svc.set_git_service_resolver(workspace_mgr.get_git_service_by_hash)

    # Start periodic cleanup
    cleanup_task = asyncio.create_task(_periodic_cleanup(workspace_mgr))
    app.state._cleanup_task = cleanup_task

    # --- AI assistant (OpenCode) startup ---
    if settings.ai_enabled:
        from lhp.api.services.ai_config import AIConfig
        from lhp.api.services.opencode_manager import OpenCodeProcessPool

        ai_config = AIConfig.load(settings.project_root)

        # Log AI-related env var presence (not values) for diagnostics
        _ai_env_vars = [
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_MODEL",
            "ANTHROPIC_CUSTOM_HEADERS",
        ]
        present = [v for v in _ai_env_vars if os.environ.get(v)]
        missing = [v for v in _ai_env_vars if not os.environ.get(v)]
        logger.info(f"AI env vars present: {present or '(none)'}")
        if missing:
            logger.info(f"AI env vars not set: {missing}")

        opencode_pool = OpenCodeProcessPool(
            ai_config=ai_config,
            dev_mode=settings.dev_mode,
            project_root=settings.project_root if settings.dev_mode else None,
            default_port=settings.opencode_port,
            password=settings.opencode_password,
            external_url=settings.opencode_url,
        )
        app.state.opencode_pool = opencode_pool
        app.state.ai_config = ai_config
        await opencode_pool.start()

    yield

    # --- AI assistant shutdown ---
    if hasattr(app.state, "opencode_pool"):
        await app.state.opencode_pool.stop()

    # --- Phase 2 shutdown ---
    app.state._cleanup_task.cancel()
    try:
        await app.state._cleanup_task
    except asyncio.CancelledError:
        pass

    await app.state.auto_commit_service.shutdown()

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
    app.add_middleware(CacheInvalidationMiddleware)
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
        ai,
        dependencies,
        environments,
        files,
        flowgroups,
        generation,
        health,
        pipelines,
        presets,
        project,
        state,
        tables,
        templates,
        validation,
        workspace,
    )

    api_prefix = "/api"
    app.include_router(health.router, prefix=api_prefix)
    app.include_router(project.router, prefix=api_prefix)
    app.include_router(pipelines.router, prefix=api_prefix)
    app.include_router(flowgroups.router, prefix=api_prefix)
    app.include_router(tables.router, prefix=api_prefix)
    app.include_router(presets.router, prefix=api_prefix)
    app.include_router(templates.router, prefix=api_prefix)
    app.include_router(generation.router, prefix=api_prefix)
    app.include_router(generation.generated_router, prefix=api_prefix)
    app.include_router(validation.router, prefix=api_prefix)
    app.include_router(state.router, prefix=api_prefix)
    app.include_router(dependencies.router, prefix=api_prefix)
    app.include_router(workspace.router, prefix=api_prefix)
    app.include_router(environments.router, prefix=api_prefix)
    app.include_router(files.router, prefix=api_prefix)
    app.include_router(ai.router, prefix=api_prefix)

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
