import hashlib
import logging
from pathlib import Path
from typing import Optional

from fastapi import Depends, Header, HTTPException, Request

from lhp.api.auth import UserContext, get_current_user
from lhp.api.config import APISettings
from lhp.api.services.auto_commit_service import AutoCommitService
from lhp.api.services.git_service import GitService
from lhp.api.services.workspace_manager import WorkspaceManager
from lhp.api.services.yaml_editor import YAMLEditor
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.layers import LakehousePlumberApplicationFacade
from lhp.core.services.dependency_analyzer import DependencyAnalyzer
from lhp.core.project_config_loader import ProjectConfigLoader
from lhp.core.state_manager import StateManager
from lhp.core.services.dependency_output_manager import DependencyOutputManager
from lhp.core.services.pipeline_config_loader import PipelineConfigLoader
from lhp.presets.preset_manager import PresetManager
from lhp.core.template_engine import TemplateEngine
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer

from lhp.api.services.ai_config import AIConfig
from lhp.api.services.opencode_manager import OpenCodeProcess, OpenCodeProcessPool

logger = logging.getLogger(__name__)


def get_settings(request: Request) -> APISettings:
    """Retrieve settings stored on app state during startup."""
    return request.app.state.settings


# --- Phase 2 workspace manager (lazily cached on app.state) ---


def get_workspace_manager(
    request: Request,
) -> WorkspaceManager:
    """Retrieve or create the WorkspaceManager, cached per-app on app.state.

    Per-app scoping ensures test isolation: each create_app() call gets
    its own WorkspaceManager. Lazy creation means this works even when
    TestClient doesn't trigger the lifespan handler.
    """
    if not hasattr(request.app.state, "workspace_manager"):
        settings: APISettings = request.app.state.settings
        source_repo = settings.source_repo or str(settings.project_root)
        request.app.state.workspace_manager = WorkspaceManager(
            workspace_root=settings.workspace_root,
            source_repo=source_repo,
            max_workspaces=settings.workspace_max_count,
            idle_ttl_hours=settings.workspace_idle_ttl_hours,
            stopped_ttl_hours=settings.workspace_stopped_ttl_hours,
        )
    return request.app.state.workspace_manager


def get_workspace_project_root(
    request: Request,
    settings: APISettings = Depends(get_settings),
    user: UserContext = Depends(get_current_user),
    workspace_mgr: WorkspaceManager = Depends(get_workspace_manager),
) -> Path:
    """Phase 2 project root -- resolves to user's workspace.

    In dev_mode: falls back to settings.project_root (same as Phase 1).
    In production: returns the user's workspace directory.
    """
    if settings.dev_mode:
        workspace_root = settings.project_root.resolve()
    else:
        workspace_root = workspace_mgr.get_workspace_project_root(user)
        if workspace_root is None:
            raise HTTPException(
                status_code=409,
                detail="No active workspace. Create one with PUT /api/workspace",
            )

    # Validate it's still an LHP project
    if not (workspace_root / "lhp.yaml").exists():
        from lhp.utils.error_formatter import ErrorCategory, LHPConfigError

        raise LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="011",
            title="Not in a LakehousePlumber project directory",
            details=f"No lhp.yaml file found at {workspace_root}",
            suggestions=[
                "Check the LHP_PROJECT_ROOT environment variable",
                "Use --repo flag with lhp serve to point to your project",
            ],
        )

    # Store on request.state for CacheInvalidationMiddleware
    request.state.workspace_project_root = workspace_root
    return workspace_root


# --- Adaptive project root (dev_mode-aware) ---
# Branches at the top level based on settings.dev_mode, avoiding the
# linear dependency chain problem where get_current_user fires 401 before
# a dev_mode fallback deeper in the chain can execute.


async def get_project_root_adaptive(
    request: Request,
    settings: APISettings = Depends(get_settings),
) -> Path:
    """Adaptive project root for all read endpoints.

    In dev_mode: resolves directly from settings (no auth required).
    In production: delegates to the full workspace auth chain.
    """
    if settings.dev_mode:
        root = settings.project_root.resolve()
        if not (root / "lhp.yaml").exists():
            from lhp.utils.error_formatter import ErrorCategory, LHPConfigError

            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="011",
                title="Not in a LakehousePlumber project directory",
                details=f"No lhp.yaml file found at {root}",
                suggestions=[
                    "Check the LHP_PROJECT_ROOT environment variable",
                    "Use --repo flag with lhp serve to point to your project",
                ],
            )
        request.state.workspace_project_root = root
        return root
    # Production: full workspace resolution with auth
    user = await get_current_user(request)
    workspace_mgr = get_workspace_manager(request)
    return get_workspace_project_root(request, settings, user, workspace_mgr)


# --- Service factories (fresh per request) ---
# Each request gets a fresh instance to avoid shared mutable state bugs.
# The CLI uses the same pattern (fresh ActionOrchestrator per invocation).
# Orchestrator creation cost (~50-200ms of YAML parsing) is negligible
# for a developer-facing API.


def get_orchestrator(
    project_root: Path = Depends(get_project_root_adaptive),
) -> ActionOrchestrator:
    """Create a fresh ActionOrchestrator for this request."""
    return ActionOrchestrator(project_root, enforce_version=False)


def get_facade(
    project_root: Path = Depends(get_project_root_adaptive),
    orchestrator: ActionOrchestrator = Depends(get_orchestrator),
) -> LakehousePlumberApplicationFacade:
    """Create application facade for generate/validate operations."""
    state_manager = StateManager(project_root)
    return LakehousePlumberApplicationFacade(orchestrator, state_manager)


def get_state_manager(
    project_root: Path = Depends(get_project_root_adaptive),
) -> StateManager:
    """Create StateManager for generation state queries."""
    return StateManager(project_root)


def get_dependency_analyzer(
    project_root: Path = Depends(get_project_root_adaptive),
) -> DependencyAnalyzer:
    """Create a fresh DependencyAnalyzer for dependency graph operations."""
    config_loader = ProjectConfigLoader(project_root)
    return DependencyAnalyzer(project_root, config_loader)


def get_dependency_output_manager() -> DependencyOutputManager:
    """Create DependencyOutputManager (stateless)."""
    return DependencyOutputManager()


def get_discoverer(
    project_root: Path = Depends(get_project_root_adaptive),
) -> FlowgroupDiscoverer:
    """Create a fresh FlowgroupDiscoverer."""
    config_loader = ProjectConfigLoader(project_root)
    return FlowgroupDiscoverer(project_root, config_loader)


def get_project_config_loader(
    project_root: Path = Depends(get_project_root_adaptive),
) -> ProjectConfigLoader:
    """Create ProjectConfigLoader."""
    return ProjectConfigLoader(project_root)


def get_pipeline_config_loader(
    project_root: Path = Depends(get_project_root_adaptive),
) -> PipelineConfigLoader:
    """Create PipelineConfigLoader for pipeline-level config access."""
    return PipelineConfigLoader(project_root)


def get_preset_manager(
    project_root: Path = Depends(get_project_root_adaptive),
) -> PresetManager:
    """Create PresetManager."""
    return PresetManager(project_root / "presets")


def get_template_engine(
    project_root: Path = Depends(get_project_root_adaptive),
) -> TemplateEngine:
    """Create TemplateEngine."""
    return TemplateEngine(project_root / "templates")


# --- Write-only workspace service factories ---
# These factories use get_workspace_project_root (always requires workspace/auth)
# for CUD endpoints that must write to the user's workspace clone.
# Read endpoints use get_project_root_adaptive via the Phase 1 factories above.
# FastAPI deduplicates shared dependencies within a request.


def get_workspace_orchestrator(
    project_root: Path = Depends(get_workspace_project_root),
) -> ActionOrchestrator:
    """Workspace-aware ActionOrchestrator — reads pipeline configs from the user's workspace."""
    return ActionOrchestrator(project_root, enforce_version=False)


def get_workspace_facade(
    project_root: Path = Depends(get_workspace_project_root),
    orchestrator: ActionOrchestrator = Depends(get_workspace_orchestrator),
) -> LakehousePlumberApplicationFacade:
    """Workspace-aware facade — reads and writes within the user's workspace."""
    state_manager = StateManager(project_root)
    return LakehousePlumberApplicationFacade(orchestrator, state_manager)


def get_workspace_discoverer(
    project_root: Path = Depends(get_workspace_project_root),
) -> FlowgroupDiscoverer:
    """Workspace-aware FlowgroupDiscoverer — discovers flowgroups from the user's workspace."""
    config_loader = ProjectConfigLoader(project_root)
    return FlowgroupDiscoverer(project_root, config_loader)


# --- Phase 2 workspace git/commit service factories ---


def get_git_service(
    user: UserContext = Depends(get_current_user),
    workspace_mgr: WorkspaceManager = Depends(get_workspace_manager),
) -> GitService:
    svc = workspace_mgr.get_git_service(user)
    if svc is None:
        raise HTTPException(409, "No active workspace")
    return svc


def get_yaml_editor() -> YAMLEditor:
    return YAMLEditor()


def get_auto_commit_service(
    request: Request,
) -> AutoCommitService:
    """Retrieve or create the AutoCommitService, cached per-app on app.state."""
    if not hasattr(request.app.state, "auto_commit_service"):
        request.app.state.auto_commit_service = AutoCommitService()
    return request.app.state.auto_commit_service


async def get_git_service_readonly(
    request: Request,
    settings: APISettings = Depends(get_settings),
) -> GitService:
    """Adaptive git service for read-only endpoints (status, log).

    In dev_mode: wraps the local repo directly (no auth required).
    In production: delegates to workspace manager for user's workspace.
    """
    if settings.dev_mode:
        repo_path = settings.project_root.resolve()
        if not (repo_path / ".git").exists():
            raise HTTPException(500, "Project directory is not a git repository")
        return GitService.init_local(repo_path)
    # Production: full workspace resolution with auth
    user = await get_current_user(request)
    workspace_mgr = get_workspace_manager(request)
    svc = workspace_mgr.get_git_service(user)
    if svc is None:
        raise HTTPException(409, "No active workspace")
    return svc


def require_not_dev_mode(
    settings: APISettings = Depends(get_settings),
) -> None:
    """Guard dependency that blocks write operations in dev_mode.

    Returns 403 Forbidden (not 405) because the HTTP method IS supported,
    just not in this operating mode.
    """
    if settings.dev_mode:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "dev_mode_restricted",
                "message": "This operation is not available in local development mode. "
                "Use your local git CLI instead.",
            },
        )


# --- ETag utilities ---


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


# --- Cache invalidation ---


def invalidate_caches(project_root: Path) -> None:
    """Invalidate cached service instances for the given project root.

    Called by CacheInvalidationMiddleware after mutating requests,
    and can be called explicitly for special cases.
    """
    # Currently a no-op since services are created fresh per request.
    # If we add caching of orchestrators/config loaders, clear them here.
    logger.debug(f"Cache invalidation triggered for {project_root}")


# --- AI assistant dependencies ---


def get_opencode_pool(request: Request) -> OpenCodeProcessPool:
    """Retrieve the OpenCodeProcessPool from app state."""
    pool = getattr(request.app.state, "opencode_pool", None)
    if pool is None:
        raise HTTPException(503, "AI assistant is not available")
    return pool


def get_ai_config(request: Request) -> AIConfig:
    """Retrieve the AIConfig from app state."""
    config = getattr(request.app.state, "ai_config", None)
    if config is None:
        raise HTTPException(503, "AI configuration is not available")
    return config


async def get_user_process(
    request: Request,
    settings: APISettings = Depends(get_settings),
) -> OpenCodeProcess:
    """Resolve the current user's OpenCode process.

    In dev mode: returns the shared ``dev-local`` process.
    In production: resolves user via auth, gets/creates process from pool.
    """
    pool = get_opencode_pool(request)

    if settings.dev_mode:
        project_root = settings.project_root.resolve()
        try:
            return await pool.get_or_create("dev-local", project_root)
        except RuntimeError as e:
            raise HTTPException(503, str(e))

    # Production: resolve user and workspace
    user = await get_current_user(request)
    workspace_mgr = get_workspace_manager(request)
    workspace_root = workspace_mgr.get_workspace_project_root(user)
    if workspace_root is None:
        raise HTTPException(
            409, "No active workspace. Create one with PUT /api/workspace"
        )

    user_id_hash = hashlib.sha256(user.user_id.encode()).hexdigest()[:16]
    try:
        return await pool.get_or_create(user_id_hash, workspace_root)
    except RuntimeError as e:
        raise HTTPException(503, str(e))
