import logging
from pathlib import Path

from fastapi import Depends, Request

from lhp.api.auth import UserContext, get_current_user
from lhp.api.config import APISettings
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

logger = logging.getLogger(__name__)


def get_settings(request: Request) -> APISettings:
    """Retrieve settings stored on app state during startup."""
    return request.app.state.settings


def get_workspace_project_root(
    settings: APISettings = Depends(get_settings),
    user: UserContext = Depends(get_current_user),
) -> Path:
    """Resolve the project root directory, workspace-aware.

    Phase 1 / dev_mode: Returns settings.project_root.
    Phase 2 production: Will resolve to user's workspace directory under workspace_root.
    """
    # Phase 2 TODO: if not settings.dev_mode, resolve workspace_root/{user.user_id_hash}/repo/
    project_root = settings.project_root.resolve()
    if not (project_root / "lhp.yaml").exists():
        from lhp.utils.error_formatter import ErrorCategory, LHPConfigError

        raise LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="011",
            title="Not in a LakehousePlumber project directory",
            details=f"No lhp.yaml file found at {project_root}",
            suggestions=[
                "Check the LHP_PROJECT_ROOT environment variable",
                "Use --repo flag with lhp serve to point to your project",
            ],
        )
    return project_root


def get_project_root(
    project_root: Path = Depends(get_workspace_project_root),
) -> Path:
    """Phase 1 compatibility alias — delegates to workspace-aware resolution.

    All Phase 1 routers depend on this. When Phase 2 adds workspace resolution
    to get_workspace_project_root(), all routers automatically get workspace-aware
    behavior without code changes.
    """
    return project_root


# --- Service factories (fresh per request) ---
# Each request gets a fresh instance to avoid shared mutable state bugs.
# The CLI uses the same pattern (fresh ActionOrchestrator per invocation).
# Orchestrator creation cost (~50-200ms of YAML parsing) is negligible
# for a developer-facing API.


def get_orchestrator(
    project_root: Path = Depends(get_project_root),
) -> ActionOrchestrator:
    """Create a fresh ActionOrchestrator for this request."""
    return ActionOrchestrator(project_root, enforce_version=False)


def get_facade(
    project_root: Path = Depends(get_project_root),
    orchestrator: ActionOrchestrator = Depends(get_orchestrator),
) -> LakehousePlumberApplicationFacade:
    """Create application facade for generate/validate operations."""
    state_manager = StateManager(project_root)
    return LakehousePlumberApplicationFacade(orchestrator, state_manager)


def get_state_manager(
    project_root: Path = Depends(get_project_root),
) -> StateManager:
    """Create StateManager for generation state queries."""
    return StateManager(project_root)


def get_dependency_analyzer(
    project_root: Path = Depends(get_project_root),
) -> DependencyAnalyzer:
    """Create a fresh DependencyAnalyzer for dependency graph operations."""
    config_loader = ProjectConfigLoader(project_root)
    return DependencyAnalyzer(project_root, config_loader)


def get_dependency_output_manager() -> DependencyOutputManager:
    """Create DependencyOutputManager (stateless)."""
    return DependencyOutputManager()


def get_discoverer(
    project_root: Path = Depends(get_project_root),
) -> FlowgroupDiscoverer:
    """Create a fresh FlowgroupDiscoverer."""
    config_loader = ProjectConfigLoader(project_root)
    return FlowgroupDiscoverer(project_root, config_loader)


def get_project_config_loader(
    project_root: Path = Depends(get_project_root),
) -> ProjectConfigLoader:
    """Create ProjectConfigLoader."""
    return ProjectConfigLoader(project_root)


def get_pipeline_config_loader(
    project_root: Path = Depends(get_project_root),
) -> PipelineConfigLoader:
    """Create PipelineConfigLoader for pipeline-level config access."""
    return PipelineConfigLoader(project_root)


def get_preset_manager(
    project_root: Path = Depends(get_project_root),
) -> PresetManager:
    """Create PresetManager."""
    return PresetManager(project_root / "presets")


def get_template_engine(
    project_root: Path = Depends(get_project_root),
) -> TemplateEngine:
    """Create TemplateEngine."""
    return TemplateEngine(project_root / "templates")
