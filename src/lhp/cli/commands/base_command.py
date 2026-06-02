"""Base command class with shared utilities for LakehousePlumber CLI commands."""

import logging
from pathlib import Path

import click

from .._project_root import _find_project_root

logger = logging.getLogger(__name__)


class BaseCommand:
    """
    Base class for all CLI commands providing shared utilities.

    This class encapsulates common patterns used across CLI commands:
    - Project root validation and discovery
    - Error handling patterns
    - Logging setup coordination
    - Common validation checks
    """

    def __init__(self):
        """Initialize base command with shared state."""
        self.logger = logging.getLogger(__name__)
        self.verbose = False
        self.log_file = None
        self.perf = False
        self._project_root = None

    def setup_from_context(self) -> None:
        """Setup command state from Click context."""
        ctx = click.get_current_context()
        if ctx.obj:
            self.verbose = ctx.obj.get("verbose", False)
            self.log_file = ctx.obj.get("log_file", None)
            self.perf = ctx.obj.get("perf", False)

    def ensure_project_root(self) -> Path:
        """
        Find and validate project root directory.

        Returns:
            Path to project root

        Raises:
            LHPError: If not in a LakehousePlumber project
        """
        from ...errors import ErrorFactory, codes

        if self._project_root is None:
            self._project_root = _find_project_root()

        if not self._project_root:
            raise ErrorFactory.config_error(
                codes.CFG_011,
                title="Not in a LakehousePlumber project directory",
                details="No lhp.yaml file found in the current directory or any parent.",
                suggestions=[
                    "Run 'lhp init <project_name>' to create a new project",
                    "Navigate to an existing project directory",
                ],
            )

        return self._project_root

    def check_substitution_file(self, env: str) -> Path:
        """
        Check that substitution file exists for the given environment.

        Args:
            env: Environment name

        Returns:
            Path to substitution file

        Raises:
            LHPFileError: If substitution file doesn't exist
        """
        from ...errors import ErrorFactory, codes

        project_root = self.ensure_project_root()
        substitution_file = project_root / "substitutions" / f"{env}.yaml"

        if not substitution_file.exists():
            # Discover available environments for suggestions
            sub_dir = project_root / "substitutions"
            available_envs = []
            if sub_dir.exists():
                available_envs = sorted(f.stem for f in sub_dir.glob("*.yaml"))

            suggestions = [
                f"Create the substitution file: substitutions/{env}.yaml",
            ]
            if available_envs:
                suggestions.insert(
                    0,
                    f"Available environments: {', '.join(available_envs)}",
                )

            raise ErrorFactory.io_error(
                codes.IO_006,
                title=f"Substitution file not found for environment '{env}'",
                details=f"Expected file: {substitution_file}",
                suggestions=suggestions,
                context={"Environment": env},
            )

        return substitution_file

    def announce_log_file(self) -> None:
        """Announce the log-file path whenever a file was written.

        Emitted independently of ``--verbose``: the ``--log-file`` opt-in flag
        writes a file even without ``-v``, so the user must be told where it landed.
        """
        if self.log_file:
            click.echo(f"Detailed logs: {self.log_file}")

    def _get_include_patterns(self, project_root: Path) -> list:
        """Get include patterns from project configuration.

        Returns the list of YAML include patterns declared in ``lhp.yaml``.
        Returns an empty list when no project config is present, no
        ``include`` field is set, or loading fails (logged as a warning).

        Goes through :class:`LakehousePlumberApplicationFacade` so the
        CLI never reaches into internal ``lhp.core`` loaders directly
        (constitution §1.10 / §9.13).
        """
        try:
            from lhp.api import LakehousePlumberApplicationFacade

            application_facade = LakehousePlumberApplicationFacade.for_project(
                project_root
            )
            return list(application_facade.inspection.get_include_patterns())
        except Exception as e:
            self.logger.warning(
                f"Could not load project config for include patterns: {e}"
            )
            return []

    def _discover_yaml_files_with_include(
        self, search_dir: Path, include_patterns: list
    ) -> list:
        """Discover YAML files under ``search_dir`` with include filtering.

        When ``include_patterns`` is non-empty, defers to
        ``discover_files_with_patterns``. Otherwise returns all ``*.yaml``
        and ``*.yml`` files via ``rglob``.
        """
        if include_patterns:
            from ...utils.file_pattern_matcher import discover_files_with_patterns

            return discover_files_with_patterns(search_dir, include_patterns)
        yaml_files = []
        yaml_files.extend(search_dir.rglob("*.yaml"))
        yaml_files.extend(search_dir.rglob("*.yml"))
        return yaml_files
