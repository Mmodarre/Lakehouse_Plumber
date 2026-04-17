"""Base command class with shared utilities for LakehousePlumber CLI commands."""

import logging
from pathlib import Path
from typing import Optional

import click

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
        from ...utils.error_formatter import ErrorCategory, LHPError

        if self._project_root is None:
            self._project_root = self._find_project_root()

        if not self._project_root:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="011",
                title="Not in a LakehousePlumber project directory",
                details="No lhp.yaml file found in the current directory or any parent.",
                suggestions=[
                    "Run 'lhp init <project_name>' to create a new project",
                    "Navigate to an existing project directory",
                ],
            )

        return self._project_root

    def _find_project_root(self) -> Optional[Path]:
        """
        Find the project root by looking for lhp.yaml.

        Searches current directory and parent directories for lhp.yaml file.

        Returns:
            Path to project root if found, None otherwise
        """
        current = Path.cwd().resolve()

        # Check current directory and parent directories
        for path in [current] + list(current.parents):
            if (path / "lhp.yaml").exists():
                return path

        return None

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
        from ...utils.error_formatter import ErrorCategory, LHPFileError

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

            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="006",
                title=f"Substitution file not found for environment '{env}'",
                details=f"Expected file: {substitution_file}",
                suggestions=suggestions,
                context={"Environment": env},
            )

        return substitution_file

    def echo_verbose_info(self, message: str) -> None:
        """Echo verbose information if verbose mode is enabled."""
        if self.verbose and self.log_file:
            click.echo(f"{message}")
            if "Detailed logs:" not in message:
                click.echo(f"Detailed logs: {self.log_file}")
