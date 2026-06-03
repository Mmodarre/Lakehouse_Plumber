"""Base command class with shared utilities for LakehousePlumber CLI commands."""

import logging
from pathlib import Path

import click

from .._project_root import _find_project_root

logger = logging.getLogger(__name__)


class BaseCommand:
    """Base class for all CLI commands providing shared utilities."""

    def __init__(self):
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
        """Find and validate project root directory. Raises LHPError if not in an LHP project."""
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
        """Return path to substitution file for env, or raise LHPFileError if absent."""
        from ...errors import ErrorFactory, codes

        project_root = self.ensure_project_root()
        substitution_file = project_root / "substitutions" / f"{env}.yaml"

        if not substitution_file.exists():
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
