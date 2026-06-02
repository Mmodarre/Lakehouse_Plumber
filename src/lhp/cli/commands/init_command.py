"""Init command implementation for LakehousePlumber CLI."""

import logging
from pathlib import Path
from typing import List, Tuple

import click
from rich.console import Group
from rich.panel import Panel
from rich.text import Text

from lhp.api import InitProjectResult, LakehousePlumberBootstrap
from lhp.errors import ErrorCategory, ErrorFactory, LHPError, codes

from .. import console as _console_module
from ..render import render_command_header
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class InitCommand(BaseCommand):
    """Initialize a LakehousePlumber project in the current working directory."""

    def execute(self, project_name: str, bundle: bool = True) -> None:
        """Initialize the project. ``bundle`` enables Databricks Asset Bundle layout."""
        render_command_header("lhp init")
        self.setup_from_context()

        project_path = Path.cwd()

        logger.info(
            f"Initializing project '{project_name}' in {project_path}, bundle={bundle}"
        )

        # LHP-specific guard: refuse to clobber an existing project. The
        # bootstrap rejects any non-empty target dir, but this finer-grained
        # check preserves the historical CLI contract (and the snapshot
        # assertion on ``LHP-IO-007`` + "already exists" rendering).
        if (project_path / "lhp.yaml").exists():
            raise ErrorFactory.io_error(
                codes.IO_007,
                title="LHP project already exists",
                details="An lhp.yaml file already exists in this directory.",
                suggestions=[
                    "Use a different directory to create a new project",
                    "Remove the existing lhp.yaml if you want to reinitialize",
                ],
                context={"Directory": str(project_path)},
            )

        logger.debug(
            f"Delegating scaffolding to LakehousePlumberBootstrap (bundle={bundle})"
        )
        bootstrap = LakehousePlumberBootstrap()
        result = bootstrap.init_project(
            project_path, bundle=bundle, project_name=project_name
        )

        if not result.success:
            self._raise_for_failure(result)

        logger.info(f"Project '{project_name}' initialized successfully")

        self._echo_created_paths(result)
        self._display_success_message(project_name, bundle, result)

    def _raise_for_failure(self, result: InitProjectResult) -> None:
        """Convert a bootstrap failure DTO into an ``LHPError`` for the
        CLI error boundary to render and translate into an exit code.

        When the bootstrap supplies a structured ``error_code`` (e.g.
        ``LHP-IO-007``) we round-trip it into an ``LHPError`` so the
        category-to-exit-code mapping in ``cli_error_boundary`` produces
        the same POSIX exit code an inline raise would. Bootstrap
        failures without a structured code fall back to the general
        category so the boundary still emits a non-zero exit code.
        """
        error_message = result.error_message or "Project initialization failed."
        error_code = result.error_code
        category, code_number = self._parse_error_code(error_code)

        raise LHPError(
            category=category,
            code_number=code_number,
            title=error_message,
            details=f"Target directory: {result.target_dir}",
            suggestions=[
                "Use a different directory to create a new project",
                "Remove the conflicting files if you want to scaffold here",
            ],
            context={"Directory": str(result.target_dir)},
        )

    @staticmethod
    def _parse_error_code(error_code: str | None) -> Tuple[ErrorCategory, str]:
        """Split ``LHP-IO-007`` into (ErrorCategory.IO, "007").

        Defaults to ``(ErrorCategory.GENERAL, "000")`` when the code is
        missing or unrecognized so the caller still surfaces an
        ``LHPError`` rather than aborting silently.
        """
        category = ErrorCategory.GENERAL
        code_number = "000"
        if not error_code:
            return category, code_number
        parts = error_code.split("-")
        if len(parts) == 3:
            try:
                category = ErrorCategory(parts[1])
            except ValueError:
                category = ErrorCategory.GENERAL
            code_number = parts[2]
        return category, code_number

    def _echo_created_paths(self, result: InitProjectResult) -> None:
        """Echo one line per created file / directory from the DTO.

        The bootstrap returns absolute paths; we render them relative to
        ``result.target_dir`` so the output stays readable regardless of
        where the user invoked ``lhp init`` from.
        """
        target_dir = result.target_dir
        for directory in result.created_dirs:
            click.echo(f"Created directory: {self._relativize(directory, target_dir)}")
        for created_file in result.created_files:
            click.echo(f"Created file: {self._relativize(created_file, target_dir)}")

    @staticmethod
    def _relativize(path: Path, base: Path) -> str:
        """Format ``path`` relative to ``base`` with a fallback to absolute."""
        try:
            return str(path.relative_to(base))
        except ValueError:
            return str(path)

    def _display_success_message(
        self, project_name: str, bundle: bool, result: InitProjectResult
    ) -> None:
        """Render the post-init success Panel (border_style="dim").

        The "Created directories" line is sourced from
        :attr:`InitProjectResult.created_dirs` so the panel reflects
        actual filesystem state rather than a hardcoded list.
        """
        target_dir = result.target_dir
        directory_names = [
            self._relativize(directory, target_dir)
            for directory in result.created_dirs
            if directory != target_dir
        ]
        directories_line = ", ".join(directory_names) if directory_names else "(none)"

        if bundle:
            title_label = f"Initialized Databricks Asset Bundle project: {project_name}"
            example_files_line = (
                "presets/bronze_layer.yaml, "
                "templates/standard_ingestion.yaml, databricks.yml"
            )
            next_step_commands: List[str] = [
                "# Create your first pipeline",
                "mkdir pipelines/my_pipeline",
                "# Add flowgroup configurations",
                "# Deploy bundle with: databricks bundle deploy",
            ]
        else:
            title_label = f"Initialized LakehousePlumber project: {project_name}"
            example_files_line = (
                "presets/bronze_layer.yaml, templates/standard_ingestion.yaml"
            )
            next_step_commands = [
                "# Create your first pipeline",
                "mkdir pipelines/my_pipeline",
                "# Add flowgroup configurations",
            ]

        title_line = Text.assemble(
            ("✓ ", "bold green"),
            (title_label, "bold"),
        )
        body_lines: List[Text] = [
            title_line,
            Text.assemble(("Created directories: ", "dim"), Text(directories_line)),
            Text.assemble(("Example files: ", "dim"), Text(example_files_line)),
            Text(
                "VS Code IntelliSense automatically configured for YAML files",
                style="dim",
            ),
            Text(""),
            Text("Next steps:", style="bold"),
        ]
        for cmd in next_step_commands:
            body_lines.append(Text(f"  {cmd}"))

        _console_module.console.print(Panel(Group(*body_lines), border_style="dim"))
