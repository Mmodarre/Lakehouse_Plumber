"""Init command implementation for LakehousePlumber CLI."""

import logging
from pathlib import Path
from typing import List

from rich.console import Group
from rich.panel import Panel
from rich.text import Text

from ...core.init_template_context import InitTemplateContext
from ...core.init_template_loader import InitTemplateLoader
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

        if (project_path / "lhp.yaml").exists():
            from ...utils.error_formatter import ErrorCategory, LHPFileError

            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="007",
                title="LHP project already exists",
                details="An lhp.yaml file already exists in this directory.",
                suggestions=[
                    "Use a different directory to create a new project",
                    "Remove the existing lhp.yaml if you want to reinitialize",
                ],
                context={"Directory": str(project_path)},
            )

        created_items: List[Path] = []
        try:
            logger.debug(f"Creating directory structure (bundle={bundle})")
            created_items = self._create_project_structure(project_path, bundle)

            context = InitTemplateContext.create(
                project_name=project_name,
                bundle_enabled=bundle,
                author="",
            )

            logger.debug("Rendering project template files")
            self._create_project_files(project_path, context)

            logger.info(f"Project '{project_name}' initialized successfully")

            self._display_success_message(project_name, bundle)

        except Exception as e:
            self.logger.error(f"Failed to create project: {e}")

            # Only remove items we created.
            for item in reversed(created_items):
                try:
                    if item.is_dir() and not any(item.iterdir()):
                        item.rmdir()
                except OSError as cleanup_err:
                    logger.debug(
                        f"Could not remove directory {item} during cleanup: {cleanup_err}"
                    )
            raise

    def _create_project_structure(self, project_path: Path, bundle: bool) -> List[Path]:
        """Returns newly created directories for cleanup tracking on failure."""
        created: List[Path] = []

        directories = [
            "presets",
            "templates",
            "pipelines",
            "substitutions",
            "schemas",
            "expectations",
            "generated",
            "config",
        ]

        for dir_name in directories:
            dir_path = project_path / dir_name
            if not dir_path.exists():
                dir_path.mkdir()
                created.append(dir_path)

        if bundle:
            resources_dir = project_path / "resources"
            resources_lhp_dir = resources_dir / "lhp"
            if not resources_dir.exists():
                resources_dir.mkdir()
                created.append(resources_dir)
            if not resources_lhp_dir.exists():
                resources_lhp_dir.mkdir(parents=True, exist_ok=True)
                created.append(resources_lhp_dir)

        return created

    def _create_project_files(
        self, project_path: Path, context: InitTemplateContext
    ) -> None:
        template_loader = InitTemplateLoader()
        template_loader.create_project_files(project_path, context)

    def _display_success_message(self, project_name: str, bundle: bool) -> None:
        """Render the post-init success Panel (border_style="dim")."""
        directories = [
            "presets",
            "templates",
            "pipelines",
            "substitutions",
            "schemas",
            "expectations",
            "generated",
            "config",
        ]

        if bundle:
            title_label = f"Initialized Databricks Asset Bundle project: {project_name}"
            directories_line = f"{', '.join(directories)}, resources"
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
            directories_line = ", ".join(directories)
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
