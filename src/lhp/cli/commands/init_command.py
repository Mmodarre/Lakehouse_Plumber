"""Init command implementation for LakehousePlumber CLI."""

import logging
from pathlib import Path
from typing import List

import click

from ...core.init_template_context import InitTemplateContext
from ...core.init_template_loader import InitTemplateLoader
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class InitCommand(BaseCommand):
    """
    Handles project initialization command.

    Creates a LakehousePlumber project in the current working directory,
    with Databricks Asset Bundle integration enabled by default.
    """

    def execute(self, project_name: str, bundle: bool = True) -> None:
        """
        Execute the init command.

        Args:
            project_name: Name used for template rendering (bundle name, lhp.yaml, etc.)
            bundle: Whether to initialize as Databricks Asset Bundle project (default True)
        """
        self.setup_from_context()

        project_path = Path.cwd()

        logger.info(
            f"Initializing project '{project_name}' in {project_path}, bundle={bundle}"
        )

        # Check for existing LHP project
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
            # Create project structure
            logger.debug(f"Creating directory structure (bundle={bundle})")
            created_items = self._create_project_structure(project_path, bundle)

            # Create template context
            context = InitTemplateContext.create(
                project_name=project_name,
                bundle_enabled=bundle,
                author="",  # Empty by default as in original code
            )

            # Create project files using template loader
            logger.debug("Rendering project template files")
            self._create_project_files(project_path, context)

            logger.info(f"Project '{project_name}' initialized successfully")

            # Display success message
            self._display_success_message(project_name, bundle)

        except Exception as e:
            self.logger.error(f"Failed to create project: {e}")

            # Selective cleanup: only remove items we created
            for item in reversed(created_items):
                try:
                    if item.is_dir() and not any(item.iterdir()):
                        item.rmdir()
                except OSError as cleanup_err:
                    logger.debug(
                        f"Could not remove directory {item} during cleanup: {cleanup_err}"
                    )
            raise  # Let cli_error_boundary handle the error

    def _create_project_structure(self, project_path: Path, bundle: bool) -> List[Path]:
        """
        Create project directory structure.

        Args:
            project_path: Path to the project (CWD)
            bundle: Whether to create bundle directories

        Returns:
            List of newly created directories for cleanup tracking
        """
        created: List[Path] = []

        # Create standard directories
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

        # Add resources directory for bundle projects
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
        """
        Create project files using template loader.

        Args:
            project_path: Path to the project
            context: Template context for file creation
        """
        template_loader = InitTemplateLoader()
        template_loader.create_project_files(project_path, context)

    def _display_success_message(self, project_name: str, bundle: bool) -> None:
        """
        Display success message after project creation.

        Args:
            project_name: Name of the created project
            bundle: Whether bundle support was enabled
        """
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
            click.echo(
                f"✅ Initialized Databricks Asset Bundle project: {project_name}"
            )
            click.echo(f"📁 Created directories: {', '.join(directories)}, resources")
            click.echo(
                "📄 Created example files: presets/bronze_layer.yaml, "
                "templates/standard_ingestion.yaml, databricks.yml"
            )
            click.echo(
                "🔧 VS Code IntelliSense automatically configured for YAML files"
            )
            click.echo("\n🚀 Next steps:")
            click.echo("   # Create your first pipeline")
            click.echo("   mkdir pipelines/my_pipeline")
            click.echo("   # Add flowgroup configurations")
            click.echo("   # Deploy bundle with: databricks bundle deploy")
        else:
            click.echo(f"✅ Initialized LakehousePlumber project: {project_name}")
            click.echo(f"📁 Created directories: {', '.join(directories)}")
            click.echo(
                "📄 Created example files: presets/bronze_layer.yaml, "
                "templates/standard_ingestion.yaml"
            )
            click.echo(
                "🔧 VS Code IntelliSense automatically configured for YAML files"
            )
            click.echo("\n🚀 Next steps:")
            click.echo("   # Create your first pipeline")
            click.echo("   mkdir pipelines/my_pipeline")
            click.echo("   # Add flowgroup configurations")
