"""Init command implementation for LakehousePlumber CLI."""

import sys
import shutil
from pathlib import Path
import click

from .base_command import BaseCommand
from ...core.init_template_loader import InitTemplateLoader
from ...core.init_template_context import InitTemplateContext


class InitCommand(BaseCommand):
    """
    Handles project initialization command.
    
    Creates new LakehousePlumber project with proper directory structure,
    template files, and optional Databricks Asset Bundle integration.
    """
    
    def execute(self, project_name: str, bundle: bool = False) -> None:
        """
        Execute the init command.
        
        Args:
            project_name: Name of the project to create
            bundle: Whether to initialize as Databricks Asset Bundle project
        """
        self.setup_from_context()
        
        # Validate project directory doesn't exist
        project_path = Path(project_name)
        if project_path.exists():
            click.echo(f"❌ Directory {project_name} already exists")
            sys.exit(1)
        
        try:
            # Create project structure
            self._create_project_structure(project_path, bundle)
            
            # Create template context
            context = InitTemplateContext.create(
                project_name=project_name,
                bundle_enabled=bundle,
                author=""  # Empty by default as in original code
            )
            
            # Create project files using template loader
            self._create_project_files(project_path, context)
            
            # Display success message
            self._display_success_message(project_name, bundle)
            
        except Exception as e:
            self.logger.error(f"Failed to create project: {e}")
            click.echo(f"❌ Failed to create project files: {e}")
            
            # Clean up on failure
            if project_path.exists():
                shutil.rmtree(project_path)
            sys.exit(1)
    
    def _create_project_structure(self, project_path: Path, bundle: bool) -> None:
        """
        Create project directory structure.
        
        Args:
            project_path: Path to the new project
            bundle: Whether to create bundle directories
        """
        # Create main project directory
        project_path.mkdir()
        
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
            (project_path / dir_name).mkdir()
        
        # Add resources directory for bundle projects
        if bundle:
            resources_lhp_dir = project_path / "resources" / "lhp"
            resources_lhp_dir.mkdir(parents=True, exist_ok=True)
    
    def _create_project_files(self, project_path: Path, context: InitTemplateContext) -> None:
        """
        Create project files using template loader.
        
        Args:
            project_path: Path to the new project
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
            "presets", "templates", "pipelines", "substitutions", 
            "schemas", "expectations", "generated", "config"
        ]
        
        if bundle:
            click.echo(f"✅ Initialized Databricks Asset Bundle project: {project_name}")
            click.echo(f"📁 Created directories: {', '.join(directories)}, resources")
            click.echo(
                "📄 Created example files: presets/bronze_layer.yaml, "
                "templates/standard_ingestion.yaml, databricks.yml"
            )
            click.echo("🔧 VS Code IntelliSense automatically configured for YAML files")
            click.echo("\n🚀 Next steps:")
            click.echo(f"   cd {project_name}")
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
            click.echo("🔧 VS Code IntelliSense automatically configured for YAML files")
            click.echo("\n🚀 Next steps:")
            click.echo(f"   cd {project_name}")
            click.echo("   # Create your first pipeline")
            click.echo("   mkdir pipelines/my_pipeline")
            click.echo("   # Add flowgroup configurations")
