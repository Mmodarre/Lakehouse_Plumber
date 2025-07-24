"""Template loader for project initialization."""

import logging
from pathlib import Path
from typing import Dict, List, Any
from jinja2 import Environment, BaseLoader

try:
    from importlib.resources import files
except ImportError:
    # Fallback for Python < 3.9
    import importlib_resources
    files = importlib_resources.files

from .init_template_context import InitTemplateContext


class PackageTemplateLoader(BaseLoader):
    """Custom Jinja2 loader for templates stored in package resources."""
    
    def __init__(self, package_path: str):
        """Initialize the loader.
        
        Args:
            package_path: Package path like 'lhp.templates.init'
        """
        self.package_path = package_path
        self.logger = logging.getLogger(__name__)
    
    def get_source(self, environment, template):
        """Get template source from package resources."""
        try:
            # Navigate to the template file
            package_files = files(self.package_path)
            template_file = package_files / template
            
            if not template_file.is_file():
                raise FileNotFoundError(f"Template {template} not found in {self.package_path}")
            
            source = template_file.read_text(encoding='utf-8')
            
            # Return source, filename, uptodate function
            return source, template, lambda: True
            
        except Exception as e:
            self.logger.error(f"Failed to load template {template}: {e}")
            raise


class InitTemplateLoader:
    """Loader for project initialization templates."""
    
    def __init__(self):
        """Initialize the template loader."""
        self.logger = logging.getLogger(__name__)
        
        # Create Jinja2 environment with package template loader
        loader = PackageTemplateLoader('lhp.templates.init')
        self.jinja_env = Environment(loader=loader)
        
    def load_template(self, template_path: str):
        """Load a template from package resources.
        
        Args:
            template_path: Path to template file (e.g., 'lhp.yaml.j2')
            
        Returns:
            Jinja2 Template object
        """
        try:
            return self.jinja_env.get_template(template_path)
        except Exception as e:
            self.logger.error(f"Failed to load template {template_path}: {e}")
            raise
    
    def render_template(self, template_path: str, context: InitTemplateContext) -> str:
        """Render a template with the given context.
        
        Args:
            template_path: Path to template file
            context: Template context containing variables
            
        Returns:
            Rendered template content
        """
        try:
            template = self.load_template(template_path)
            
            # Convert context to dict for Jinja2
            context_dict = {
                'project_name': context.project_name,
                'current_date': context.current_date,
                'author': context.author,
                'bundle_enabled': context.bundle_enabled
            }
            
            return template.render(**context_dict)
            
        except Exception as e:
            self.logger.error(f"Failed to render template {template_path}: {e}")
            raise
    
    def get_template_files(self, bundle_enabled: bool = False) -> List[str]:
        """Get list of template files to process by auto-discovering all files.
        
        Args:
            bundle_enabled: Whether to include bundle-specific templates
            
        Returns:
            List of template file paths relative to the init template directory
        """
        try:
            # Get all files from the package resources
            package_files = files('lhp.templates.init')
            template_files = []
            
            # Directories to exclude from initialization
            excluded_dirs = {'__pycache__'}
            
            def collect_files(current_path, relative_path=""):
                """Recursively collect all files, excluding certain directories."""
                for item in current_path.iterdir():
                    if item.is_dir():
                        # Skip excluded directories
                        if item.name in excluded_dirs:
                            continue
                        
                        # Skip bundle directory if bundle not enabled
                        if not bundle_enabled and item.name == 'bundle':
                            continue
                            
                        # Recursively collect from subdirectory
                        subdir_path = f"{relative_path}/{item.name}" if relative_path else item.name
                        collect_files(item, subdir_path)
                    elif item.is_file():
                        # Add file to list
                        file_path = f"{relative_path}/{item.name}" if relative_path else item.name
                        template_files.append(file_path)
            
            collect_files(package_files)
            
            # Sort for consistent ordering
            template_files.sort()
            
            self.logger.debug(f"Discovered {len(template_files)} template files")
            return template_files
            
        except Exception as e:
            self.logger.error(f"Failed to discover template files: {e}")
            # Fallback to basic files if discovery fails
            basic_files = [
                'lhp.yaml.j2',
                'substitutions/dev.yaml.j2', 
                'presets/bronze_layer.yaml.j2',
                'templates/standard_ingestion.yaml.j2',
                'README.md.j2',
                '.gitignore.j2'
            ]
            if bundle_enabled:
                basic_files.extend(['bundle/databricks.yml.j2', 'bundle/resources/.gitkeep'])
            return basic_files
    
    def _copy_latest_schemas(self, project_path: Path):
        """Copy the latest schema files from lhp.schemas package to .vscode/schemas/."""
        try:
            schemas_dir = project_path / ".vscode" / "schemas"
            schemas_dir.mkdir(parents=True, exist_ok=True)
            
            # Schema files to copy
            schema_files = {
                "flowgroup.schema.json",
                "template.schema.json", 
                "substitution.schema.json",
                "project.schema.json",
                "preset.schema.json"
            }
            
            # Copy each schema file from lhp.schemas package
            package_files = files('lhp.schemas')
            copied_count = 0
            
            for schema_file in schema_files:
                try:
                    source_file = package_files / schema_file
                    target_file = schemas_dir / schema_file
                    
                    content = source_file.read_text(encoding='utf-8')
                    target_file.write_text(content, encoding='utf-8')
                    copied_count += 1
                    
                except Exception as e:
                    self.logger.warning(f"Failed to copy schema {schema_file}: {e}")
            
            self.logger.debug(f"Copied {copied_count} schema files to {schemas_dir}")
            
        except Exception as e:
            self.logger.error(f"Failed to copy schemas: {e}")
            # Don't fail the entire init process for schema copy errors
    
    def create_project_files(self, project_path: Path, context: InitTemplateContext):
        """Create all project files by rendering templates.
        
        Args:
            project_path: Path where project should be created
            context: Template context containing variables
        """
        self.logger.info(f"Creating project files at {project_path}")
        
        # Get list of templates to process
        template_files = self.get_template_files(context.bundle_enabled)
        
        for template_file in template_files:
            try:
                # Determine if this is a Jinja2 template or should be copied directly
                is_jinja_template = template_file.endswith('.j2')
                
                # Handle special case for .gitkeep (no rendering needed)
                if template_file.endswith('.gitkeep'):
                    target_path = project_path / template_file.replace('bundle/', '')
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    target_path.write_text("# This file ensures the resources directory is tracked in git\n")
                    continue
                
                if is_jinja_template:
                    # Render Jinja2 template
                    rendered_content = self.render_template(template_file, context)
                    
                    # Determine target file path (remove .j2 extension and bundle/ prefix for bundle files)
                    target_file = template_file.replace('.j2', '')
                    if target_file.startswith('bundle/'):
                        target_file = target_file.replace('bundle/', '')
                    
                    target_path = project_path / target_file
                    
                    # Create parent directories if needed
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Write rendered content
                    target_path.write_text(rendered_content, encoding='utf-8')
                else:
                    # Copy file directly without rendering
                    package_files = files('lhp.templates.init')
                    source_file = package_files / template_file
                    
                    # Determine target file path (bundle/ prefix handling)
                    target_file = template_file
                    if target_file.startswith('bundle/'):
                        target_file = target_file.replace('bundle/', '')
                    
                    target_path = project_path / target_file
                    
                    # Create parent directories if needed
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Copy file content directly
                    content = source_file.read_text(encoding='utf-8')
                    target_path.write_text(content, encoding='utf-8')
                
                self.logger.debug(f"Created file: {target_path.relative_to(project_path)}")
                
            except Exception as e:
                self.logger.error(f"Failed to create file from template {template_file}: {e}")
                raise
        
        # Copy latest schemas to .vscode/schemas/
        self._copy_latest_schemas(project_path)
        
        self.logger.info(f"Successfully created project structure with {len(template_files)} files") 