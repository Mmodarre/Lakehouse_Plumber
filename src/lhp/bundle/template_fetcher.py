"""
Databricks template processing functionality for LHP bundle integration.

This module handles processing embedded Databricks bundle templates using Jinja2
for variable substitution and creating bundle configuration files locally.
"""

import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from jinja2 import Template

from .exceptions import TemplateError


logger = logging.getLogger(__name__)


class DatabricksTemplateFetcher:
    """
    Processes embedded Databricks bundle templates for LHP projects.
    
    This class handles local template processing using Jinja2 templates,
    creating databricks.yml configuration files and bundle directory structure.
    """

    def __init__(self, project_root: Path):
        """
        Initialize the template fetcher.
        
        Args:
            project_root: Path to the project root directory
        """
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)

    def fetch_and_apply_template(self, project_name: str,
                                 template_vars: Dict[str, Any]):
        """
        Create bundle files using embedded local template.
        
        Args:
            project_name: Name of the project for template variable substitution
            template_vars: Additional template variables for substitution
            
        Raises:
            TemplateError: If template processing or file creation fails
        """
        try:
            self.logger.info(
                f"Creating Databricks bundle files for project: {project_name}"
            )

            # Use new method name for consistency with test expectations
            self.create_bundle_files(project_name, template_vars)

            self.logger.info("Successfully created bundle files")

        except TemplateError:
            # Re-raise template errors as-is
            raise
        except Exception as e:
            raise TemplateError(
                f"Unexpected error during bundle file creation: {e}\n"
                f"Troubleshooting: Check file permissions and disk space.", e)

    def _get_embedded_template(self) -> str:
        """
        Get embedded databricks.yml template content.
        
        Returns:
            Template content as string with Jinja variables
        """
        return """# This is a Databricks asset bundle definition for {{ project_name }}.
# See https://docs.databricks.com/dev-tools/bundles/index.html for documentation.
bundle:
  name: {{ project_name }}

include:
  # LHP-managed resource files
  - resources/lhp/*.yml
  # User-managed resource files
  - resources/*.yml

targets:
  dev:
    # The default target uses 'mode: development' to create a development copy.
    # - Deployed resources get prefixed with '[dev my_user_name]'
    # - Any job schedules and triggers are paused by default.
    # See also https://docs.databricks.com/dev-tools/bundles/deployment-modes.html.
    mode: development
    default: true
    workspace:
      host: <databricks_host>

  prod:
    mode: production
    workspace:
      host: <databricks_host>
      # We explicitly deploy to /Workspace/Users/<USERNAME> to make sure we only have a single copy.
      root_path: /Workspace/Users/<USERNAME@COMPANY.com>/.bundle/${bundle.name}/${bundle.target}
    permissions:
      - user_name: <USERNAME@COMPANY.com>
        level: CAN_MANAGE
"""

    def _process_local_template(self, project_name: str,
                                template_vars: Dict[str, Any]) -> str:
        """
        Process embedded Jinja template with project variables.
        
        Args:
            project_name: Project name for template substitution
            template_vars: Additional template variables
            
        Returns:
            Processed template content as string
            
        Raises:
            TemplateError: If template processing fails
        """
        try:
            if template_vars is None:
                raise TemplateError("Template variables cannot be None")

            # Get embedded template content
            template_content = self._get_embedded_template()

            # Create Jinja template
            template = Template(template_content)

            # Combine variables
            variables = {'project_name': project_name, **template_vars}

            self.logger.debug(
                f"Processing template with variables: {list(variables.keys())}"
            )

            # Render template
            rendered_content = template.render(**variables)

            return rendered_content

        except Exception as e:
            raise TemplateError(f"Failed to process template: {e}", e)

    def create_bundle_files(self, project_name: str, template_vars: Dict[str,
                                                                         Any]):
        """
        Create bundle files using local template processing.
        
        Args:
            project_name: Project name for template substitution
            template_vars: Additional template variables
            
        Raises:
            TemplateError: If file creation fails
        """
        try:
            # Process template
            databricks_yml_content = self._process_local_template(
                project_name, template_vars)

            # Create databricks.yml
            databricks_yml_path = self.project_root / "databricks.yml"
            databricks_yml_path.write_text(databricks_yml_content,
                                           encoding='utf-8')

            # Create resources/lhp directory for LHP-managed resource files
            resources_lhp_dir = self.project_root / "resources" / "lhp"
            resources_lhp_dir.mkdir(parents=True, exist_ok=True)

            self.logger.info(
                f"Created databricks.yml and resources/lhp/ directory")

        except (OSError, PermissionError) as e:
            raise TemplateError(f"Failed to create bundle files: {e}", e)
        except Exception as e:
            raise TemplateError(f"Unexpected error creating bundle files: {e}",
                                e)
