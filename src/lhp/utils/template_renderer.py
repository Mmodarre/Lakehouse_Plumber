"""
Template rendering utility for LakehousePlumber.

This module provides the TemplateRenderer class that encapsulates Jinja2 template
rendering functionality, promoting composition over inheritance.
"""

from pathlib import Path
from typing import Dict, Any
from jinja2 import Environment, FileSystemLoader
import json
import yaml


class TemplateRenderer:
    """
    Template rendering utility using Jinja2.
    
    Provides a composition-based approach to template rendering to promote
    clear separation of concerns.
    """
    
    def __init__(self, template_dir: Path):
        """
        Initialize template renderer.
        
        Args:
            template_dir: Directory containing template files
        """
        self.template_dir = template_dir
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Add common filters
        self.env.filters["tojson"] = json.dumps
        self.env.filters["toyaml"] = yaml.dump
    
    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Render a template with the given context.
        
        Args:
            template_name: Name of the template file (e.g., "pipeline_resource.yml.j2")
            context: Template context variables
            
        Returns:
            Rendered template content as string
            
        Raises:
            TemplateNotFound: If the template file doesn't exist
            TemplateError: If there's an error during rendering
        """
        template = self.env.get_template(template_name)
        return template.render(**context)

