"""
Template rendering utility for LakehousePlumber.

This module provides the TemplateRenderer class that encapsulates Jinja2 template
rendering functionality, promoting composition over inheritance.
"""

import json
import logging
from typing import Any, Dict, Optional

import yaml
from jinja2 import (
    BaseLoader,
    Environment,
    PackageLoader,
    TemplateSyntaxError,
    UndefinedError,
)
from jinja2.exceptions import TemplateNotFound

from .error_formatter import ErrorCategory, ErrorFormatter, LHPConfigError

logger = logging.getLogger(__name__)


def get_lhp_template_loader() -> PackageLoader:
    """Return a Jinja2 loader rooted at ``src/lhp/templates/``.

    Uses :class:`jinja2.PackageLoader`, which resolves templates via
    ``importlib.util.find_spec()`` (PEP 451). This is the single source of
    truth for the LHP package-template location; renames/moves are a one-line
    change here.
    """
    return PackageLoader("lhp", "templates")


class TemplateRenderer:
    """
    Template rendering utility using Jinja2.

    Provides a composition-based approach to template rendering to promote
    clear separation of concerns.
    """

    def __init__(self, loader: Optional[BaseLoader] = None):
        """
        Initialize template renderer.

        Args:
            loader: Jinja2 loader. Defaults to the LHP package loader
                (``PackageLoader("lhp", "templates")``).
        """
        if loader is None:
            loader = get_lhp_template_loader()
        self.env = Environment(  # nosec B701 — generates Python, not HTML
            loader=loader, trim_blocks=True, lstrip_blocks=True
        )

        # Add common filters
        self.env.filters["tojson"] = json.dumps
        self.env.filters["toyaml"] = yaml.dump

    @classmethod
    def from_package(cls) -> "TemplateRenderer":
        """Construct a renderer backed by the LHP package template tree."""
        return cls(get_lhp_template_loader())

    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Render a template with the given context.

        Args:
            template_name: Name of the template file (e.g., "pipeline_resource.yml.j2")
            context: Template context variables

        Returns:
            Rendered template content as string

        Raises:
            LHPConfigError: If the template file doesn't exist or rendering fails
        """
        try:
            template = self.env.get_template(template_name)
        except TemplateNotFound as e:
            raise ErrorFormatter.template_not_found(
                template_name=template_name,
                available_templates=[],
                templates_dir="lhp.templates package",
            ) from e

        try:
            return template.render(**context)
        except UndefinedError as e:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="029",
                title="Template rendering error: undefined variable",
                details=f"Template '{template_name}' references an undefined variable: {e}",
                suggestions=[
                    "Check that all required template variables are provided in context",
                    "Review the template file for typos in variable names",
                ],
                context={"Template": template_name, "Error": str(e)},
            ) from e
        except TemplateSyntaxError as e:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="030",
                title="Template syntax error",
                details=f"Template '{template_name}' has a syntax error: {e}",
                suggestions=[
                    "Check the Jinja2 syntax in the template file",
                    f"Review the template at: lhp/templates/{template_name}",
                ],
                context={"Template": template_name, "Line": str(e.lineno or "unknown")},
            ) from e
