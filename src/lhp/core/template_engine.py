"""Template engine for LakehousePlumber YAML templates."""

import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from jinja2 import Environment

from ..models.config import Template as TemplateModel, Action
from ..parsers.yaml_parser import YAMLParser


class TemplateEngine:
    """Engine for handling YAML templates with parameter expansion."""

    def __init__(self, templates_dir: Path = None):
        """Initialize template engine.

        Args:
            templates_dir: Directory containing template YAML files
        """
        self.templates_dir = templates_dir
        self.logger = logging.getLogger(__name__)
        self.yaml_parser = YAMLParser()
        self._template_cache: Dict[str, TemplateModel] = {}

        # Step 4.2.1: Create Jinja2 environment for parameter expansion
        self.jinja_env = Environment()

        # Load templates if directory provided
        if templates_dir and templates_dir.exists():
            self._load_templates()

    def _load_templates(self):
        """Load all templates from templates directory."""
        if not self.templates_dir:
            return

        template_files = list(self.templates_dir.glob("*.yaml"))
        self.logger.info(
            f"Loading {len(template_files)} templates from {self.templates_dir}"
        )

        for template_file in template_files:
            try:
                template = self.yaml_parser.parse_template(template_file)
                self._template_cache[template.name] = template
                self.logger.debug(f"Loaded template: {template.name}")
            except Exception as e:
                self.logger.warning(f"Failed to load template {template_file}: {e}")

    def get_template(self, template_name: str) -> Optional[TemplateModel]:
        """Get a template by name.

        Args:
            template_name: Name of the template

        Returns:
            Template model or None if not found
        """
        if template_name not in self._template_cache:
            # Try to load from file if not in cache
            if self.templates_dir:
                template_file = self.templates_dir / f"{template_name}.yaml"
                if template_file.exists():
                    try:
                        template = self.yaml_parser.parse_template(template_file)
                        self._template_cache[template_name] = template
                    except Exception as e:
                        self.logger.error(
                            f"Failed to load template {template_name}: {e}"
                        )
                        return None

        return self._template_cache.get(template_name)

    def render_template(
        self, template_name: str, parameters: Dict[str, Any]
    ) -> List[Action]:
        """Step 4.2.2: Implement template parameter handling.

        Render a template with given parameters, returning expanded actions.

        Args:
            template_name: Name of the template to render
            parameters: Parameters to apply to the template

        Returns:
            List of actions with parameters expanded
        """
        template = self.get_template(template_name)
        if not template:
            raise ValueError(f"Template not found: {template_name}")

        # Validate required parameters
        self._validate_parameters(template, parameters)

        # Apply defaults for missing parameters
        final_params = self._apply_parameter_defaults(template, parameters)

        # Render actions with parameters
        rendered_actions = []
        for action in template.actions:
            rendered_action = self._render_action(action, final_params)
            rendered_actions.append(rendered_action)

        return rendered_actions

    def _validate_parameters(self, template: TemplateModel, parameters: Dict[str, Any]):
        """Validate that all required parameters are provided."""
        required_params = {
            p["name"] for p in template.parameters if p.get("required", False)
        }

        provided_params = set(parameters.keys())
        missing_params = required_params - provided_params

        if missing_params:
            raise ValueError(
                f"Missing required parameters for template '{template.name}': {missing_params}"
            )

    def _apply_parameter_defaults(
        self, template: TemplateModel, parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply default values for parameters not provided."""
        final_params = parameters.copy()

        for param_def in template.parameters:
            param_name = param_def["name"]
            if param_name not in final_params and "default" in param_def:
                final_params[param_name] = param_def["default"]

        return final_params

    def _render_action(self, action: Action, parameters: Dict[str, Any]) -> Action:
        """Render a single action with parameter substitution."""
        # Convert action to dict for manipulation
        # Use mode='json' to ensure enums are serialized properly
        action_dict = action.model_dump(mode="json")

        # Recursively render all string values
        rendered_dict = self._render_value(action_dict, parameters)

        # Create new action from rendered dict
        return Action(**rendered_dict)

    def _render_value(self, value: Any, parameters: Dict[str, Any]) -> Any:
        """Recursively render values with Jinja2 parameter substitution."""
        if isinstance(value, str):
            # Render string with Jinja2
            template = self.jinja_env.from_string(value)
            return template.render(**parameters)

        elif isinstance(value, dict):
            # Recursively render dictionary values
            return {k: self._render_value(v, parameters) for k, v in value.items()}

        elif isinstance(value, list):
            # Recursively render list items
            return [self._render_value(item, parameters) for item in value]

        else:
            # Return other types as-is
            return value

    def list_templates(self) -> List[str]:
        """List all available template names."""
        return list(self._template_cache.keys())

    def get_template_info(self, template_name: str) -> Dict[str, Any]:
        """Get information about a template including parameters."""
        template = self.get_template(template_name)
        if not template:
            return {}

        return {
            "name": template.name,
            "version": template.version,
            "description": template.description,
            "parameters": template.parameters,
            "action_count": len(template.actions),
        }
