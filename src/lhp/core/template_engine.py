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
                # Use raw parsing to avoid validation of template syntax like {{ table_properties }}
                template = self.yaml_parser.parse_template_raw(template_file)
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
                        # Use raw parsing to avoid validation of template syntax like {{ table_properties }}
                        template = self.yaml_parser.parse_template_raw(template_file)
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
        
        if template.has_raw_actions():
            # Template has raw action dictionaries - render them first, then create Action objects
            for action_dict in template.actions:
                # Render the raw action dictionary with parameters
                rendered_dict = self._render_value(action_dict, final_params)
                # Now create Action object from rendered dictionary (this will validate)
                rendered_action = Action(**rendered_dict)
                rendered_actions.append(rendered_action)
        else:
            # Template has Action objects (backward compatibility)
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
        """Recursively render values with smart template detection."""
        if isinstance(value, str):
            # Only process strings that contain template syntax
            if "{{" in value and "}}" in value:
                # Render template expression
                template = self.jinja_env.from_string(value)
                rendered = template.render(**parameters)
                
                # Convert rendered result to appropriate type
                return self._convert_template_result(rendered)
            else:
                # Pass through non-template strings (substitutions, static values)
                return value
        elif isinstance(value, dict):
            return {k: self._render_value(v, parameters) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._render_value(item, parameters) for item in value]
        else:
            return value

    def _convert_template_result(self, rendered: str) -> Any:
        """Convert rendered string back to appropriate data type with fail-fast error handling.
        
        Args:
            rendered: The rendered string value from template substitution
            
        Returns:
            Converted value with appropriate type
            
        Raises:
            ValueError: If template parameter conversion fails with clear user message
        """
        if not rendered:
            return rendered
            
        # Handle None value (Jinja2 converts None to "None" string)
        if rendered == "None":
            return None
            
        # Handle empty object (Jinja2 converts {} to "{}" string)
        if rendered == "{}":
            return {}
            
        # Handle empty array (Jinja2 converts [] to "[]" string)  
        if rendered == "[]":
            return []
            
        # Try array conversion first (JSON format, then Python literal)
        if rendered.startswith('[') and rendered.endswith(']'):
            try:
                import json
                result = json.loads(rendered)
                if not isinstance(result, list):
                    raise ValueError(f"Expected array but got {type(result).__name__}")
                return result
            except json.JSONDecodeError:
                # JSON failed, try Python literal eval (for single quotes)
                try:
                    import ast
                    result = ast.literal_eval(rendered)
                    if not isinstance(result, list):
                        raise ValueError(f"Expected array but got {type(result).__name__}")
                    return result
                except (ValueError, SyntaxError) as e:
                    raise ValueError(
                        f"Invalid array template parameter: '{rendered}'. "
                        f"Arrays must be valid JSON format like ['item1', 'item2'] "
                        f"or Python format like ['item1', 'item2']. "
                        f"Error: {e}"
                    )
            except Exception as e:
                raise ValueError(f"Failed to parse array template parameter: '{rendered}'. Error: {e}")
        
        # Try object conversion (JSON format, then Python literal) - but be more selective
        # Only try to parse as object if it looks like a proper JSON/Python object
        elif (rendered.startswith('{') and rendered.endswith('}') and 
              ':' in rendered and 
              ('"' in rendered or "'" in rendered)):  # Must contain quotes for proper object
            try:
                import json
                result = json.loads(rendered)
                if not isinstance(result, dict):
                    raise ValueError(f"Expected object but got {type(result).__name__}")
                return result
            except json.JSONDecodeError:
                # JSON failed, try Python literal eval (for single quotes)
                try:
                    import ast
                    result = ast.literal_eval(rendered)
                    if not isinstance(result, dict):
                        raise ValueError(f"Expected object but got {type(result).__name__}")
                    return result
                except (ValueError, SyntaxError) as e:
                    raise ValueError(
                        f"Invalid object template parameter: '{rendered}'. "
                        f"Objects must be valid JSON format like {{'key': 'value'}} "
                        f"or Python format like {{'key': 'value'}}. "
                        f"Error: {e}"
                    )
            except Exception as e:
                raise ValueError(f"Failed to parse object template parameter: '{rendered}'. Error: {e}")
        
        # Try boolean conversion (strict true/false only)
        elif rendered.lower() in ('true', 'false'):
            return rendered.lower() == 'true'
        
        # Try integer conversion (integers only, no decimals or scientific notation)
        elif self._is_integer_string(rendered):
            try:
                return int(rendered)
            except (ValueError, OverflowError) as e:
                raise ValueError(
                    f"Invalid integer template parameter: '{rendered}'. "
                    f"Error: {e}"
                )
        
        # Return as string if no conversion needed
        return rendered
    
    def _is_integer_string(self, value: str) -> bool:
        """Check if string represents a valid integer (no decimals or scientific notation).
        
        Args:
            value: String to check
            
        Returns:
            True if string represents a valid integer
        """
        if not value:
            return False
            
        # Handle negative numbers
        if value.startswith('-'):
            if len(value) == 1:
                return False
            value = value[1:]
        
        # Must be all digits (no decimals, no scientific notation)
        return value.isdigit()

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
