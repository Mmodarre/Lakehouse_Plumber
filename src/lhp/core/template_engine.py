"""Template engine for LakehousePlumber YAML templates."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from jinja2 import Environment

from ..models.config import Action
from ..models.config import Template as TemplateModel
from ..parsers.yaml_parser import YAMLParser
from ..utils.error_formatter import (
    ErrorCategory,
    ErrorFormatter,
    LHPConfigError,
    LHPValidationError,
)


class TemplateEngine:
    """Engine for handling YAML templates with parameter expansion."""

    def __init__(self, templates_dir: Path = None):
        """Initialize template engine with lazy loading.

        Args:
            templates_dir: Directory containing template YAML files
        """
        self.templates_dir = templates_dir
        self.logger = logging.getLogger(__name__)
        self.yaml_parser = YAMLParser()
        self._template_cache: Dict[str, TemplateModel] = {}

        # Create Jinja2 environment for parameter expansion
        self.jinja_env = Environment()

        # Discover available template files (but don't parse them yet - lazy loading)
        self._available_templates: Set[str] = set()
        if templates_dir and templates_dir.exists():
            self._discover_template_files()

    def _discover_template_files(self):
        """Discover available template files without parsing them (lazy loading optimization).

        This replaces eager loading in __init__ to avoid parsing all templates upfront.
        Templates are parsed on-demand when get_template() is called.
        """
        if not self.templates_dir:
            return

        template_files = list(self.templates_dir.glob("*.yaml"))

        # Extract template names from filenames
        for template_file in template_files:
            template_name = template_file.stem  # filename without extension
            self._available_templates.add(template_name)

        self.logger.debug(
            f"Discovered {len(self._available_templates)} template files in {self.templates_dir} (lazy loading enabled)"
        )

    def get_template(self, template_name: str) -> Optional[TemplateModel]:
        """Get a template by name.

        Args:
            template_name: Name of the template

        Returns:
            Template model or None if not found
        """
        if template_name not in self._template_cache:
            self.logger.debug(
                f"Template '{template_name}' not in cache, loading from file"
            )
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

        result = self._template_cache.get(template_name)
        if result:
            self.logger.debug(f"Template '{template_name}' cache hit")
        else:
            self.logger.debug(f"Template '{template_name}' not found")
        return result

    def render_template(
        self, template_name: str, parameters: Dict[str, Any]
    ) -> List[Action]:
        """Implement template parameter handling.

        Render a template with given parameters, returning expanded actions.

        Args:
            template_name: Name of the template to render
            parameters: Parameters to apply to the template

        Returns:
            List of actions with parameters expanded
        """
        template = self.get_template(template_name)
        if not template:
            raise ErrorFormatter.template_not_found(
                template_name=template_name,
                available_templates=sorted(self._available_templates),
                templates_dir=str(self.templates_dir) if self.templates_dir else None,
            )

        # Validate required parameters
        self._validate_parameters(template, parameters)

        # Apply defaults for missing parameters
        final_params = self._apply_parameter_defaults(template, parameters)
        self.logger.debug(
            f"Rendering template '{template_name}' with {len(final_params)} parameter(s): {list(final_params.keys())}"
        )

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

        self.logger.debug(
            f"Template '{template_name}' rendered {len(rendered_actions)} action(s)"
        )
        return rendered_actions

    def _validate_parameters(self, template: TemplateModel, parameters: Dict[str, Any]):
        """Validate that all required parameters are provided."""
        required_params = {
            p["name"] for p in template.parameters if p.get("required", False)
        }

        provided_params = set(parameters.keys())
        missing_params = required_params - provided_params

        if missing_params:
            all_param_names = [p["name"] for p in template.parameters]
            raise ErrorFormatter.missing_template_parameters(
                template_name=template.name,
                missing_params=sorted(missing_params),
                available_params=all_param_names,
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
        if rendered.startswith("[") and rendered.endswith("]"):
            self.logger.debug(
                f"Converting template result to array: '{rendered[:80]}{'...' if len(rendered) > 80 else ''}'"
            )

            try:
                import json

                result = json.loads(rendered)
                if not isinstance(result, list):
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="009",
                        title="Invalid template parameter type",
                        details=f"Expected array but got {type(result).__name__}.",
                        suggestions=[
                            "Ensure the template parameter value is a list/array"
                        ],
                        context={"Rendered Value": rendered[:100]},
                    )
                return result
            except json.JSONDecodeError:
                # JSON failed, try Python literal eval (for single quotes)
                try:
                    import ast

                    result = ast.literal_eval(rendered)
                    if not isinstance(result, list):
                        raise LHPValidationError(
                            category=ErrorCategory.VALIDATION,
                            code_number="009",
                            title="Invalid template parameter type",
                            details=f"Expected array but got {type(result).__name__}.",
                            suggestions=[
                                "Ensure the template parameter value is a list/array"
                            ],
                            context={"Rendered Value": rendered[:100]},
                        )
                    return result
                except (ValueError, SyntaxError) as e:
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="009",
                        title="Invalid array template parameter",
                        details=(
                            f"Invalid array template parameter: '{rendered}'. "
                            f"Arrays must be valid JSON format like ['item1', 'item2'] "
                            f"or Python format like ['item1', 'item2']. "
                            f"Error: {e}"
                        ),
                        suggestions=[
                            'Use valid JSON array syntax: ["item1", "item2"]',
                            "Or Python list syntax: ['item1', 'item2']",
                        ],
                        context={"Rendered Value": rendered[:100]},
                    )
            except LHPValidationError:
                raise
            except Exception as e:
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="009",
                    title="Failed to parse array template parameter",
                    details=f"Failed to parse array template parameter: '{rendered}'. Error: {e}",
                    suggestions=[
                        "Ensure the array parameter is valid JSON or Python format",
                    ],
                    context={"Rendered Value": rendered[:100]},
                )

        # Try object conversion (JSON format, then Python literal) - but be more selective
        # Only try to parse as object if it looks like a proper JSON/Python object
        elif (
            rendered.startswith("{")
            and rendered.endswith("}")
            and ":" in rendered
            and ('"' in rendered or "'" in rendered)
        ):  # Must contain quotes for proper object
            try:
                import json

                result = json.loads(rendered)
                if not isinstance(result, dict):
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="009",
                        title="Invalid template parameter type",
                        details=f"Expected object but got {type(result).__name__}.",
                        suggestions=[
                            "Ensure the template parameter value is a dictionary/object"
                        ],
                        context={"Rendered Value": rendered[:100]},
                    )
                return result
            except json.JSONDecodeError:
                # JSON failed, try Python literal eval (for single quotes)
                try:
                    import ast

                    result = ast.literal_eval(rendered)
                    if not isinstance(result, dict):
                        raise LHPValidationError(
                            category=ErrorCategory.VALIDATION,
                            code_number="009",
                            title="Invalid template parameter type",
                            details=f"Expected object but got {type(result).__name__}.",
                            suggestions=[
                                "Ensure the template parameter value is a dictionary/object"
                            ],
                            context={"Rendered Value": rendered[:100]},
                        )
                    return result
                except (ValueError, SyntaxError) as e:
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="009",
                        title="Invalid object template parameter",
                        details=(
                            f"Invalid object template parameter: '{rendered}'. "
                            f"Objects must be valid JSON format like {{'key': 'value'}} "
                            f"or Python format like {{'key': 'value'}}. "
                            f"Error: {e}"
                        ),
                        suggestions=[
                            'Use valid JSON object syntax: {"key": "value"}',
                            "Or Python dict syntax: {'key': 'value'}",
                        ],
                        context={"Rendered Value": rendered[:100]},
                    )
            except LHPValidationError:
                raise
            except Exception as e:
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="009",
                    title="Failed to parse object template parameter",
                    details=f"Failed to parse object template parameter: '{rendered}'. Error: {e}",
                    suggestions=[
                        "Ensure the object parameter is valid JSON or Python format",
                    ],
                    context={"Rendered Value": rendered[:100]},
                )

        # Try boolean conversion (strict true/false only)
        elif rendered.lower() in ("true", "false"):
            return rendered.lower() == "true"

        # Try integer conversion (integers only, no decimals or scientific notation)
        elif self._is_integer_string(rendered):
            try:
                return int(rendered)
            except (ValueError, OverflowError) as e:
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="009",
                    title="Invalid integer template parameter",
                    details=f"Invalid integer template parameter: '{rendered}'. Error: {e}",
                    suggestions=[
                        "Ensure the template parameter is a valid integer",
                        "Check that the value does not exceed integer limits",
                    ],
                    context={"Rendered Value": rendered[:100]},
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
        if value.startswith("-"):
            if len(value) == 1:
                return False
            value = value[1:]

        # Must be all digits (no decimals, no scientific notation)
        return value.isdigit()

    def list_templates(self) -> List[str]:
        """List all available template names."""
        return list(self._available_templates)

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
