"""Local variable resolution for LakehousePlumber flowgroups."""

import logging
import re
from typing import Any, Dict, List

from ...errors import ErrorFactory, codes


class LocalVariableResolver:
    """Resolver for flowgroup-scoped local variables using %{var} syntax.

    Variables are scoped to a single flowgroup and resolved before
    template expansion and environment substitution.

    Examples:
        >>> resolver = LocalVariableResolver({"table": "customers"})
        >>> resolver.resolve({"name": "load_%{table}"})
        {'name': 'load_customers'}
    """

    LOCAL_VAR_PATTERN = re.compile(r"%\{(\w+)\}")

    def __init__(self, variables: Dict[str, str]):
        self.variables = variables or {}
        self.logger = logging.getLogger(__name__)

    def resolve(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Raises LHPError if any undefined variables are found."""
        self._expand_variable_definitions()
        resolved = self._substitute_recursive(data)
        self._validate_no_unresolved(resolved)
        return resolved

    def _expand_variable_definitions(self) -> None:
        """Recursively expand variable definitions that reference other variables.

        Example:
            variables = {"schema": "bronze", "table": "%{schema}_customers"}
            After expansion: {"schema": "bronze", "table": "bronze_customers"}
        """
        max_iterations = 10
        for _ in range(max_iterations):
            changed = False
            for var_name, var_value in self.variables.items():
                if isinstance(var_value, str):
                    expanded = self._replace_in_string(var_value)
                    if expanded != var_value:
                        self.variables[var_name] = expanded
                        changed = True
            if not changed:
                break
        else:
            # Reached max iterations - likely circular reference
            self.logger.warning(
                f"Variable expansion reached maximum iterations ({max_iterations}). "
                f"Possible circular reference in local variables."
            )

    def _substitute_recursive(self, obj: Any) -> Any:
        if isinstance(obj, str):
            return self._replace_in_string(obj)
        if isinstance(obj, dict):
            return {k: self._substitute_recursive(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._substitute_recursive(item) for item in obj]
        return obj

    def _replace_in_string(self, text: str) -> str:
        def replacer(match):
            var_name = match.group(1)
            if var_name not in self.variables:
                # Leave unresolved for validation to catch
                return match.group(0)
            return self.variables[var_name]

        return self.LOCAL_VAR_PATTERN.sub(replacer, text)

    def _validate_no_unresolved(self, data: Any, path: str = "config") -> None:
        errors = self._find_unresolved(data, path)
        if errors:
            raise ErrorFactory.config_error(
                codes.CFG_011,
                title="Undefined local variable(s) detected",
                details=f"Found {len(errors)} undefined variable(s):\n\n"
                + "\n".join(f"  • {e}" for e in errors[:10]),
                suggestions=[
                    "Add missing variables to the 'variables' section",
                    "Check for typos in variable names (case-sensitive)",
                    f"Available variables: {', '.join(sorted(self.variables.keys())) or 'none'}",
                ],
                context={
                    "Total Undefined": len(errors),
                    "Showing": min(10, len(errors)),
                },
            )

    def _find_unresolved(self, data: Any, path: str = "config") -> List[str]:
        errors = []

        if isinstance(data, str):
            matches = self.LOCAL_VAR_PATTERN.findall(data)
            if matches:
                var_list = ", ".join(f"%{{{v}}}" for v in matches)
                errors.append(f"{var_list} at {path}")
        elif isinstance(data, dict):
            for key, value in data.items():
                errors.extend(self._find_unresolved(value, f"{path}.{key}"))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                errors.extend(self._find_unresolved(item, f"{path}[{i}]"))

        return errors
