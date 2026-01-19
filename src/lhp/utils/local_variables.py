"""Local variable resolution for LakehousePlumber flowgroups."""

import logging
import re
from typing import Dict, Any, List

from .error_formatter import LHPError, ErrorCategory


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
        """Initialize resolver with variable definitions.
        
        Args:
            variables: Dictionary of variable name to value mappings
        """
        self.variables = variables or {}
        self.logger = logging.getLogger(__name__)
    
    def resolve(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve all %{var} patterns in data structure.
        
        Processing steps:
        1. Expand recursive variable definitions
        2. Substitute variables in data structure
        3. Validate no unresolved patterns remain
        
        Args:
            data: Configuration data structure to process
            
        Returns:
            Data with all %{var} patterns resolved
            
        Raises:
            LHPError: If any undefined variables are found
        """
        self._expand_variable_definitions()  # Handle recursive vars
        resolved = self._substitute_recursive(data)
        self._validate_no_unresolved(resolved)  # Strict validation
        return resolved
    
    def _expand_variable_definitions(self) -> None:
        """Recursively expand variable definitions that reference other variables.
        
        Example:
            variables = {"schema": "bronze", "table": "%{schema}_customers"}
            After expansion: {"schema": "bronze", "table": "bronze_customers"}
        """
        max_iterations = 10
        for iteration in range(max_iterations):
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
        """Recursively substitute %{var} patterns in any data structure.
        
        Args:
            obj: Object to process (str, dict, list, or other)
            
        Returns:
            Object with variables substituted
        """
        if isinstance(obj, str):
            return self._replace_in_string(obj)
        elif isinstance(obj, dict):
            return {k: self._substitute_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._substitute_recursive(item) for item in obj]
        else:
            return obj
    
    def _replace_in_string(self, text: str) -> str:
        """Replace all %{var} patterns in a string.
        
        Supports inline substitution: prefix_%{var}_suffix
        
        Args:
            text: String to process
            
        Returns:
            String with variables replaced
        """
        def replacer(match):
            var_name = match.group(1)
            if var_name not in self.variables:
                # Leave unresolved for validation to catch
                return match.group(0)
            return self.variables[var_name]
        
        return self.LOCAL_VAR_PATTERN.sub(replacer, text)
    
    def _validate_no_unresolved(self, data: Any, path: str = "config") -> None:
        """Raise LHPError if any %{var} patterns remain unresolved.
        
        Args:
            data: Data structure to validate
            path: Current path in config tree (for error reporting)
            
        Raises:
            LHPError: If any unresolved variables are found
        """
        errors = self._find_unresolved(data, path)
        if errors:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="011",
                title="Undefined local variable(s) detected",
                details=f"Found {len(errors)} undefined variable(s):\n\n" +
                        "\n".join(f"  • {e}" for e in errors[:10]),
                suggestions=[
                    "Add missing variables to the 'variables' section",
                    "Check for typos in variable names (case-sensitive)",
                    f"Available variables: {', '.join(sorted(self.variables.keys())) or 'none'}"
                ],
                context={
                    "Total Undefined": len(errors),
                    "Showing": min(10, len(errors))
                }
            )
    
    def _find_unresolved(self, data: Any, path: str = "config") -> List[str]:
        """Find all unresolved %{var} patterns in data structure.
        
        Args:
            data: Data structure to scan
            path: Current path in config tree
            
        Returns:
            List of error messages with paths to unresolved variables
        """
        errors = []
        
        if isinstance(data, str):
            # Find all %{var} patterns that remain after substitution
            matches = self.LOCAL_VAR_PATTERN.findall(data)
            if matches:
                # Any remaining %{var} pattern is an error
                # Either the variable doesn't exist, or it has a circular reference
                var_list = ", ".join(f"%{{{v}}}" for v in matches)
                errors.append(f"{var_list} at {path}")
        elif isinstance(data, dict):
            for key, value in data.items():
                errors.extend(self._find_unresolved(value, f"{path}.{key}"))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                errors.extend(self._find_unresolved(item, f"{path}[{i}]"))
        # For other types (int, bool, None, etc.), nothing to validate
        
        return errors
