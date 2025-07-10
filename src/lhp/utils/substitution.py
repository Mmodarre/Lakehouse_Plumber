"""Enhanced token and secret substitution for LakehousePlumber."""

import os
import re
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Set
from .error_formatter import LHPError


class SecretReference:
    """Represents a secret reference with scope and key."""

    def __init__(self, scope: str, key: str):
        self.scope = scope
        self.key = key

    def __hash__(self):
        return hash((self.scope, self.key))

    def __eq__(self, other):
        if isinstance(other, SecretReference):
            return self.scope == other.scope and self.key == other.key
        return False

    def to_dbutils_call(self) -> str:
        """Generate dbutils.secrets.get() call."""
        return f'dbutils.secrets.get(scope="{self.scope}", key="{self.key}")'


class EnhancedSubstitutionManager:
    """Enhanced substitution manager with YAML and secret support."""

    # Regex patterns for token matching
    DEFAULT_TOKEN_PATTERN = re.compile(r"\{(\w+)\}")
    DOLLAR_TOKEN_PATTERN = re.compile(r"\$\{(\w+)\}")
    DOLLAR_TOKEN_SIMPLE_PATTERN = re.compile(r"\$(\w+)")
    SECRET_PATTERN = re.compile(r"\$\{secret:([^}]+)\}")

    def __init__(self, substitution_file: Path = None, env: str = "dev"):
        self.env = env
        self.mappings: Dict[str, str] = {}
        self.prefix_suffix_rules: Dict[str, Dict[str, str]] = {}
        self.secret_scopes: Dict[str, str] = {}
        self.default_secret_scope: Optional[str] = None
        self.secret_references: Set[SecretReference] = set()

        # Add reserved tokens
        self._add_reserved_tokens()

        # Load substitutions and secret configuration
        if substitution_file and substitution_file.exists():
            self._load_config_from_file(substitution_file, env)

        # Recursively expand tokens
        self._expand_recursive_tokens()

    def _add_reserved_tokens(self):
        """Add reserved tokens automatically available."""
        self.mappings["workspace_env"] = self.env
        self.mappings["logical_env"] = self.env

        # From environment variables
        if "WORKSPACE_ENV" in os.environ:
            self.mappings["workspace_env"] = os.environ["WORKSPACE_ENV"]
        if "LOGICAL_ENV" in os.environ:
            self.mappings["logical_env"] = os.environ["LOGICAL_ENV"]

    def _load_config_from_file(self, file_path: Path, env: str):
        """Load tokens, secrets, and rules from YAML file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            raise ValueError(f"Error loading substitution file {file_path}: {e}")

        if not config:
            return

        # Load token substitutions
        env_tokens = config.get(env, {})
        global_tokens = config.get("global", {})

        # Merge tokens (environment-specific overrides global)
        if isinstance(env_tokens, dict):
            self.mappings.update(env_tokens)
        if isinstance(global_tokens, dict):
            # Only add global tokens that aren't already set
            for key, value in global_tokens.items():
                if key not in self.mappings:
                    self.mappings[key] = value

        # Load secret configuration
        secrets_config = config.get("secrets", {})
        if isinstance(secrets_config, dict):
            self.default_secret_scope = secrets_config.get("default_scope")
            self.secret_scopes = secrets_config.get("scopes", {})

        # Load prefix/suffix rules
        prefix_suffix = config.get("prefix_suffix_rules", {})
        if isinstance(prefix_suffix, dict):
            self.prefix_suffix_rules = prefix_suffix

    def _expand_recursive_tokens(self):
        """Recursively expand tokens that reference other tokens."""
        max_iterations = 10
        for _ in range(max_iterations):
            changed = False
            for token, value in self.mappings.items():
                if isinstance(value, str):
                    expanded = self._replace_tokens_in_string(value)
                    if expanded != value:
                        self.mappings[token] = expanded
                        changed = True
            if not changed:
                break

    def substitute_yaml(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively substitute tokens and collect secret references."""
        return self._substitute_recursive(data)

    def _substitute_recursive(self, obj: Any) -> Any:
        """Recursively substitute tokens and secrets in any object."""
        if isinstance(obj, str):
            return self._process_string(obj)
        elif isinstance(obj, dict):
            return {k: self._substitute_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._substitute_recursive(item) for item in obj]
        else:
            return obj

    def _process_string(self, text: str) -> str:
        """Process string for both token and secret substitution."""
        # First handle regular token substitution
        text = self._replace_tokens_in_string(text)

        # Handle secret references
        def secret_replacer(match):
            secret_ref = match.group(1)
            if "/" in secret_ref:
                scope, key = secret_ref.split("/", 1)
            else:
                scope = self.default_secret_scope
                key = secret_ref
                if not scope:
                    raise ValueError(
                        f"No default secret scope configured for secret: {secret_ref}"
                    )

            # Resolve scope alias if it exists
            actual_scope = self.secret_scopes.get(scope, scope)

            # Store reference for validation and code generation
            secret_reference = SecretReference(actual_scope, key)
            self.secret_references.add(secret_reference)

            # Return placeholder for later replacement
            return f"__SECRET_{actual_scope}_{key}__"

        return self.SECRET_PATTERN.sub(secret_replacer, text)

    def _replace_tokens_in_string(self, text: str) -> str:
        """Replace all {TOKEN} and ${TOKEN} patterns in a string."""

        def default_replacer(match):
            token = match.group(1)
            return self.mappings.get(token, match.group(0))

        def dollar_replacer(match):
            token = match.group(1)
            # For ${TOKEN} pattern, just return the replacement value
            return self.mappings.get(token, match.group(0))

        # Apply patterns - dollar pattern first to avoid conflicts
        text = self.DOLLAR_TOKEN_PATTERN.sub(dollar_replacer, text)
        text = self.DEFAULT_TOKEN_PATTERN.sub(default_replacer, text)
        return text

    def get_secret_references(self) -> Set[SecretReference]:
        """Get all secret references found during substitution."""
        return self.secret_references
