"""Enhanced token and secret substitution for LakehousePlumber."""

import os
import re
import threading
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Set, List
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
    UNRESOLVED_TOKEN_PATTERN = re.compile(r'\{(?!dbutils\.)([^}]+)\}')

    def __init__(self, substitution_file: Path = None, env: str = "dev", skip_validation: bool = False):
        self.env = env
        self.skip_validation = skip_validation  # Flag to skip unresolved token validation
        self.mappings: Dict[str, str] = {}
        self.prefix_suffix_rules: Dict[str, Dict[str, str]] = {}
        self.secret_scopes: Dict[str, str] = {}
        self.default_secret_scope: Optional[str] = None
        self.secret_references: Set[SecretReference] = set()  # Thread-safe with lock
        
        # Key access tracking for granular dependency detection (thread-safe)
        self._accessed_keys: Dict[str, Set[str]] = {}
        self._accessed_keys_lock = threading.RLock()  # Lock for thread-safe dict access
        self._secret_references_lock = threading.RLock()  # Lock for thread-safe secret references
        self._thread_local = threading.local()  # Per-thread storage for current flowgroup

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
            from .yaml_loader import load_yaml_file
            config = load_yaml_file(file_path, error_context="substitution file")
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except ValueError as e:
            # yaml_loader provides clear context, keep as ValueError for existing error handling
            raise ValueError(str(e))
        except Exception as e:
            raise ValueError(f"Error loading substitution file {file_path}: {e}")

        if not config:
            return

        # Load token substitutions
        env_tokens = config.get(env, {})
        global_tokens = config.get("global", {})

        # Merge tokens (environment-specific overrides global)
        # Convert primitive types to strings for text substitution
        if isinstance(env_tokens, dict):
            for key, value in env_tokens.items():
                # Convert primitive types to strings for text-based substitution
                if isinstance(value, bool):
                    # Convert booleans to lowercase for YAML compatibility
                    self.mappings[key] = str(value).lower()
                elif isinstance(value, (str, int, float)):
                    self.mappings[key] = str(value)
                elif not isinstance(value, (dict, list)):
                    # Handle other non-nested types
                    self.mappings[key] = str(value)
                else:
                    # Keep nested structures (dicts/lists) as-is for prefix_suffix handling
                    self.mappings[key] = value
        if isinstance(global_tokens, dict):
            # Only add global tokens that aren't already set
            for key, value in global_tokens.items():
                if key not in self.mappings:
                    # Convert primitive types to strings
                    if isinstance(value, bool):
                        # Convert booleans to lowercase for YAML compatibility
                        self.mappings[key] = str(value).lower()
                    elif isinstance(value, (str, int, float)):
                        self.mappings[key] = str(value)
                    elif not isinstance(value, (dict, list)):
                        self.mappings[key] = str(value)
                    else:
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
        import logging
        logger = logging.getLogger(__name__)
        
        max_iterations = 10
        for iteration in range(max_iterations):
            changed = False
            for token, value in self.mappings.items():
                if isinstance(value, str):
                    expanded = self._replace_tokens_in_string(value)
                    if expanded != value:
                        self.mappings[token] = expanded
                        changed = True
            if not changed:
                break
        else:
            # Reached max iterations - likely circular reference
            # Log warning but don't fail here - validation will catch it
            logger.warning(
                f"Token expansion reached maximum iterations ({max_iterations}). "
                f"Possible circular reference in substitutions/{self.env}.yaml. "
                f"Unresolved tokens will be caught by validation."
            )

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

            # Store reference for validation and code generation (thread-safe)
            secret_reference = SecretReference(actual_scope, key)
            with self._secret_references_lock:
                self.secret_references.add(secret_reference)

            # Return placeholder for later replacement
            return f"__SECRET_{actual_scope}_{key}__"

        return self.SECRET_PATTERN.sub(secret_replacer, text)

    def set_tracking_context(self, flowgroup_name: str) -> None:
        """Set current flowgroup for key access tracking.
        
        Thread-safe: Uses thread-local storage for current flowgroup.
        
        Args:
            flowgroup_name: Name of the flowgroup being processed
        """
        self._thread_local.current_flowgroup = flowgroup_name
        with self._accessed_keys_lock:
            if flowgroup_name not in self._accessed_keys:
                self._accessed_keys[flowgroup_name] = set()

    def clear_tracking_context(self) -> None:
        """Clear current tracking context.
        
        Thread-safe: Clears thread-local storage.
        """
        self._thread_local.current_flowgroup = None

    def get_accessed_keys(self, flowgroup_name: str) -> Set[str]:
        """Get keys accessed by a flowgroup.
        
        Thread-safe: Uses lock for reading shared state.
        
        Args:
            flowgroup_name: Name of the flowgroup
            
        Returns:
            Set of keys accessed by the flowgroup (copy for safety)
        """
        with self._accessed_keys_lock:
            return self._accessed_keys.get(flowgroup_name, set()).copy()

    def _replace_tokens_in_string(self, text: str) -> str:
        """Replace all {TOKEN} and ${TOKEN} patterns in a string."""

        def default_replacer(match):
            token = match.group(1)
            # Track key access (thread-safe)
            current_fg = getattr(self._thread_local, 'current_flowgroup', None)
            if current_fg:
                with self._accessed_keys_lock:
                    if current_fg in self._accessed_keys:
                        self._accessed_keys[current_fg].add(token)
            return self.mappings.get(token, match.group(0))

        def dollar_replacer(match):
            token = match.group(1)
            # Track key access (thread-safe)
            current_fg = getattr(self._thread_local, 'current_flowgroup', None)
            if current_fg:
                with self._accessed_keys_lock:
                    if current_fg in self._accessed_keys:
                        self._accessed_keys[current_fg].add(token)
            # For ${TOKEN} pattern, just return the replacement value
            return self.mappings.get(token, match.group(0))

        # Apply patterns - dollar pattern first to avoid conflicts
        text = self.DOLLAR_TOKEN_PATTERN.sub(dollar_replacer, text)
        text = self.DEFAULT_TOKEN_PATTERN.sub(default_replacer, text)
        return text

    def get_secret_references(self) -> Set[SecretReference]:
        """Get all secret references found during substitution.
        
        Thread-safe: Uses lock for reading shared state.
        
        Returns:
            Set of secret references (copy for safety)
        """
        with self._secret_references_lock:
            return self.secret_references.copy()

    def validate_no_unresolved_tokens(self, data: Any, path: str = "config") -> List[str]:
        """Detect unresolved tokens after substitution.
        
        Scans configuration for any remaining {token} patterns that weren't
        resolved during substitution, indicating missing values in substitutions file.
        
        Args:
            data: Configuration data to validate (dict, list, str, or other)
            path: Current path in config tree for error reporting
            
        Returns:
            List of error messages describing unresolved tokens with their locations
            
        Examples:
            >>> mgr = EnhancedSubstitutionManager()
            >>> mgr.mappings = {"catalog": "main"}
            >>> data = {"path": "s3://{bucket}/{missing}/data"}
            >>> errors = mgr.validate_no_unresolved_tokens(data)
            >>> print(errors[0])
            "Unresolved token '{missing}' found at config.path. Check substitutions/dev.yaml"
        """
        errors = []
        
        if isinstance(data, str):
            # Find all unresolved tokens except dbutils expressions
            matches = self.UNRESOLVED_TOKEN_PATTERN.findall(data)
            if matches:
                # Format all unresolved tokens in this string
                token_list = ", ".join(f"{{{m}}}" for m in matches)
                errors.append(
                    f"Unresolved token(s) {token_list} found at {path}. "
                    f"Check substitutions/{self.env}.yaml for missing value(s)."
                )
        elif isinstance(data, dict):
            for key, value in data.items():
                errors.extend(self.validate_no_unresolved_tokens(value, f"{path}.{key}"))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                errors.extend(self.validate_no_unresolved_tokens(item, f"{path}[{i}]"))
        # For other types (int, bool, None, etc.), nothing to validate
        
        return errors
