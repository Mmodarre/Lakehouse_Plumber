"""Enhanced token and secret substitution for LakehousePlumber."""

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ...errors import ErrorFactory, LHPError, codes

logger = logging.getLogger(__name__)


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
        """Generate a dbutils.secrets.get() call as a Python expression.

        Single-quoted scope/key arguments so the call is safe to embed
        inside double-quoted string literals (e.g. inside JDBC URL
        templates) without breaking the outer quote nesting. This
        matches the format the legacy post-pass SecretCodeGenerator
        emitted before inline emission replaced it.
        """
        return f"dbutils.secrets.get(scope='{self.scope}', key='{self.key}')"


class EnhancedSubstitutionManager:
    """Enhanced substitution manager with YAML and secret support.

    :stability: provisional
    """

    # Regex patterns for token matching.
    # DEFAULT_TOKEN_PATTERN intentionally has no lookbehind/lookahead guards
    # against ``${X}``, ``%{X}``, or ``{{X}}`` because it is applied AFTER
    # Jinja2 template rendering and the ``%{}`` local-variable pass have
    # already consumed those syntaxes — the blind spot is not exercised in
    # this context. The standalone YAML scanner in ``lhp.cli.yaml_scanner``
    # runs on raw YAML and therefore uses a stricter pattern.
    DEFAULT_TOKEN_PATTERN = re.compile(r"\{(\w+)\}")
    DOLLAR_TOKEN_PATTERN = re.compile(r"\$\{(\w+)\}")
    SECRET_PATTERN = re.compile(r"\$\{secret:([^}]+)\}")
    UNRESOLVED_TOKEN_PATTERN = re.compile(r"\{(?!dbutils\.)(\w+)\}")

    def __init__(
        self,
        substitution_file: Path | None = None,
        env: str = "dev",
        skip_validation: bool = False,
    ):
        self.env = env
        self.skip_validation = (
            skip_validation  # Flag to skip unresolved token validation
        )
        self.mappings: Dict[str, str] = {}
        self.prefix_suffix_rules: Dict[str, Dict[str, str]] = {}
        self.secret_scopes: Dict[str, str] = {}
        self.default_secret_scope: Optional[str] = None
        self.secret_references: Set[SecretReference] = set()
        # Per-instance flag flipped in ``_replace_tokens_in_string`` when
        # the deprecated bare ``{token}`` syntax is encountered. Callers
        # (the orchestrator's pipeline-by-fields methods) consult this
        # after constructing the manager and forward a single
        # ``WarningCollector`` entry; the worker-side ``logger.warning``
        # path was removed because workers attach a ``NullHandler`` and
        # never reach the user.
        self.has_deprecated_bare_tokens: bool = False

        # Add reserved tokens
        self._add_reserved_tokens()

        # Load substitutions and secret configuration
        if substitution_file and substitution_file.exists():
            logger.debug(
                f"Loading substitution file: {substitution_file} for env '{env}'"
            )
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
            from lhp.parsers.yaml_loader import load_yaml_file

            config = load_yaml_file(file_path, error_context="substitution file")
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            raise ErrorFactory.config_error(
                codes.CFG_020,
                title="Failed to load substitution file",
                details=f"Error loading substitution file {file_path}: {e}",
                suggestions=[
                    "Check the substitution file for YAML syntax errors",
                    "Ensure the file is readable and not corrupted",
                ],
                context={"File": str(file_path), "Environment": env},
            ) from e

        if not config:
            return

        # Load token substitutions
        env_tokens = config.get(env, {})
        global_tokens = config.get("global", {})

        # Merge tokens (environment-specific overrides global)
        # Convert primitive types to strings for text substitution
        logger.debug(
            f"Loaded {len(env_tokens) if isinstance(env_tokens, dict) else 0} env-specific "
            f"and {len(global_tokens) if isinstance(global_tokens, dict) else 0} global token(s)"
        )
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
        logger.debug(
            f"Substituting tokens in YAML data ({len(self.mappings)} mapping(s) available)"
        )
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
                    raise ErrorFactory.config_error(
                        codes.CFG_008,
                        title="Missing default secret scope",
                        details=f"No default secret scope configured for secret reference: {secret_ref}",
                        suggestions=[
                            "Add a 'secrets.default_scope' to your substitutions YAML file",
                            "Or use the full scope/key format: ${secret:scope/key}",
                        ],
                        example="secrets:\n  default_scope: my-scope\n\n# Then use: ${secret:my_key}\n# Or explicit: ${secret:my-scope/my_key}",
                        context={"secret_ref": secret_ref, "env": self.env},
                    )

            # Resolve scope alias if it exists
            actual_scope = self.secret_scopes.get(scope, scope)

            # Store reference for validation
            secret_reference = SecretReference(actual_scope, key)
            self.secret_references.add(secret_reference)

            # Return placeholder; SecretCodeGenerator post-pass converts these
            # to bare dbutils calls or f-strings after Jinja templates have
            # wrapped values in Python string literals.
            return f"__SECRET_{actual_scope}_{key}__"

        return self.SECRET_PATTERN.sub(secret_replacer, text)

    def _replace_tokens_in_string(self, text: str) -> str:
        """Replace all {TOKEN} and ${TOKEN} patterns in a string."""

        def _lookup(match):
            return self.mappings.get(match.group(1), match.group(0))

        # Apply patterns - dollar pattern first to avoid conflicts
        text = self.DOLLAR_TOKEN_PATTERN.sub(_lookup, text)

        # Flip per-instance flag the first time a deprecated bare
        # ``{token}`` substitution actually happens. The orchestrator
        # inspects this flag after constructing the manager and forwards
        # a single warning to the per-run WarningCollector; emitting via
        # logger.warning here would be silenced inside worker processes
        # (NullHandler-only loggers).
        if not self.has_deprecated_bare_tokens:
            text, n = self.DEFAULT_TOKEN_PATTERN.subn(_lookup, text)
            if n > 0:
                self.has_deprecated_bare_tokens = True
        else:
            text = self.DEFAULT_TOKEN_PATTERN.sub(_lookup, text)
        return text

    def validate_no_unresolved_tokens(
        self, data: Any, path: str = "config"
    ) -> List[str]:
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
                errors.extend(
                    self.validate_no_unresolved_tokens(value, f"{path}.{key}")
                )
        elif isinstance(data, list):
            for i, item in enumerate(data):
                errors.extend(self.validate_no_unresolved_tokens(item, f"{path}[{i}]"))
        # For other types (int, bool, None, etc.), nothing to validate

        return errors
