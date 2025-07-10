"""Secret reference validation for LakehousePlumber."""

import logging
from typing import List, Set, Optional
from ..utils.substitution import SecretReference


class SecretValidator:
    """Validate secret references and scopes."""

    def __init__(self, available_scopes: Set[str] = None):
        """Initialize secret validator.

        Args:
            available_scopes: Set of available secret scopes
        """
        self.available_scopes = available_scopes or set()
        self.logger = logging.getLogger(__name__)

    def validate_secret_references(
        self, secret_refs: Set[SecretReference]
    ) -> List[str]:
        """Validate secret references.

        Args:
            secret_refs: Set of secret references to validate

        Returns:
            List of validation error messages
        """
        errors = []
        seen_refs = set()

        for secret_ref in secret_refs:
            # Check for duplicates
            ref_key = f"{secret_ref.scope}/{secret_ref.key}"
            if ref_key in seen_refs:
                self.logger.warning(f"Duplicate secret reference: ${{{ref_key}}}")
            seen_refs.add(ref_key)

            # Check scope exists if available_scopes is provided
            if self.available_scopes and secret_ref.scope not in self.available_scopes:
                errors.append(
                    f"Secret scope '{secret_ref.scope}' not found in available scopes"
                )

            # Check key format
            if not self._is_valid_key_format(secret_ref.key):
                errors.append(
                    f"Invalid secret key format: '{secret_ref.key}' (must contain only alphanumeric, underscore, or hyphen)"
                )

        return errors

    def _is_valid_key_format(self, key: str) -> bool:
        """Check if secret key has valid format.

        Args:
            key: Secret key to validate

        Returns:
            True if valid, False otherwise
        """
        if not key:
            return False

        # Allow alphanumeric, underscore, and hyphen
        allowed_chars = set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        )
        return all(c in allowed_chars for c in key)

    def set_available_scopes(self, scopes: Set[str]):
        """Update available scopes.

        Args:
            scopes: Set of available secret scopes
        """
        self.available_scopes = scopes

    def validate_scope_syntax(self, scope: str) -> Optional[str]:
        """Validate scope name syntax.

        Args:
            scope: Scope name to validate

        Returns:
            Error message if invalid, None if valid
        """
        if not scope:
            return "Scope name cannot be empty"

        if len(scope) > 128:
            return f"Scope name too long: {len(scope)} characters (max 128)"

        # Check for valid characters (similar to Databricks scope naming)
        if not all(c.isalnum() or c in "_-" for c in scope):
            return "Scope name can only contain alphanumeric characters, underscores, and hyphens"

        return None
