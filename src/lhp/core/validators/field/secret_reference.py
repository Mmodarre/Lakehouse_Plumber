"""Secret reference validation for LakehousePlumber."""

import logging
from typing import List, Optional, Set

from ...processing.substitution import SecretReference


class SecretValidator:
    """Validate secret references — syntax-only by design.

    LHP does not contact the Databricks workspace at generate/validate time,
    so we cannot know which secret scopes actually exist. Rather than maintain
    a stale local list, this validator only checks that scope and key names
    conform to the syntactic rules Databricks itself enforces (alphanumeric
    plus underscore/hyphen, length cap on scope names). Existence is verified
    by Databricks when the generated pipeline runs.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def validate_secret_references(
        self, secret_refs: Set[SecretReference]
    ) -> List[str]:
        errors = []
        seen_refs = set()

        for secret_ref in secret_refs:
            ref_key = f"{secret_ref.scope}/{secret_ref.key}"
            if ref_key in seen_refs:
                self.logger.warning(f"Duplicate secret reference: ${{{ref_key}}}")
            seen_refs.add(ref_key)

            scope_error = self.validate_scope_syntax(secret_ref.scope)
            if scope_error is not None:
                errors.append(
                    f"Invalid secret scope '{secret_ref.scope}': {scope_error}"
                )

            if not self._is_valid_key_format(secret_ref.key):
                errors.append(
                    f"Invalid secret key format: '{secret_ref.key}' (must contain only alphanumeric, underscore, or hyphen)"
                )

        return errors

    def _is_valid_key_format(self, key: str) -> bool:
        if not key:
            return False

        allowed_chars = set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        )
        return all(c in allowed_chars for c in key)

    def validate_scope_syntax(self, scope: str) -> Optional[str]:
        """Validate scope name syntax.

        Returns:
            Error message if invalid, None if valid
        """
        if not scope:
            return "Scope name cannot be empty"

        if len(scope) > 128:
            return f"Scope name too long: {len(scope)} characters (max 128)"

        if not all(c.isalnum() or c in "_-" for c in scope):
            return "Scope name can only contain alphanumeric characters, underscores, and hyphens"

        return None
