"""Tests for secret validator."""

import logging

import pytest

from lhp.core.secret_validator import SecretValidator
from lhp.utils.substitution import SecretReference


class TestSecretValidator:
    """Tests for SecretValidator validation methods."""

    def test_validate_scope_syntax_empty_scope(self):
        """Should return error message for empty scope name."""
        validator = SecretValidator()
        result = validator.validate_scope_syntax("")

        assert result is not None
        assert "cannot be empty" in result

    def test_validate_scope_syntax_too_long(self):
        """Should return error message for scope name exceeding 128 characters."""
        validator = SecretValidator()
        long_scope = "a" * 129
        result = validator.validate_scope_syntax(long_scope)

        assert result is not None
        assert "too long" in result
        assert "129" in result

    def test_validate_scope_syntax_exactly_128_chars(self):
        """Should accept scope name of exactly 128 characters."""
        validator = SecretValidator()
        scope = "a" * 128
        result = validator.validate_scope_syntax(scope)

        assert result is None

    def test_validate_scope_syntax_invalid_chars_space(self):
        """Should return error for scope name containing spaces."""
        validator = SecretValidator()
        result = validator.validate_scope_syntax("my scope")

        assert result is not None
        assert "can only contain" in result

    def test_validate_scope_syntax_invalid_chars_at_sign(self):
        """Should return error for scope name containing @ symbol."""
        validator = SecretValidator()
        result = validator.validate_scope_syntax("scope@name")

        assert result is not None
        assert "can only contain" in result

    def test_validate_scope_syntax_invalid_chars_dot(self):
        """Should return error for scope name containing dot."""
        validator = SecretValidator()
        result = validator.validate_scope_syntax("scope.name")

        assert result is not None
        assert "can only contain" in result

    def test_validate_scope_syntax_valid_alphanumeric(self):
        """Should return None for valid alphanumeric scope name."""
        validator = SecretValidator()
        result = validator.validate_scope_syntax("myScope123")

        assert result is None

    def test_validate_scope_syntax_valid_with_underscore(self):
        """Should return None for scope name with underscores."""
        validator = SecretValidator()
        result = validator.validate_scope_syntax("my_scope_name")

        assert result is None

    def test_validate_scope_syntax_valid_with_hyphen(self):
        """Should return None for scope name with hyphens."""
        validator = SecretValidator()
        result = validator.validate_scope_syntax("my-scope-name")

        assert result is None

    def test_is_valid_key_format_empty_string(self):
        """Should return False for empty key string."""
        validator = SecretValidator()
        assert validator._is_valid_key_format("") is False

    def test_is_valid_key_format_valid_alphanumeric(self):
        """Should return True for valid alphanumeric key."""
        validator = SecretValidator()
        assert validator._is_valid_key_format("dbPassword123") is True

    def test_is_valid_key_format_valid_with_underscore(self):
        """Should return True for key with underscores."""
        validator = SecretValidator()
        assert validator._is_valid_key_format("db_password") is True

    def test_is_valid_key_format_valid_with_hyphen(self):
        """Should return True for key with hyphens."""
        validator = SecretValidator()
        assert validator._is_valid_key_format("api-key-v2") is True

    def test_is_valid_key_format_invalid_with_space(self):
        """Should return False for key containing spaces."""
        validator = SecretValidator()
        assert validator._is_valid_key_format("db password") is False

    def test_is_valid_key_format_invalid_with_special_chars(self):
        """Should return False for key containing special characters."""
        validator = SecretValidator()
        assert validator._is_valid_key_format("key@value") is False
        assert validator._is_valid_key_format("key!value") is False
        assert validator._is_valid_key_format("key.value") is False

    def test_set_available_scopes(self):
        """Should update available scopes via set_available_scopes."""
        validator = SecretValidator()
        assert validator.available_scopes == set()

        validator.set_available_scopes({"prod_secrets", "dev_secrets"})
        assert validator.available_scopes == {"prod_secrets", "dev_secrets"}

    def test_validate_secret_references_scope_not_available(self):
        """Should return error when scope is not in available scopes."""
        validator = SecretValidator()
        validator.set_available_scopes({"prod_secrets", "dev_secrets"})

        refs = {SecretReference(scope="unknown_scope", key="mykey")}
        errors = validator.validate_secret_references(refs)

        assert len(errors) == 1
        assert "unknown_scope" in errors[0]
        assert "not found" in errors[0]

    def test_validate_secret_references_multiple_invalid_scopes(self):
        """Should return an error for each invalid scope."""
        validator = SecretValidator()
        validator.set_available_scopes({"prod_secrets"})

        refs = {
            SecretReference(scope="bad_scope_1", key="key1"),
            SecretReference(scope="bad_scope_2", key="key2"),
        }
        errors = validator.validate_secret_references(refs)

        assert len(errors) == 2

    def test_validate_secret_references_valid_refs_empty_errors(self):
        """Should return empty errors list for valid references."""
        validator = SecretValidator(available_scopes={"my_scope"})

        refs = {
            SecretReference(scope="my_scope", key="db_password"),
            SecretReference(scope="my_scope", key="api-key"),
        }
        errors = validator.validate_secret_references(refs)

        assert errors == []

    def test_validate_secret_references_no_available_scopes_skips_scope_check(self):
        """Should skip scope validation when no available_scopes are set."""
        validator = SecretValidator()  # No available_scopes

        refs = {SecretReference(scope="any_scope", key="any_key")}
        errors = validator.validate_secret_references(refs)

        assert errors == []

    def test_validate_secret_references_invalid_key_format(self):
        """Should return error for invalid key format."""
        validator = SecretValidator()

        refs = {SecretReference(scope="myscope", key="bad key!")}
        errors = validator.validate_secret_references(refs)

        assert len(errors) == 1
        assert "Invalid secret key format" in errors[0]

    def test_validate_secret_references_empty_key(self):
        """Should return error for empty key."""
        validator = SecretValidator()

        refs = {SecretReference(scope="myscope", key="")}
        errors = validator.validate_secret_references(refs)

        assert len(errors) == 1
        assert "Invalid secret key format" in errors[0]

    def test_validate_secret_references_duplicate_logs_warning(self, caplog):
        """Should log a warning for duplicate secret references."""
        validator = SecretValidator()

        # SecretReference uses __hash__ and __eq__ on (scope, key),
        # so a set would deduplicate. We need a list passed to the method.
        # But the method accepts a Set[SecretReference] -- duplicates are
        # impossible in a set. The warning is triggered by seen_refs tracking,
        # so we need to pass something iterable that has duplicates.
        # Since the type hint says Set, but the code iterates, we can pass a list.
        refs = [
            SecretReference(scope="myscope", key="mykey"),
            SecretReference(scope="myscope", key="mykey"),
        ]

        with caplog.at_level(logging.WARNING, logger="lhp.core.secret_validator"):
            errors = validator.validate_secret_references(refs)

        assert any("Duplicate secret reference" in record.message for record in caplog.records)
        # Duplicates produce a warning, not an error
        assert errors == []

    def test_validate_secret_references_scope_and_key_errors_combined(self):
        """Should return errors for both invalid scope and invalid key."""
        validator = SecretValidator(available_scopes={"valid_scope"})

        refs = {SecretReference(scope="bad_scope", key="bad key!")}
        errors = validator.validate_secret_references(refs)

        assert len(errors) == 2
        assert any("not found" in e for e in errors)
        assert any("Invalid secret key format" in e for e in errors)

    def test_init_with_available_scopes(self):
        """Should accept available_scopes in constructor."""
        scopes = {"scope_a", "scope_b"}
        validator = SecretValidator(available_scopes=scopes)

        assert validator.available_scopes == scopes

    def test_init_without_available_scopes(self):
        """Should default to empty set when no available_scopes provided."""
        validator = SecretValidator()

        assert validator.available_scopes == set()
