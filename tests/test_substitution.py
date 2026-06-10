"""Tests for substitution functionality of LakehousePlumber."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.core.processing.substitution import (
    EnhancedSubstitutionManager,
    SecretReference,
)
from lhp.errors import LHPConfigError, LHPValidationError


class TestEnhancedSubstitutionManager:
    """Test the enhanced substitution manager."""

    def test_token_substitution(self):
        """Test basic token substitution."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            config = """
dev:
  catalog: dev_catalog
  database: dev_bronze
global:
  company: acme_corp
"""
            f.write(config)
            f.flush()

            try:
                mgr = EnhancedSubstitutionManager(Path(f.name), env="dev")

                # Test token replacement
                result = mgr._replace_tokens_in_string(
                    "Use {catalog}.{database} from {company}"
                )
                assert result == "Use dev_catalog.dev_bronze from acme_corp"

                # Test dollar-sign tokens
                result = mgr._replace_tokens_in_string("${catalog}_table")
                assert result == "dev_catalog_table"
            finally:
                Path(f.name).unlink()

    def test_secret_substitution(self):
        """Test secret reference handling at the substitution layer.

        Substitution emits a sentinel placeholder ``__SECRET_scope_key__``
        rather than the dbutils call text. The post-pass
        (`SecretCodeGenerator`, exercised in `test_secret_code_generator.py`)
        decides whether to rewrite the placeholder as a bare call or an
        f-string based on the surrounding string-literal context.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            config = """
dev:
  database: dev_db
secrets:
  default_scope: dev_secrets
  scopes:
    db: dev_database_secrets
    storage: dev_storage_secrets
"""
            f.write(config)
            f.flush()

            try:
                mgr = EnhancedSubstitutionManager(Path(f.name), env="dev")

                # Explicit scope with alias: scope alias 'db' resolves to
                # 'dev_database_secrets', token resolves to dev_db.
                result = mgr._process_string(
                    "jdbc://${secret:db/host}:5432/${database}"
                )
                assert "__SECRET_dev_database_secrets_host__" in result
                assert "dev_db" in result
                # The plain dbutils text must NOT appear at this layer —
                # it would mean substitution skipped the placeholder step.
                assert "dbutils.secrets.get" not in result

                # Default-scope secret falls back to dev_secrets.
                result = mgr._process_string("password=${secret:admin_password}")
                assert "__SECRET_dev_secrets_admin_password__" in result
                assert "dbutils.secrets.get" not in result

                # Both secret references were captured on the manager.
                assert len(mgr.secret_references) == 2
                scopes = {ref.scope for ref in mgr.secret_references}
                keys = {ref.key for ref in mgr.secret_references}
                assert scopes == {"dev_database_secrets", "dev_secrets"}
                assert keys == {"host", "admin_password"}
            finally:
                Path(f.name).unlink()

    def test_yaml_substitution(self):
        """Test substitution in YAML data structures.

        Secrets become ``__SECRET_scope_key__`` placeholders at the YAML
        substitution layer; the post-pass converts them to dbutils calls
        later, after Jinja templates have rendered the surrounding code.
        """
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {"env": "dev", "catalog": "main"}

        data = {
            "database": "{env}_bronze",
            "table": "{catalog}.users",
            "config": {"path": "/mnt/{env}/data", "secret": "${secret:storage/key}"},
        }

        result = mgr.substitute_yaml(data)

        assert result["database"] == "dev_bronze"
        assert result["table"] == "main.users"
        assert result["config"]["path"] == "/mnt/dev/data"
        # Secret value is a placeholder; the post-pass at code-generation
        # time decides whether to emit a bare call or an f-string.
        assert result["config"]["secret"] == "__SECRET_storage_key__"
        assert "dbutils.secrets.get" not in result["config"]["secret"]
        # The reference is registered on the manager for the post-pass to
        # find later.
        assert SecretReference("storage", "key") in mgr.secret_references


class TestUnresolvedTokenValidation:
    """Test validation of unresolved tokens."""

    def test_validation_detects_simple_unresolved_token(self):
        """Detect simple unresolved token like {missing_token}."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {"existing": "value"}

        data = {"path": "s3://bucket/{missing_token}/data"}
        errors = mgr.validate_no_unresolved_tokens(data)

        assert len(errors) == 1
        assert "missing_token" in errors[0]
        assert "config.path" in errors[0]

    def test_validation_ignores_dbutils_expressions(self):
        """Don't flag dbutils.secrets.get() as unresolved."""
        mgr = EnhancedSubstitutionManager()

        # After secret substitution, these are valid Python code
        data = {"password": "f\"{dbutils.secrets.get(scope='scope', key='key')}\""}
        errors = mgr.validate_no_unresolved_tokens(data)

        assert len(errors) == 0

    def test_validation_in_nested_structures(self):
        """Detect unresolved tokens in nested dicts and lists."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {}

        data = {
            "config": {
                "paths": ["s3://{bucket1}/data", "s3://{bucket2}/logs"],
                "settings": {"host": "{db_host}", "port": 5432},
            }
        }
        errors = mgr.validate_no_unresolved_tokens(data)

        assert len(errors) == 3
        assert any("bucket1" in e for e in errors)
        assert any("bucket2" in e for e in errors)
        assert any("db_host" in e for e in errors)

    def test_validation_error_includes_path(self):
        """Error messages include the config path for debugging."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {}

        data = {"database": {"connection": {"host": "{db_host}"}}}
        errors = mgr.validate_no_unresolved_tokens(data)

        assert "config.database.connection.host" in errors[0]

    def test_validation_with_multiple_tokens_in_one_string(self):
        """Detect multiple unresolved tokens in single string."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {}

        data = {"url": "jdbc://{host}:{port}/{database}"}
        errors = mgr.validate_no_unresolved_tokens(data)

        # Should find all three tokens
        assert len(errors) == 1  # One error for the path
        assert "host" in errors[0]
        assert "port" in errors[0]
        assert "database" in errors[0]

    def test_circular_reference_detection(self):
        """Detect circular references in token expansion."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            config = """
dev:
  token_a: "{token_b}"
  token_b: "{token_c}"
  token_c: "{token_a}"
"""
            f.write(config)
            f.flush()

            try:
                # Should complete without infinite loop
                mgr = EnhancedSubstitutionManager(Path(f.name), env="dev")

                # Tokens should still be unresolved after max iterations
                assert (
                    "{token_b}" in mgr.mappings["token_a"]
                    or "{token_c}" in mgr.mappings["token_a"]
                )
            finally:
                Path(f.name).unlink()

    def test_circular_reference_caught_by_validation(self):
        """Circular references should be caught by unresolved token validation."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {"a": "{b}", "b": "{a}"}

        # Run recursive expansion
        mgr._expand_recursive_tokens()

        # Tokens should still be unresolved
        data = {"value": "{a}"}
        substituted = mgr.substitute_yaml(data)
        errors = mgr.validate_no_unresolved_tokens(substituted)

        assert len(errors) > 0
        assert "a" in errors[0] or "b" in errors[0]


class TestSubstitutionErrorPaths:
    """Test error paths in substitution processing."""

    def test_secret_without_default_scope_raises_validation_error(self):
        """Secret reference without scope or default_scope should raise LHPValidationError."""
        mgr = EnhancedSubstitutionManager()

        with pytest.raises(LHPConfigError) as exc_info:
            mgr._process_string("${secret:my_key}")

        assert exc_info.value.code == "LHP-CFG-008"

    def test_corrupted_substitution_file_raises_config_error(self):
        """Corrupted substitution file should raise LHPConfigError with code 020."""
        mgr = EnhancedSubstitutionManager()

        with patch(
            "lhp.parsers.yaml_loader.load_yaml_file",
            side_effect=RuntimeError("file corrupted"),
        ):
            with pytest.raises(LHPConfigError) as exc_info:
                mgr._load_config_from_file(Path("/fake/substitutions.yaml"), "dev")

        assert exc_info.value.code == "LHP-CFG-020"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
