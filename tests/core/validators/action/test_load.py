"""Negative tests for LoadActionValidator missing-field validation.

Covers the type-specific required-field checks for Delta and JDBC load
sources. Each test omits exactly one required field (while supplying all
other required fields) so that the validator surfaces a single, specific
error attributable to that field.

Calling convention mirrors ``tests/test_action_validators_kafka.py``:
construct the validator from ``ActionRegistry`` + ``ConfigFieldValidator``,
build a ``models.Action`` with a dict ``source``, and invoke
``validator.validate(action, prefix)``. The Delta/JDBC type-specific checks
return plain error strings, so assertions match on the error message text.
"""

import pytest

from lhp.core.registry import ActionRegistry
from lhp.core.validators import ConfigFieldValidator, LoadActionValidator
from lhp.models import Action


def _make_validator() -> LoadActionValidator:
    return LoadActionValidator(ActionRegistry(), ConfigFieldValidator())


def _errors_for(source: dict) -> list:
    validator = _make_validator()
    action = Action(
        name="test_load",
        type="load",
        source=source,
        target="v_test",
    )
    return validator.validate(action, "test_action")


# ---------------------------------------------------------------------------
# Delta source: required fields catalog / schema / table
# ---------------------------------------------------------------------------


class TestDeltaSourceMissingFields:
    """Each Delta required field, omitted in isolation, raises its own error."""

    def test_missing_catalog(self):
        # Omit only 'catalog'; supply 'schema' + 'table'.
        errors = _errors_for(
            {
                "type": "delta",
                "schema": "bronze",
                "table": "events",
            }
        )
        assert "test_action: Delta source must have 'catalog'" in errors
        # The other Delta field errors must NOT fire.
        assert "test_action: Delta source must have 'schema'" not in errors
        assert "test_action: Delta source must have 'table'" not in errors

    def test_missing_schema(self):
        # Omit only 'schema'; supply 'catalog' + 'table'.
        errors = _errors_for(
            {
                "type": "delta",
                "catalog": "main",
                "table": "events",
            }
        )
        assert "test_action: Delta source must have 'schema'" in errors
        assert "test_action: Delta source must have 'catalog'" not in errors
        assert "test_action: Delta source must have 'table'" not in errors

    def test_missing_table(self):
        # Omit only 'table'; supply 'catalog' + 'schema'.
        errors = _errors_for(
            {
                "type": "delta",
                "catalog": "main",
                "schema": "bronze",
            }
        )
        assert "test_action: Delta source must have 'table'" in errors
        assert "test_action: Delta source must have 'catalog'" not in errors
        assert "test_action: Delta source must have 'schema'" not in errors


# ---------------------------------------------------------------------------
# JDBC source: required fields url / user / password / driver
# ---------------------------------------------------------------------------


class TestJdbcSourceMissingFields:
    """Each JDBC required field, omitted in isolation, raises its own error."""

    # 'table' is supplied in every case so the separate "must have either
    # 'query' or 'table'" error never fires, isolating the credential checks.
    _BASE = {
        "type": "jdbc",
        "url": "jdbc:postgresql://host:5432/db",
        "user": "svc",
        "password": "secret",
        "driver": "org.postgresql.Driver",
        "table": "public.events",
    }

    def _without(self, field: str) -> dict:
        source = dict(self._BASE)
        del source[field]
        return source

    def test_missing_url(self):
        errors = _errors_for(self._without("url"))
        assert "test_action: JDBC source must have 'url'" in errors
        assert "test_action: JDBC source must have 'user'" not in errors
        assert "test_action: JDBC source must have 'password'" not in errors
        assert "test_action: JDBC source must have 'driver'" not in errors

    def test_missing_user(self):
        errors = _errors_for(self._without("user"))
        assert "test_action: JDBC source must have 'user'" in errors
        assert "test_action: JDBC source must have 'url'" not in errors
        assert "test_action: JDBC source must have 'password'" not in errors
        assert "test_action: JDBC source must have 'driver'" not in errors

    def test_missing_password(self):
        errors = _errors_for(self._without("password"))
        assert "test_action: JDBC source must have 'password'" in errors
        assert "test_action: JDBC source must have 'url'" not in errors
        assert "test_action: JDBC source must have 'user'" not in errors
        assert "test_action: JDBC source must have 'driver'" not in errors

    def test_missing_driver(self):
        errors = _errors_for(self._without("driver"))
        assert "test_action: JDBC source must have 'driver'" in errors
        assert "test_action: JDBC source must have 'url'" not in errors
        assert "test_action: JDBC source must have 'user'" not in errors
        assert "test_action: JDBC source must have 'password'" not in errors


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
