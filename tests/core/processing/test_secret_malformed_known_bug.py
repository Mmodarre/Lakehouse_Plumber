"""KNOWN-FAILING (owner-authorized). Documents finding #3: src/lhp/core/processing/substitution.py ~L228-261 does not validate malformed secret references; ${secret:} and ${secret:scope/} (empty key) silently produce empty-key secret lookups in generated code instead of raising. The CODE is wrong, not this test — add validation that raises a clear LHP error for empty scope/key; do not weaken this assertion."""

import tempfile
from pathlib import Path

import pytest

from lhp.core.processing.substitution import (
    EnhancedSubstitutionManager,
    SecretReference,
)
from lhp.errors import LHPError

# ---------------------------------------------------------------------------
# Assumed correct contract (so the fix-agent can revise if it differs):
#
#   A malformed secret reference with an EMPTY scope or EMPTY key MUST raise a
#   clear LHP error (fail-fast) at substitution time — it must NOT silently
#   produce a degenerate ``dbutils.secrets.get(scope=..., key='')`` lookup in
#   generated code.
#
# Observed buggy behavior with a default scope configured (so the existing
# no-default-scope guard at substitution.py:235 does NOT pre-empt):
#   ${secret:scope/} -> "__SECRET_scope___"  (key='' silently)   <- empty KEY
#   ${secret:/key}   -> "__SECRET__key__"    (scope='' silently)  <- empty SCOPE
#   ${secret:}       -> "${secret:}"  (regex [^}]+ never matches; passes
#                        through as literal text, also silently)
#
# None of these raise. That is the bug this file documents.
# ---------------------------------------------------------------------------


def _make_manager_with_default_scope() -> EnhancedSubstitutionManager:
    """Build a REAL manager that has a default secret scope configured.

    With ``secrets.default_scope`` set, the existing no-scope guard
    (LHP-CFG-008 at substitution.py:235) does NOT fire for default-scope
    refs, so it cannot mask the malformed-empty-key/scope cases.
    """
    config = """
dev:
  database: dev_db
secrets:
  default_scope: dev_secrets
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        f.flush()
        path = Path(f.name)
    return EnhancedSubstitutionManager(path, env="dev")


@pytest.mark.parametrize(
    "malformed_ref",
    [
        "${secret:scope/}",  # explicit scope, EMPTY key
        "${secret:/key}",  # EMPTY scope, explicit key
    ],
)
def test_malformed_secret_empty_scope_or_key_raises_known_bug(malformed_ref):
    """KNOWN-FAILING: empty-scope / empty-key secret refs must fail-fast.

    Drives the REAL EnhancedSubstitutionManager with a default scope
    configured and feeds a malformed ``${secret:scope/}`` (empty key) or
    ``${secret:/key}`` (empty scope) reference.

    CORRECT behavior asserted here: a clear LHPError is raised.
    ACTUAL (buggy) behavior: no error — a degenerate ``__SECRET_*__``
    placeholder with an empty scope or key is produced and reaches codegen,
    yielding ``dbutils.secrets.get(scope=..., key='')`` in generated code.

    Leave this test RED. Fix the CODE (add empty scope/key validation in
    ``substitution.py`` ``_process_string`` / ``secret_replacer``), not this
    assertion.
    """
    mgr = _make_manager_with_default_scope()

    with pytest.raises(LHPError):
        mgr._process_string(malformed_ref)


def test_wellformed_secret_still_resolves_regression():
    """PASSING anchor: a well-formed ${secret:scope/key} must still work.

    Ensures the fix for the malformed cases does not break valid references.
    """
    mgr = _make_manager_with_default_scope()

    result = mgr._process_string("password=${secret:storage/key}")

    assert result == "password=__SECRET_storage_key__"
    assert SecretReference("storage", "key") in mgr.secret_references


def test_wellformed_default_scope_secret_still_resolves_regression():
    """PASSING anchor: a well-formed default-scope ${secret:key} must work."""
    mgr = _make_manager_with_default_scope()

    result = mgr._process_string("password=${secret:admin_password}")

    assert result == "password=__SECRET_dev_secrets_admin_password__"
    assert SecretReference("dev_secrets", "admin_password") in mgr.secret_references
