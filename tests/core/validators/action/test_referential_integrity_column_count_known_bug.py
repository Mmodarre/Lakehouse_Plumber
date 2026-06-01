"""KNOWN-FAILING (owner-authorized).

Documents finding #1: TestActionValidator does not reject referential_integrity
actions whose source_columns/reference_columns differ in length
(src/lhp/core/validators/action/test.py ~L118-125; generator
src/lhp/generators/test/referential_integrity.py ~L44 zip-truncates silently).
The CODE is wrong, not this test — fix the validator, do not weaken this
assertion.

Calling convention mirrors ``tests/core/validators/action/test_test_action.py``:
construct the validator from ``ActionRegistry`` + ``ConfigFieldValidator``, build
a ``models.Action``, and invoke ``validator.validate(action, prefix)``. The
validator returns plain error strings (NOT raised exceptions), so assertions
match on error-message substrings within the returned list.

The generator at ``referential_integrity.py`` does
``zip(source_columns, reference_columns)``, which silently TRUNCATES to the
shorter list. A column-count mismatch therefore passes validation today and the
generated FK join silently omits columns, testing the wrong constraint.
"""

from lhp.core.registry import ActionRegistry
from lhp.core.validators import TestActionValidator
from lhp.core.validators.config_field_validator import ConfigFieldValidator
from lhp.models import Action

PREFIX = "Action[0] 'test_action'"


def _make_validator() -> TestActionValidator:
    return TestActionValidator(ActionRegistry(), ConfigFieldValidator())


def _errors_for(**kwargs) -> list:
    """Build a test Action from kwargs and return the validator's error list."""
    validator = _make_validator()
    action = Action(name="test_action", type="test", **kwargs)
    return validator.validate(action, PREFIX)


def test_known_bug_refintegrity_column_count_mismatch_rejected():
    """KNOWN-FAILING: a referential_integrity action whose source_columns and
    reference_columns differ in length must be rejected.

    Otherwise-valid action (3-part reference, bare source, both column lists
    present) with mismatched counts: 2 source columns vs 1 reference column.
    The generator's ``zip`` would silently drop ``customer_id``, joining only on
    ``order_id = id`` — the wrong FK constraint. The validator should catch this
    BEFORE generation. No length-equality rule exists yet, so this FAILS by
    design — fix the validator, do not weaken this assertion.
    """
    errors = _errors_for(
        test_type="referential_integrity",
        source="v_orders",
        reference="cat.sch.customers",
        source_columns=["order_id", "customer_id"],
        reference_columns=["id"],
    )
    assert any(
        "must have the same number of columns" in e or "same number" in e
        for e in errors
    ), (
        "Expected a column-count mismatch / length error for "
        "referential_integrity with 2 source_columns vs 1 reference_column; "
        f"got: {errors}"
    )


def test_refintegrity_equal_column_counts_accepted():
    """Contrast/regression anchor: equal-length column lists are accepted.

    This is the correct, well-formed shape — the future validator fix must NOT
    flag it. Expected GREEN today and after the fix.
    """
    errors = _errors_for(
        test_type="referential_integrity",
        source="v_orders",
        reference="cat.sch.customers",
        source_columns=["order_id", "customer_id"],
        reference_columns=["id", "cust_id"],
    )
    assert errors == [], f"Expected zero errors for equal-length columns; got: {errors}"
