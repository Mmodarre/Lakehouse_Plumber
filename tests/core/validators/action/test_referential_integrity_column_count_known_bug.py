"""KNOWN-FAILING (owner-authorized).

TestActionValidator does not reject referential_integrity actions whose
source_columns/reference_columns differ in length. The generator's
``zip(source_columns, reference_columns)`` silently truncates to the shorter
list, so a mismatch passes validation and the generated FK join omits columns.
The CODE is wrong, not this test — fix the validator, do not weaken this
assertion.
"""

from lhp.core.registry import ActionRegistry
from lhp.core.validators import ConfigFieldValidator, TestActionValidator
from lhp.models import Action

PREFIX = "Action[0] 'test_action'"


def _make_validator() -> TestActionValidator:
    return TestActionValidator(ActionRegistry(), ConfigFieldValidator())


def _errors_for(**kwargs) -> list:
    validator = _make_validator()
    action = Action(name="test_action", type="test", **kwargs)
    return validator.validate(action, PREFIX)


def test_known_bug_refintegrity_column_count_mismatch_rejected():
    """KNOWN-FAILING: mismatched source_columns/reference_columns length must be rejected.

    No length-equality rule exists yet — this FAILS by design. Fix the validator,
    do not weaken this assertion.
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
    """Regression anchor: the future validator fix must NOT flag equal-length column lists."""
    errors = _errors_for(
        test_type="referential_integrity",
        source="v_orders",
        reference="cat.sch.customers",
        source_columns=["order_id", "customer_id"],
        reference_columns=["id", "cust_id"],
    )
    assert errors == [], f"Expected zero errors for equal-length columns; got: {errors}"
