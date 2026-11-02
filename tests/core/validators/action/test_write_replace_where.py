"""``mode: replace_where`` through ``WriteActionValidator`` (#346).

The write validator accepts the mode, requires a single source view and folds
``ReplaceWhereConfigValidator``'s plain-string errors into its own result.
"""

import pytest

from lhp.core.registry import ActionRegistry
from lhp.core.validators import ConfigFieldValidator, WriteActionValidator
from lhp.errors import LHPError
from lhp.models import Action

pytestmark = pytest.mark.unit

_PREFIX = "Action[0] 'write_orders_recent'"
_PREDICATE = "order_date >= date_add(current_date(), -7)"


def _action(source="v_orders_source", **write_target) -> Action:
    return Action(
        name="write_orders_recent",
        type="write",
        source=source,
        write_target={
            "type": "streaming_table",
            "catalog": "main",
            "schema": "silver",
            "table": "orders_recent",
            "mode": "replace_where",
            "replace_where_config": {"replace_where": _PREDICATE},
            **write_target,
        },
    )


def _validate(action: Action) -> list:
    validator = WriteActionValidator(ActionRegistry(), ConfigFieldValidator())
    return validator.validate(action, _PREFIX)


def test_valid_replace_where_action_passes():
    assert _validate(_action()) == []


def test_valid_replace_where_action_with_table_options_passes():
    action = _action(
        private=True,
        create_table=True,
        table_properties={"pipelines.reset.allowed": "true"},
        cluster_columns=["order_date"],
    )

    assert _validate(action) == []


@pytest.mark.parametrize(
    "source",
    [["v_a", "v_b"], ["v_a", "v_b", "v_c"]],
    ids=["two_views", "three_views"],
)
def test_list_source_rejected(source):
    errors = _validate(_action(source=source))

    assert len(errors) == 1
    [error] = errors
    assert isinstance(error, str)
    assert error.startswith(f"{_PREFIX}: ")
    assert "replace_where mode does not support multiple source views" in error


def test_one_element_list_source_accepted():
    # Same as replace: a one-element list is a single view.
    assert _validate(_action(source=["v_orders_source"])) == []


def test_typo_mode_suggests_replace_where():
    errors = _validate(_action(mode="replace-where"))

    [error] = [e for e in errors if isinstance(e, LHPError)]
    assert error.code == "LHP-VAL-006"
    assert error.suggestions[0] == "Did you mean 'replace_where'?"
    assert "replace_where" in str(error)


def test_config_errors_surface_through_write_validator():
    action = _action(
        replace_where_config={"replace_where": "  ", "partition": "x"},
        temporary=True,
    )

    errors = _validate(action)

    assert all(isinstance(e, str) and e.startswith(f"{_PREFIX}: ") for e in errors)
    assert any("'replace_where' cannot be blank" in e for e in errors)
    assert any("unknown key(s) 'partition'" in e for e in errors)
    assert any("does not support 'temporary'" in e for e in errors)
    assert len(errors) == 3


def test_missing_config_block_surfaces_through_write_validator():
    action = _action()
    del action.write_target["replace_where_config"]

    errors = _validate(action)

    assert errors == [f"{_PREFIX}: replace_where mode requires 'replace_where_config'"]
