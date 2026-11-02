"""Fan-in rules for ``mode: replace_where`` targets (#346).

A REPLACE WHERE target is defined together with its single flow, so the
replace_where action always creates its table, a second creator trips
CFG-004 and a ``create_table: false`` writer is rejected by the replace
fan-in validator with a message naming REPLACE WHERE.
"""

import pytest

from lhp.core.validators import TableCreationValidator
from lhp.core.validators.compatibility import (
    ReplaceFanInCompatibilityValidator,
    action_creates_table,
)
from lhp.errors import LHPError
from lhp.models import Action, ActionType, FlowGroup

pytestmark = pytest.mark.unit

_TABLE = "cat.silver.orders_recent"


def _replace_where(name: str = "write_orders_recent", **extra) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source="v_orders_source",
        write_target={
            "type": "streaming_table",
            "mode": "replace_where",
            "catalog": "cat",
            "schema": "silver",
            "table": "orders_recent",
            "replace_where_config": {
                "replace_where": "order_date >= date_add(current_date(), -7)"
            },
            **extra,
        },
    )


def _append(name: str, create_table: bool) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source="v_other",
        write_target={
            "type": "streaming_table",
            "catalog": "cat",
            "schema": "silver",
            "table": "orders_recent",
            "create_table": create_table,
        },
    )


def _flowgroup(*actions: Action) -> FlowGroup:
    return FlowGroup(pipeline="p", flowgroup="fg", actions=list(actions))


@pytest.mark.parametrize(
    "extra",
    [{}, {"create_table": True}, {"create_table": False}],
    ids=["absent", "true", "false"],
)
def test_replace_where_action_always_creates_its_table(extra):
    assert action_creates_table(_replace_where(**extra)) is True


def test_sole_replace_where_flow_is_allowed():
    fg = _flowgroup(_replace_where())

    assert ReplaceFanInCompatibilityValidator().validate([fg]) == []
    assert TableCreationValidator().validate([fg]) == []


def test_append_user_into_replace_where_target_is_rejected():
    fg = _flowgroup(_replace_where(), _append("write_orders_append", False))

    errors = ReplaceFanInCompatibilityValidator().validate([fg])

    assert len(errors) == 1
    [error] = errors
    assert _TABLE in error
    assert "fg.write_orders_append" in error
    assert "REPLACE WHERE" in error
    assert "REPLACE USING" not in error


def test_two_creators_raise_cfg_004():
    fg = _flowgroup(_replace_where(), _append("write_orders_append", True))

    # Both are creators, so the fan-in validator defers to CFG-004.
    assert ReplaceFanInCompatibilityValidator().validate([fg]) == []
    with pytest.raises(LHPError) as exc_info:
        TableCreationValidator().validate([fg])
    assert exc_info.value.code == "LHP-CFG-004"
