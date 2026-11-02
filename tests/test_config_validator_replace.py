"""Tests for replace mode in write-action validation, table creation, and fan-in.

Covers:
- ConfigValidator wiring: multiple sources + replace mode → error.
- ConfigValidator wiring: batch readMode + replace mode → error.
- Table creation predicate: replace action → creates_table is True.
- Replace fan-in: sole replace flow allowed.
- Replace fan-in: replace + another write on same table → error.
"""

import pytest

from lhp.core.validators import ConfigValidator, TableCreationValidator
from lhp.core.validators.compatibility import (
    ReplaceFanInCompatibilityValidator,
    action_creates_table,
)
from lhp.models import Action, ActionType, FlowGroup


def _replace_action(
    name: str,
    source: str | list = "v_order_updates",
    table: str = "orders_current",
    catalog: str = "cat",
    schema: str = "silver",
    create_table: bool = True,
    readMode: str | None = None,
) -> Action:
    """Build a replace mode write action."""
    write_target = {
        "type": "streaming_table",
        "mode": "replace",
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "create_table": create_table,
        "replace_config": {
            "replace_using": ["order_id"],
            "sequence_by": "updated_at",
        },
    }

    action = Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target=write_target,
    )
    if readMode:
        action.readMode = readMode

    return action


def test_replace_mode_rejects_multiple_sources():
    """replace mode + multiple sources → error mentioning multiple/single source."""
    validator = ConfigValidator()
    action = _replace_action(
        "write_orders",
        source=["v_a", "v_b"],
    )

    errors = validator.validate_action(action, 1)

    assert any(
        "replace" in str(e).lower() and "multiple" in str(e).lower() for e in errors
    ), f"Expected error about replace and multiple sources, got: {errors}"


def test_replace_mode_rejects_batch_readmode():
    """replace mode + readMode: batch → error mentioning batch/streaming."""
    validator = ConfigValidator()
    action = _replace_action(
        "write_orders",
        readMode="batch",
    )

    errors = validator.validate_action(action, 1)

    assert any(
        "replace" in str(e).lower() and "stream" in str(e).lower() for e in errors
    ), f"Expected error about replace and streaming, got: {errors}"


def test_replace_action_is_table_creator_when_create_table_false():
    """replace mode always creates its table; action_creates_table returns True even when create_table: false."""
    action = _replace_action(
        "write_orders",
        create_table=False,
    )

    result = action_creates_table(action)

    assert result is True


def test_replace_action_is_table_creator_when_create_table_true():
    """replace mode creates its table when create_table: true."""
    action = _replace_action(
        "write_orders",
        create_table=True,
    )

    result = action_creates_table(action)

    assert result is True


def test_replace_fanin_allows_sole_replace_flow():
    """A single replace action targeting cat.silver.orders_current → no errors."""
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _replace_action(
                "write_orders",
                create_table=True,
            )
        ],
    )

    validator = ReplaceFanInCompatibilityValidator()
    errors = validator.validate([fg])

    assert errors == []


def test_replace_fanin_rejects_replace_sharing_target_with_another_flow():
    """replace action + another write action on same table → error mentioning table name and sole flow."""
    replace = _replace_action(
        "write_orders_replace",
        create_table=True,
    )

    standard = Action(
        name="write_orders_standard",
        type=ActionType.WRITE,
        source="v_other",
        write_target={
            "type": "streaming_table",
            "catalog": "cat",
            "schema": "silver",
            "table": "orders_current",
            "create_table": False,
        },
    )

    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[replace, standard],
    )

    validator = ReplaceFanInCompatibilityValidator()
    errors = validator.validate([fg])

    assert len(errors) > 0
    assert any("replace" in str(e).lower() for e in errors)
    assert any("cat.silver.orders_current" in str(e) for e in errors)


def test_two_replace_actions_same_table_rejected_by_table_creation():
    """Two replace actions are both table creators (replace forces create_table),
    so the multiple-creators diagnostic belongs to TableCreationValidator (CFG_004);
    the replace fan-in validator defers rather than double-reporting."""
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _replace_action("write_orders_1", create_table=True),
            _replace_action("write_orders_2", create_table=False),
        ],
    )

    # Fan-in defers: both are creators, so there is no create_table:false gap.
    assert ReplaceFanInCompatibilityValidator().validate([fg]) == []

    # TableCreationValidator owns the multiple-creators error (CFG_004).
    from lhp.errors import LHPError

    with pytest.raises(LHPError):
        TableCreationValidator().validate([fg])


def test_replace_fanin_cross_flowgroup_sole_flow_allowed():
    """Replace in one flowgroup, no other write action in other flowgroup → allowed."""
    fg_replace = FlowGroup(
        pipeline="p",
        flowgroup="fg_replace",
        actions=[
            _replace_action(
                "write_orders_replace",
                create_table=True,
            )
        ],
    )

    fg_other = FlowGroup(
        pipeline="p",
        flowgroup="fg_other",
        actions=[
            Action(
                name="write_unrelated",
                type=ActionType.WRITE,
                source="v_unrelated",
                write_target={
                    "type": "streaming_table",
                    "catalog": "cat",
                    "schema": "silver",
                    "table": "other_table",
                    "create_table": True,
                },
            )
        ],
    )

    validator = ReplaceFanInCompatibilityValidator()
    errors = validator.validate([fg_replace, fg_other])

    assert errors == []


def test_replace_fanin_rejects_cdc_with_replace_on_same_table():
    """CDC action + replace action on same table → error."""
    replace = _replace_action(
        "write_orders_replace",
        table="orders_current",
        create_table=True,
    )

    cdc = Action(
        name="write_orders_cdc",
        type=ActionType.WRITE,
        source="v_cdc_stream",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "catalog": "cat",
            "schema": "silver",
            "table": "orders_current",
            "create_table": False,
            "cdc_config": {
                "keys": ["order_id"],
                "sequence_by": "_timestamp",
                "scd_type": 1,
            },
        },
    )

    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[replace, cdc],
    )

    validator = ReplaceFanInCompatibilityValidator()
    errors = validator.validate([fg])

    assert len(errors) > 0


# ---------------------------------------------------------------------------
# Unknown streaming_table modes (#345)
# ---------------------------------------------------------------------------


def _cdc_typo_action(mode) -> Action:
    return Action(
        name="write_customer_scd2",
        type=ActionType.WRITE,
        source="v_customer",
        write_target={
            "type": "streaming_table",
            "mode": mode,
            "catalog": "cat",
            "schema": "silver",
            "table": "dim_customer",
            "cdc_config": {
                "keys": ["c_custkey"],
                "sequence_by": "c_acctbal",
                "scd_type": 2,
            },
        },
    )


def test_unknown_streaming_table_mode_is_rejected_with_suggestion():
    """A typo such as 'cdcc' is an error naming the valid modes, not an append flow."""
    errors = ConfigValidator().validate_action(_cdc_typo_action("cdcc"), 1)

    messages = [str(e) for e in errors]
    assert any("Unknown streaming_table write mode 'cdcc'" in m for m in messages), (
        messages
    )
    assert any("Did you mean 'cdc'?" in m for m in messages), messages
    assert any("standard, cdc, snapshot_cdc, replace" in m for m in messages)


@pytest.mark.parametrize("mode", ["replace_where", "CDC", ""])
def test_other_unknown_streaming_table_modes_are_rejected(mode):
    """Any value outside the valid set is rejected, including case variants."""
    errors = ConfigValidator().validate_action(_cdc_typo_action(mode), 1)

    assert any(
        f"Unknown streaming_table write mode '{mode}'" in str(e) for e in errors
    ), errors


@pytest.mark.parametrize("mode", ["standard", "cdc", "snapshot_cdc", "replace"])
def test_valid_streaming_table_modes_are_not_reported_as_unknown(mode):
    errors = ConfigValidator().validate_action(_cdc_typo_action(mode), 1)

    assert not any("Unknown streaming_table write mode" in str(e) for e in errors)


def test_mode_config_ignored_by_selected_mode_is_warned(caplog):
    """cdc_config next to mode: standard is ignored, so say so."""
    with caplog.at_level("WARNING"):
        errors = ConfigValidator().validate_action(_cdc_typo_action("standard"), 1)

    assert not errors
    assert "'cdc_config' is ignored because write_target.mode is 'standard'" in (
        caplog.text
    )
