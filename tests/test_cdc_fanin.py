"""Tests for multi-CDC fan-in to a single streaming table.

Covers:
- Single CDC action (regression — existing behavior preserved).
- Two CDC actions in one flowgroup sharing a target.
- Cross-flowgroup CDC fan-in (creator in one file, contributor in another).
- Per-flow params (``once``, ``ignore_null_updates``, ``apply_as_deletes``,
  ``apply_as_truncates``, ``column_list``, ``except_column_list``).
- Compatibility validator (shared-field mismatches and mode-mixing).
- Write validator rejection of list-source + CDC mode.
- Snapshot CDC regression (per-plan: output must be unchanged).
"""

from pathlib import Path

import pytest

from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.validator import ConfigValidator
from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models.config import Action, ActionType, FlowGroup
from lhp.utils.error_formatter import LHPConfigError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cdc_action(
    name: str,
    source: str,
    table: str = "dim_customer",
    catalog: str = "cat",
    schema: str = "sch",
    create_table: bool = True,
    cdc_overrides: dict = None,
    target_overrides: dict = None,
    once: bool = False,
) -> Action:
    """Build a CDC write action with a baseline valid cdc_config."""
    cdc_config = {
        "keys": ["customer_id"],
        "sequence_by": "_commit_timestamp",
        "scd_type": 2,
    }
    if cdc_overrides:
        cdc_config.update(cdc_overrides)

    write_target = {
        "type": "streaming_table",
        "mode": "cdc",
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "create_table": create_table,
        "cdc_config": cdc_config,
    }
    if target_overrides:
        write_target.update(target_overrides)

    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        once=once,
        write_target=write_target,
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_single_cdc_action_still_renders_table_and_flow():
    """Regression: single CDC action emits table + one auto_cdc_flow with name=."""
    action = _cdc_action(
        "write_customer_dim",
        source="v_customer_changes",
        cdc_overrides={"scd_type": 2, "track_history_column_list": ["name"]},
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert "dp.create_streaming_table(" in code
    assert 'name="cat.sch.dim_customer"' in code
    # Per-flow name= is now rendered for all CDC flows, including single-action.
    assert "dp.create_auto_cdc_flow(" in code
    assert 'source="v_customer_changes"' in code
    assert 'name="f_customer_dim"' in code
    assert 'keys=["customer_id"]' in code
    assert "stored_as_scd_type=2" in code


def test_two_cdc_actions_one_flowgroup_combine_into_one_file():
    """Two CDC actions in one flowgroup: one table + two auto_cdc_flow calls."""
    creator = _cdc_action(
        "write_cdc_primary",
        source="v_primary",
        create_table=True,
    )
    contributor = _cdc_action(
        "write_cdc_secondary",
        source="v_secondary",
        create_table=False,
    )

    orchestrator = ActionOrchestrator(Path("."))
    combined = orchestrator.create_combined_write_action(
        [creator, contributor], "cat.sch.dim_customer"
    )

    assert hasattr(combined, "_action_metadata")
    assert len(combined._action_metadata) == 2
    assert {m["flow_name"] for m in combined._action_metadata} == {
        "f_cdc_primary",
        "f_cdc_secondary",
    }

    generator = StreamingTableWriteGenerator()
    code = generator.generate(combined, {"expectations": []})

    # Exactly one create_streaming_table + two create_auto_cdc_flow calls.
    assert code.count("dp.create_streaming_table(") == 1
    assert code.count("dp.create_auto_cdc_flow(") == 2
    assert 'name="f_cdc_primary"' in code
    assert 'name="f_cdc_secondary"' in code
    assert 'source="v_primary"' in code
    assert 'source="v_secondary"' in code


def test_cdc_contributor_suppresses_streaming_table_emission():
    """A contributor (create_table=False) emits only create_auto_cdc_flow."""
    contributor = _cdc_action(
        "write_cdc_contributor",
        source="v_contributor",
        create_table=False,
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(contributor, {"expectations": []})

    assert "dp.create_streaming_table(" not in code
    assert "dp.create_auto_cdc_flow(" in code
    assert 'name="f_cdc_contributor"' in code


def test_cdc_fanin_with_once_backfill():
    """Per-flow once=True renders on backfill flow only, not on streaming flow."""
    streaming = _cdc_action(
        "write_cdc_stream",
        source="v_cdc_stream",
        create_table=True,
        once=False,
    )
    backfill = _cdc_action(
        "write_cdc_backfill",
        source="v_cdc_backfill",
        create_table=False,
        once=True,
    )

    orchestrator = ActionOrchestrator(Path("."))
    combined = orchestrator.create_combined_write_action(
        [streaming, backfill], "cat.sch.dim_customer"
    )
    code = StreamingTableWriteGenerator().generate(combined, {"expectations": []})

    # once=True must appear in backfill flow's block, but not in streaming flow's.
    stream_section = code.split('name="f_cdc_stream"')[1].split(")")[0]
    backfill_section = code.split('name="f_cdc_backfill"')[1].split(")")[0]
    assert "once=True" not in stream_section
    assert "once=True" in backfill_section


def test_cdc_fanin_per_flow_params_render_correctly():
    """Per-flow params come from each contributor's own cdc_config block."""
    creator = _cdc_action(
        "write_cdc_a",
        source="v_a",
        create_table=True,
        cdc_overrides={"ignore_null_updates": True},
    )
    contributor = _cdc_action(
        "write_cdc_b",
        source="v_b",
        create_table=False,
        cdc_overrides={
            "apply_as_deletes": "op = 'D'",
            "apply_as_truncates": "op = 'T'",
            "column_list": ["customer_id", "name"],
        },
    )

    orchestrator = ActionOrchestrator(Path("."))
    combined = orchestrator.create_combined_write_action(
        [creator, contributor], "cat.sch.dim_customer"
    )
    code = StreamingTableWriteGenerator().generate(combined, {"expectations": []})

    a_section = code.split('name="f_cdc_a"')[1].split("dp.create_auto_cdc_flow(")[0]
    b_section = code.split('name="f_cdc_b"')[1].split(")")[0]

    assert "ignore_null_updates=True" in a_section
    assert "apply_as_deletes" not in a_section
    assert "apply_as_deletes=\"op = 'D'\"" in b_section
    assert "apply_as_truncates=\"op = 'T'\"" in b_section
    assert 'column_list=["customer_id", "name"]' in b_section


def test_cdc_except_column_list_per_flow():
    """except_column_list is per-flow (DLP API), not shared at table level."""
    creator = _cdc_action(
        "write_cdc_x",
        source="v_x",
        create_table=True,
    )
    contributor = _cdc_action(
        "write_cdc_y",
        source="v_y",
        create_table=False,
        cdc_overrides={"except_column_list": ["internal_field"]},
    )

    orchestrator = ActionOrchestrator(Path("."))
    combined = orchestrator.create_combined_write_action(
        [creator, contributor], "cat.sch.dim_customer"
    )
    code = StreamingTableWriteGenerator().generate(combined, {"expectations": []})

    y_section = code.split('name="f_cdc_y"')[1].split(")")[0]
    x_section = code.split('name="f_cdc_x"')[1].split("dp.create_auto_cdc_flow(")[0]
    assert 'except_column_list=["internal_field"]' in y_section
    assert "except_column_list" not in x_section


# ---------------------------------------------------------------------------
# Cross-flowgroup fan-in
# ---------------------------------------------------------------------------


def test_cross_flowgroup_cdc_fanin_table_creator_validation():
    """Two CDC actions across two flowgroups: one creator wins, other must be user."""
    validator = ConfigValidator()

    fg_creator = FlowGroup(
        pipeline="p",
        flowgroup="fg_creator",
        actions=[
            _cdc_action(
                "write_cdc_creator",
                source="v_creator",
                create_table=True,
            )
        ],
    )
    fg_contrib = FlowGroup(
        pipeline="p",
        flowgroup="fg_contrib",
        actions=[
            _cdc_action(
                "write_cdc_contrib",
                source="v_contrib",
                create_table=False,
            )
        ],
    )

    # Passes with one creator + one contributor.
    errors = validator.validate_table_creation_rules([fg_creator, fg_contrib])
    assert errors == []

    # Cross-flowgroup fan-in compatibility validator agrees.
    fanin_errors = validator.validate_cdc_fanin_compatibility([fg_creator, fg_contrib])
    assert fanin_errors == []


# ---------------------------------------------------------------------------
# Compatibility-validator: mismatches
# ---------------------------------------------------------------------------


def test_cdc_fanin_mismatch_keys_raises():
    """Mismatched cdc_config.keys across contributors raises LHPConfigError."""
    validator = ConfigValidator()
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _cdc_action(
                "write_a",
                source="v_a",
                cdc_overrides={"keys": ["customer_id"]},
                create_table=True,
            ),
            _cdc_action(
                "write_b",
                source="v_b",
                cdc_overrides={"keys": ["id"]},
                create_table=False,
            ),
        ],
    )
    with pytest.raises(LHPConfigError) as exc:
        validator.validate_cdc_fanin_compatibility([fg])
    msg = str(exc.value)
    assert "cdc_config.keys" in msg
    assert "fg.write_a" in msg
    assert "fg.write_b" in msg


def test_cdc_fanin_mismatch_scd_type_raises():
    """Mismatched stored_as_scd_type raises LHPConfigError."""
    validator = ConfigValidator()
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _cdc_action(
                "write_a",
                source="v_a",
                cdc_overrides={"scd_type": 2},
                create_table=True,
            ),
            _cdc_action(
                "write_b",
                source="v_b",
                cdc_overrides={"scd_type": 1},
                create_table=False,
            ),
        ],
    )
    with pytest.raises(LHPConfigError):
        validator.validate_cdc_fanin_compatibility([fg])


def test_cdc_fanin_mismatch_sequence_by_raises():
    """Mismatched sequence_by raises LHPConfigError."""
    validator = ConfigValidator()
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _cdc_action(
                "write_a",
                source="v_a",
                cdc_overrides={"sequence_by": "ts"},
                create_table=True,
            ),
            _cdc_action(
                "write_b",
                source="v_b",
                cdc_overrides={"sequence_by": "other_ts"},
                create_table=False,
            ),
        ],
    )
    with pytest.raises(LHPConfigError):
        validator.validate_cdc_fanin_compatibility([fg])


def test_cdc_fanin_mismatch_partition_columns_raises():
    """Mismatched partition_columns (table-level field) raises LHPConfigError."""
    validator = ConfigValidator()
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _cdc_action(
                "write_a",
                source="v_a",
                create_table=True,
                target_overrides={"partition_columns": ["d"]},
            ),
            _cdc_action(
                "write_b",
                source="v_b",
                create_table=False,
                target_overrides={"partition_columns": ["other"]},
            ),
        ],
    )
    with pytest.raises(LHPConfigError) as exc:
        validator.validate_cdc_fanin_compatibility([fg])
    assert "partition_columns" in str(exc.value)


def test_cdc_fanin_mismatch_table_properties_raises():
    """Mismatched table_properties raises LHPConfigError."""
    validator = ConfigValidator()
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _cdc_action(
                "write_a",
                source="v_a",
                create_table=True,
                target_overrides={"table_properties": {"x": "1"}},
            ),
            _cdc_action(
                "write_b",
                source="v_b",
                create_table=False,
                target_overrides={"table_properties": {"x": "2"}},
            ),
        ],
    )
    with pytest.raises(LHPConfigError):
        validator.validate_cdc_fanin_compatibility([fg])


def test_cdc_fanin_mode_mixing_rejected():
    """CDC + non-CDC actions on same table produces a mode-mix error."""
    validator = ConfigValidator()
    cdc = _cdc_action("write_cdc", source="v_cdc", create_table=True)
    standard = Action(
        name="write_standard",
        type=ActionType.WRITE,
        source="v_std",
        write_target={
            "type": "streaming_table",
            "catalog": "cat",
            "schema": "sch",
            "table": "dim_customer",
            "create_table": False,
        },
    )
    fg = FlowGroup(pipeline="p", flowgroup="fg", actions=[cdc, standard])

    errors = validator.validate_cdc_fanin_compatibility([fg])
    assert len(errors) == 1
    assert "cannot mix CDC and non-CDC" in errors[0]
    assert "fg.write_cdc" in errors[0]
    assert "fg.write_standard" in errors[0]


def test_cdc_fanin_multiple_creators_reuses_existing_check():
    """Two CDC actions both with create_table=True still tripped by TableCreationValidator."""
    validator = ConfigValidator()
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _cdc_action("write_a", source="v_a", create_table=True),
            _cdc_action("write_b", source="v_b", create_table=True),
        ],
    )
    with pytest.raises(Exception) as exc:
        validator.validate_table_creation_rules([fg])
    assert "Multiple table creators" in str(exc.value)


# ---------------------------------------------------------------------------
# Write validator: reject list-source + cdc
# ---------------------------------------------------------------------------


def test_list_source_with_cdc_rejected():
    """`source: [v1, v2]` + `mode: cdc` emits a clear error (Shape A guidance)."""
    validator = ConfigValidator()
    action = Action(
        name="write_bad",
        type=ActionType.WRITE,
        source=["v_a", "v_b"],
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "catalog": "cat",
            "schema": "sch",
            "table": "dim_customer",
            "cdc_config": {"keys": ["id"], "sequence_by": "ts", "scd_type": 1},
        },
    )
    errors = validator.validate_action(action, 0)
    assert any(
        "CDC mode does not support multiple source views" in str(e) for e in errors
    )


def test_list_source_with_standard_still_works():
    """`source: [v1, v2]` + `mode: standard` is still valid (append fan-out)."""
    validator = ConfigValidator()
    action = Action(
        name="write_ok",
        type=ActionType.WRITE,
        source=["v_a", "v_b"],
        write_target={
            "type": "streaming_table",
            "catalog": "cat",
            "schema": "sch",
            "table": "ev",
            "create_table": True,
        },
    )
    errors = validator.validate_action(action, 0)
    assert errors == []


def test_single_element_list_source_with_cdc_allowed():
    """`source: [v1]` + `mode: cdc` is allowed — normalized to a single source."""
    validator = ConfigValidator()
    action = Action(
        name="write_ok_cdc",
        type=ActionType.WRITE,
        source=["v_only"],
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "catalog": "cat",
            "schema": "sch",
            "table": "dim_customer",
            "create_table": True,
            "cdc_config": {"keys": ["id"], "sequence_by": "ts", "scd_type": 1},
        },
    )
    errors = validator.validate_action(action, 0)
    assert errors == []


# ---------------------------------------------------------------------------
# Snapshot CDC regression (should be unchanged)
# ---------------------------------------------------------------------------


def test_snapshot_cdc_single_action_unchanged():
    """Snapshot CDC action renders its own primitive; no CDC fan-in changes apply."""
    action = Action(
        name="write_snapshot",
        type=ActionType.WRITE,
        write_target={
            "type": "streaming_table",
            "mode": "snapshot_cdc",
            "catalog": "cat",
            "schema": "sch",
            "table": "dim_snap",
            "snapshot_cdc_config": {
                "source": "some.source.table",
                "keys": ["id"],
                "stored_as_scd_type": 1,
            },
        },
    )
    code = StreamingTableWriteGenerator().generate(action, {"expectations": []})
    assert "dp.create_auto_cdc_from_snapshot_flow(" in code
    assert "dp.create_auto_cdc_flow(" not in code
    assert "dp.create_streaming_table(" in code


def test_snapshot_cdc_excluded_from_fanin_validator():
    """snapshot_cdc contributors are invisible to CDC fan-in validator."""
    validator = ConfigValidator()
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            Action(
                name="write_snap",
                type=ActionType.WRITE,
                write_target={
                    "type": "streaming_table",
                    "mode": "snapshot_cdc",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "dim_snap",
                    "snapshot_cdc_config": {
                        "source": "some.source.table",
                        "keys": ["id"],
                        "stored_as_scd_type": 1,
                    },
                },
            )
        ],
    )
    errors = validator.validate_cdc_fanin_compatibility([fg])
    assert errors == []
