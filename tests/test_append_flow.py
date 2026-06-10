"""Tests for append flow pattern in streaming tables."""

from pathlib import Path

from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models import Action, FlowGroup


def test_streaming_table_with_multiple_sources():
    """Test streaming table with multiple sources using append flow."""
    generator = StreamingTableWriteGenerator()

    action = Action(
        name="write_all_events",
        type="write",
        source=["v_orders", "v_returns", "v_cancellations"],
        write_target={
            "type": "streaming_table",
            "catalog": "silver_cat",
            "schema": "silver_sch",
            "table": "all_events",
            "create_table": True,
            "partition_columns": ["event_date"],
            "comment": "Unified events table",
        },
    )

    context = {"expectations": []}
    code = generator.generate(action, context)

    assert "dp.create_streaming_table(" in code
    assert 'name="silver_cat.silver_sch.all_events"' in code
    assert "@dp.append_flow(" in code
    assert 'target="silver_cat.silver_sch.all_events"' in code
    assert "def f_all_events_1():" in code
    assert "def f_all_events_2():" in code
    assert "def f_all_events_3():" in code
    assert 'spark.readStream.table("v_orders")' in code
    assert 'spark.readStream.table("v_returns")' in code
    assert 'spark.readStream.table("v_cancellations")' in code


def test_streaming_table_with_backfill():
    """Test streaming table with one-time flow (backfill)."""
    generator = StreamingTableWriteGenerator()

    action = Action(
        name="backfill_historical",
        type="write",
        source="v_historical_orders",
        once=True,
        readMode="batch",
        write_target={
            "type": "streaming_table",
            "catalog": "silver_cat",
            "schema": "silver_sch",
            "table": "events",
            "create_table": True,
        },
    )

    context = {"expectations": []}
    code = generator.generate(action, context)

    assert "once=True" in code
    assert 'spark.read.table("v_historical_orders")' in code
    assert "spark.readStream." not in code
    assert "Batch mode" in code


def test_streaming_table_cdc_mode():
    """Test streaming table in CDC mode."""
    generator = StreamingTableWriteGenerator()

    action = Action(
        name="write_customer_dimension",
        type="write",
        source="v_customer_changes",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "catalog": "silver_cat",
            "schema": "silver_sch",
            "table": "dim_customer",
            "create_table": True,
            "cdc_config": {
                "keys": ["customer_id"],
                "sequence_by": "_commit_timestamp",
                "scd_type": 2,
            },
        },
    )

    context = {"expectations": []}
    code = generator.generate(action, context)

    assert "dp.create_streaming_table(" in code
    assert 'name="silver_cat.silver_sch.dim_customer"' in code
    assert "dp.create_auto_cdc_flow(" in code
    assert 'target="silver_cat.silver_sch.dim_customer"' in code
    assert 'source="v_customer_changes"' in code
    assert 'keys=["customer_id"]' in code
    assert "stored_as_scd_type=2" in code
    assert "@dp.append_flow" not in code


def test_streaming_table_single_source():
    """Test streaming table with single source."""
    generator = StreamingTableWriteGenerator()

    action = Action(
        name="write_events",
        type="write",
        source="v_events",
        write_target={
            "type": "streaming_table",
            "catalog": "silver_cat",
            "schema": "silver_sch",
            "table": "events",
            "create_table": True,
        },
    )

    context = {"expectations": []}
    code = generator.generate(action, context)

    assert "dp.create_streaming_table(" in code
    assert "@dp.append_flow(" in code
    assert "def f_events():" in code
    assert 'spark.readStream.table("v_events")' in code


def test_source_list_validation():
    """Test that source can be a list in write actions."""
    from lhp.core.validators import ConfigValidator

    validator = ConfigValidator()

    action = Action(
        name="write_multi",
        type="write",
        source=["v_view1", "v_view2"],
        write_target={
            "type": "streaming_table",
            "catalog": "silver_cat",
            "schema": "silver_sch",
            "table": "multi_source",
            "create_table": True,
        },
    )

    errors = validator.validate_action(action, 0)
    assert len(errors) == 0

    action.source = "v_single"
    errors = validator.validate_action(action, 0)
    assert len(errors) == 0

    action.source = {"view": "v_test"}
    errors = validator.validate_action(action, 0)
    assert any("source must be a string or list" in e for e in errors)


def test_multiple_write_actions_same_table_mixed_once_flags():
    """Test multiple write actions targeting the same table with mixed once flags."""

    from lhp.core.coordination.layers import build_facade_orchestrator

    # Create actions with mixed once flags
    streaming_action = Action(
        name="write_lineitem_streaming",
        type="write",
        source="v_lineitem_processed",
        write_target={
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "lineitem",
            "create_table": True,
        },
    )

    backfill_action = Action(
        name="write_lineitem_backfill",
        type="write",
        source="v_lineitem_historical",
        once=True,
        readMode="batch",
        write_target={
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "lineitem",
            "create_table": False,
        },
    )

    orchestrator = build_facade_orchestrator(Path("."), enforce_version=False)
    actions = [streaming_action, backfill_action]
    target_table = "catalog.schema.lineitem"

    combined_action = orchestrator.generator.create_combined_write_action(
        actions, target_table
    )

    assert hasattr(combined_action, "_action_metadata")
    assert len(combined_action._action_metadata) == 2

    streaming_meta = combined_action._action_metadata[0]
    assert streaming_meta["action_name"] == "write_lineitem_streaming"
    assert streaming_meta["source_view"] == "v_lineitem_processed"
    assert streaming_meta["once"] is False
    assert streaming_meta["flow_name"] == "f_lineitem_streaming"

    backfill_meta = combined_action._action_metadata[1]
    assert backfill_meta["action_name"] == "write_lineitem_backfill"
    assert backfill_meta["source_view"] == "v_lineitem_historical"
    assert backfill_meta["once"] is True
    assert backfill_meta["flow_name"] == "f_lineitem_backfill"

    assert hasattr(combined_action, "_table_creator")
    assert combined_action._table_creator.name == "write_lineitem_streaming"

    generator = StreamingTableWriteGenerator()
    context = {"expectations": []}
    code = generator.generate(combined_action, context)

    assert "dp.create_streaming_table(" in code
    assert 'name="catalog.schema.lineitem"' in code
    assert "@dp.append_flow(" in code
    assert "def f_lineitem_streaming():" in code
    assert "def f_lineitem_backfill():" in code

    streaming_flow_section = code.split("def f_lineitem_streaming():")[0]
    streaming_append = streaming_flow_section.split("@dp.append_flow(")[-1]
    assert "once=True" not in streaming_append

    backfill_flow_section = code.split("def f_lineitem_backfill():")[0]
    backfill_append = backfill_flow_section.split("@dp.append_flow(")[-1]
    assert "once=True" in backfill_append

    assert 'spark.readStream.table("v_lineitem_processed")' in code
    assert 'spark.read.table("v_lineitem_historical")' in code


def test_table_creation_validation_multiple_creators():
    """Test that table creation validation catches multiple creators."""
    from lhp.core.validators import TableCreationValidator

    # Create two actions that both try to create the same table
    action1 = Action(
        name="write_events_1",
        type="write",
        source="v_events_1",
        write_target={
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "events",
            "create_table": True,
        },
    )

    action2 = Action(
        name="write_events_2",
        type="write",
        source="v_events_2",
        write_target={
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "events",
            "create_table": True,
        },
    )

    flowgroup = FlowGroup(
        pipeline="test_pipeline", flowgroup="test_flowgroup", actions=[action1, action2]
    )

    try:
        TableCreationValidator().validate([flowgroup])
        raise AssertionError(
            "Expected LHPError to be raised for multiple table creators"
        )
    except Exception as e:
        error_str = str(e)
        assert (
            "multiple creators" in error_str.lower()
            or "Multiple table creators" in error_str
        )
        assert "catalog.schema.events" in error_str


def test_table_creation_validation_no_creators():
    """Test that table creation validation catches tables with no creators."""
    from lhp.core.validators import TableCreationValidator

    action1 = Action(
        name="write_events_1",
        type="write",
        source="v_events_1",
        write_target={
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "events",
            "create_table": False,
        },
    )

    action2 = Action(
        name="write_events_2",
        type="write",
        source="v_events_2",
        write_target={
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "events",
            "create_table": False,
        },
    )

    flowgroup = FlowGroup(
        pipeline="test_pipeline", flowgroup="test_flowgroup", actions=[action1, action2]
    )

    errors = TableCreationValidator().validate([flowgroup])

    assert len(errors) == 1
    assert "no creator" in errors[0].lower()
    assert "catalog.schema.events" in errors[0]


def test_backward_compatibility_single_action():
    """Test that single write actions still work correctly (backward compatibility)."""
    generator = StreamingTableWriteGenerator()

    action = Action(
        name="write_single_events",
        type="write",
        source="v_events",
        once=True,
        readMode="batch",
        write_target={
            "type": "streaming_table",
            "catalog": "silver_cat",
            "schema": "silver_sch",
            "table": "events",
            "create_table": True,
        },
    )

    context = {"expectations": []}
    code = generator.generate(action, context)

    # Should still work as before
    assert "dp.create_streaming_table(" in code
    assert "@dp.append_flow(" in code
    assert "once=True" in code
    assert "def f_single_events():" in code
    assert 'spark.read.table("v_events")' in code


def test_orchestrator_preserves_table_creation_logic():
    """Test that orchestrator preserves correct table creation logic from validation."""

    from lhp.core.coordination.layers import build_facade_orchestrator

    orchestrator = build_facade_orchestrator(Path("."), enforce_version=False)

    action1 = Action(
        name="write_events_append",
        type="write",
        source="v_events_new",
        write_target={
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "events",
            "create_table": False,
        },
    )

    action2 = Action(
        name="write_events_creator",
        type="write",
        source="v_events_base",
        write_target={
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "events",
            "create_table": True,
        },
    )

    actions = [action1, action2]
    target_table = "catalog.schema.events"

    combined_action = orchestrator.generator.create_combined_write_action(
        actions, target_table
    )

    assert combined_action._table_creator.name == "write_events_creator"
    assert combined_action.write_target.get("create_table") is True

    generator = StreamingTableWriteGenerator()
    context = {"expectations": []}
    code = generator.generate(combined_action, context)

    assert "dp.create_streaming_table(" in code
