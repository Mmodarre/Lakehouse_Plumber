"""Tests for append flow pattern in streaming tables."""

import pytest
from pathlib import Path
from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models.config import Action, FlowGroup


def test_streaming_table_with_multiple_sources():
    """Test streaming table with multiple sources using append flow."""
    generator = StreamingTableWriteGenerator()
    
    # Create action with multiple sources
    action = Action(
        name="write_all_events",
        type="write",
        source=["v_orders", "v_returns", "v_cancellations"],
        write_target={
            "type": "streaming_table",
            "database": "silver",
            "table": "all_events",
            "create_table": True,  # ← Add explicit table creation flag
            "partition_columns": ["event_date"],
            "comment": "Unified events table"
        }
    )
    
    context = {"expectations": []}
    code = generator.generate(action, context)
    
    # Check that create_streaming_table is used
    assert "dlt.create_streaming_table(" in code
    assert 'name="silver.all_events"' in code
    
    # Check that multiple append_flows are created for single action with multiple sources
    assert "@dlt.append_flow(" in code
    assert 'target="silver.all_events"' in code
    assert "def f_all_events_1():" in code
    assert "def f_all_events_2():" in code
    assert "def f_all_events_3():" in code
    
    # Each append flow should read from its respective source
    assert 'spark.readStream.table("v_orders")' in code
    assert 'spark.readStream.table("v_returns")' in code
    assert 'spark.readStream.table("v_cancellations")' in code


def test_streaming_table_with_backfill():
    """Test streaming table with one-time flow (backfill)."""
    generator = StreamingTableWriteGenerator()
    
    # Create action with once=True for backfill
    action = Action(
        name="backfill_historical",
        type="write",
        source="v_historical_orders",
        once=True,
        write_target={
            "type": "streaming_table",
            "database": "silver",
            "table": "events",
            "create_table": True  # ← Add explicit table creation flag
        }
    )
    
    context = {"expectations": []}
    code = generator.generate(action, context)
    
    # Check that append_flow has once=True
    assert "once=True" in code
    
    # Check that it uses spark.read.table() instead of spark.readStream.table()
    assert 'spark.read.table("v_historical_orders")' in code
    assert 'spark.readStream.' not in code
    
    # Check comment indicates backfill
    assert "One-time flow (backfill)" in code


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
            "database": "silver",
            "table": "dim_customer",
            "create_table": True,  # ← Add explicit table creation flag
            "cdc_config": {
                "keys": ["customer_id"],
                "sequence_by": "_commit_timestamp",
                "scd_type": 2
            }
        }
    )
    
    context = {"expectations": []}
    code = generator.generate(action, context)
    
    # Check that create_streaming_table is used first
    assert "dlt.create_streaming_table(" in code
    assert 'name="silver.dim_customer"' in code
    
    # Check that create_auto_cdc_flow is used
    assert "dlt.create_auto_cdc_flow(" in code
    assert 'target="silver.dim_customer"' in code
    assert 'source="v_customer_changes"' in code
    assert 'keys=["customer_id"]' in code
    assert 'stored_as_scd_type=2' in code
    
    # Should not have append_flow decorator
    assert "@dlt.append_flow" not in code


def test_streaming_table_single_source():
    """Test streaming table with single source."""
    generator = StreamingTableWriteGenerator()
    
    action = Action(
        name="write_events",
        type="write",
        source="v_events",
        write_target={
            "type": "streaming_table",
            "database": "silver",
            "table": "events",
            "create_table": True  # ← Add explicit table creation flag
        }
    )
    
    context = {"expectations": []}
    code = generator.generate(action, context)
    
    # Check basic structure
    assert "dlt.create_streaming_table(" in code
    assert "@dlt.append_flow(" in code
    assert "def f_events():" in code
    assert 'spark.readStream.table("v_events")' in code


def test_source_list_validation():
    """Test that source can be a list in write actions."""
    from lhp.core.validator import ConfigValidator
    
    validator = ConfigValidator()
    
    # Valid: source as list
    action = Action(
        name="write_multi",
        type="write",
        source=["v_view1", "v_view2"],
        write_target={
            "type": "streaming_table",
            "database": "silver",
            "table": "multi_source",
            "create_table": True  # ← Add explicit table creation flag
        }
    )
    
    errors = validator.validate_action(action, 0)
    assert len(errors) == 0
    
    # Valid: source as string
    action.source = "v_single"
    errors = validator.validate_action(action, 0)
    assert len(errors) == 0
    
    # Invalid: source as dict (not allowed for write)
    action.source = {"view": "v_test"}
    errors = validator.validate_action(action, 0)
    assert any("source must be a string or list" in e for e in errors)


def test_multiple_write_actions_same_table_mixed_once_flags():
    """Test multiple write actions targeting the same table with mixed once flags."""
    from lhp.core.orchestrator import ActionOrchestrator
    from pathlib import Path
    
    # Create actions with mixed once flags
    streaming_action = Action(
        name="write_lineitem_streaming",
        type="write",
        source="v_lineitem_processed",
        write_target={
            "type": "streaming_table",
            "database": "catalog.schema",
            "table": "lineitem",
            "create_table": True
        }
    )
    
    backfill_action = Action(
        name="write_lineitem_backfill",
        type="write",
        source="v_lineitem_historical",
        once=True,  # One-time backfill
        write_target={
            "type": "streaming_table",
            "database": "catalog.schema", 
            "table": "lineitem",
            "create_table": False  # Don't create table again
        }
    )
    
    # Test orchestrator combination logic
    orchestrator = ActionOrchestrator(Path('.'))
    actions = [streaming_action, backfill_action]
    target_table = "catalog.schema.lineitem"
    
    combined_action = orchestrator._create_combined_write_action(actions, target_table)
    
    # Verify individual action metadata is preserved
    assert hasattr(combined_action, '_action_metadata')
    assert len(combined_action._action_metadata) == 2
    
    # Check streaming action metadata
    streaming_meta = combined_action._action_metadata[0]
    assert streaming_meta["action_name"] == "write_lineitem_streaming"
    assert streaming_meta["source_view"] == "v_lineitem_processed"
    assert streaming_meta["once"] is False
    assert streaming_meta["flow_name"] == "f_lineitem_streaming"
    
    # Check backfill action metadata
    backfill_meta = combined_action._action_metadata[1]
    assert backfill_meta["action_name"] == "write_lineitem_backfill"
    assert backfill_meta["source_view"] == "v_lineitem_historical"
    assert backfill_meta["once"] is True
    assert backfill_meta["flow_name"] == "f_lineitem_backfill"
    
    # Verify table creator is correctly identified
    assert hasattr(combined_action, '_table_creator')
    assert combined_action._table_creator.name == "write_lineitem_streaming"  # Has create_table=True
    
    # Test code generation
    generator = StreamingTableWriteGenerator()
    context = {"expectations": []}
    code = generator.generate(combined_action, context)
    
    # Verify streaming table is created
    assert "dlt.create_streaming_table(" in code
    assert 'name="catalog.schema.lineitem"' in code
    
    # Verify both append flows are generated with correct once flags
    assert "@dlt.append_flow(" in code
    assert "def f_lineitem_streaming():" in code
    assert "def f_lineitem_backfill():" in code
    
    # Check that streaming flow doesn't have once=True
    streaming_flow_section = code.split("def f_lineitem_streaming():")[0]
    streaming_append = streaming_flow_section.split("@dlt.append_flow(")[-1]
    assert "once=True" not in streaming_append
    
    # Check that backfill flow has once=True
    backfill_flow_section = code.split("def f_lineitem_backfill():")[0]
    backfill_append = backfill_flow_section.split("@dlt.append_flow(")[-1]
    assert "once=True" in backfill_append
    
    # Verify correct read methods
    assert 'spark.readStream.table("v_lineitem_processed")' in code  # Streaming
    assert 'spark.read.table("v_lineitem_historical")' in code  # Batch for once=True


def test_table_creation_validation_multiple_creators():
    """Test that table creation validation catches multiple creators."""
    from lhp.core.validator import ConfigValidator
    from lhp.models.config import FlowGroup
    
    validator = ConfigValidator()
    
    # Create two actions that both try to create the same table
    action1 = Action(
        name="write_events_1",
        type="write",
        source="v_events_1",
        write_target={
            "type": "streaming_table",
            "database": "catalog.schema",
            "table": "events",
            "create_table": True  # ← First creator
        }
    )
    
    action2 = Action(
        name="write_events_2", 
        type="write",
        source="v_events_2",
        write_target={
            "type": "streaming_table",
            "database": "catalog.schema",
            "table": "events",
            "create_table": True  # ← Second creator (should cause error)
        }
    )
    
    flowgroup = FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        actions=[action1, action2]
    )
    
    # Validate table creation rules - should raise LHPError for multiple creators
    try:
        errors = validator.validate_table_creation_rules([flowgroup])
        # If we get here, validation unexpectedly passed - this is an error
        assert False, "Expected LHPError to be raised for multiple table creators"
    except Exception as e:
        # Handle LHPError by converting to string (like the orchestrator does)
        error_str = str(e)
        assert "multiple creators" in error_str.lower() or "Multiple table creators" in error_str
        assert "catalog.schema.events" in error_str


def test_table_creation_validation_no_creators():
    """Test that table creation validation catches tables with no creators."""
    from lhp.core.validator import ConfigValidator
    from lhp.models.config import FlowGroup
    
    validator = ConfigValidator()
    
    # Create two actions that both have create_table=False
    action1 = Action(
        name="write_events_1",
        type="write", 
        source="v_events_1",
        write_target={
            "type": "streaming_table",
            "database": "catalog.schema",
            "table": "events",
            "create_table": False  # ← No creator
        }
    )
    
    action2 = Action(
        name="write_events_2",
        type="write",
        source="v_events_2", 
        write_target={
            "type": "streaming_table",
            "database": "catalog.schema",
            "table": "events",
            "create_table": False  # ← No creator
        }
    )
    
    flowgroup = FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        actions=[action1, action2]
    )
    
    # Validate table creation rules - should return errors for no creators
    errors = validator.validate_table_creation_rules([flowgroup])
    
    # Should detect no creators (this case returns errors instead of raising exception)
    assert len(errors) == 1
    assert "no creator" in errors[0].lower()
    assert "catalog.schema.events" in errors[0]


def test_backward_compatibility_single_action():
    """Test that single write actions still work correctly (backward compatibility)."""
    generator = StreamingTableWriteGenerator()
    
    # Single action (existing functionality should still work)
    action = Action(
        name="write_single_events",
        type="write",
        source="v_events",
        once=True,
        write_target={
            "type": "streaming_table",
            "database": "silver",
            "table": "events",
            "create_table": True
        }
    )
    
    context = {"expectations": []}
    code = generator.generate(action, context)
    
    # Should still work as before
    assert "dlt.create_streaming_table(" in code
    assert "@dlt.append_flow(" in code
    assert "once=True" in code
    assert "def f_single_events():" in code
    assert 'spark.read.table("v_events")' in code  # Batch for once=True


def test_orchestrator_preserves_table_creation_logic():
    """Test that orchestrator preserves correct table creation logic from validation."""
    from lhp.core.orchestrator import ActionOrchestrator
    from pathlib import Path
    
    orchestrator = ActionOrchestrator(Path('.'))
    
    # Test scenario: first action has create_table=False, second has create_table=True
    action1 = Action(
        name="write_events_append",
        type="write",
        source="v_events_new",
        write_target={
            "type": "streaming_table",
            "database": "catalog.schema",
            "table": "events",
            "create_table": False  # Should not be the table creator
        }
    )
    
    action2 = Action(
        name="write_events_creator",
        type="write",
        source="v_events_base",
        write_target={
            "type": "streaming_table",
            "database": "catalog.schema", 
            "table": "events",
            "create_table": True  # Should be the table creator
        }
    )
    
    actions = [action1, action2]
    target_table = "catalog.schema.events"
    
    combined_action = orchestrator._create_combined_write_action(actions, target_table)
    
    # Verify that the table creator is correctly identified (action2, not action1)
    assert combined_action._table_creator.name == "write_events_creator"
    assert combined_action.write_target.get("create_table") is True
    
    # Test code generation uses the correct table creator config
    generator = StreamingTableWriteGenerator()
    context = {"expectations": []}
    code = generator.generate(combined_action, context)
    
    # Should create the table because table creator has create_table=True
    assert "dlt.create_streaming_table(" in code 