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
    
    # Check that append_flow decorators are created for each source
    assert "@dlt.append_flow(" in code
    assert 'target="silver.all_events"' in code
    assert "def f_all_events_1():" in code
    assert "def f_all_events_2():" in code
    assert "def f_all_events_3():" in code
    
    # Check that each flow reads from the correct source
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