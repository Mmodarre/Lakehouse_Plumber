"""Tests for mode: replace for the streaming_table write action.

Covers:
- Basic replace flow generation (streaming table + replace_flow decorator).
- replace_config emits replace_using and sequence_by.
- create_table forced True even when YAML sets create_table: false.
- Source read as a STREAM (readStream), not batch.
- Data quality expectations applied when present.
"""

import pytest

from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models import Action, ActionType


def _replace_action(
    name: str,
    source: str,
    table: str = "orders_current",
    catalog: str = "cat",
    schema: str = "silver",
    create_table: bool = True,
    replace_using: list | None = None,
    sequence_by: str | None = None,
) -> Action:
    """Build a replace mode write action with baseline valid replace_config."""
    if replace_using is None:
        replace_using = ["order_id"]
    if sequence_by is None:
        sequence_by = "updated_at"

    replace_config = {
        "replace_using": replace_using,
        "sequence_by": sequence_by,
    }

    write_target = {
        "type": "streaming_table",
        "mode": "replace",
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "create_table": create_table,
        "replace_config": replace_config,
    }

    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target=write_target,
    )


def test_replace_flow_renders_streaming_table_and_replace_flow():
    """Replace mode emits both dp.create_streaming_table(...) and @dp.replace_flow(...)."""
    action = _replace_action(
        "write_orders_current",
        source="v_order_updates",
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert "dp.create_streaming_table(" in code
    assert "dp.replace_flow(" in code


def test_replace_flow_emits_replace_using_and_sequence_by():
    """Replace flow emits replace_using=[...] and sequence_by=... from replace_config."""
    action = _replace_action(
        "write_orders_current",
        source="v_order_updates",
        replace_using=["order_id"],
        sequence_by="updated_at",
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert 'replace_using=["order_id"]' in code
    assert 'sequence_by="updated_at"' in code


def test_replace_flow_forces_table_creation_when_create_table_false():
    """Replace mode forces create_table=True even when YAML sets create_table: false."""
    action = _replace_action(
        "write_orders_current",
        source="v_order_updates",
        create_table=False,
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert "dp.create_streaming_table(" in code


def test_replace_flow_reads_source_as_stream():
    """Replace flow reads source as spark.readStream (not batch)."""
    action = _replace_action(
        "write_orders_current",
        source="v_order_updates",
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert 'spark.readStream.table("v_order_updates")' in code
    assert "spark.read.table(" not in code


def test_replace_flow_includes_correct_target_name():
    """Replace flow targets the correct full table name (catalog.schema.table)."""
    action = _replace_action(
        "write_orders_current",
        source="v_order_updates",
        catalog="cat",
        schema="silver",
        table="orders_current",
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert 'name="cat.silver.orders_current"' in code
    assert 'target="cat.silver.orders_current"' in code


def test_replace_flow_emits_flow_name():
    """Replace flow uses the generated flow name (f_<action_name_without_write_>)."""
    action = _replace_action(
        "write_orders_current",
        source="v_order_updates",
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert 'name="f_orders_current"' in code
    assert "def f_orders_current():" in code


def test_replace_flow_returns_dataframe():
    """Replace flow function returns the dataframe."""
    action = _replace_action(
        "write_orders_current",
        source="v_order_updates",
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert "return df" in code


def test_replace_flow_applies_expectations_when_present():
    """Replace flow applies expectations when passed in context."""
    action = _replace_action(
        "write_orders_current",
        source="v_order_updates",
    )

    expectations = [
        {"expression": "col('order_id').isNotNull()"},
        {"expression": "col('amount') > 0"},
    ]

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": expectations})

    # Should have at least one @dp.expect_* decorator
    assert any(f"@dp.expect_{x}" in code for x in ["all", "all_or_drop", "all_or_fail"])


def test_replace_flow_multiple_replace_using_columns():
    """Replace flow handles multiple columns in replace_using."""
    action = _replace_action(
        "write_orders_multi",
        source="v_order_updates",
        table="orders_multi",
        replace_using=["order_id", "customer_id"],
        sequence_by="modified_timestamp",
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert 'replace_using=["order_id", "customer_id"]' in code
    assert 'sequence_by="modified_timestamp"' in code


def test_replace_flow_does_not_emit_once_parameter():
    """@dp.replace_flow has no 'once' parameter, so it is never emitted (even if the
    action sets once: true — a REPLACE USING flow is continuous, not a one-shot backfill)."""
    write_target = {
        "type": "streaming_table",
        "mode": "replace",
        "catalog": "cat",
        "schema": "silver",
        "table": "orders_current",
        "create_table": True,
        "replace_config": {
            "replace_using": ["order_id"],
            "sequence_by": "updated_at",
        },
    }

    action = Action(
        name="write_orders_current",
        type=ActionType.WRITE,
        source="v_order_updates",
        write_target=write_target,
        once=True,
    )

    generator = StreamingTableWriteGenerator()
    code = generator.generate(action, {"expectations": []})

    assert "dp.replace_flow(" in code
    assert "once=True" not in code
