"""Tests for combining cluster_columns with cluster_by_auto (issue #282).

Databricks accepts both kwargs together: cluster_by seeds the initial clustering
keys and cluster_by_auto lets the platform change them based on the workload.
Every table-creating write target must therefore emit both on the same call.
"""

import ast

import pytest

from lhp.generators.write import (
    MaterializedViewWriteGenerator,
    StreamingTableWriteGenerator,
)
from lhp.models import Action, ActionType


def _table_call_kwargs(code: str, func_name: str) -> dict:
    """Return the literal keyword arguments of the single ``dp.<func_name>`` call."""
    calls = [
        node
        for node in ast.walk(ast.parse(code))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == func_name
    ]
    assert len(calls) == 1, f"expected one dp.{func_name} call, got {len(calls)}"
    return {kw.arg: ast.literal_eval(kw.value) for kw in calls[0].keywords}


@pytest.mark.parametrize(
    ("generator_cls", "source", "mode_config", "func_name"),
    [
        pytest.param(
            StreamingTableWriteGenerator,
            "v_orders",
            {"create_table": True},
            "create_streaming_table",
            id="streaming_table_standard",
        ),
        pytest.param(
            StreamingTableWriteGenerator,
            "v_orders",
            {
                "mode": "cdc",
                "create_table": True,
                "cdc_config": {"keys": ["order_id"], "sequence_by": "updated_at"},
            },
            "create_streaming_table",
            id="streaming_table_cdc",
        ),
        pytest.param(
            StreamingTableWriteGenerator,
            None,
            {
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source": "raw.order_snapshots",
                    "keys": ["order_id"],
                },
            },
            "create_streaming_table",
            id="streaming_table_snapshot_cdc",
        ),
        pytest.param(
            StreamingTableWriteGenerator,
            "v_orders",
            {
                "mode": "replace",
                "replace_config": {
                    "replace_using": ["order_id"],
                    "sequence_by": "updated_at",
                },
            },
            "create_streaming_table",
            id="streaming_table_replace",
        ),
        pytest.param(
            MaterializedViewWriteGenerator,
            "v_orders",
            {"type": "materialized_view", "sql": "SELECT * FROM silver.orders"},
            "materialized_view",
            id="materialized_view",
        ),
    ],
)
def test_cluster_columns_and_cluster_by_auto_both_emitted(
    generator_cls, source, mode_config, func_name
):
    action = Action(
        name="write_orders",
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "streaming_table",
            "catalog": "gold_cat",
            "schema": "gold_sch",
            "table": "orders",
            "cluster_columns": ["order_id", "order_date"],
            "cluster_by_auto": True,
            **mode_config,
        },
    )

    code = generator_cls().generate(action, {"expectations": []})

    kwargs = _table_call_kwargs(code, func_name)
    assert kwargs["cluster_by"] == ["order_id", "order_date"]
    assert kwargs["cluster_by_auto"] is True
