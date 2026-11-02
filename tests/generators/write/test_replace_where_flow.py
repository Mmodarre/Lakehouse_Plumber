"""Tests for mode: replace_where on the streaming_table write action (#346).

A REPLACE WHERE flow is one ``@dp.table(..., replace_where=...)`` statement over
a batch read: Databricks defines the table and its flow together, so there is
no ``dp.create_streaming_table``, no flow decorator and no streaming read.
"""

import ast

import pytest

from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models import Action, ActionType

pytestmark = pytest.mark.unit

_PREDICATE = "order_date >= date_add(current_date(), -7)"


def _replace_where_action(
    name: str = "write_orders_recent",
    source: str | list[str] = "v_orders_source",
    table: str = "orders_recent",
    catalog: str = "cat",
    schema: str = "silver",
    predicate: str = _PREDICATE,
    **write_target_extra,
) -> Action:
    """Build a replace_where mode write action with a valid replace_where_config."""
    write_target = {
        "type": "streaming_table",
        "mode": "replace_where",
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "replace_where_config": {"replace_where": predicate},
        **write_target_extra,
    }

    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target=write_target,
    )


def _generate(action: Action, context: dict | None = None) -> str:
    return StreamingTableWriteGenerator().generate(
        action, {"expectations": [], **(context or {})}
    )


def _dp_table_kwargs(code: str) -> dict:
    """Literal keyword arguments of the single ``@dp.table(...)`` decorator."""
    tree = ast.parse(code)
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "table"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "dp"
    ]
    assert len(calls) == 1, code
    return {kw.arg: ast.literal_eval(kw.value) for kw in calls[0].keywords}


def test_emits_single_dp_table_with_replace_where():
    code = _generate(_replace_where_action())

    assert "@dp.table(" in code
    assert code.count("@dp.table(") == 1
    assert 'name="cat.silver.orders_recent"' in code
    assert f'replace_where="{_PREDICATE}"' in code
    assert "def orders_recent():" in code
    assert 'spark.read.table("v_orders_source")' in code
    assert "return df" in code
    for absent in ("create_streaming_table", "append_flow", "replace_flow"):
        assert absent not in code
    assert "readStream" not in code


def test_reset_allowed_defaults_false():
    kwargs = _dp_table_kwargs(_generate(_replace_where_action()))

    assert kwargs["table_properties"] == {"pipelines.reset.allowed": "false"}


def test_reset_default_keeps_other_table_properties():
    action = _replace_where_action(
        table_properties={"delta.enableChangeDataFeed": "true"}
    )

    kwargs = _dp_table_kwargs(_generate(action))

    assert kwargs["table_properties"] == {
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",
    }


def test_explicit_reset_true_wins():
    action = _replace_where_action(table_properties={"pipelines.reset.allowed": "true"})

    code = _generate(action)

    assert _dp_table_kwargs(code)["table_properties"] == {
        "pipelines.reset.allowed": "true"
    }
    assert code.count("pipelines.reset.allowed") == 1


def test_reset_key_case_insensitive_no_duplicate():
    action = _replace_where_action(table_properties={"Pipelines.Reset.Allowed": "true"})

    code = _generate(action)

    assert _dp_table_kwargs(code)["table_properties"] == {
        "Pipelines.Reset.Allowed": "true"
    }
    assert code.lower().count("pipelines.reset.allowed") == 1


def test_optional_kwargs_rendered():
    action = _replace_where_action(
        comment="Recent orders",
        spark_conf={"spark.sql.shuffle.partitions": "8"},
        partition_columns=["order_date"],
        cluster_columns=["region"],
        cluster_by_auto=True,
        path="/mnt/tables/orders_recent",
        table_schema="order_id BIGINT, order_date DATE",
        row_filter="ROW FILTER cat.sec.region_filter ON (region)",
        private=True,
    )

    code = _generate(action)
    kwargs = _dp_table_kwargs(code)

    assert kwargs == {
        "name": "cat.silver.orders_recent",
        "comment": "Recent orders",
        "table_properties": {"pipelines.reset.allowed": "false"},
        "spark_conf": {"spark.sql.shuffle.partitions": "8"},
        "partition_cols": ["order_date"],
        "cluster_by": ["region"],
        "cluster_by_auto": True,
        "path": "/mnt/tables/orders_recent",
        "schema": "order_id BIGINT, order_date DATE",
        "row_filter": "ROW FILTER cat.sec.region_filter ON (region)",
        "private": True,
        "replace_where": _PREDICATE,
    }
    assert 'schema="""order_id BIGINT, order_date DATE"""' in code


def test_unset_optional_kwargs_are_omitted():
    kwargs = _dp_table_kwargs(_generate(_replace_where_action()))

    assert set(kwargs) == {"name", "comment", "table_properties", "replace_where"}


def test_no_expectation_decorators():
    expectations = [
        {
            "name": "valid_id",
            "expression": "order_id IS NOT NULL",
            "failureAction": "fail",
        },
        {"name": "positive", "expression": "amount > 0", "failureAction": "drop"},
        {
            "name": "warn_only",
            "expression": "region IS NOT NULL",
            "failureAction": "warn",
        },
    ]

    code = _generate(_replace_where_action(), {"expectations": expectations})

    # Databricks rejects expectations on a REPLACE WHERE target
    # (UNSUPPORTED_FEATURE.REPLACE_WHERE_FLOW_WITH_EXPECTATIONS).
    assert "@dp.expect" not in code


def test_temporary_and_metadata_columns_are_never_emitted():
    code = _generate(_replace_where_action(temporary=True))

    assert "temporary=" not in code
    assert "withColumn" not in code


def test_description_fallback_names_the_replace_where_flow():
    code = _generate(_replace_where_action())

    assert '"""Replace-where flow to cat.silver.orders_recent"""' in code


def test_generated_code_compiles():
    action = _replace_where_action(
        table_properties={"delta.enableChangeDataFeed": "true"},
        cluster_columns=["region"],
        private=True,
    )

    ast.parse(_generate(action))


def test_one_element_list_source_renders_single_batch_read():
    code = _generate(_replace_where_action(source=["v_orders_source"]))

    assert 'spark.read.table("v_orders_source")' in code
    assert "readStream" not in code
    ast.parse(code)


@pytest.mark.parametrize("read_mode", [None, "batch", "stream"])
def test_consumer_read_mode_is_always_batch(read_mode):
    # The template always renders spark.read.table(...), so the read the
    # lineage check (VAL-070) sees is batch whatever readMode says.
    action = _replace_where_action().model_copy(update={"readMode": read_mode})

    assert StreamingTableWriteGenerator.consumer_read_mode(action) == "batch"
