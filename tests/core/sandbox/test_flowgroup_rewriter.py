"""Tests for the structured-model rewrite pass (``_flowgroup_rewriter.py``).

The pass must rename ONLY the structured table-reference sites whose refs
canonically match the rename set, through the matcher
(:func:`match_renamed_table`) and the choke point
(:func:`rename_parts`): part count and catalog/schema spelling preserved,
table leaf replaced. ``depends_on``, out-of-scope refs, bare view names,
and ambiguous 2-part refs must pass through byte-identical, and the input
flowgroup must never be mutated.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from lhp.core.sandbox import (
    SandboxTableRenames,
    TableRenameStrategy,
    build_sandbox_table_renames,
)
from lhp.core.sandbox._flowgroup_rewriter import rewrite_flowgroup_tables
from lhp.models import Action, ActionType, FlowGroup, WriteTarget


def _table_write_action(
    name: str, source: str, write_type: str, catalog: str, schema: str, table: str
) -> Action:
    """A streaming_table / materialized_view WRITE to catalog.schema.table."""
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": write_type,
            "catalog": catalog,
            "schema": schema,
            "table": table,
        },
    )


def _delta_sink_action(name: str, source: str, table_name: str) -> Action:
    """A delta-sink WRITE whose destination lives in ``options.tableName``."""
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "sink",
            "sink_type": "delta",
            "options": {"tableName": table_name},
        },
    )


def _producer_flowgroups() -> list[FlowGroup]:
    """In-scope producers: the rename set the rewriter operates against.

    - ST -> ``dev.bronze.RawOrders`` (CamelCase leaf: spelling check).
    - MV -> ``dev.silver.orders``.
    - delta sink -> 2-part ``stg.events`` (no catalog in ``tableName``).
    """
    return [
        FlowGroup(
            pipeline="p_ingest",
            flowgroup="fg_ingest",
            actions=[
                _table_write_action(
                    "wr_raw", "v_raw", "streaming_table", "dev", "bronze", "RawOrders"
                ),
                _delta_sink_action("wr_sink", "v_sink", "stg.events"),
            ],
        ),
        FlowGroup(
            pipeline="p_curate",
            flowgroup="fg_curate",
            actions=[
                _table_write_action(
                    "wr_orders",
                    "v_orders",
                    "materialized_view",
                    "dev",
                    "silver",
                    "orders",
                ),
            ],
        ),
    ]


def _strategy() -> TableRenameStrategy:
    return TableRenameStrategy(namespace="alice", table_pattern="{namespace}_{table}")


def _renames() -> SandboxTableRenames:
    return build_sandbox_table_renames(_producer_flowgroups(), _strategy())


def _fg(*actions: Action) -> FlowGroup:
    return FlowGroup(
        pipeline="p_consume", flowgroup="fg_consume", actions=list(actions)
    )


def _wt_field(write_target: Any, key: str) -> Optional[Any]:
    """Read a write_target field from either the model or the dict form."""
    if isinstance(write_target, dict):
        return write_target.get(key)
    return getattr(write_target, key)


@pytest.mark.unit
class TestWriteTargetRewrite:
    def test_streaming_table_model_form_renames_leaf_only(self):
        """WriteTarget MODEL form: table leaf renamed, catalog/schema intact."""
        action = Action(
            name="wr_raw",
            type=ActionType.WRITE,
            source="v_raw",
            write_target=WriteTarget(
                type="streaming_table",
                catalog="dev",
                schema="bronze",
                table="RawOrders",
            ),
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        write_target = result.actions[0].write_target
        assert _wt_field(write_target, "catalog") == "dev"
        assert _wt_field(write_target, "schema") == "bronze"
        assert _wt_field(write_target, "table") == "alice_RawOrders"

    def test_materialized_view_dict_form_renames_leaf_only(self):
        """Dict-form write_target: renamed, and STAYS a dict after round-trip
        (grouping reads it with ``.get``)."""
        action = _table_write_action(
            "wr_orders", "v_orders", "materialized_view", "dev", "silver", "orders"
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        write_target = result.actions[0].write_target
        assert isinstance(write_target, dict)
        assert write_target["catalog"] == "dev"
        assert write_target["schema"] == "silver"
        assert write_target["table"] == "alice_orders"

    def test_out_of_scope_write_target_untouched(self):
        action = _table_write_action(
            "wr_gold", "v_gold", "materialized_view", "dev", "gold", "summary"
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert _wt_field(result.actions[0].write_target, "table") == "summary"


@pytest.mark.unit
class TestDeltaSinkRewrite:
    def test_delta_sink_table_name_renamed(self):
        action = _delta_sink_action("wr_sink", "v_sink", "stg.events")

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        options = _wt_field(result.actions[0].write_target, "options")
        assert options["tableName"] == "stg.alice_events"

    def test_non_delta_sink_table_name_like_option_untouched(self):
        """Only ``sink_type: delta`` has table identity; a kafka sink with a
        tableName-spelled option must pass through untouched."""
        action = Action(
            name="wr_kafka",
            type=ActionType.WRITE,
            source="v_kafka",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "options": {"tableName": "stg.events"},
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        options = _wt_field(result.actions[0].write_target, "options")
        assert options["tableName"] == "stg.events"


@pytest.mark.unit
class TestDeltaLoadRewrite:
    def test_delta_load_source_triple_renamed(self):
        action = Action(
            name="ld_orders",
            type=ActionType.LOAD,
            target="v_orders_in",
            source={
                "type": "delta",
                "catalog": "dev",
                "schema": "silver",
                "table": "orders",
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        source = result.actions[0].source
        assert source["catalog"] == "dev"
        assert source["schema"] == "silver"
        assert source["table"] == "alice_orders"

    def test_delta_load_out_of_scope_untouched(self):
        action = Action(
            name="ld_ext",
            type=ActionType.LOAD,
            target="v_ext",
            source={
                "type": "delta",
                "catalog": "prod",
                "schema": "external",
                "table": "customers",
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].source["table"] == "customers"

    def test_non_delta_load_source_untouched(self):
        """Cloudfiles/jdbc-style sources have no table identity to rewrite."""
        action = Action(
            name="ld_files",
            type=ActionType.LOAD,
            target="v_files",
            source={
                "type": "cloudfiles",
                "path": "/mnt/landing/orders",
                "format": "json",
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].source == {
            "type": "cloudfiles",
            "path": "/mnt/landing/orders",
            "format": "json",
        }


@pytest.mark.unit
class TestSourceRefRewrite:
    def test_str_source_renamed_preserving_part_count(self):
        """A 2-part read of a 3-part producer stays 2-part: leaf-only rename."""
        action = Action(
            name="tr_orders",
            type=ActionType.TRANSFORM,
            transform_type="sql",
            source="silver.orders",
            target="v_orders_enriched",
            sql="SELECT * FROM silver.orders",
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].source == "silver.alice_orders"

    def test_list_source_rewrites_only_in_scope_entries(self):
        """[in-scope ref, out-of-scope ref, bare view name] -> only the first
        is renamed; bare view names (1-part) can NEVER match the index."""
        action = Action(
            name="tr_join",
            type=ActionType.TRANSFORM,
            transform_type="sql",
            source=["dev.silver.orders", "prod.external.customers", "v_lookup"],
            target="v_joined",
            sql="SELECT 1",
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].source == [
            "dev.silver.alice_orders",
            "prod.external.customers",
            "v_lookup",
        ]

    def test_bare_view_name_matching_a_produced_leaf_never_renamed(self):
        """Even a bare name spelled exactly like a produced table's leaf is a
        VIEW reference, not a table ref: 1-part refs never match."""
        action = Action(
            name="wr_orders",
            type=ActionType.WRITE,
            source="orders",
            write_target={
                "type": "streaming_table",
                "catalog": "dev",
                "schema": "gold",
                "table": "facts",
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].source == "orders"

    def test_ambiguous_two_part_ref_untouched(self):
        """schema.table produced in MULTIPLE catalogs -> no match -> no rename."""
        producers = [
            FlowGroup(
                pipeline="p_a",
                flowgroup="fg_a",
                actions=[
                    _table_write_action(
                        "wr_a", "v_a", "streaming_table", "dev", "silver", "orders"
                    )
                ],
            ),
            FlowGroup(
                pipeline="p_b",
                flowgroup="fg_b",
                actions=[
                    _table_write_action(
                        "wr_b", "v_b", "streaming_table", "prod", "silver", "orders"
                    )
                ],
            ),
        ]
        renames = build_sandbox_table_renames(producers, _strategy())
        action = Action(
            name="tr_amb",
            type=ActionType.TRANSFORM,
            transform_type="sql",
            source="silver.orders",
            target="v_amb",
            sql="SELECT 1",
        )

        result = rewrite_flowgroup_tables(_fg(action), renames)

        assert result.actions[0].source == "silver.orders"

    def test_original_spelling_preserved(self):
        """Matching is canonical; the rewrite keeps each site's spelling:
        CamelCase survives in the leaf, backticks are re-emitted."""
        camel = Action(
            name="tr_camel",
            type=ActionType.TRANSFORM,
            transform_type="sql",
            source="DEV.Bronze.RawOrders",
            target="v_camel",
            sql="SELECT 1",
        )
        ticked = Action(
            name="tr_ticked",
            type=ActionType.TRANSFORM,
            transform_type="sql",
            source="`dev`.`silver`.`orders`",
            target="v_ticked",
            sql="SELECT 1",
        )

        result = rewrite_flowgroup_tables(_fg(camel, ticked), _renames())

        assert result.actions[0].source == "DEV.Bronze.alice_RawOrders"
        assert result.actions[1].source == "`dev`.`silver`.`alice_orders`"


@pytest.mark.unit
class TestTestActionRewrite:
    def test_source_reference_and_lookup_table_renamed(self):
        action = Action(
            name="t_ri",
            type=ActionType.TEST,
            test_type="referential_integrity",
            source="dev.silver.orders",
            reference="dev.bronze.RawOrders",
            lookup_table="stg.events",
            source_columns=["order_id"],
            reference_columns=["order_id"],
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        rewritten = result.actions[0]
        assert rewritten.source == "dev.silver.alice_orders"
        assert rewritten.reference == "dev.bronze.alice_RawOrders"
        assert rewritten.lookup_table == "stg.alice_events"

    def test_out_of_scope_test_refs_untouched(self):
        action = Action(
            name="t_ext",
            type=ActionType.TEST,
            test_type="referential_integrity",
            source="prod.external.orders",
            reference="prod.external.customers",
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].source == "prod.external.orders"
        assert result.actions[0].reference == "prod.external.customers"


@pytest.mark.unit
class TestSnapshotCdcRewrite:
    def test_snapshot_source_renamed_dict_form(self):
        """Dict-form write_target carrying snapshot_cdc_config.source.

        (No model form exists: WriteTarget has no snapshot_cdc_config field,
        so snapshot configs only ever ride dict-form write_targets.)
        """
        action = Action(
            name="wr_snap",
            type=ActionType.WRITE,
            source="v_snap",
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "catalog": "dev",
                "schema": "gold",
                "table": "orders_scd",
                "snapshot_cdc_config": {
                    "source": "dev.silver.orders",
                    "keys": ["order_id"],
                },
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        write_target = result.actions[0].write_target
        assert isinstance(write_target, dict)
        assert (
            write_target["snapshot_cdc_config"]["source"] == "dev.silver.alice_orders"
        )
        # The other snapshot keys pass through untouched.
        assert write_target["snapshot_cdc_config"]["keys"] == ["order_id"]

    def test_snapshot_source_function_untouched(self):
        """source_function points at a FILE, not a table — never rewritten."""
        snapshot_config = {
            "source_function": {
                "file": "py_functions/snap.py",
                "function": "next_snapshot",
            },
            "keys": ["id"],
        }
        action = Action(
            name="wr_snapfn",
            type=ActionType.WRITE,
            source="v_snapfn",
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "catalog": "dev",
                "schema": "gold",
                "table": "scd",
                "snapshot_cdc_config": snapshot_config,
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        rewritten = result.actions[0].write_target["snapshot_cdc_config"]
        assert rewritten["source_function"] == snapshot_config["source_function"]


@pytest.mark.unit
class TestParameterBindingRewrite:
    """Table refs passed as YAML parameters into user Python code.

    Three containers feed generated reads (``spark.read.table(param)``)
    that the per-site rewrites never reach — kept in lock-step with
    :mod:`lhp.core.dependencies._binding_rules`: python-transform
    ``action.parameters``, python-load ``source.parameters``, and
    snapshot-CDC ``source_function.parameters``. Only values that
    canonically match the rename set change.
    """

    def test_snapshot_source_function_parameters_value_renamed(self):
        """A source_function.parameters value matching the rename set is
        renamed (leaf only, spelling preserved); a non-matching value in the
        same dict is untouched."""
        action = Action(
            name="wr_snapfn",
            type=ActionType.WRITE,
            source="v_snapfn",
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "catalog": "dev",
                "schema": "gold",
                "table": "scd",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": "py_functions/snap.py",
                        "function": "next_snapshot",
                        "parameters": {
                            "source_table": "dev.silver.orders",
                            "other_table": "prod.external.customers",
                        },
                    },
                    "keys": ["id"],
                },
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        source_function = result.actions[0].write_target["snapshot_cdc_config"][
            "source_function"
        ]
        params = source_function["parameters"]
        assert params["source_table"] == "dev.silver.alice_orders"
        assert params["other_table"] == "prod.external.customers"
        # The function FILE/function identity fields are untouched.
        assert source_function["file"] == "py_functions/snap.py"
        assert source_function["function"] == "next_snapshot"

    def test_python_transform_parameters_list_value_only_in_scope_renamed(self):
        """A python-transform parameters LIST (loop-unroll shape): only the
        in-scope element is renamed; the out-of-scope element passes through."""
        action = Action(
            name="tr_py",
            type=ActionType.TRANSFORM,
            transform_type="python",
            source="v_in",
            target="v_out",
            module_path="py_functions/transform.py",
            function_name="enrich",
            parameters={
                "tables": ["dev.silver.orders", "prod.external.customers"],
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].parameters["tables"] == [
            "dev.silver.alice_orders",
            "prod.external.customers",
        ]

    def test_python_load_source_parameters_value_renamed(self):
        """A python-load source.parameters value matching the rename set is
        renamed; an out-of-scope value passes through untouched."""
        action = Action(
            name="ld_py",
            type=ActionType.LOAD,
            target="v_py_in",
            source={
                "type": "python",
                "module_path": "py_functions/load.py",
                "function_name": "get_df",
                "parameters": {
                    "source_table": "dev.silver.orders",
                    "ext_table": "prod.external.customers",
                },
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        params = result.actions[0].source["parameters"]
        assert params["source_table"] == "dev.silver.alice_orders"
        assert params["ext_table"] == "prod.external.customers"

    def test_non_python_transform_parameters_untouched(self):
        """A non-python transform (data_quality) carrying a parameters-shaped
        field with an in-scope ref must NOT be rewritten — the parameters
        rule fires only for python transforms."""
        action = Action(
            name="tr_dq",
            type=ActionType.TRANSFORM,
            transform_type="data_quality",
            source="v_in",
            target="v_out",
            parameters={"source_table": "dev.silver.orders"},
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].parameters == {"source_table": "dev.silver.orders"}

    def test_parameter_rewrite_is_idempotent(self):
        """Running the pass twice equals running it once: a renamed value no
        longer matches the rename set."""
        action = Action(
            name="ld_py",
            type=ActionType.LOAD,
            target="v_py_in",
            source={
                "type": "python",
                "module_path": "py_functions/load.py",
                "function_name": "get_df",
                "parameters": {"source_table": "dev.silver.orders"},
            },
        )

        once = rewrite_flowgroup_tables(_fg(action), _renames())
        twice = rewrite_flowgroup_tables(once, _renames())

        assert (
            once.actions[0].source["parameters"]["source_table"]
            == "dev.silver.alice_orders"
        )
        assert twice.model_dump() == once.model_dump()

    def test_non_str_scalar_parameters_untouched(self):
        """int / bool / None / float parameter values pass through without a
        crash; a matching str alongside them is still renamed."""
        action = Action(
            name="tr_py",
            type=ActionType.TRANSFORM,
            transform_type="python",
            source="v_in",
            target="v_out",
            module_path="py_functions/transform.py",
            function_name="enrich",
            parameters={
                "source_table": "dev.silver.orders",
                "batch_size": 100,
                "enabled": True,
                "threshold": 1.5,
                "fallback": None,
            },
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        params = result.actions[0].parameters
        assert params["source_table"] == "dev.silver.alice_orders"
        assert params["batch_size"] == 100
        assert params["enabled"] is True
        assert params["threshold"] == 1.5
        assert params["fallback"] is None


@pytest.mark.unit
class TestDependsOnUntouched:
    def test_depends_on_never_rewritten_even_when_matching(self):
        """depends_on is DAG-only and never appears in generated code."""
        action = Action(
            name="tr_dep",
            type=ActionType.TRANSFORM,
            transform_type="sql",
            source="v_input",
            target="v_output",
            sql="SELECT 1",
            depends_on=["dev.silver.orders", "stg.events"],
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert result.actions[0].depends_on == ["dev.silver.orders", "stg.events"]


@pytest.mark.unit
class TestPassMechanics:
    def test_input_flowgroup_never_mutated(self):
        """Deep-compare the input's dump before and after the pass."""
        flowgroup = _fg(
            _table_write_action(
                "wr_orders", "v_orders", "materialized_view", "dev", "silver", "orders"
            ),
            Action(
                name="tr_orders",
                type=ActionType.TRANSFORM,
                transform_type="sql",
                source=["dev.silver.orders", "v_lookup"],
                target="v_enriched",
                sql="SELECT 1",
            ),
        )
        before = flowgroup.model_dump()

        rewrite_flowgroup_tables(flowgroup, _renames())

        assert flowgroup.model_dump() == before

    def test_empty_rename_set_returns_input_unchanged(self):
        """Fast path: no producers -> identity (same object is fine)."""
        renames = SandboxTableRenames(
            strategy=_strategy(), table_producers={}, table_short_to_catalogs={}
        )
        flowgroup = _fg(
            _table_write_action(
                "wr_orders", "v_orders", "materialized_view", "dev", "silver", "orders"
            )
        )

        result = rewrite_flowgroup_tables(flowgroup, renames)

        assert result is flowgroup

    def test_output_is_a_valid_round_trippable_flowgroup(self):
        flowgroup = _fg(
            _table_write_action(
                "wr_raw", "v_raw", "streaming_table", "dev", "bronze", "RawOrders"
            ),
            _delta_sink_action("wr_sink", "v_sink", "stg.events"),
        )

        result = rewrite_flowgroup_tables(flowgroup, _renames())

        assert isinstance(result, FlowGroup)
        assert FlowGroup(**result.model_dump()) == result
        assert result.pipeline == flowgroup.pipeline
        assert result.flowgroup == flowgroup.flowgroup

    def test_model_form_write_target_stays_a_model(self):
        """A WriteTarget model survives the dump/validate round-trip as a
        model — the pass must not silently change the field's form."""
        action = Action(
            name="wr_raw",
            type=ActionType.WRITE,
            source="v_raw",
            write_target=WriteTarget(
                type="streaming_table",
                catalog="dev",
                schema="bronze",
                table="RawOrders",
            ),
        )

        result = rewrite_flowgroup_tables(_fg(action), _renames())

        assert isinstance(result.actions[0].write_target, WriteTarget)
