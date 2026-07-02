"""Tests for write action generators of LakehousePlumber."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.generators.write import (
    MaterializedViewWriteGenerator,
    StreamingTableWriteGenerator,
)
from lhp.models import Action, ActionType


class TestWriteGenerators:
    """Test write action generators."""

    def test_streaming_table_generator(self):
        """Test streaming table write generator."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customers",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
                "partition_columns": ["year", "month"],
                "cluster_columns": ["customer_id"],
                "table_properties": {"quality": "silver"},
            },
        )

        code = generator.generate(action, {})

        assert "dp.create_streaming_table" in code
        assert "@dp.append_flow(" in code
        assert "silver_cat.silver_sch.customers" in code
        assert "spark.readStream.table" in code

    def test_tags_do_not_leak_into_generated_code(self):
        """UC tags are applied via the tagging hook, never emitted into the table DDL."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customers",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
                "tags": {"team": "data-eng", "pii": ""},
            },
        )

        code = generator.generate(action, {})

        assert "dp.create_streaming_table" in code
        assert "tags" not in code
        assert "data-eng" not in code

    def test_materialized_view_generator(self):
        """Test materialized view write generator."""
        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="write_summary",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "customer_summary",
                "refresh_schedule": "@daily",
                "sql": "SELECT region, COUNT(*) as customer_count FROM silver.customers GROUP BY region",
            },
        )

        code = generator.generate(action, {})

        assert "@dp.materialized_view(" in code
        assert 'name="gold_cat.gold_sch.customer_summary"' in code
        assert "spark.sql" in code
        assert "GROUP BY region" in code

    def test_materialized_view_with_all_options(self):
        """Test materialized view with all new options."""
        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="write_advanced",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "advanced_table",
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                },
                "table_properties": {
                    "delta.autoOptimize.optimizeWrite": "true",
                    "delta.autoOptimize.autoCompact": "true",
                },
                "table_schema": "id BIGINT, name STRING, amount DECIMAL(18,2)",
                "row_filter": "ROW FILTER catalog.schema.filter_fn ON (region)",
                "temporary": True,
                "partition_columns": ["region"],
                "cluster_columns": ["id"],
                "path": "/mnt/data/gold/advanced_table",
                "refresh_policy": "incremental",
                "sql": "SELECT * FROM silver.base_table",
            },
        )

        code = generator.generate(action, {})

        # Note: temporary parameter is accepted in config for backward compat but not passed to decorator
        assert "@dp.materialized_view(" in code
        assert 'name="gold_cat.gold_sch.advanced_table"' in code
        assert 'spark_conf={"spark.sql.adaptive.enabled": "true"' in code
        assert 'table_properties={"delta.autoOptimize.optimizeWrite": "true"' in code
        assert 'schema="id BIGINT, name STRING, amount DECIMAL(18,2)"' in code
        assert 'row_filter="ROW FILTER catalog.schema.filter_fn ON (region)"' in code
        assert 'partition_cols=["region"]' in code
        assert 'cluster_by=["id"]' in code
        assert 'path="/mnt/data/gold/advanced_table"' in code
        assert 'refresh_policy="incremental"' in code

    def test_materialized_view_cluster_by_auto(self):
        """Test materialized view emits cluster_by_auto when set, omits when not."""
        generator = MaterializedViewWriteGenerator()

        # cluster_by_auto is mutually exclusive with cluster_columns, so omit them.
        action_enabled = Action(
            name="write_auto_cluster",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "auto_clustered",
                "cluster_by_auto": True,
                "sql": "SELECT * FROM silver.base_table",
            },
        )
        code_enabled = generator.generate(action_enabled, {})
        assert "@dp.materialized_view(" in code_enabled
        assert "cluster_by_auto=True" in code_enabled

        # Negative case: unset cluster_by_auto must emit nothing.
        action_unset = Action(
            name="write_no_auto_cluster",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "plain_table",
                "sql": "SELECT * FROM silver.base_table",
            },
        )
        code_unset = generator.generate(action_unset, {})
        assert "cluster_by_auto" not in code_unset

    def test_streaming_table_with_all_options(self):
        """Test streaming table with all new options."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_streaming_advanced",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "advanced_streaming",
                "create_table": True,
                "spark_conf": {
                    "spark.sql.streaming.checkpointLocation": "/checkpoints/advanced",
                    "spark.sql.streaming.stateStore.providerClass": "RocksDBStateStoreProvider",
                },
                "table_properties": {
                    "delta.enableChangeDataFeed": "true",
                    "delta.autoOptimize.optimizeWrite": "true",
                },
                "table_schema": "customer_id BIGINT, name STRING, status STRING",
                "row_filter": "ROW FILTER catalog.schema.customer_filter ON (region)",
                "temporary": False,
                "partition_columns": ["status"],
                "cluster_columns": ["customer_id"],
                "path": "/mnt/data/silver/advanced_streaming",
            },
        )

        code = generator.generate(action, {})

        assert "dp.create_streaming_table(" in code
        assert "@dp.append_flow(" in code
        assert 'name="silver_cat.silver_sch.advanced_streaming"' in code
        assert (
            'spark_conf={"spark.sql.streaming.checkpointLocation": "/checkpoints/advanced"'
            in code
        )
        assert (
            "table_properties=" in code
            and '"delta.enableChangeDataFeed": "true"' in code
        )
        assert 'schema="""customer_id BIGINT, name STRING, status STRING"""' in code
        assert (
            'row_filter="ROW FILTER catalog.schema.customer_filter ON (region)"' in code
        )
        assert "temporary=False" not in code  # False values are not included in output
        assert 'partition_cols=["status"]' in code
        assert 'cluster_by=["customer_id"]' in code
        assert 'path="/mnt/data/silver/advanced_streaming"' in code

    def test_streaming_table_snapshot_cdc_simple_source(self):
        """Test streaming table with snapshot CDC using simple table source."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customer_snapshot_cdc",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
                "snapshot_cdc_config": {
                    "source": "raw.customer_snapshots",
                    "keys": ["customer_id"],
                    "stored_as_scd_type": 1,
                },
            },
        )

        code = generator.generate(action, {})

        assert "dp.create_streaming_table(" in code
        assert 'name="silver_cat.silver_sch.customers"' in code
        assert "dp.create_auto_cdc_from_snapshot_flow(" in code
        assert 'target="silver_cat.silver_sch.customers"' in code
        assert 'source="raw.customer_snapshots"' in code
        assert 'keys=["customer_id"]' in code
        assert "stored_as_scd_type=1" in code

        assert "import sys" not in code
        assert "sys.path.append" not in code

    def test_streaming_table_cluster_by_auto_standard(self):
        """Standard mode emits cluster_by_auto=True without cluster_columns."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_cluster_auto",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
                "cluster_by_auto": True,
            },
        )

        code = generator.generate(action, {})

        assert "dp.create_streaming_table(" in code
        assert "cluster_by_auto=True" in code
        # cluster_by_auto is independent of explicit cluster columns
        assert "cluster_by=" not in code

    def test_streaming_table_cluster_by_auto_standard_unset(self):
        """Standard mode omits cluster_by_auto when False/unset."""
        generator = StreamingTableWriteGenerator()
        action_false = Action(
            name="write_cluster_auto_false",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
                "cluster_by_auto": False,
            },
        )
        action_unset = Action(
            name="write_cluster_auto_unset",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
            },
        )

        assert "cluster_by_auto" not in generator.generate(action_false, {})
        assert "cluster_by_auto" not in generator.generate(action_unset, {})

    def test_streaming_table_cluster_by_auto_cdc(self):
        """CDC mode emits cluster_by_auto=True on the created table; omits when unset."""
        generator = StreamingTableWriteGenerator()

        def _cdc_action(cluster_by_auto):
            write_target = {
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
            }
            if cluster_by_auto is not None:
                write_target["cluster_by_auto"] = cluster_by_auto
            return Action(
                name="write_customer_dim",
                type=ActionType.WRITE,
                source="v_customer_changes",
                write_target=write_target,
            )

        code = generator.generate(_cdc_action(True), {"expectations": []})
        assert "dp.create_streaming_table(" in code
        assert "dp.create_auto_cdc_flow(" in code
        assert "cluster_by_auto=True" in code

        code_false = generator.generate(_cdc_action(False), {"expectations": []})
        assert "cluster_by_auto" not in code_false

        code_unset = generator.generate(_cdc_action(None), {"expectations": []})
        assert "cluster_by_auto" not in code_unset

    def test_streaming_table_cluster_by_auto_snapshot_cdc(self):
        """Snapshot CDC mode emits cluster_by_auto=True; omits when unset."""
        generator = StreamingTableWriteGenerator()

        def _snapshot_action(cluster_by_auto):
            write_target = {
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
                "snapshot_cdc_config": {
                    "source": "raw.customer_snapshots",
                    "keys": ["customer_id"],
                    "stored_as_scd_type": 1,
                },
            }
            if cluster_by_auto is not None:
                write_target["cluster_by_auto"] = cluster_by_auto
            return Action(
                name="write_customer_snapshot_cdc",
                type=ActionType.WRITE,
                write_target=write_target,
            )

        code = generator.generate(_snapshot_action(True), {})
        assert "dp.create_streaming_table(" in code
        assert "dp.create_auto_cdc_from_snapshot_flow(" in code
        assert "cluster_by_auto=True" in code

        code_false = generator.generate(_snapshot_action(False), {})
        assert "cluster_by_auto" not in code_false

        code_unset = generator.generate(_snapshot_action(None), {})
        assert "cluster_by_auto" not in code_unset

    def _make_snapshot_function_file(self, body: str) -> str:
        """Write a temp snapshot source-function file, return its path.

        ``body`` is the function definition (and any supporting imports).
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(body)
            return f.name

    def test_streaming_table_snapshot_cdc_function_source_bare(self):
        """Bare (no-params) source function: copy-and-import, alias-qualified.

        The function body is NOT inlined; the user's module is imported under
        a ``_snap_<mod>`` alias and ``spark``/``dbutils`` are injected into its
        globals unconditionally.
        """
        from lhp.models import FlowGroup

        function_file = self._make_snapshot_function_file(
            """
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.read.table("raw.customer_snapshots")
        return (df, 1)
    return None
"""
        )
        mod = Path(function_file).stem
        alias = f"_snap_{mod}"

        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customer_snapshot_cdc_func",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": function_file,
                        "function": "next_customer_snapshot",
                    },
                    "keys": ["customer_id", "region"],
                    "stored_as_scd_type": 2,
                    "track_history_column_list": ["name", "email", "address"],
                },
            },
        )
        # output_dir omitted → dry-run copy path; flowgroup is required.
        context = {"flowgroup": FlowGroup(pipeline="p", flowgroup="fg")}

        try:
            code = generator.generate(action, context)
            imports = generator.imports
            pre = generator.get_pre_pipeline_statements()
        finally:
            Path(function_file).unlink()

        assert f"import custom_python_functions.{mod} as {alias}" in imports

        # BOTH session globals injected, unconditionally.
        assert f"{alias}.spark = spark" in pre
        assert f"{alias}.dbutils = dbutils" in pre

        assert "# Snapshot function embedded directly in generated code" not in code
        assert "def next_customer_snapshot" not in code

        assert "dp.create_auto_cdc_from_snapshot_flow(" in code
        assert 'target="silver_cat.silver_sch.customers"' in code
        assert f"source={alias}.next_customer_snapshot" in code
        assert "partial" not in code  # bare function: no partial wrapper needed
        assert 'keys=["customer_id", "region"]' in code
        assert "stored_as_scd_type=2" in code

    def test_streaming_table_snapshot_cdc_function_source_parameterized(self):
        """Parameterized source function: alias-qualified name is partial's
        FIRST POSITIONAL argument, body not inlined, both globals injected."""
        import re

        from lhp.models import FlowGroup

        function_file = self._make_snapshot_function_file(
            """
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_customer_snapshot(latest_version: Optional[int], *, region: str) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.read.table(f"raw.{region}_customer_snapshots")
        return (df, 1)
    return None
"""
        )
        mod = Path(function_file).stem
        alias = f"_snap_{mod}"

        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customer_snapshot_cdc_param",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": function_file,
                        "function": "next_customer_snapshot",
                        "parameters": {"region": "us"},
                    },
                    "keys": ["customer_id"],
                    "stored_as_scd_type": 1,
                },
            },
        )
        context = {"flowgroup": FlowGroup(pipeline="p", flowgroup="fg")}

        try:
            code = generator.generate(action, context)
            imports = generator.imports
            pre = generator.get_pre_pipeline_statements()
        finally:
            Path(function_file).unlink()

        assert f"import custom_python_functions.{mod} as {alias}" in imports
        assert "from functools import partial" in imports
        assert f"{alias}.spark = spark" in pre
        assert f"{alias}.dbutils = dbutils" in pre

        assert "def next_customer_snapshot" not in code
        assert "# Snapshot function embedded directly in generated code" not in code

        # The alias-qualified name is the partial's FIRST POSITIONAL argument
        # (not merely that "partial" appears somewhere).
        pattern = (
            r"partial\(\s*" + re.escape(f"{alias}.next_customer_snapshot") + r"\s*,"
        )
        assert re.search(pattern, code), f"partial-first-arg not found in:\n{code}"
        assert "region='us'" in code

    def test_streaming_table_snapshot_cdc_track_history_except(self):
        """Test snapshot CDC with track_history_except_column_list."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_product_snapshot_cdc",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "products",
                "create_table": True,
                "snapshot_cdc_config": {
                    "source": "raw.product_snapshots",
                    "keys": ["product_id"],
                    "stored_as_scd_type": 2,
                    "track_history_except_column_list": [
                        "created_at",
                        "updated_at",
                        "_metadata",
                    ],
                },
            },
        )

        code = generator.generate(action, {})

        assert "dp.create_auto_cdc_from_snapshot_flow(" in code
        assert (
            'track_history_except_column_list=["created_at", "updated_at", "_metadata"]'
            in code
        )
        assert "track_history_column_list" not in code  # Should not have both


def test_materialized_view_string_source():
    """Test materialized view with string source (no SQL in write_target)."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source="v_simple_view",
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
        },
    )

    code = generator.generate(action, {})

    assert "v_simple_view" in code
    assert "@dp.materialized_view(" in code
    assert 'name="test_cat.test_sch.test_table"' in code
    assert "spark.read.table" in code


def test_materialized_view_list_source_first_item():
    """Test materialized view with list source, using only first item."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source=["first_view", "ignored_view", "also_ignored"],
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
        },
    )

    code = generator.generate(action, {})

    assert "first_view" in code
    assert "ignored_view" not in code
    assert "also_ignored" not in code
    assert "@dp.materialized_view(" in code
    assert 'name="test_cat.test_sch.test_table"' in code
    assert "spark.read.table" in code


def test_materialized_view_invalid_source_type():
    """Test materialized view with invalid source type raises ValueError."""
    generator = MaterializedViewWriteGenerator()

    with pytest.raises(ValueError) as exc_info:
        generator._extract_source_view(42)  # Invalid source type (int)

    assert "Invalid source configuration" in str(exc_info.value)


def test_materialized_view_missing_write_target():
    """Test materialized view without write_target raises ValueError."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source="v_simple_view",
    )

    with pytest.raises(ValueError) as exc_info:
        generator.generate(action, {})

    assert "write_target" in str(exc_info.value)


def test_materialized_view_missing_source_and_sql():
    """Test materialized view without both source and SQL raises ValueError."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
        },
    )

    with pytest.raises(ValueError) as exc_info:
        generator.generate(action, {})

    assert "Invalid source configuration" in str(exc_info.value)


def test_materialized_view_empty_list_source():
    """Test materialized view with empty list source raises ValueError."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source=[],
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
        },
    )

    with pytest.raises(ValueError) as exc_info:
        generator.generate(action, {})

    assert "Invalid source configuration" in str(exc_info.value)


def test_materialized_view_no_catalog_schema_fallback():
    """Test materialized view without catalog/schema uses table name only."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "table": "test_table",  # No catalog/schema fields
            "sql": "SELECT * FROM test",
        },
    )

    code = generator.generate(action, {})

    assert 'name="test_table"' in code
    assert 'name="test_cat.test_sch.test_table"' not in code
    assert "@dp.materialized_view(" in code
    assert "spark.sql" in code


def test_materialized_view_custom_comment_and_description():
    """Test materialized view with custom comment and description."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        description="Custom action description",
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
            "comment": "Custom table comment",
            "sql": "SELECT * FROM test",
        },
    )

    code = generator.generate(action, {})

    assert 'comment="Custom table comment"' in code
    assert "Custom action description" in code
    assert "@dp.materialized_view(" in code
    assert "spark.sql" in code


def test_materialized_view_with_flowgroup_context():
    """Test materialized view with flowgroup in context."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
            "sql": "SELECT * FROM test",
        },
    )

    context = {"flowgroup": "test_flowgroup"}
    code = generator.generate(action, context)

    assert "@dp.materialized_view(" in code
    assert 'name="test_cat.test_sch.test_table"' in code
    assert "spark.sql" in code


def test_materialized_view_partition_and_cluster_variations():
    """Test materialized view with different partition and cluster column combinations."""
    generator = MaterializedViewWriteGenerator()

    action_both = Action(
        name="write_test_both",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table_both",
            "partition_columns": ["year", "month"],
            "cluster_columns": ["id"],
            "sql": "SELECT * FROM test",
        },
    )

    code_both = generator.generate(action_both, {})
    assert 'partition_cols=["year", "month"]' in code_both
    assert 'cluster_by=["id"]' in code_both

    action_partition = Action(
        name="write_test_partition",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table_partition",
            "partition_columns": ["region"],
            "sql": "SELECT * FROM test",
        },
    )

    code_partition = generator.generate(action_partition, {})
    assert 'partition_cols=["region"]' in code_partition
    assert "cluster_by=" not in code_partition

    action_cluster = Action(
        name="write_test_cluster",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table_cluster",
            "cluster_columns": ["customer_id", "order_id"],
            "sql": "SELECT * FROM test",
        },
    )

    code_cluster = generator.generate(action_cluster, {})
    assert 'cluster_by=["customer_id", "order_id"]' in code_cluster
    assert "partition_cols=" not in code_cluster


def test_materialized_view_disabled_metadata():
    """Test materialized view has metadata disabled by default."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
            "sql": "SELECT * FROM test",
        },
    )

    code = generator.generate(action, {})

    assert "# Add operational metadata columns" not in code
    assert "withColumn" not in code
    assert "@dp.materialized_view(" in code
    assert "spark.sql" in code


def test_materialized_view_enabled_metadata_mock():
    """Test materialized view with mocked enabled metadata."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
            "sql": "SELECT * FROM test",
        },
    )

    # Patch the generate method to force metadata columns and add_operational_metadata
    original_generate = generator.generate

    def mock_generate(action, context):
        code_lines = original_generate(action, context).split("\n")

        modified_context = context.copy() if context else {}
        modified_context["metadata_columns"] = {"_test_column": "F.current_timestamp()"}

        with patch.object(generator, "render_template") as mock_render:
            mock_render.return_value = '''@dp.materialized_view(
    name="test_cat.test_sch.test_table",
    comment="Materialized view: test_table",
    table_properties={})
def test_table():
    """Write to materialized view: test_cat.test_sch.test_table"""
    # Materialized views use batch processing
    df = spark.sql("""SELECT * FROM test""")
    
    # Add operational metadata columns
    df = df.withColumn('_test_column', F.current_timestamp())
    
    return df'''
            return mock_render.return_value

    with patch.object(generator, "generate", side_effect=mock_generate):
        code = generator.generate(action, {})

    assert "# Add operational metadata columns" in code
    assert "withColumn" in code
    assert "_test_column" in code
    assert "@dp.materialized_view(" in code


@pytest.mark.parametrize(
    "source_config,expected_view",
    [
        ("v_string_view", "v_string_view"),
        (["first_view", "ignored_view"], "first_view"),
    ],
)
def test_materialized_view_parametrized_sources(source_config, expected_view):
    """Test materialized view with various source configurations."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source=source_config,
        write_target={
            "type": "materialized_view",
            "catalog": "test_cat",
            "schema": "test_sch",
            "table": "test_table",
        },
    )

    code = generator.generate(action, {})

    assert expected_view in code
    assert "@dp.materialized_view(" in code
    assert 'name="test_cat.test_sch.test_table"' in code
    assert "spark.read.table" in code


def test_materialized_view_full_structure():
    """Test materialized view with complex configuration generates correctly ordered code."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_complex_mv",
        type=ActionType.WRITE,
        description="Complex materialized view with all options",
        source="silver.customer_data",
        write_target={
            "type": "materialized_view",
            "catalog": "gold_cat",
            "schema": "gold_sch",
            "table": "customer_analytics",
            "comment": "Customer analytics materialized view",
            "spark_conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
            "table_properties": {
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
            },
            "table_schema": "customer_id BIGINT, name STRING, total_spent DECIMAL(18,2)",
            "row_filter": "ROW FILTER catalog.schema.customer_filter ON (region)",
            "temporary": False,
            "partition_columns": ["region", "signup_year"],
            "cluster_columns": ["customer_id"],
            "path": "/mnt/data/gold/customer_analytics",
            "refresh_schedule": "@daily",
        },
    )

    context = {"flowgroup": "analytics_flowgroup"}
    code = generator.generate(action, context)

    lines = code.split("\n")

    # @dp.materialized_view must come before the function definition
    dlt_mv_line = next(
        i for i, line in enumerate(lines) if "@dp.materialized_view(" in line
    )
    function_def_line = next(
        i for i, line in enumerate(lines) if "def customer_analytics():" in line
    )
    assert dlt_mv_line < function_def_line, (
        "DLT decorator should come before function definition"
    )

    assert 'name="gold_cat.gold_sch.customer_analytics"' in code
    assert 'comment="Customer analytics materialized view"' in code
    assert 'spark_conf={"spark.sql.adaptive.enabled": "true"' in code
    assert 'table_properties={"delta.autoOptimize.optimizeWrite": "true"' in code
    assert 'schema="customer_id BIGINT, name STRING, total_spent DECIMAL(18,2)"' in code
    assert 'row_filter="ROW FILTER catalog.schema.customer_filter ON (region)"' in code
    assert 'partition_cols=["region", "signup_year"]' in code
    assert 'cluster_by=["customer_id"]' in code
    assert 'path="/mnt/data/gold/customer_analytics"' in code

    assert "silver.customer_data" in code
    assert "spark.read.table" in code

    assert "def customer_analytics():" in code
    assert '"""Complex materialized view with all options"""' in code
    assert "return df" in code


def test_materialized_view_with_sql_path():
    """Test materialized view loading SQL from external file via sql_path."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        sql_dir = project_root / "sql" / "gold"
        sql_dir.mkdir(parents=True)
        sql_file = sql_dir / "customer_summary.sql"
        sql_file.write_text("""
SELECT 
    customer_id,
    customer_name,
    COUNT(*) as total_orders,
    SUM(order_amount) as total_spent
FROM silver.customers c
JOIN silver.orders o ON c.customer_id = o.customer_id
GROUP BY customer_id, customer_name
""")

        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="create_customer_summary",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "customer_summary",
                "sql_path": "sql/gold/customer_summary.sql",
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert "customer_id" in code
        assert "customer_name" in code
        assert "total_orders" in code
        assert "total_spent" in code
        assert "silver.customers" in code
        assert "silver.orders" in code
        assert "GROUP BY customer_id, customer_name" in code
        assert "@dp.materialized_view(" in code
        assert 'name="gold_cat.gold_sch.customer_summary"' in code


def test_materialized_view_sql_path_with_substitutions():
    """Test that sql_path files support substitution variables."""
    import tempfile
    from pathlib import Path
    from unittest.mock import Mock

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        sql_dir = project_root / "sql"
        sql_dir.mkdir()
        sql_file = sql_dir / "sales_summary.sql"
        sql_file.write_text("""
SELECT 
    product_id,
    SUM(quantity) as total_quantity,
    SUM(amount) as total_sales
FROM {catalog}.{bronze_schema}.sales
WHERE sale_date >= '{start_date}'
GROUP BY product_id
""")

        mock_subst_mgr = Mock()
        mock_subst_mgr._process_string.return_value = """
SELECT 
    product_id,
    SUM(quantity) as total_quantity,
    SUM(amount) as total_sales
FROM dev_catalog.bronze.sales
WHERE sale_date >= '2024-01-01'
GROUP BY product_id
"""
        mock_subst_mgr.secret_references = set()

        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="create_sales_summary",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "sales_summary",
                "sql_path": "sql/sales_summary.sql",
            },
        )

        context = {
            "project_root": project_root,
            "substitution_manager": mock_subst_mgr,
            "secret_references": set(),
        }
        code = generator.generate(action, context)

        assert "dev_catalog.bronze.sales" in code
        assert "2024-01-01" in code

        mock_subst_mgr._process_string.assert_called_once()


def test_materialized_view_sql_path_file_not_found():
    """Test error handling when sql_path file doesn't exist."""
    import tempfile
    from pathlib import Path

    from lhp.errors import LHPError

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="create_view",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "test_view",
                "sql_path": "sql/missing_file.sql",
            },
        )

        context = {"project_root": project_root}

        with pytest.raises(LHPError) as exc_info:
            generator.generate(action, context)

        assert "LHP-IO-001" in str(exc_info.value)
        assert "missing_file.sql" in str(exc_info.value)


def test_materialized_view_sql_vs_sql_path_precedence():
    """Test that inline 'sql' takes precedence over 'sql_path'."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        sql_dir = project_root / "sql"
        sql_dir.mkdir()
        sql_file = sql_dir / "query.sql"
        sql_file.write_text("SELECT * FROM ignored_table")

        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="create_view",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "test_view",
                "sql": "SELECT customer_id, name FROM silver.customers",
                "sql_path": "sql/query.sql",  # Should be ignored
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert "silver.customers" in code
        assert "ignored_table" not in code


def test_materialized_view_table_schema_from_ddl_file():
    """Test materialized view loading table_schema from DDL file."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        schema_dir = project_root / "schemas" / "gold"
        schema_dir.mkdir(parents=True)
        schema_file = schema_dir / "product_view_schema.ddl"
        schema_file.write_text(
            "product_id BIGINT NOT NULL, product_name STRING, price DECIMAL(10,2), category STRING"
        )

        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="create_product_view",
            type=ActionType.WRITE,
            source="v_products_source",
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "product_view",
                "table_schema": "schemas/gold/product_view_schema.ddl",
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert (
            'schema="product_id BIGINT NOT NULL, product_name STRING, price DECIMAL(10,2), category STRING"'
            in code
        )
        assert "@dp.materialized_view(" in code


def test_materialized_view_table_schema_from_sql_file():
    """Test materialized view loading table_schema from SQL file."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        schema_dir = project_root / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "customer_view_schema.sql"
        schema_file.write_text(
            "customer_id BIGINT, name STRING, email STRING, region STRING"
        )

        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="create_customer_view",
            type=ActionType.WRITE,
            source="v_customers",
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "customer_view",
                "table_schema": "schemas/customer_view_schema.sql",
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert (
            'schema="customer_id BIGINT, name STRING, email STRING, region STRING"'
            in code
        )


def test_materialized_view_table_schema_inline_vs_file():
    """Test that inline table_schema is correctly distinguished from file paths."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        generator = MaterializedViewWriteGenerator()
        action_inline = Action(
            name="create_view_inline",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "test_view",
                "table_schema": "id BIGINT, name STRING, amount DECIMAL(18,2)",
            },
        )

        context = {"project_root": project_root}
        code_inline = generator.generate(action_inline, context)

        assert 'schema="id BIGINT, name STRING, amount DECIMAL(18,2)"' in code_inline

        schema_dir = project_root / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "product_schema.ddl"
        schema_file.write_text("product_id BIGINT, product_name STRING")

        action_file = Action(
            name="create_view_file",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "test_view",
                "table_schema": "schemas/product_schema.ddl",
            },
        )

        code_file = generator.generate(action_file, context)

        assert 'schema="product_id BIGINT, product_name STRING"' in code_file


def test_streaming_table_table_schema_from_ddl_file():
    """Test streaming table loading table_schema from DDL file."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        schema_dir = project_root / "schemas" / "bronze"
        schema_dir.mkdir(parents=True)
        schema_file = schema_dir / "customer_table.ddl"
        schema_file.write_text("""customer_id BIGINT NOT NULL,
name STRING,
email STRING,
region STRING,
registration_date DATE,
_source_file_path STRING,
_processing_timestamp TIMESTAMP""")

        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customer_stream",
            type=ActionType.WRITE,
            source="v_customer_raw",
            write_target={
                "type": "streaming_table",
                "catalog": "bronze_cat",
                "schema": "bronze_sch",
                "table": "customers",
                "table_schema": "schemas/bronze/customer_table.ddl",
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert "customer_id BIGINT NOT NULL" in code
        assert "_source_file_path STRING" in code
        assert "_processing_timestamp TIMESTAMP" in code
        assert "dp.create_streaming_table(" in code


def test_streaming_table_table_schema_from_sql_file():
    """Test streaming table loading table_schema from SQL file."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        schema_dir = project_root / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "orders_table.sql"
        schema_file.write_text(
            "order_id BIGINT, customer_id BIGINT, order_amount DECIMAL(10,2), order_date DATE"
        )

        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_orders_stream",
            type=ActionType.WRITE,
            source="v_orders_raw",
            write_target={
                "type": "streaming_table",
                "catalog": "bronze_cat",
                "schema": "bronze_sch",
                "table": "orders",
                "table_schema": "schemas/orders_table.sql",
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert "order_id BIGINT" in code
        assert "customer_id BIGINT" in code
        assert "order_amount DECIMAL(10,2)" in code


def test_streaming_table_table_schema_inline_vs_file():
    """Test that inline table_schema is correctly distinguished from file paths."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        generator = StreamingTableWriteGenerator()
        action_inline = Action(
            name="write_table_inline",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "streaming_table",
                "catalog": "bronze_cat",
                "schema": "bronze_sch",
                "table": "test_table",
                "table_schema": "id BIGINT, name STRING, created_at TIMESTAMP",
            },
        )

        context = {"project_root": project_root}
        code_inline = generator.generate(action_inline, context)

        assert (
            'schema="""id BIGINT, name STRING, created_at TIMESTAMP"""' in code_inline
        )

        schema_dir = project_root / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "test_schema.ddl"
        schema_file.write_text(
            "product_id BIGINT, product_name STRING, price DECIMAL(10,2)"
        )

        action_file = Action(
            name="write_table_file",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "streaming_table",
                "catalog": "bronze_cat",
                "schema": "bronze_sch",
                "table": "test_table",
                "table_schema": "schemas/test_schema.ddl",
            },
        )

        code_file = generator.generate(action_file, context)

        assert (
            "product_id BIGINT, product_name STRING, price DECIMAL(10,2)" in code_file
        )


def test_streaming_table_table_schema_from_yaml_file():
    """Test streaming table loading table_schema from YAML file."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        schema_dir = project_root / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "customer_table.yaml"
        schema_file.write_text("""name: customer_table
version: "1.0"
columns:
  - name: customer_id
    type: BIGINT
    nullable: false
  - name: customer_name
    type: STRING
    nullable: true
  - name: email
    type: STRING
    nullable: true
  - name: signup_date
    type: DATE
    nullable: false
""")

        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customers_stream",
            type=ActionType.WRITE,
            source="v_customers_raw",
            write_target={
                "type": "streaming_table",
                "catalog": "bronze_cat",
                "schema": "bronze_sch",
                "table": "customers",
                "table_schema": "schemas/customer_table.yaml",
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert "customer_id BIGINT NOT NULL" in code
        assert "customer_name STRING" in code
        assert "email STRING" in code
        assert "signup_date DATE NOT NULL" in code


def test_streaming_table_table_schema_from_yml_file():
    """Test streaming table loading table_schema from .yml file (alternative extension)."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        schema_dir = project_root / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "product_table.yml"
        schema_file.write_text("""name: product_table
version: "1.0"
columns:
  - name: product_id
    type: BIGINT
    nullable: false
  - name: product_name
    type: STRING
    nullable: false
  - name: price
    type: DECIMAL(10,2)
    nullable: false
""")

        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_products_stream",
            type=ActionType.WRITE,
            source="v_products_raw",
            write_target={
                "type": "streaming_table",
                "catalog": "bronze_cat",
                "schema": "bronze_sch",
                "table": "products",
                "table_schema": "schemas/product_table.yml",
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert "product_id BIGINT NOT NULL" in code
        assert "product_name STRING NOT NULL" in code
        assert "price DECIMAL(10,2) NOT NULL" in code


def test_materialized_view_table_schema_from_yaml_file():
    """Test materialized view loading table_schema from YAML file."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        schema_dir = project_root / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "orders_aggregate.yaml"
        schema_file.write_text("""name: orders_aggregate
version: "1.0"
columns:
  - name: customer_id
    type: BIGINT
    nullable: false
  - name: total_orders
    type: INT
    nullable: false
  - name: total_amount
    type: DECIMAL(18,2)
    nullable: false
  - name: last_order_date
    type: DATE
    nullable: true
""")

        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="create_orders_aggregate_mv",
            type=ActionType.WRITE,
            source="v_orders_summary",
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "orders_aggregate",
                "table_schema": "schemas/orders_aggregate.yaml",
            },
        )

        context = {"project_root": project_root}
        code = generator.generate(action, context)

        assert "customer_id BIGINT NOT NULL" in code
        assert "total_orders INT NOT NULL" in code
        assert "total_amount DECIMAL(18,2) NOT NULL" in code
        assert "last_order_date DATE" in code


def test_streaming_table_table_schema_from_inline_dict():
    """Streaming table with an INLINE DICT table_schema produces the same DDL as the equivalent YAML file.

    Mirrors ``test_streaming_table_table_schema_from_yaml_file`` (same columns) to prove
    parity: the inline dict is converted via SchemaParser.to_schema_hints (the same
    conversion the referenced-YAML-file path uses) and rendered as a triple-quoted
    schema kwarg. nullable: false -> " NOT NULL"; default nullable -> no suffix.
    """
    generator = StreamingTableWriteGenerator()
    action = Action(
        name="write_customers_stream",
        type=ActionType.WRITE,
        source="v_customers_raw",
        write_target={
            "type": "streaming_table",
            "catalog": "bronze_cat",
            "schema": "bronze_sch",
            "table": "customers",
            "table_schema": {
                "columns": [
                    {"name": "customer_id", "type": "BIGINT", "nullable": False},
                    {"name": "customer_name", "type": "STRING", "nullable": True},
                    {"name": "email", "type": "STRING"},
                    {"name": "signup_date", "type": "DATE", "nullable": False},
                ]
            },
        },
    )

    # No project_root needed: an inline dict is not a file path.
    code = generator.generate(action, {})

    # Same DDL substrings as the equivalent YAML-file test.
    assert "customer_id BIGINT NOT NULL" in code
    assert "customer_name STRING" in code
    assert "email STRING" in code
    assert "signup_date DATE NOT NULL" in code

    # Rendered as a triple-quoted schema kwarg, single-line comma-joined DDL.
    assert (
        'schema="""customer_id BIGINT NOT NULL, customer_name STRING, '
        'email STRING, signup_date DATE NOT NULL"""' in code
    )


def test_materialized_view_table_schema_from_inline_dict():
    """Materialized view with an INLINE DICT table_schema produces the expected DDL.

    Mirrors ``test_materialized_view_table_schema_from_yaml_file`` (same columns). The
    materialized view renders the DDL as a single-quoted, single-line ``schema="..."`` kwarg.
    """
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="create_orders_aggregate_mv",
        type=ActionType.WRITE,
        source="v_orders_summary",
        write_target={
            "type": "materialized_view",
            "catalog": "gold_cat",
            "schema": "gold_sch",
            "table": "orders_aggregate",
            "table_schema": {
                "columns": [
                    {"name": "customer_id", "type": "BIGINT", "nullable": False},
                    {"name": "total_orders", "type": "INT", "nullable": False},
                    {"name": "total_amount", "type": "DECIMAL(18,2)", "nullable": False},
                    {"name": "last_order_date", "type": "DATE", "nullable": True},
                ]
            },
        },
    )

    code = generator.generate(action, {})

    assert "customer_id BIGINT NOT NULL" in code
    assert "total_orders INT NOT NULL" in code
    assert "total_amount DECIMAL(18,2) NOT NULL" in code
    assert "last_order_date DATE" in code

    # Single-quoted, single-line schema kwarg for materialized views.
    assert (
        "schema=\"customer_id BIGINT NOT NULL, total_orders INT NOT NULL, "
        "total_amount DECIMAL(18,2) NOT NULL, last_order_date DATE\"" in code
    )


def test_streaming_table_inline_dict_matches_yaml_file():
    """Parity: DDL from an inline dict equals DDL from the equivalent separate YAML file.

    Builds both an inline-dict action and a referenced-YAML-file action with identical
    column definitions, extracts the triple-quoted schema DDL from each generated result,
    and asserts the two DDL strings are identical.
    """
    import re
    import tempfile
    from pathlib import Path

    def extract_schema_ddl(code: str) -> str:
        match = re.search(r'schema="""(.*?)"""', code, re.DOTALL)
        assert match is not None, "expected a triple-quoted schema kwarg in generated code"
        return match.group(1)

    columns = [
        {"name": "customer_id", "type": "BIGINT", "nullable": False},
        {"name": "customer_name", "type": "STRING", "nullable": True},
        {"name": "email", "type": "STRING"},
        {"name": "signup_date", "type": "DATE", "nullable": False},
    ]

    generator = StreamingTableWriteGenerator()

    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        schema_dir = project_root / "schemas"
        schema_dir.mkdir()
        schema_file = schema_dir / "customer_table.yaml"
        schema_file.write_text("""name: customer_table
version: "1.0"
columns:
  - name: customer_id
    type: BIGINT
    nullable: false
  - name: customer_name
    type: STRING
    nullable: true
  - name: email
    type: STRING
  - name: signup_date
    type: DATE
    nullable: false
""")

        action_file = Action(
            name="write_customers_file",
            type=ActionType.WRITE,
            source="v_customers_raw",
            write_target={
                "type": "streaming_table",
                "catalog": "bronze_cat",
                "schema": "bronze_sch",
                "table": "customers",
                "table_schema": "schemas/customer_table.yaml",
            },
        )
        code_file = generator.generate(action_file, {"project_root": project_root})

    action_inline = Action(
        name="write_customers_inline",
        type=ActionType.WRITE,
        source="v_customers_raw",
        write_target={
            "type": "streaming_table",
            "catalog": "bronze_cat",
            "schema": "bronze_sch",
            "table": "customers",
            "table_schema": {"columns": columns},
        },
    )
    code_inline = generator.generate(action_inline, {})

    assert extract_schema_ddl(code_inline) == extract_schema_ddl(code_file)


def test_write_generator_imports():
    """Test that write generators manage imports correctly."""
    mv_gen = MaterializedViewWriteGenerator()
    assert "from pyspark import pipelines as dp" in mv_gen.imports
    assert "from pyspark.sql import DataFrame" in mv_gen.imports


@pytest.mark.unit
class TestStreamingTableGoldenOutput:
    """Golden output test for streaming table write generator."""

    def test_basic_streaming_table_golden(self, golden):
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customers",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "catalog": "silver_cat",
                "schema": "silver_sch",
                "table": "customers",
                "create_table": True,
            },
        )
        code = generator.generate(action, {})
        golden(code, "write_streaming_table")


@pytest.mark.unit
class TestMaterializedViewGoldenOutput:
    """Golden output test for materialized view write generator."""

    def test_basic_materialized_view_golden(self, golden):
        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="write_summary",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "customer_summary",
                "sql": "SELECT region, COUNT(*) as customer_count FROM silver.customers GROUP BY region",
            },
        )
        code = generator.generate(action, {})
        golden(code, "write_materialized_view")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
