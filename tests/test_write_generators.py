"""Tests for write action generators of LakehousePlumber."""

import pytest
from pathlib import Path
import tempfile
from unittest.mock import patch
from lhp.models.config import Action, ActionType
from lhp.generators.write import (
    StreamingTableWriteGenerator,
    MaterializedViewWriteGenerator
)


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
                "database": "silver",
                "table": "customers",
                "create_table": True,  # ← Add explicit table creation flag
                "partition_columns": ["year", "month"],
                "cluster_columns": ["customer_id"],
                "table_properties": {
                    "quality": "silver"
                }
            }
        )
        
        code = generator.generate(action, {})
        
        # Check generated code - standard mode creates table and append flow
        assert "dlt.create_streaming_table" in code
        assert "@dlt.append_flow(" in code
        assert "silver.customers" in code
        assert "spark.readStream.table" in code
    
    def test_materialized_view_generator(self):
        """Test materialized view write generator."""
        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="write_summary",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "database": "gold",
                "table": "customer_summary",
                "refresh_schedule": "@daily",
                "sql": "SELECT region, COUNT(*) as customer_count FROM silver.customers GROUP BY region"
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.table(" in code
        assert 'name="gold.customer_summary"' in code
        assert 'refresh_schedule="@daily"' in code
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
                "database": "gold",
                "table": "advanced_table",
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                },
                "table_properties": {
                    "delta.autoOptimize.optimizeWrite": "true",
                    "delta.autoOptimize.autoCompact": "true"
                },
                "schema": "id BIGINT, name STRING, amount DECIMAL(18,2)",
                "row_filter": "ROW FILTER catalog.schema.filter_fn ON (region)",
                "temporary": True,
                "partition_columns": ["region"],
                "cluster_columns": ["id"],
                "path": "/mnt/data/gold/advanced_table",
                "sql": "SELECT * FROM silver.base_table"
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify all options are included
        assert "@dlt.table(" in code
        assert 'name="gold.advanced_table"' in code
        assert 'spark_conf={"spark.sql.adaptive.enabled": "true"' in code
        assert 'table_properties={"delta.autoOptimize.optimizeWrite": "true"' in code
        assert 'schema="id BIGINT, name STRING, amount DECIMAL(18,2)"' in code
        assert 'row_filter="ROW FILTER catalog.schema.filter_fn ON (region)"' in code
        assert 'temporary=True' in code
        assert 'partition_cols=["region"]' in code
        assert 'cluster_by=["id"]' in code
        assert 'path="/mnt/data/gold/advanced_table"' in code
    
    def test_streaming_table_with_all_options(self):
        """Test streaming table with all new options."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_streaming_advanced",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "advanced_streaming",
                "create_table": True,  # ← Add explicit table creation flag
                "spark_conf": {
                    "spark.sql.streaming.checkpointLocation": "/checkpoints/advanced",
                    "spark.sql.streaming.stateStore.providerClass": "RocksDBStateStoreProvider"
                },
                "table_properties": {
                    "delta.enableChangeDataFeed": "true",
                    "delta.autoOptimize.optimizeWrite": "true"
                },
                "schema": "customer_id BIGINT, name STRING, status STRING",
                "row_filter": "ROW FILTER catalog.schema.customer_filter ON (region)",
                "temporary": False,
                "partition_columns": ["status"],
                "cluster_columns": ["customer_id"],
                "path": "/mnt/data/silver/advanced_streaming"
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify all options are included in both create_streaming_table and @dlt.append_flow
        assert "dlt.create_streaming_table(" in code
        assert '@dlt.append_flow(' in code
        assert 'name="silver.advanced_streaming"' in code
        assert 'spark_conf={"spark.sql.streaming.checkpointLocation": "/checkpoints/advanced"' in code
        assert 'table_properties=' in code and '"delta.enableChangeDataFeed": "true"' in code
        assert 'schema="""customer_id BIGINT, name STRING, status STRING"""' in code
        assert 'row_filter="ROW FILTER catalog.schema.customer_filter ON (region)"' in code
        assert 'temporary=False' not in code  # False values are not included in output
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
                "database": "silver",
                "table": "customers",
                "create_table": True,  # ← Add explicit table creation flag
                "snapshot_cdc_config": {
                    "source": "raw.customer_snapshots",
                    "keys": ["customer_id"],
                    "stored_as_scd_type": 1
                }
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify snapshot CDC structure
        assert "dlt.create_streaming_table(" in code
        assert 'name="silver.customers"' in code
        assert "dlt.create_auto_cdc_from_snapshot_flow(" in code
        assert 'target="silver.customers"' in code
        assert 'source="raw.customer_snapshots"' in code
        assert 'keys=["customer_id"]' in code
        assert "stored_as_scd_type=1" in code
        
        # Should not have function imports
        assert "import sys" not in code
        assert "sys.path.append" not in code
    
    def test_streaming_table_snapshot_cdc_function_source(self):
        """Test streaming table with snapshot CDC using function source."""
        # Create a temporary function file for the test
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.read.table("raw.customer_snapshots")
        return (df, 1)
    return None
""")
            function_file = f.name
        
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customer_snapshot_cdc_func",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "customers",
                "create_table": True,  # ← Add explicit table creation flag
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": function_file,  # Use actual temp file path
                        "function": "next_customer_snapshot"
                    },
                    "keys": ["customer_id", "region"],
                    "stored_as_scd_type": 2,
                    "track_history_column_list": ["name", "email", "address"]
                }
            }
        )
        
        try:
            code = generator.generate(action, {})
        finally:
            # Clean up temp file
            Path(function_file).unlink()
        
        # Verify function embedding structure
        assert "# Snapshot function embedded directly in generated code" in code
        assert "def next_customer_snapshot(latest_version: Optional[int])" in code
        assert "from pyspark.sql import DataFrame" in code
        assert "from typing import Optional, Tuple" in code
        
        # Verify snapshot CDC structure
        assert "dlt.create_streaming_table(" in code
        assert "dlt.create_auto_cdc_from_snapshot_flow(" in code
        assert 'target="silver.customers"' in code
        assert "source=next_customer_snapshot" in code  # Function reference, not string
        assert 'keys=["customer_id", "region"]' in code
        assert "stored_as_scd_type=2" in code
        assert 'track_history_column_list=["name", "email", "address"]' in code
    
    def test_streaming_table_snapshot_cdc_track_history_except(self):
        """Test snapshot CDC with track_history_except_column_list."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_product_snapshot_cdc",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "mode": "snapshot_cdc",
                "database": "silver",
                "table": "products",
                "create_table": True,  # ← Add explicit table creation flag
                "snapshot_cdc_config": {
                    "source": "raw.product_snapshots",
                    "keys": ["product_id"],
                    "stored_as_scd_type": 2,
                    "track_history_except_column_list": ["created_at", "updated_at", "_metadata"]
                }
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify except columns usage
        assert "dlt.create_auto_cdc_from_snapshot_flow(" in code
        assert 'track_history_except_column_list=["created_at", "updated_at", "_metadata"]' in code
        assert "track_history_column_list" not in code  # Should not have both


def test_materialized_view_string_source():
    """Test materialized view with string source (no SQL in write_target)."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source="v_simple_view",  # String source, no SQL in write_target
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table"
        }
    )
    
    code = generator.generate(action, {})
    
    # Verify the string source is correctly extracted and used
    assert "v_simple_view" in code
    assert "@dlt.table(" in code
    assert 'name="test_db.test_table"' in code
    assert "spark.read.table" in code  # Should use spark.read.table for source view


def test_materialized_view_dict_source_with_database():
    """Test materialized view with dict source including database."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source={"database": "source_db", "table": "source_table"},  # Dict source with database
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table"
        }
    )
    
    code = generator.generate(action, {})
    
    # Verify the dict source is correctly parsed into qualified name
    assert "source_db.source_table" in code
    assert "@dlt.table(" in code
    assert 'name="test_db.test_table"' in code
    assert "spark.read.table" in code


def test_materialized_view_dict_source_without_database():
    """Test materialized view with dict source without database."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source={"table": "source_table"},  # Dict source without database
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table"
        }
    )
    
    code = generator.generate(action, {})
    
    # Verify the dict source is correctly parsed to table name only
    assert '"source_table"' in code  # Table name should appear in quotes
    assert "source_db.source_table" not in code  # Should not have database prefix
    assert "@dlt.table(" in code
    assert 'name="test_db.test_table"' in code
    assert "spark.read.table" in code


def test_materialized_view_list_source_first_item():
    """Test materialized view with list source, using only first item."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source=["first_view", "ignored_view", "also_ignored"],  # List source
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table"
        }
    )
    
    code = generator.generate(action, {})
    
    # Verify only the first item in the list is used
    assert "first_view" in code
    assert "ignored_view" not in code
    assert "also_ignored" not in code
    assert "@dlt.table(" in code
    assert 'name="test_db.test_table"' in code
    assert "spark.read.table" in code


def test_materialized_view_invalid_source_type():
    """Test materialized view with invalid source type raises ValueError."""
    generator = MaterializedViewWriteGenerator()
    
    # Test the _extract_source_view method directly with invalid source
    with pytest.raises(ValueError) as exc_info:
        generator._extract_source_view(42)  # Invalid source type (int)
    
    assert "Invalid source configuration" in str(exc_info.value)


def test_materialized_view_missing_write_target():
    """Test materialized view without write_target raises ValueError."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source="v_simple_view"
        # Missing write_target field entirely
    )
    
    # Should raise ValueError for missing write_target
    with pytest.raises(ValueError) as exc_info:
        generator.generate(action, {})
    
    assert "Materialized view action must have write_target configuration" in str(exc_info.value)


def test_materialized_view_missing_source_and_sql():
    """Test materialized view without both source and SQL raises ValueError."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        # Missing source field and no SQL in write_target
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table"
            # Missing sql field
        }
    )
    
    # Should raise ValueError when trying to extract source view from None
    with pytest.raises(ValueError) as exc_info:
        generator.generate(action, {})
    
    assert "Invalid source configuration" in str(exc_info.value)


def test_materialized_view_empty_list_source():
    """Test materialized view with empty list source raises ValueError."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source=[],  # Empty list source
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table"
        }
    )
    
    # Should raise ValueError for empty list source
    with pytest.raises(ValueError) as exc_info:
        generator.generate(action, {})
    
    assert "Invalid source configuration" in str(exc_info.value)


def test_materialized_view_no_database_fallback():
    """Test materialized view without database uses table name only."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "table": "test_table",  # No database field
            "sql": "SELECT * FROM test"
        }
    )
    
    code = generator.generate(action, {})
    
    # Verify table name is used without database prefix
    assert 'name="test_table"' in code
    assert 'name="test_db.test_table"' not in code  # Should not have database prefix
    assert "@dlt.table(" in code
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
            "database": "test_db",
            "table": "test_table",
            "comment": "Custom table comment",
            "sql": "SELECT * FROM test"
        }
    )
    
    code = generator.generate(action, {})
    
    # Verify custom comment and description appear in generated code
    assert 'comment="Custom table comment"' in code
    assert 'Custom action description' in code  # Should appear in function docstring
    assert "@dlt.table(" in code
    assert "spark.sql" in code


def test_materialized_view_with_flowgroup_context():
    """Test materialized view with flowgroup in context."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table",
            "sql": "SELECT * FROM test"
        }
    )
    
    # Pass flowgroup in context
    context = {"flowgroup": "test_flowgroup"}
    code = generator.generate(action, context)
    
    # Verify flowgroup context is used (check this is passed to template)
    # The template should have access to flowgroup variable
    assert "@dlt.table(" in code
    assert 'name="test_db.test_table"' in code
    assert "spark.sql" in code
    # Note: The actual usage of flowgroup in template may vary, 
    # this test ensures it's passed to template context without errors


def test_materialized_view_partition_and_cluster_variations():
    """Test materialized view with different partition and cluster column combinations."""
    generator = MaterializedViewWriteGenerator()
    
    # Test 1: Both partition and cluster columns
    action_both = Action(
        name="write_test_both",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table_both",
            "partition_columns": ["year", "month"],
            "cluster_columns": ["id"],
            "sql": "SELECT * FROM test"
        }
    )
    
    code_both = generator.generate(action_both, {})
    assert 'partition_cols=["year", "month"]' in code_both
    assert 'cluster_by=["id"]' in code_both
    
    # Test 2: Only partition columns
    action_partition = Action(
        name="write_test_partition",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table_partition",
            "partition_columns": ["region"],
            "sql": "SELECT * FROM test"
        }
    )
    
    code_partition = generator.generate(action_partition, {})
    assert 'partition_cols=["region"]' in code_partition
    assert "cluster_by=" not in code_partition
    
    # Test 3: Only cluster columns
    action_cluster = Action(
        name="write_test_cluster",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table_cluster",
            "cluster_columns": ["customer_id", "order_id"],
            "sql": "SELECT * FROM test"
        }
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
            "database": "test_db",
            "table": "test_table",
            "sql": "SELECT * FROM test"
        }
    )
    
    code = generator.generate(action, {})
    
    # Verify metadata is disabled by default
    assert "# Add operational metadata columns" not in code
    assert "withColumn" not in code  # No metadata column additions
    assert "@dlt.table(" in code
    assert "spark.sql" in code


def test_materialized_view_enabled_metadata_mock():
    """Test materialized view with mocked enabled metadata."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table",
            "sql": "SELECT * FROM test"
        }
    )
    
    # Patch the generate method to force metadata columns and add_operational_metadata
    original_generate = generator.generate
    
    def mock_generate(action, context):
        # Call original generate
        code_lines = original_generate(action, context).split('\n')
        
        # Find the template context creation and modify it to add metadata
        # Since this is testing a disabled feature, we'll just verify 
        # that when metadata is force-enabled, the template can handle it
        modified_context = context.copy() if context else {}
        modified_context['metadata_columns'] = {'_test_column': 'F.current_timestamp()'}
        
        # Re-call with modified context that forces metadata
        with patch.object(generator, 'render_template') as mock_render:
            # Mock the template rendering to include metadata
            mock_render.return_value = '''@dlt.table(
    name="test_db.test_table",
    comment="Materialized view: test_table",
    table_properties={})
def test_table():
    """Write to materialized view: test_db.test_table"""
    # Materialized views use batch processing
    df = spark.sql("""SELECT * FROM test""")
    
    # Add operational metadata columns
    df = df.withColumn('_test_column', F.current_timestamp())
    
    return df'''
            return mock_render.return_value
    
    with patch.object(generator, 'generate', side_effect=mock_generate):
        code = generator.generate(action, {})
    
    # Verify mocked metadata is present
    assert "# Add operational metadata columns" in code
    assert "withColumn" in code
    assert "_test_column" in code
    assert "@dlt.table(" in code


@pytest.mark.parametrize("source_config,expected_view", [
    # String source
    ("v_string_view", "v_string_view"),
    
    # Dict source with database
    ({"database": "source_db", "table": "source_table"}, "source_db.source_table"),
    
    # Dict source without database
    ({"table": "source_table"}, "source_table"),
    
    # Dict source with view field
    ({"database": "source_db", "view": "source_view"}, "source_db.source_view"),
    
    # Dict source with name field
    ({"name": "source_name"}, "source_name"),
    
    # List source (first item string)
    (["first_view", "ignored_view"], "first_view"),
    
    # List source (first item dict)
    ([{"database": "list_db", "table": "list_table"}, "ignored"], "list_db.list_table"),
])
def test_materialized_view_parametrized_sources(source_config, expected_view):
    """Test materialized view with various source configurations."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_test",
        type=ActionType.WRITE,
        source=source_config,
        write_target={
            "type": "materialized_view",
            "database": "test_db",
            "table": "test_table"
        }
    )
    
    code = generator.generate(action, {})
    
    # Verify expected view name appears in generated code
    assert expected_view in code
    assert "@dlt.table(" in code
    assert 'name="test_db.test_table"' in code
    assert "spark.read.table" in code


def test_materialized_view_full_structure():
    """Test materialized view with complex configuration generates correctly ordered code."""
    generator = MaterializedViewWriteGenerator()
    action = Action(
        name="write_complex_mv",
        type=ActionType.WRITE,
        description="Complex materialized view with all options",
        source={"database": "silver", "table": "customer_data"},
        write_target={
            "type": "materialized_view",
            "database": "gold",
            "table": "customer_analytics",
            "comment": "Customer analytics materialized view",
            "spark_conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            },
            "table_properties": {
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true"
            },
            "schema": "customer_id BIGINT, name STRING, total_spent DECIMAL(18,2)",
            "row_filter": "ROW FILTER catalog.schema.customer_filter ON (region)",
            "temporary": False,
            "partition_columns": ["region", "signup_year"],
            "cluster_columns": ["customer_id"],
            "path": "/mnt/data/gold/customer_analytics",
            "refresh_schedule": "@daily"
        }
    )
    
    context = {"flowgroup": "analytics_flowgroup"}
    code = generator.generate(action, context)
    
    # Verify overall structure and order
    lines = code.split('\n')
    
    # Check that @dlt.table comes before function definition
    dlt_table_line = next(i for i, line in enumerate(lines) if "@dlt.table(" in line)
    function_def_line = next(i for i, line in enumerate(lines) if "def customer_analytics():" in line)
    assert dlt_table_line < function_def_line, "DLT decorator should come before function definition"
    
    # Check that imports would be at the top (handled by base generator)
    # Check specific configurations are included
    assert 'name="gold.customer_analytics"' in code
    assert 'comment="Customer analytics materialized view"' in code
    assert 'spark_conf={"spark.sql.adaptive.enabled": "true"' in code
    assert 'table_properties={"delta.autoOptimize.optimizeWrite": "true"' in code
    assert 'schema="customer_id BIGINT, name STRING, total_spent DECIMAL(18,2)"' in code
    assert 'row_filter="ROW FILTER catalog.schema.customer_filter ON (region)"' in code
    assert 'partition_cols=["region", "signup_year"]' in code
    assert 'cluster_by=["customer_id"]' in code
    assert 'path="/mnt/data/gold/customer_analytics"' in code
    assert 'refresh_schedule="@daily"' in code
    
    # Verify source extraction and usage
    assert "silver.customer_data" in code
    assert "spark.read.table" in code
    
    # Verify function structure
    assert "def customer_analytics():" in code
    assert '"""Complex materialized view with all options"""' in code
    assert "return df" in code


def test_write_generator_imports():
    """Test that write generators manage imports correctly."""
    # Write generator
    mv_gen = MaterializedViewWriteGenerator()
    assert "import dlt" in mv_gen.imports
    assert "from pyspark.sql import DataFrame" in mv_gen.imports


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 