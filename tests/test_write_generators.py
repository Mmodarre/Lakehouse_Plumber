"""Tests for write action generators of LakehousePlumber."""

import pytest
from pathlib import Path
import tempfile
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


def test_write_generator_imports():
    """Test that write generators manage imports correctly."""
    # Write generator
    mv_gen = MaterializedViewWriteGenerator()
    assert "import dlt" in mv_gen.imports
    assert "from pyspark.sql import DataFrame" in mv_gen.imports


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 