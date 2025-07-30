"""Advanced feature tests for LakehousePlumber."""

import pytest
import tempfile
import yaml
from pathlib import Path
from datetime import datetime

from lhp.core.orchestrator import ActionOrchestrator
from lhp.models.config import FlowGroup, Action, ActionType


class TestAdvancedFeatures:
    """Test advanced features and edge cases."""
    
    @pytest.fixture
    def project_root(self):
        """Create a temporary project with standard structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            
            # Create standard directories
            for dir_name in ['presets', 'templates', 'pipelines', 'substitutions', 'schemas', 'expectations']:
                (root / dir_name).mkdir()
            
            # Create basic project config
            (root / "lhp.yaml").write_text("""
name: test_project
version: "1.0"
""")
            
            # Create basic substitutions
            (root / "substitutions" / "dev.yaml").write_text("""
dev:
  env: dev
  catalog: dev_catalog
""")
            
            yield root
    
    def test_python_source_and_transform(self, project_root):
        """Test Python-based load and transform actions."""
        # Create flowgroup with Python actions
        pipeline_dir = project_root / "pipelines" / "python_pipeline"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "python_processing.yaml").write_text("""
pipeline: python_pipeline
flowgroup: python_processing

actions:
  - name: load_from_python
    type: load
    source:
      type: python
      module_path: "my_project.loaders.customer_loader"
    target: v_customers_python
    description: "Load data using Python function"
    
  - name: transform_with_python
    type: transform
    transform_type: python
    source: v_customers_python
    module_path: "transformers/enrich_customers.py"
    function_name: "enrich_customers"
    target: v_customers_enriched
    description: "Enrich customers with Python"
    
  - name: save_enriched
    type: write
    source: v_customers_enriched
    write_target:
      type: streaming_table
      database: "{catalog}.silver"
      table: customers_enriched
      create_table: true
""")
        
        # Create the Python transform function file
        transformers_dir = project_root / "transformers"
        transformers_dir.mkdir(parents=True)
        (transformers_dir / "enrich_customers.py").write_text("""
def enrich_customers(df, spark, parameters):
    # Mock enrichment function
    return df.withColumn("enriched", "true")
""")
        
        # Generate pipeline
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("python_pipeline", "dev")
        
        code = generated_files["python_processing.py"]
        
        # Check for Python imports
        assert "from my_project.loaders.customer_loader import" in code
        assert "from custom_python_functions.enrich_customers import" in code
        
        # Check for DLT decorators
        assert "@dlt.view()" in code
        
        # Python sources should still be wrapped in DLT views
        assert "def v_customers_python" in code
        assert "def v_customers_enriched" in code
    
    def test_temp_table_transform(self, project_root):
        """Test temporary table transformation."""
        pipeline_dir = project_root / "pipelines" / "temp_tables"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "temp_processing.yaml").write_text("""
pipeline: temp_tables
flowgroup: temp_processing

actions:
  - name: load_raw_data
    type: load
    source:
      type: sql
      sql: "SELECT * FROM source_table"
    target: v_raw_data
    
  - name: create_temp_aggregate
    type: transform
    transform_type: temp_table
    source: v_raw_data
    target: temp_daily_aggregates
    sql: |
      SELECT 
        DATE(timestamp) as date,
        COUNT(*) as record_count,
        SUM(amount) as total_amount
      FROM {source}
      GROUP BY DATE(timestamp)
    description: "Create temporary aggregate table"
    
  - name: join_with_temp
    type: transform
    transform_type: sql
    source: [v_raw_data, temp_daily_aggregates]
    target: v_enriched_data
    sql: |
      SELECT 
        r.*,
        t.record_count as daily_count,
        t.total_amount as daily_total
      FROM v_raw_data r
      JOIN temp_daily_aggregates t ON DATE(r.timestamp) = t.date
      
  - name: save_final
    type: write
    source: v_enriched_data
    write_target:
      type: streaming_table
      database: "silver"
      table: enriched_data
      create_table: true
""")
        
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("temp_tables", "dev")
        
        code = generated_files["temp_processing.py"]
        
        # Check for correct temporary table implementation
        assert "@dlt.table(" in code
        assert "temporary=True" in code
        assert "def temp_daily_aggregates():" in code
        # Verify it does NOT use the old incorrect temp table pattern
        assert "temp_daily_aggregates_temp" not in code
        # Verify the temp table section uses @dlt.table, not dlt.create_streaming_table for temp
        temp_table_section = code.split("# TRANSFORMATION VIEWS")[1].split("# TARGET TABLES")[0]
        assert "@dlt.table(" in temp_table_section
        assert "dlt.create_streaming_table" not in temp_table_section
        
        # Check for SQL with multiple sources
        assert "v_raw_data r" in code
        assert "temp_daily_aggregates t" in code
    
    def test_schema_transform(self, project_root):
        """Test schema application transformation."""
        pipeline_dir = project_root / "pipelines" / "schema_pipeline"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "schema_application.yaml").write_text("""
pipeline: schema_pipeline
flowgroup: schema_application

actions:
  - name: load_unstructured
    type: load
    source:
      type: cloudfiles
      path: "/mnt/landing/json/*.json"
      format: json
      schema_evolution_mode: "rescue"
    target: v_raw_json
    
  - name: apply_schema
    type: transform
    transform_type: schema
    source:
      view: v_raw_json
      schema:
        enforcement: strict
        type_casting:
          customer_id: "BIGINT"
          amount: "DOUBLE"
          created_date: "DATE"
        column_mapping:
          cust_id: customer_id
          amt: amount
    target: v_structured_data
    description: "Apply schema and type casting"
    
  - name: save_structured
    type: write
    source: v_structured_data
    write_target:
      type: materialized_view
      database: "silver"
      table: structured_customers
      refresh_schedule: "CRON '0 0 * * *'"
      create_table: true
""")
        
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("schema_pipeline", "dev")
        
        code = generated_files["schema_application.py"]
        
        # Check for schema enforcement
        assert "cast" in code.lower()
        assert "customer_id" in code
        assert "BIGINT" in code
        
        # Check for column mapping
        assert "withColumnRenamed" in code
        assert '"cust_id"' in code
        assert '"customer_id"' in code
        
        # Check for materialized view with refresh schedule
        assert "@dlt.table(" in code
        assert 'refresh_schedule="CRON \'0 0 * * *\'"' in code
    
    def test_many_to_many_relationships(self, project_root):
        """Test many-to-many action relationships."""
        pipeline_dir = project_root / "pipelines" / "complex_flows"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "many_to_many.yaml").write_text("""
pipeline: complex_flows
flowgroup: many_to_many

actions:
  - name: load_orders
    type: load
    source:
      type: delta
      database: "bronze"
      table: "orders"
    target: v_orders
    
  - name: load_customers
    type: load
    source:
      type: delta
      database: "bronze"
      table: "customers"
    target: v_customers
    
  - name: join_orders_customers
    type: transform
    transform_type: sql
    source: [v_orders, v_customers]
    target: v_order_details
    sql: |
      SELECT 
        o.*,
        c.customer_name,
        c.customer_segment
      FROM v_orders o
      JOIN v_customers c ON o.customer_id = c.customer_id
      
  - name: calculate_metrics
    type: transform
    transform_type: sql
    source: v_order_details
    target: v_customer_metrics
    sql: |
      SELECT
        customer_id,
        customer_name,
        customer_segment,
        COUNT(*) as order_count,
        SUM(amount) as total_revenue
      FROM {source}
      GROUP BY customer_id, customer_name, customer_segment
      
  - name: write_to_orders_fact
    type: write
    source: v_order_details
    write_target:
      type: streaming_table
      database: "gold"
      table: "fact_orders"
      create_table: true
      
  - name: write_to_customer_metrics
    type: write
    source: v_customer_metrics
    write_target:
      type: materialized_view
      database: "gold"
      table: "dim_customer_metrics"
      create_table: true
""")
        
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("complex_flows", "dev")
        
        code = generated_files["many_to_many.py"]
        
        # Check for multiple sources in SQL
        assert "v_orders o" in code
        assert "v_customers c" in code
        
        # Check for both write targets
        assert "fact_orders" in code
        assert "dim_customer_metrics" in code
        
        # Check proper ordering (loads before transforms before writes)
        code_lines = code.split('\n')
        v_orders_idx = next(i for i, line in enumerate(code_lines) if "def v_orders" in line)
        v_customers_idx = next(i for i, line in enumerate(code_lines) if "def v_customers" in line)
        v_order_details_idx = next(i for i, line in enumerate(code_lines) if "def v_order_details" in line)
        
        # Transforms should come after loads
        assert v_order_details_idx > v_orders_idx
        assert v_order_details_idx > v_customers_idx
    
    def test_operational_metadata_configurations(self, project_root):
        """Test different operational metadata configurations."""
        # Create preset with operational metadata
        (project_root / "presets" / "with_metadata.yaml").write_text("""
name: with_metadata
version: "1.0"
defaults:
  operational_metadata: true
""")
        
        pipeline_dir = project_root / "pipelines" / "metadata_test"
        pipeline_dir.mkdir(parents=True)
        
        # Flowgroup 1: Operational metadata from preset
        (pipeline_dir / "with_preset_metadata.yaml").write_text("""
pipeline: metadata_test
flowgroup: with_preset_metadata
presets:
  - with_metadata

actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT * FROM source"
    target: v_data
    
  - name: write_with_metadata
    type: write
    source: v_data
    write_target:
      type: streaming_table
      database: "bronze"
      table: "with_metadata"
      create_table: true
""")
        
        # Flowgroup 2: Operational metadata override
        (pipeline_dir / "override_metadata.yaml").write_text("""
pipeline: metadata_test
flowgroup: override_metadata
operational_metadata: true

actions:
  - name: load_data2
    type: load
    source:
      type: sql
      sql: "SELECT * FROM source2"
    target: v_data2
    
  - name: write_with_override
    type: write
    source: v_data2
    write_target:
      type: streaming_table
      database: "bronze"
      table: "override_metadata"
      create_table: true
""")
        
        orchestrator = ActionOrchestrator(project_root)
        
        # Test preset metadata
        files1 = orchestrator.generate_pipeline("metadata_test", "dev")
        code1 = files1["with_preset_metadata.py"]
        
        # Should have metadata columns
        assert "_ingestion_timestamp" in code1
        assert "F.current_timestamp()" in code1
        assert "_pipeline_name" in code1
        
        # Test override metadata
        files2 = orchestrator.generate_pipeline("metadata_test", "dev")
        code2 = files2["override_metadata.py"]
        
        # Should also have metadata columns
        assert "_ingestion_timestamp" in code2
        assert "_pipeline_name" in code2
    
    def test_error_handling_invalid_configurations(self, project_root):
        """Test error handling for invalid configurations."""
        # Test 1: Missing required Load action
        pipeline_dir1 = project_root / "pipelines" / "invalid_no_load"
        pipeline_dir1.mkdir(parents=True)
        
        (pipeline_dir1 / "no_load.yaml").write_text("""
pipeline: invalid_no_load
flowgroup: no_load

actions:
  - name: transform_only
    type: transform
    transform_type: sql
    source: v_nonexistent
    target: v_transformed
    sql: "SELECT * FROM {source}"
""")
        
        orchestrator = ActionOrchestrator(project_root)
        
        # The orphaned transform error is more specific and helpful than "missing load action"
        with pytest.raises(ValueError, match="Unused transform action"):
            orchestrator.generate_pipeline("invalid_no_load", "dev")
        
        # Test 2: Circular dependency - Create separate pipeline
        pipeline_dir2 = project_root / "pipelines" / "invalid_circular"
        pipeline_dir2.mkdir(parents=True)
        
        (pipeline_dir2 / "circular.yaml").write_text("""
pipeline: invalid_circular
flowgroup: circular

actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT * FROM raw"
    target: v_data
    
  - name: transform_a
    type: transform
    transform_type: sql
    source: v_transform_b
    target: v_transform_a
    sql: "SELECT * FROM {source}"
    
  - name: transform_b
    type: transform
    transform_type: sql
    source: v_transform_a
    target: v_transform_b
    sql: "SELECT * FROM {source}"
    
  - name: write_result
    type: write
    source: v_transform_a
    write_target:
      type: streaming_table
      database: "test"
      table: "result"
      create_table: true
""")
        
        # Create fresh orchestrator instance
        orchestrator2 = ActionOrchestrator(project_root)
        
        with pytest.raises(ValueError, match="Circular dependency"):
            orchestrator2.generate_pipeline("invalid_circular", "dev")
        
        # Test 3: Multiple table creators (rich error formatting)
        pipeline_dir3 = project_root / "pipelines" / "invalid_multiple_creators"
        pipeline_dir3.mkdir(parents=True)
        
        (pipeline_dir3 / "multiple_creators.yaml").write_text("""
pipeline: invalid_multiple_creators
flowgroup: multiple_creators

actions:
  - name: load_data
    type: load
    source:
      type: cloudfiles
      path: "/mnt/data"
      format: json
    target: v_data
    
  - name: write_lineitem_countries
    type: write
    source: v_data
    write_target:
      type: streaming_table
      database: "catalog.schema"
      table: "lineitem"
      create_table: true
      
  - name: write_lineitem_history
    type: write
    source: v_data
    write_target:
      type: streaming_table
      database: "catalog.schema"
      table: "lineitem"
      create_table: true
""")
        
        # Create fresh orchestrator instance
        orchestrator3 = ActionOrchestrator(project_root)
        
        # Should raise a ValueError containing the rich LHPError formatting
        with pytest.raises(ValueError) as exc_info:
            orchestrator3.generate_pipeline("invalid_multiple_creators", "dev")
        
        error_str = str(exc_info.value)
        # Verify it contains rich error formatting
        assert "❌ Error [LHP-CFG-004]" in error_str
        assert "Multiple table creators detected" in error_str
        assert "Context:" in error_str
        assert "How to fix:" in error_str
        assert "Example:" in error_str
    
    def test_preset_inheritance_chain(self, project_root):
        """Test complex preset inheritance chains."""
        # Create base preset
        (project_root / "presets" / "base.yaml").write_text("""
name: base
version: "1.0"
defaults:
  table_properties:
    delta.autoOptimize.optimizeWrite: "true"
  load_actions:
    cloudfiles:
      schema_evolution_mode: "failOnNewColumns"
""")
        
        # Create bronze preset extending base
        (project_root / "presets" / "bronze.yaml").write_text("""
name: bronze
version: "1.0"
extends: base
defaults:
  operational_metadata: true
  table_properties:
    quality: "bronze"
  load_actions:
    cloudfiles:
      schema_evolution_mode: "addNewColumns"  # Override base
      rescue_data_column: "_rescued_data"
""")
        
        # Create source-specific preset extending bronze
        (project_root / "presets" / "customer_bronze.yaml").write_text("""
name: customer_bronze
version: "1.0"
extends: bronze
defaults:
  table_properties:
    retention_days: "30"
  specific_source: "customer_system"
""")
        
        pipeline_dir = project_root / "pipelines" / "preset_test"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "preset_inheritance.yaml").write_text("""
pipeline: preset_test
flowgroup: preset_inheritance
presets:
  - customer_bronze

actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: "/mnt/data/*.json"
      format: json
    target: v_customers
    
  - name: write_customers
    type: write
    source: v_customers
    write_target:
      type: streaming_table
      database: "bronze"
      table: "customers"
      create_table: true
""") 