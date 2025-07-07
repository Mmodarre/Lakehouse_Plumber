"""Performance and stress tests for LakehousePlumber."""

import pytest
import tempfile
import time
import yaml
from pathlib import Path
from datetime import datetime

from lhp.core.orchestrator import ActionOrchestrator


class TestPerformance:
    """Performance and stress tests to ensure scalability."""
    
    @pytest.fixture
    def large_project(self):
        """Create a large project for performance testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            
            # Create standard directories
            for dir_name in ['presets', 'templates', 'pipelines', 'substitutions']:
                (root / dir_name).mkdir()
            
            # Create project config
            (root / "lhp.yaml").write_text("""
name: performance_test_project
version: "1.0"
""")
            
            # Create substitutions
            (root / "substitutions" / "dev.yaml").write_text("""
dev:
  env: dev
  catalog: dev_catalog
  schema_prefix: dev
""")
            
            yield root
    
    def test_generate_100_flowgroups(self, large_project):
        """Test generating 100+ flowgroups within performance limits.
        
        Requirement: Generate 100+ flowgroups in < 10 seconds
        """
        # Create 10 pipelines with 10 flowgroups each
        num_pipelines = 10
        num_flowgroups_per_pipeline = 10
        
        for pipeline_idx in range(num_pipelines):
            pipeline_name = f"pipeline_{pipeline_idx}"
            pipeline_dir = large_project / "pipelines" / pipeline_name
            pipeline_dir.mkdir(parents=True)
            
            for flowgroup_idx in range(num_flowgroups_per_pipeline):
                flowgroup_name = f"flowgroup_{flowgroup_idx}"
                
                # Create flowgroup with moderate complexity
                (pipeline_dir / f"{flowgroup_name}.yaml").write_text(f"""
pipeline: {pipeline_name}
flowgroup: {flowgroup_name}

actions:
  - name: load_source_{flowgroup_idx}
    type: load
    source:
      type: delta
      database: "bronze"
      table: "source_table_{flowgroup_idx}"
    target: v_source_{flowgroup_idx}
    
  - name: transform_data_{flowgroup_idx}
    type: transform
    transform_type: sql
    source: v_source_{flowgroup_idx}
    target: v_transformed_{flowgroup_idx}
    sql: |
      SELECT 
        *,
        current_timestamp() as processing_time
      FROM {{source}}
      WHERE active = true
      
  - name: aggregate_data_{flowgroup_idx}
    type: transform
    transform_type: sql
    source: v_transformed_{flowgroup_idx}
    target: v_aggregated_{flowgroup_idx}
    sql: |
      SELECT 
        date_trunc('day', processing_time) as day,
        COUNT(*) as record_count,
        SUM(amount) as total_amount
      FROM {{source}}
      GROUP BY date_trunc('day', processing_time)
      
  - name: save_detailed_{flowgroup_idx}
    type: write
    source:
      type: streaming_table
      database: "silver"
      table: "detailed_table_{flowgroup_idx}"
      view: v_transformed_{flowgroup_idx}
      
  - name: save_aggregated_{flowgroup_idx}
    type: write
    source:
      type: materialized_view
      database: "gold"
      table: "aggregated_table_{flowgroup_idx}"
      view: v_aggregated_{flowgroup_idx}
""")
        
        # Time the generation
        orchestrator = ActionOrchestrator(large_project)
        
        start_time = time.time()
        
        # Generate all pipelines
        for pipeline_idx in range(num_pipelines):
            pipeline_name = f"pipeline_{pipeline_idx}"
            orchestrator.generate_pipeline(pipeline_name, "dev")
        
        end_time = time.time()
        generation_time = end_time - start_time
        
        # Assert performance requirement
        assert generation_time < 10.0, f"Generation took {generation_time:.2f} seconds, expected < 10 seconds"
        
        # Verify files were generated
        total_flowgroups = num_pipelines * num_flowgroups_per_pipeline
        generated_files = list((large_project / "generated").rglob("*.py"))
        assert len(generated_files) == total_flowgroups
    
    def test_memory_usage_large_project(self, large_project):
        """Test memory usage stays within limits for large projects.
        
        Requirement: Memory usage < 500MB for large projects
        """
        import psutil
        import os
        
        # Create a large pipeline with many actions
        pipeline_dir = large_project / "pipelines" / "large_pipeline"
        pipeline_dir.mkdir(parents=True)
        
        # Create flowgroup with 100 actions
        actions = []
        
        # Add 20 load actions
        for i in range(20):
            actions.append(f"""
  - name: load_source_{i}
    type: load
    source:
      type: sql
      sql: "SELECT * FROM table_{i}"
    target: v_source_{i}""")
        
        # Add 60 transform actions (3 per source)
        for i in range(20):
            for j in range(3):
                source = f"v_source_{i}" if j == 0 else f"v_transform_{i}_{j-1}"
                actions.append(f"""
  - name: transform_{i}_{j}
    type: transform
    transform_type: sql
    source: {source}
    target: v_transform_{i}_{j}
    sql: "SELECT *, {j} as step FROM {{source}}"  """)
        
        # Add 20 write actions
        for i in range(20):
            actions.append(f"""
  - name: write_result_{i}
    type: write
    source:
      type: streaming_table
      database: "silver"
      table: "result_table_{i}"
      view: v_transform_{i}_2""")
        
        flowgroup_content = f"""
pipeline: large_pipeline
flowgroup: large_flowgroup

actions:{chr(10).join(actions)}
"""
        
        (pipeline_dir / "large_flowgroup.yaml").write_text(flowgroup_content)
        
        # Get process before generation
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        
        # Generate pipeline
        orchestrator = ActionOrchestrator(large_project)
        orchestrator.generate_pipeline("large_pipeline", "dev")
        
        # Get memory after generation
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        memory_used = memory_after - memory_before
        
        # Assert memory requirement
        assert memory_used < 500, f"Memory usage was {memory_used:.2f} MB, expected < 500 MB"
    
    def test_complex_dependency_resolution(self, large_project):
        """Test dependency resolution performance with complex graphs."""
        pipeline_dir = large_project / "pipelines" / "complex_deps"
        pipeline_dir.mkdir(parents=True)
        
        # Create a complex dependency graph
        # Layer 1: 5 load actions
        # Layer 2: 10 transforms (each depends on 2 loads)
        # Layer 3: 5 transforms (each depends on 2-3 from layer 2)
        # Layer 4: 2 writes (each depends on all from layer 3)
        
        actions = []
        
        # Layer 1: Loads
        for i in range(5):
            actions.append({
                "name": f"load_{i}",
                "type": "load",
                "source": {"type": "sql", "sql": f"SELECT * FROM source_{i}"},
                "target": f"v_load_{i}"
            })
        
        # Layer 2: First transforms
        for i in range(10):
            source_1 = f"v_load_{i % 5}"
            source_2 = f"v_load_{(i + 1) % 5}"
            actions.append({
                "name": f"transform_l2_{i}",
                "type": "transform",
                "transform_type": "sql",
                "source": [source_1, source_2],
                "target": f"v_transform_l2_{i}",
                "sql": f"SELECT * FROM {source_1} UNION ALL SELECT * FROM {source_2}"
            })
        
        # Layer 3: Second transforms
        for i in range(5):
            sources = [f"v_transform_l2_{j}" for j in range(i*2, min((i+1)*2 + 1, 10))]
            actions.append({
                "name": f"transform_l3_{i}",
                "type": "transform",
                "transform_type": "sql",
                "source": sources,
                "target": f"v_transform_l3_{i}",
                "sql": "SELECT * FROM " + " UNION ALL SELECT * FROM ".join(sources)
            })
        
        # Layer 4: Writes
        all_l3 = [f"v_transform_l3_{i}" for i in range(5)]
        for i in range(2):
            actions.append({
                "name": f"write_final_{i}",
                "type": "write",
                "source": {
                    "type": "streaming_table",
                    "database": "gold",
                    "table": f"final_table_{i}",
                    "view": all_l3[0]  # Just use first one for simplicity
                }
            })
        
        flowgroup = {
            "pipeline": "complex_deps",
            "flowgroup": "complex_flowgroup",
            "actions": actions
        }
        
        (pipeline_dir / "complex_flowgroup.yaml").write_text(yaml.dump(flowgroup))
        
        # Time dependency resolution
        orchestrator = ActionOrchestrator(large_project)
        
        start_time = time.time()
        orchestrator.generate_pipeline("complex_deps", "dev")
        end_time = time.time()
        
        resolution_time = end_time - start_time
        
        # Should handle complex dependencies quickly
        assert resolution_time < 2.0, f"Dependency resolution took {resolution_time:.2f} seconds"
    
    def test_large_sql_transforms(self, large_project):
        """Test handling of large SQL transformation queries."""
        pipeline_dir = large_project / "pipelines" / "large_sql"
        pipeline_dir.mkdir(parents=True)
        
        # Create a very large SQL query (realistic complex business logic)
        large_sql = """
WITH base_data AS (
    SELECT 
        customer_id,
        order_id,
        product_id,
        order_date,
        quantity,
        unit_price,
        discount_percent,
        shipping_cost,
        tax_rate,
        payment_method,
        shipping_address_id,
        billing_address_id,
        warehouse_id,
        fulfillment_status,
        return_status,
        created_timestamp,
        updated_timestamp
    FROM {source}
    WHERE order_date >= current_date - interval '90 days'
),

customer_metrics AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) as order_count,
        SUM(quantity * unit_price * (1 - discount_percent/100)) as gross_revenue,
        AVG(quantity * unit_price * (1 - discount_percent/100)) as avg_order_value,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        COUNT(DISTINCT product_id) as unique_products_purchased,
        COUNT(DISTINCT DATE_TRUNC('month', order_date)) as active_months,
        SUM(CASE WHEN return_status = 'returned' THEN 1 ELSE 0 END) as return_count
    FROM base_data
    GROUP BY customer_id
),

product_performance AS (
    SELECT 
        product_id,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(quantity) as total_quantity_sold,
        SUM(quantity * unit_price * (1 - discount_percent/100)) as total_revenue,
        AVG(discount_percent) as avg_discount_given,
        COUNT(DISTINCT order_id) as order_appearances,
        SUM(CASE WHEN fulfillment_status = 'delayed' THEN 1 ELSE 0 END) as delayed_fulfillments
    FROM base_data
    GROUP BY product_id
),

time_series_analysis AS (
    SELECT 
        DATE_TRUNC('day', order_date) as order_day,
        COUNT(DISTINCT customer_id) as daily_customers,
        COUNT(order_id) as daily_orders,
        SUM(quantity * unit_price * (1 - discount_percent/100)) as daily_revenue,
        AVG(shipping_cost) as avg_shipping_cost,
        -- Moving averages
        AVG(COUNT(order_id)) OVER (ORDER BY DATE_TRUNC('day', order_date) ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma7_orders,
        AVG(SUM(quantity * unit_price * (1 - discount_percent/100))) OVER (ORDER BY DATE_TRUNC('day', order_date) ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as ma30_revenue
    FROM base_data
    GROUP BY DATE_TRUNC('day', order_date)
),

cohort_analysis AS (
    SELECT 
        DATE_TRUNC('month', first_order_date) as cohort_month,
        DATE_TRUNC('month', order_date) as order_month,
        COUNT(DISTINCT bd.customer_id) as customers_in_cohort,
        SUM(quantity * unit_price * (1 - discount_percent/100)) as cohort_revenue
    FROM base_data bd
    JOIN (
        SELECT customer_id, MIN(order_date) as first_order_date
        FROM base_data
        GROUP BY customer_id
    ) first_orders ON bd.customer_id = first_orders.customer_id
    GROUP BY DATE_TRUNC('month', first_order_date), DATE_TRUNC('month', order_date)
)

SELECT 
    bd.*,
    cm.order_count as customer_order_count,
    cm.gross_revenue as customer_total_revenue,
    cm.avg_order_value as customer_avg_order_value,
    cm.active_months as customer_active_months,
    pp.unique_customers as product_unique_customers,
    pp.total_revenue as product_total_revenue,
    pp.avg_discount_given as product_avg_discount,
    ts.daily_customers,
    ts.daily_orders,
    ts.daily_revenue,
    ts.ma7_orders,
    ts.ma30_revenue,
    ca.cohort_month,
    ca.customers_in_cohort,
    -- Additional calculated fields
    CASE 
        WHEN cm.order_count >= 10 THEN 'VIP'
        WHEN cm.order_count >= 5 THEN 'Regular'
        WHEN cm.order_count >= 2 THEN 'Returning'
        ELSE 'New'
    END as customer_segment,
    CASE
        WHEN pp.total_revenue > 100000 THEN 'Star'
        WHEN pp.total_revenue > 50000 THEN 'High'
        WHEN pp.total_revenue > 10000 THEN 'Medium'
        ELSE 'Low'
    END as product_tier,
    -- Complex business logic
    CASE
        WHEN payment_method = 'credit_card' AND quantity * unit_price > 1000 THEN 'High-Value CC'
        WHEN return_status = 'returned' AND cm.return_count > 2 THEN 'Frequent Returner'
        WHEN shipping_cost > quantity * unit_price * 0.1 THEN 'High Shipping Cost'
        ELSE 'Standard'
    END as order_flag
FROM base_data bd
LEFT JOIN customer_metrics cm ON bd.customer_id = cm.customer_id
LEFT JOIN product_performance pp ON bd.product_id = pp.product_id
LEFT JOIN time_series_analysis ts ON DATE_TRUNC('day', bd.order_date) = ts.order_day
LEFT JOIN cohort_analysis ca ON 
    DATE_TRUNC('month', cm.first_order_date) = ca.cohort_month 
    AND DATE_TRUNC('month', bd.order_date) = ca.order_month
"""
        
        (pipeline_dir / "large_sql.yaml").write_text(f"""
pipeline: large_sql
flowgroup: large_sql_flowgroup

actions:
  - name: load_orders
    type: load
    source:
      type: delta
      database: "bronze"
      table: "orders"
    target: v_orders
    
  - name: complex_transform
    type: transform
    transform_type: sql
    source: v_orders
    target: v_complex_analysis
    sql: |
{large_sql}
    
  - name: save_analysis
    type: write
    source:
      type: streaming_table
      database: "gold"
      table: "order_analysis"
      view: v_complex_analysis
""")
        
        # Generate and verify it handles large SQL
        orchestrator = ActionOrchestrator(large_project)
        generated_files = orchestrator.generate_pipeline("large_sql", "dev")
        
        assert "large_sql_flowgroup.py" in generated_files
        code = generated_files["large_sql_flowgroup.py"]
        
        # Verify the large SQL was included properly
        assert "base_data AS" in code
        assert "customer_metrics AS" in code
        assert "cohort_analysis" in code
    
    def test_concurrent_pipeline_generation(self, large_project):
        """Test thread safety with concurrent generation (if implemented)."""
        # Create multiple small pipelines
        for i in range(5):
            pipeline_dir = large_project / "pipelines" / f"concurrent_{i}"
            pipeline_dir.mkdir(parents=True)
            
            (pipeline_dir / "flowgroup.yaml").write_text(f"""
pipeline: concurrent_{i}
flowgroup: flowgroup_{i}

actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT * FROM table_{i}"
    target: v_data
    
  - name: save_data
    type: write
    source:
      type: streaming_table
      database: "bronze"
      table: "table_{i}"
      view: v_data
""")
        
        # Note: Current implementation is synchronous
        # This test documents expected behavior if async/concurrent generation is added
        orchestrator = ActionOrchestrator(large_project)
        
        # Generate all pipelines sequentially (current behavior)
        start_time = time.time()
        for i in range(5):
            orchestrator.generate_pipeline(f"concurrent_{i}", "dev")
        end_time = time.time()
        
        sequential_time = end_time - start_time
        
        # If concurrent generation is implemented in the future,
        # it should be faster than sequential
        assert sequential_time < 5.0, "Sequential generation is too slow"
    
    @pytest.mark.slow
    def test_extreme_stress_1000_actions(self, large_project):
        """Extreme stress test with 1000 actions in a single flowgroup."""
        pipeline_dir = large_project / "pipelines" / "extreme"
        pipeline_dir.mkdir(parents=True)
        
        actions = []
        
        # Create 100 sources
        for i in range(100):
            actions.append({
                "name": f"load_{i}",
                "type": "load", 
                "source": {"type": "sql", "sql": f"SELECT {i} as id"},
                "target": f"v_{i}"
            })
        
        # Create 800 transforms in waves
        for wave in range(8):
            for i in range(100):
                source_idx = (wave * 100 + i) % 100
                actions.append({
                    "name": f"transform_{wave}_{i}",
                    "type": "transform",
                    "transform_type": "sql",
                    "source": f"v_{source_idx}" if wave == 0 else f"v_t{wave-1}_{i}",
                    "target": f"v_t{wave}_{i}",
                    "sql": f"SELECT *, {wave} as wave FROM {{source}}"
                })
        
        # Create 100 writes
        for i in range(100):
            actions.append({
                "name": f"write_{i}",
                "type": "write",
                "source": {
                    "type": "streaming_table",
                    "database": "bronze",
                    "table": f"result_{i}",
                    "view": f"v_t7_{i}"
                }
            })
        
        flowgroup = {
            "pipeline": "extreme",
            "flowgroup": "extreme_flowgroup",
            "actions": actions
        }
        
        (pipeline_dir / "extreme_flowgroup.yaml").write_text(yaml.dump(flowgroup))
        
        # Should still complete in reasonable time
        orchestrator = ActionOrchestrator(large_project)
        
        start_time = time.time()
        orchestrator.generate_pipeline("extreme", "dev")
        end_time = time.time()
        
        generation_time = end_time - start_time
        assert generation_time < 30.0, f"Extreme case took {generation_time:.2f} seconds" 