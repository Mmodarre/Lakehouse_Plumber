"""Tests for ActionOrchestrator flowgroup discovery with include filtering."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Note: The actual implementation will be done later - these tests define the expected behavior


class TestActionOrchestratorIncludeFiltering:
    """Test cases for ActionOrchestrator flowgroup discovery with include patterns."""

    def test_discover_flowgroups_with_include_patterns(self, tmp_path):
        """Test _discover_flowgroups with include patterns."""
        # Given: A pipeline directory with various YAML files and include patterns
        project_root = tmp_path
        pipeline_dir = project_root / "pipelines" / "bronze_pipeline"
        pipeline_dir.mkdir(parents=True)
        
        # Create test files
        (pipeline_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipeline_dir / "bronze_orders.yaml").write_text("""
pipeline: bronze
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        (pipeline_dir / "silver_customers.yaml").write_text("""
pipeline: silver
flowgroup: customers
actions:
  - name: transform_customers
    type: transform
    source: v_customers
    target: v_customers_silver
""")
        
        # Create project config with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering flowgroups with include patterns
        # Expected: Only files matching include patterns should be discovered
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator._discover_flowgroups(pipeline_dir)
        # 
        # # Should only find bronze flowgroups (silver excluded by include pattern)
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names
        # 
        # # Verify pipeline fields
        # for fg in flowgroups:
        #     assert fg.pipeline == "bronze"

    def test_discover_flowgroups_no_include_patterns(self, tmp_path):
        """Test _discover_flowgroups without include patterns (backwards compatibility)."""
        # Given: A pipeline directory with various YAML files and no include patterns
        project_root = tmp_path
        pipeline_dir = project_root / "pipelines" / "test_pipeline"
        pipeline_dir.mkdir(parents=True)
        
        # Create test files
        (pipeline_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipeline_dir / "silver_orders.yaml").write_text("""
pipeline: silver
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")
        
        # Create project config without include patterns
        config_content = """
name: test_project
version: "1.0"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering flowgroups without include patterns
        # Expected: All YAML files should be discovered (backwards compatibility)
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator._discover_flowgroups(pipeline_dir)
        # 
        # # Should find all flowgroups
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names

    def test_discover_flowgroups_recursive_patterns(self, tmp_path):
        """Test _discover_flowgroups with recursive include patterns."""
        # Given: A pipeline directory with nested structure and recursive patterns
        project_root = tmp_path
        pipeline_dir = project_root / "pipelines" / "nested_pipeline"
        
        # Create nested directory structure
        bronze_dir = pipeline_dir / "bronze"
        bronze_raw_dir = bronze_dir / "raw"
        silver_dir = pipeline_dir / "silver"
        
        bronze_dir.mkdir(parents=True)
        bronze_raw_dir.mkdir(parents=True)
        silver_dir.mkdir(parents=True)
        
        # Create test files
        (bronze_dir / "customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (bronze_raw_dir / "events.yaml").write_text("""
pipeline: bronze
flowgroup: events
actions:
  - name: load_events
    type: load
    source:
      type: cloudfiles
      path: /data/events
    target: v_events
""")
        (silver_dir / "products.yaml").write_text("""
pipeline: silver
flowgroup: products
actions:
  - name: transform_products
    type: transform
    source: v_products
    target: v_products_silver
""")
        
        # Create project config with recursive include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze/**/*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering flowgroups with recursive patterns
        # Expected: Only files in bronze directory tree should be discovered
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator._discover_flowgroups(pipeline_dir)
        # 
        # # Should find bronze flowgroups only
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "events" in flowgroup_names
        # 
        # # Verify pipeline fields
        # for fg in flowgroups:
        #     assert fg.pipeline == "bronze"

    def test_discover_all_flowgroups_with_include_patterns(self, tmp_path):
        """Test discover_all_flowgroups with include patterns."""
        # Given: Multiple pipeline directories with various YAML files and include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        
        # Create multiple pipeline directories
        bronze_pipeline = pipelines_dir / "bronze_pipeline"
        silver_pipeline = pipelines_dir / "silver_pipeline"
        gold_pipeline = pipelines_dir / "gold_pipeline"
        
        bronze_pipeline.mkdir(parents=True)
        silver_pipeline.mkdir(parents=True)
        gold_pipeline.mkdir(parents=True)
        
        # Create test files
        (bronze_pipeline / "customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (silver_pipeline / "orders.yaml").write_text("""
pipeline: silver
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")
        (gold_pipeline / "analytics.yaml").write_text("""
pipeline: gold
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")
        
        # Create project config with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_pipeline/*.yaml"
  - "silver_pipeline/*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering all flowgroups with include patterns
        # Expected: Only files matching include patterns should be discovered
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator.discover_all_flowgroups()
        # 
        # # Should find bronze and silver flowgroups (gold excluded by include pattern)
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names
        # 
        # # Verify pipeline fields
        # pipelines = [fg.pipeline for fg in flowgroups]
        # assert "bronze" in pipelines
        # assert "silver" in pipelines
        # assert "gold" not in pipelines

    def test_discover_all_flowgroups_no_include_patterns(self, tmp_path):
        """Test discover_all_flowgroups without include patterns (backwards compatibility)."""
        # Given: Multiple pipeline directories with various YAML files and no include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        
        # Create multiple pipeline directories
        bronze_pipeline = pipelines_dir / "bronze_pipeline"
        silver_pipeline = pipelines_dir / "silver_pipeline"
        
        bronze_pipeline.mkdir(parents=True)
        silver_pipeline.mkdir(parents=True)
        
        # Create test files
        (bronze_pipeline / "customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (silver_pipeline / "orders.yaml").write_text("""
pipeline: silver
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")
        
        # Create project config without include patterns
        config_content = """
name: test_project
version: "1.0"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering all flowgroups without include patterns
        # Expected: All YAML files should be discovered (backwards compatibility)
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator.discover_all_flowgroups()
        # 
        # # Should find all flowgroups
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names

    def test_discover_flowgroups_by_pipeline_field_with_include_patterns(self, tmp_path):
        """Test discover_flowgroups_by_pipeline_field with include patterns."""
        # Given: Multiple flowgroups with same pipeline field and include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        
        # Create different directories but same pipeline field
        ingestion_dir = pipelines_dir / "01_ingestion"
        processing_dir = pipelines_dir / "02_processing"
        
        ingestion_dir.mkdir(parents=True)
        processing_dir.mkdir(parents=True)
        
        # Create test files with same pipeline field
        (ingestion_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (processing_dir / "bronze_orders.yaml").write_text("""
pipeline: bronze_layer
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        (processing_dir / "silver_customers.yaml").write_text("""
pipeline: silver_layer
flowgroup: customers
actions:
  - name: transform_customers
    type: transform
    source: v_customers
    target: v_customers_silver
""")
        
        # Create project config with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "*/bronze_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering flowgroups by pipeline field with include patterns
        # Expected: Only files matching both pipeline field and include patterns
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator.discover_flowgroups_by_pipeline_field("bronze_layer")
        # 
        # # Should find bronze flowgroups only (silver excluded by include pattern)
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names
        # 
        # # Verify pipeline fields
        # for fg in flowgroups:
        #     assert fg.pipeline == "bronze_layer"

    def test_discover_flowgroups_complex_patterns(self, tmp_path):
        """Test flowgroup discovery with complex include patterns."""
        # Given: Complex directory structure with complex include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        
        # Create complex directory structure
        data_bronze = pipelines_dir / "data" / "bronze"
        events_bronze = pipelines_dir / "events" / "bronze"
        ingestion_raw = pipelines_dir / "ingestion" / "raw"
        ingestion_nested = pipelines_dir / "ingestion" / "nested" / "deep"
        
        data_bronze.mkdir(parents=True)
        events_bronze.mkdir(parents=True)
        ingestion_raw.mkdir(parents=True)
        ingestion_nested.mkdir(parents=True)
        
        # Create test files
        (data_bronze / "customers.yaml").write_text("""
pipeline: data_bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (events_bronze / "orders.yaml").write_text("""
pipeline: events_bronze
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /events/orders
    target: v_orders
""")
        (ingestion_raw / "raw_events.yaml").write_text("""
pipeline: ingestion
flowgroup: raw_events
actions:
  - name: load_raw_events
    type: load
    source:
      type: cloudfiles
      path: /raw/events
    target: v_raw_events
""")
        (ingestion_nested / "raw_logs.yaml").write_text("""
pipeline: ingestion
flowgroup: raw_logs
actions:
  - name: load_raw_logs
    type: load
    source:
      type: cloudfiles
      path: /raw/logs
    target: v_raw_logs
""")
        (pipelines_dir / "gold_summary.yaml").write_text("""
pipeline: gold
flowgroup: summary
actions:
  - name: aggregate_summary
    type: transform
    source: v_data
    target: v_summary
""")
        
        # Create project config with complex include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "*/bronze/*.yaml"
  - "ingestion/**/raw_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering all flowgroups with complex patterns
        # Expected: Files matching either pattern should be discovered
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator.discover_all_flowgroups()
        # 
        # # Should find 4 flowgroups (2 from bronze dirs, 2 from ingestion dirs)
        # assert len(flowgroups) == 4
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names
        # assert "raw_events" in flowgroup_names
        # assert "raw_logs" in flowgroup_names
        # assert "summary" not in flowgroup_names  # Excluded by include pattern

    def test_discover_flowgroups_empty_include_patterns(self, tmp_path):
        """Test flowgroup discovery with empty include patterns."""
        # Given: A project with empty include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "customers.yaml").write_text("""
pipeline: test
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "orders.yaml").write_text("""
pipeline: test
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        
        # Create project config with empty include patterns
        config_content = """
name: test_project
version: "1.0"
include: []
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering flowgroups with empty include patterns
        # Expected: All files should be discovered (empty list = no filtering)
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator.discover_all_flowgroups()
        # 
        # # Should find all flowgroups
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names

    def test_discover_flowgroups_no_matches(self, tmp_path):
        """Test flowgroup discovery when no files match include patterns."""
        # Given: A project with files that don't match include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")
        
        # Create project config with non-matching include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "gold_*.yaml"
  - "platinum_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering flowgroups with non-matching patterns
        # Expected: No flowgroups should be discovered
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator.discover_all_flowgroups()
        # 
        # # Should find no flowgroups
        # assert len(flowgroups) == 0

    def test_discover_flowgroups_case_sensitivity(self, tmp_path):
        """Test flowgroup discovery with case-sensitive patterns."""
        # Given: A project with files with different cases
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files with different cases
        (pipelines_dir / "Bronze_customers.yaml").write_text("""
pipeline: Bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "bronze_orders.yaml").write_text("""
pipeline: bronze
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        (pipelines_dir / "BRONZE_products.yaml").write_text("""
pipeline: BRONZE
flowgroup: products
actions:
  - name: load_products
    type: load
    source:
      type: cloudfiles
      path: /data/products
    target: v_products
""")
        
        # Create project config with case-sensitive include pattern
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering flowgroups with case-sensitive pattern
        # Expected: Only exact case matches should be discovered
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator.discover_all_flowgroups()
        # 
        # # Should find only the exact case match
        # assert len(flowgroups) == 1
        # assert flowgroups[0].flowgroup == "orders"
        # assert flowgroups[0].pipeline == "bronze"

    def test_discover_flowgroups_both_extensions(self, tmp_path):
        """Test flowgroup discovery with both .yaml and .yml extensions."""
        # Given: A project with both .yaml and .yml files
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files with both extensions
        (pipelines_dir / "customers.yaml").write_text("""
pipeline: test
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "orders.yml").write_text("""
pipeline: test
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        (pipelines_dir / "products.yaml").write_text("""
pipeline: test
flowgroup: products
actions:
  - name: load_products
    type: load
    source:
      type: cloudfiles
      path: /data/products
    target: v_products
""")
        
        # Create project config with include patterns for both extensions
        config_content = """
name: test_project
version: "1.0"
include:
  - "*.yaml"
  - "*.yml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # When: Discovering flowgroups with patterns for both extensions
        # Expected: Files with both extensions should be discovered
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # flowgroups = orchestrator.discover_all_flowgroups()
        # 
        # # Should find all flowgroups with both extensions
        # assert len(flowgroups) == 3
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names
        # assert "products" in flowgroup_names


class TestActionOrchestratorIncludeIntegration:
    """Integration tests for ActionOrchestrator with include functionality."""

    def test_generate_pipeline_by_field_with_include_patterns(self, tmp_path):
        """Test generate_pipeline_by_field with include patterns."""
        # Given: A project with flowgroups and include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "bronze_orders.yaml").write_text("""
pipeline: bronze_layer
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        (pipelines_dir / "silver_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers_silver
actions:
  - name: transform_customers
    type: transform
    source: v_customers
    target: v_customers_silver
""")
        
        # Create project config with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # Create substitution file
        substitution_content = """
dev:
  catalog: dev_catalog
  bronze_schema: bronze
"""
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text(substitution_content)
        
        # When: Generating pipeline by field with include patterns
        # Expected: Only flowgroups matching include patterns should be processed
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # generated_files = orchestrator.generate_pipeline_by_field("bronze_layer", "dev")
        # 
        # # Should generate files for bronze flowgroups only (silver excluded)
        # assert len(generated_files) == 2
        # assert "customers.py" in generated_files
        # assert "orders.py" in generated_files
        # assert "customers_silver.py" not in generated_files

    def test_validate_pipeline_by_field_with_include_patterns(self, tmp_path):
        """Test validate_pipeline_by_field with include patterns."""
        # Given: A project with flowgroups and include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "invalid_file.yaml").write_text("""
pipeline: bronze_layer
flowgroup: invalid
actions:
  - name: invalid_action
    type: invalid_type
    source:
      type: invalid_source
    target: v_invalid
""")
        
        # Create project config with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # Create substitution file
        substitution_content = """
dev:
  catalog: dev_catalog
  bronze_schema: bronze
"""
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text(substitution_content)
        
        # When: Validating pipeline by field with include patterns
        # Expected: Only flowgroups matching include patterns should be validated
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(project_root)
        # 
        # errors, warnings = orchestrator.validate_pipeline_by_field("bronze_layer", "dev")
        # 
        # # Should validate only bronze flowgroups (invalid file excluded)
        # assert len(errors) == 0  # No errors since invalid file is excluded
        # assert len(warnings) == 1  # One warning for valid bronze file
        # assert "customers" in warnings[0] 