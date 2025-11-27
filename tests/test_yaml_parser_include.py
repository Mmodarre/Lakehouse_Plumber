"""Tests for YAMLParser.discover_flowgroups() with include filtering."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Note: The actual implementation will be done later - these tests define the expected behavior


class TestYAMLParserIncludeFiltering:
    """Test cases for YAMLParser flowgroup discovery with include patterns."""

    def test_discover_flowgroups_with_include_patterns(self, tmp_path):
        """Test discover_flowgroups with include patterns."""
        # Given: A directory with various YAML files and include patterns
        pipelines_dir = tmp_path / "pipelines"
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
        (pipelines_dir / "silver_customers.yaml").write_text("""
pipeline: silver
flowgroup: customers
actions:
  - name: transform_customers
    type: transform
    source: v_customers
    target: v_customers_silver
""")
        (pipelines_dir / "gold_analytics.yaml").write_text("""
pipeline: gold
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")
        
        # Include patterns
        include_patterns = ["bronze_*.yaml", "silver_*.yaml"]
        
        # When: Discovering flowgroups with include patterns
        # Expected: Only files matching include patterns should be discovered
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find bronze and silver flowgroups (gold excluded)
        # assert len(flowgroups) == 3
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names  # From bronze
        # assert "orders" in flowgroup_names     # From bronze
        # # Note: silver customers would also be named "customers" but different pipeline
        # 
        # # Verify pipeline fields
        # pipelines = [fg.pipeline for fg in flowgroups]
        # assert "bronze" in pipelines
        # assert "silver" in pipelines
        # assert "gold" not in pipelines

    def test_discover_flowgroups_no_include_patterns(self, tmp_path):
        """Test discover_flowgroups without include patterns (backwards compatibility)."""
        # Given: A directory with various YAML files and no include patterns
        pipelines_dir = tmp_path / "pipelines"
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
        (pipelines_dir / "gold_analytics.yaml").write_text("""
pipeline: gold
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")
        
        # When: Discovering flowgroups without include patterns
        # Expected: All YAML files should be discovered (backwards compatibility)
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir)
        # 
        # # Should find all flowgroups
        # assert len(flowgroups) == 3
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names
        # assert "analytics" in flowgroup_names

    def test_discover_flowgroups_empty_include_patterns(self, tmp_path):
        """Test discover_flowgroups with empty include patterns list."""
        # Given: A directory with YAML files and empty include patterns
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
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
        
        # When: Discovering flowgroups with empty include patterns
        # Expected: All YAML files should be discovered (empty list = no filtering)
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=[])
        # 
        # # Should find all flowgroups
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names

    def test_discover_flowgroups_recursive_patterns(self, tmp_path):
        """Test discover_flowgroups with recursive include patterns."""
        # Given: A directory with nested structure and recursive patterns
        pipelines_dir = tmp_path / "pipelines"
        
        # Create nested directory structure
        bronze_dir = pipelines_dir / "bronze"
        bronze_raw_dir = bronze_dir / "raw"
        silver_dir = pipelines_dir / "silver"
        
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
        (pipelines_dir / "gold_summary.yaml").write_text("""
pipeline: gold
flowgroup: summary
actions:
  - name: aggregate_summary
    type: transform
    source: v_data
    target: v_summary
""")
        
        # Include pattern for recursive matching
        include_patterns = ["bronze/**/*.yaml"]
        
        # When: Discovering flowgroups with recursive patterns
        # Expected: Only files in bronze directory tree should be discovered
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
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

    def test_discover_flowgroups_multiple_patterns(self, tmp_path):
        """Test discover_flowgroups with multiple include patterns."""
        # Given: A directory with files and multiple include patterns
        pipelines_dir = tmp_path / "pipelines"
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
        (pipelines_dir / "gold_products.yaml").write_text("""
pipeline: gold
flowgroup: products
actions:
  - name: aggregate_products
    type: transform
    source: v_products
    target: v_products_gold
""")
        (pipelines_dir / "config.json").write_text("{}")  # Non-YAML file
        
        # Multiple include patterns
        include_patterns = ["bronze_*.yaml", "silver_*.yaml"]
        
        # When: Discovering flowgroups with multiple patterns
        # Expected: Files matching any of the patterns should be discovered
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find bronze and silver flowgroups
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

    def test_discover_flowgroups_both_yaml_and_yml_extensions(self, tmp_path):
        """Test discover_flowgroups with both .yaml and .yml extensions."""
        # Given: A directory with both .yaml and .yml files
        pipelines_dir = tmp_path / "pipelines"
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
        
        # Include patterns for both extensions
        include_patterns = ["*.yaml", "*.yml"]
        
        # When: Discovering flowgroups with patterns for both extensions
        # Expected: Files with both extensions should be discovered
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find all flowgroups with both extensions
        # assert len(flowgroups) == 3
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names
        # assert "products" in flowgroup_names

    def test_discover_flowgroups_no_matches(self, tmp_path):
        """Test discover_flowgroups when no files match include patterns."""
        # Given: A directory with files that don't match include patterns
        pipelines_dir = tmp_path / "pipelines"
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
        
        # Include patterns that don't match any files
        include_patterns = ["gold_*.yaml", "platinum_*.yaml"]
        
        # When: Discovering flowgroups with non-matching patterns
        # Expected: Empty list should be returned
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find no flowgroups
        # assert len(flowgroups) == 0

    def test_discover_flowgroups_case_sensitivity(self, tmp_path):
        """Test discover_flowgroups with case-sensitive patterns."""
        # Given: A directory with files with different cases
        pipelines_dir = tmp_path / "pipelines"
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
        
        # Case-sensitive include pattern
        include_patterns = ["bronze_*.yaml"]
        
        # When: Discovering flowgroups with case-sensitive pattern
        # Expected: Only exact case matches should be discovered
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find only the exact case match
        # assert len(flowgroups) == 1
        # assert flowgroups[0].flowgroup == "orders"
        # assert flowgroups[0].pipeline == "bronze"

    def test_discover_flowgroups_complex_nested_patterns(self, tmp_path):
        """Test discover_flowgroups with complex nested directory patterns."""
        # Given: A directory with complex nested structure
        pipelines_dir = tmp_path / "pipelines"
        
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
        
        # Complex include patterns
        include_patterns = [
            "*/bronze/*.yaml",
            "ingestion/**/raw_*.yaml"
        ]
        
        # When: Discovering flowgroups with complex patterns
        # Expected: Files matching either pattern should be discovered
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find 4 flowgroups (2 from bronze dirs, 2 from ingestion dirs)
        # assert len(flowgroups) == 4
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names
        # assert "raw_events" in flowgroup_names
        # assert "raw_logs" in flowgroup_names
        # assert "summary" not in flowgroup_names  # Excluded by include pattern

    def test_discover_flowgroups_invalid_yaml_files(self, tmp_path):
        """Test discover_flowgroups with invalid YAML files."""
        # Given: A directory with valid and invalid YAML files
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        # Create valid YAML file
        (pipelines_dir / "valid_customers.yaml").write_text("""
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
        
        # Create invalid YAML file
        (pipelines_dir / "invalid_orders.yaml").write_text("""
pipeline: bronze
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
    invalid_yaml: [unclosed_bracket
""")
        
        # Include patterns
        include_patterns = ["*_*.yaml"]
        
        # When: Discovering flowgroups with invalid YAML files
        # Expected: Valid files should be processed, invalid ones should be skipped with warnings
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find only valid flowgroups
        # assert len(flowgroups) == 1
        # assert flowgroups[0].flowgroup == "customers"
        # assert flowgroups[0].pipeline == "bronze"

    def test_discover_flowgroups_missing_required_fields(self, tmp_path):
        """Test discover_flowgroups with YAML files missing required fields."""
        # Given: A directory with YAML files missing required fields
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        # Create valid YAML file
        (pipelines_dir / "valid_customers.yaml").write_text("""
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
        
        # Create YAML file missing required fields
        (pipelines_dir / "missing_fields.yaml").write_text("""
# Missing pipeline and flowgroup fields
actions:
  - name: load_something
    type: load
    source:
      type: cloudfiles
      path: /data/something
    target: v_something
""")
        
        # Include patterns
        include_patterns = ["*.yaml"]
        
        # When: Discovering flowgroups with files missing required fields
        # Expected: Valid files should be processed, invalid ones should be skipped with warnings
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find only valid flowgroups
        # assert len(flowgroups) == 1
        # assert flowgroups[0].flowgroup == "customers"
        # assert flowgroups[0].pipeline == "bronze"

    def test_discover_flowgroups_nonexistent_directory(self, tmp_path):
        """Test discover_flowgroups with non-existent directory."""
        # Given: A non-existent directory
        nonexistent_dir = tmp_path / "nonexistent"
        
        # Include patterns
        include_patterns = ["*.yaml"]
        
        # When: Discovering flowgroups in non-existent directory
        # Expected: Empty list should be returned
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(nonexistent_dir, include_patterns=include_patterns)
        # 
        # # Should find no flowgroups
        # assert len(flowgroups) == 0

    def test_discover_flowgroups_empty_directory(self, tmp_path):
        """Test discover_flowgroups with empty directory."""
        # Given: An empty directory
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        
        # Include patterns
        include_patterns = ["*.yaml"]
        
        # When: Discovering flowgroups in empty directory
        # Expected: Empty list should be returned
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(empty_dir, include_patterns=include_patterns)
        # 
        # # Should find no flowgroups
        # assert len(flowgroups) == 0

    def test_discover_flowgroups_edge_case_dot_patterns(self, tmp_path):
        """Test discover_flowgroups with edge cases involving dot patterns."""
        # Given: A directory with files and dot patterns
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "test.yaml").write_text("""
pipeline: test
flowgroup: simple
actions:
  - name: load_data
    type: load
    source:
      type: cloudfiles
      path: /data
    target: v_data
""")
        (pipelines_dir / "test.config.yaml").write_text("""
pipeline: test
flowgroup: config
actions:
  - name: load_config
    type: load
    source:
      type: cloudfiles
      path: /config
    target: v_config
""")
        (pipelines_dir / "test.backup.yaml").write_text("""
pipeline: test
flowgroup: backup
actions:
  - name: load_backup
    type: load
    source:
      type: cloudfiles
      path: /backup
    target: v_backup
""")
        
        # Include patterns with dots
        include_patterns = [
            "*.yaml",
            "test.*.yaml"
        ]
        
        # When: Discovering flowgroups with dot patterns
        # Expected: All files should match at least one pattern
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # parser = YAMLParser()
        # 
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # 
        # # Should find all flowgroups
        # assert len(flowgroups) == 3
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "simple" in flowgroup_names
        # assert "config" in flowgroup_names
        # assert "backup" in flowgroup_names

    def test_discover_flowgroups_performance_with_many_files(self, tmp_path):
        """Test discover_flowgroups performance with many files."""
        # Given: A directory with many files
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        # Create many test files
        for i in range(100):
            if i % 3 == 0:
                filename = f"bronze_file_{i}.yaml"
            elif i % 3 == 1:
                filename = f"silver_file_{i}.yaml"
            else:
                filename = f"gold_file_{i}.yaml"
            
            (pipelines_dir / filename).write_text(f"""
pipeline: {"bronze" if i % 3 == 0 else "silver" if i % 3 == 1 else "gold"}
flowgroup: file_{i}
actions:
  - name: load_file_{i}
    type: load
    source:
      type: cloudfiles
      path: /data/file_{i}
    target: v_file_{i}
""")
        
        # Include patterns
        include_patterns = ["bronze_*.yaml", "silver_*.yaml"]
        
        # When: Discovering flowgroups with many files
        # Expected: Should complete in reasonable time and return correct results
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # import time
        # 
        # parser = YAMLParser()
        # start_time = time.time()
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=include_patterns)
        # end_time = time.time()
        # 
        # # Should complete in reasonable time
        # assert end_time - start_time < 5.0  # Should complete in under 5 seconds
        # 
        # # Should find correct number of flowgroups (2/3 of files)
        # assert len(flowgroups) == 67  # 67 files match the patterns


class TestYAMLParserIncludeIntegration:
    """Integration tests for YAMLParser with include functionality."""

    def test_discover_flowgroups_with_project_config_integration(self, tmp_path):
        """Test discover_flowgroups integrating with project configuration."""
        # Given: A project with lhp.yaml containing include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create project configuration
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
  - "silver/**/*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
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
        
        silver_dir = pipelines_dir / "silver"
        silver_dir.mkdir()
        (silver_dir / "customers.yaml").write_text("""
pipeline: silver
flowgroup: customers
actions:
  - name: transform_customers
    type: transform
    source: v_customers
    target: v_customers_silver
""")
        
        (pipelines_dir / "gold_products.yaml").write_text("""
pipeline: gold
flowgroup: products
actions:
  - name: aggregate_products
    type: transform
    source: v_products
    target: v_products_gold
""")
        
        # When: Discovering flowgroups with project config integration
        # Expected: Should use include patterns from project config
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # 
        # # Load project config
        # config_loader = ProjectConfigLoader(project_root)
        # project_config = config_loader.load_project_config()
        # 
        # # Use include patterns from project config
        # parser = YAMLParser()
        # flowgroups = parser.discover_flowgroups(pipelines_dir, include_patterns=project_config.include)
        # 
        # # Should find bronze and silver flowgroups (gold excluded)
        # assert len(flowgroups) == 3
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names  # From both bronze and silver
        # assert "orders" in flowgroup_names     # From bronze
        # 
        # # Verify pipeline fields
        # pipelines = [fg.pipeline for fg in flowgroups]
        # assert "bronze" in pipelines
        # assert "silver" in pipelines
        # assert "gold" not in pipelines

    def test_discover_flowgroups_backwards_compatibility(self, tmp_path):
        """Test discover_flowgroups backwards compatibility when no include patterns."""
        # Given: A project without include patterns (legacy behavior)
        pipelines_dir = tmp_path / "pipelines"
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
        
        # When: Discovering flowgroups without include patterns
        # Expected: Should behave as before (include all files)
        
        # This will be implemented later
        # from lhp.parsers.yaml_parser import YAMLParser
        # 
        # parser = YAMLParser()
        # flowgroups = parser.discover_flowgroups(pipelines_dir)
        # 
        # # Should find all flowgroups (backwards compatibility)
        # assert len(flowgroups) == 2
        # flowgroup_names = [fg.flowgroup for fg in flowgroups]
        # assert "customers" in flowgroup_names
        # assert "orders" in flowgroup_names 