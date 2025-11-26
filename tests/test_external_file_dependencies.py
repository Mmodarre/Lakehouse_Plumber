"""Tests for external file dependency tracking (Python, SQL, etc.)."""

import tempfile
import pytest
from pathlib import Path

from lhp.core.state_dependency_resolver import StateDependencyResolver


class TestExternalFileDependencyTracking:
    """Test external file dependency tracking functionality."""

    def test_python_transform_module_path_dependency_tracking(self):
        """Test that Python transform module_path files are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create Python transform file
            transform_dir = project_root / "transformations"
            transform_dir.mkdir()
            transform_file = transform_dir / "customer_enrich.py"
            transform_file.write_text("""
from pyspark.sql import DataFrame

def enrich_customers(df: DataFrame, spark, parameters):
    return df.withColumn("enriched", lit(True))
""")
            
            # Create YAML file with Python transform action
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: enrich_customer_data
    type: transform
    transform_type: python
    source: v_customers_raw
    module_path: "transformations/customer_enrich.py"
    function_name: "enrich_customers"
    target: v_customers_enriched
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the Python transform file dependency
            assert "transformations/customer_enrich.py" in dependencies
            assert dependencies["transformations/customer_enrich.py"].type == "external_file"
            assert dependencies["transformations/customer_enrich.py"].path == "transformations/customer_enrich.py"

    def test_python_load_module_path_dependency_tracking(self):
        """Test that Python load module_path files are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create Python load file
            extractors_dir = project_root / "extractors"
            extractors_dir.mkdir()
            extractor_file = extractors_dir / "api_extractor.py"
            extractor_file.write_text("""
from pyspark.sql import DataFrame

def extract_api_data(spark, parameters):
    # Mock API extraction
    return spark.createDataFrame([("data1",), ("data2",)], ["value"])
""")
            
            # Create YAML file with Python load action
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_api_data
    type: load
    source:
      type: python
      module_path: "extractors/api_extractor.py"
      function_name: "extract_api_data"
      parameters:
        api_endpoint: "https://api.example.com"
    target: v_api_data
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the Python load file dependency
            assert "extractors/api_extractor.py" in dependencies
            assert dependencies["extractors/api_extractor.py"].type == "external_file"
            assert dependencies["extractors/api_extractor.py"].path == "extractors/api_extractor.py"

    def test_snapshot_cdc_source_function_dependency_tracking(self):
        """Test that snapshot CDC source_function files are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create snapshot CDC Python function file
            py_functions_dir = project_root / "py_functions"
            py_functions_dir.mkdir()
            snapshot_func_file = py_functions_dir / "customer_snapshot.py"
            snapshot_func_file.write_text("""
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.sql("SELECT * FROM customers WHERE snapshot_id = 1")
        return (df, 1)
    return None
""")
            
            # Create YAML file with snapshot CDC action
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: write_customer_cdc
    type: write
    write_target:
      type: streaming_table
      mode: snapshot_cdc
      database: "test_db.silver"
      table: "customer_cdc"
      snapshot_cdc_config:
        source_function:
          file: "py_functions/customer_snapshot.py"
          function: "next_customer_snapshot"
        keys: ["customer_id"]
        stored_as_scd_type: "2"
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the snapshot CDC function file dependency
            assert "py_functions/customer_snapshot.py" in dependencies
            assert dependencies["py_functions/customer_snapshot.py"].type == "external_file"
            assert dependencies["py_functions/customer_snapshot.py"].path == "py_functions/customer_snapshot.py"

    def test_sql_file_dependency_tracking(self):
        """Test that SQL files (sql_path) are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create SQL file
            sql_dir = project_root / "sql"
            sql_dir.mkdir()
            sql_file = sql_dir / "customer_metrics.sql"
            sql_file.write_text("""
SELECT 
    customer_id,
    customer_name,
    COUNT(*) as total_orders,
    SUM(order_amount) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY customer_id, customer_name
""")
            
            # Create YAML file with SQL load action using sql_path
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_customer_metrics
    type: load
    source:
      sql_path: "sql/customer_metrics.sql"
    target: v_customer_metrics
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the SQL file dependency
            assert "sql/customer_metrics.sql" in dependencies
            assert dependencies["sql/customer_metrics.sql"].type == "external_file"
            assert dependencies["sql/customer_metrics.sql"].path == "sql/customer_metrics.sql"

    def test_custom_datasource_module_path_dependency_tracking(self):
        """Test that custom datasource module_path files are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create custom datasource Python file
            datasources_dir = project_root / "datasources"
            datasources_dir.mkdir()
            datasource_file = datasources_dir / "api_datasource.py"
            datasource_file.write_text("""
from pyspark.sql import DataFrame

class APIDataSource:
    @staticmethod
    def get_format_name():
        return "api_source"
    
    def load(self, spark) -> DataFrame:
        return spark.createDataFrame([("data",)], ["value"])
""")
            
            # Create YAML file with custom datasource action
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_api_data
    type: load
    source:
      type: custom_datasource
      module_path: "datasources/api_datasource.py"
      custom_datasource_class: "APIDataSource"
      options:
        timeout: 30
    target: v_api_data
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the custom datasource file dependency
            # Note: Custom datasources use specific "custom_datasource_module" type (not "external_file")
            assert "datasources/api_datasource.py" in dependencies
            assert dependencies["datasources/api_datasource.py"].type == "custom_datasource_module"
            assert dependencies["datasources/api_datasource.py"].path == "datasources/api_datasource.py"

    def test_multiple_external_file_types_in_single_flowgroup(self):
        """Test tracking multiple external file types in a single flowgroup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create multiple external files
            
            # Python transform file
            transform_dir = project_root / "transformations"
            transform_dir.mkdir()
            transform_file = transform_dir / "process.py"
            transform_file.write_text("def process_data(df, spark, params): return df")
            
            # SQL file
            sql_dir = project_root / "sql"
            sql_dir.mkdir()
            sql_file = sql_dir / "query.sql"
            sql_file.write_text("SELECT * FROM table")
            
            # Snapshot CDC function file
            functions_dir = project_root / "functions"
            functions_dir.mkdir()
            cdc_file = functions_dir / "cdc.py"
            cdc_file.write_text("def next_snapshot(v): return None")
            
            # Create YAML file with multiple external file references
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_data
    type: load
    source:
      sql_path: "sql/query.sql"
    target: v_raw_data
  - name: transform_data
    type: transform
    transform_type: python
    source: v_raw_data
    module_path: "transformations/process.py"
    function_name: "process_data"
    target: v_processed_data
  - name: write_cdc_data
    type: write
    write_target:
      type: streaming_table
      mode: snapshot_cdc
      database: "test.silver"
      table: "cdc_table"
      snapshot_cdc_config:
        source_function:
          file: "functions/cdc.py"
          function: "next_snapshot"
        keys: ["id"]
        stored_as_scd_type: "2"
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find all external file dependencies
            expected_files = [
                "sql/query.sql",
                "transformations/process.py", 
                "functions/cdc.py"
            ]
            
            for file_path in expected_files:
                assert file_path in dependencies, f"Missing dependency: {file_path}"
                assert dependencies[file_path].type == "external_file"
                assert dependencies[file_path].path == file_path

    def test_backward_compatibility_with_existing_yaml_dependencies(self):
        """Test that existing YAML dependencies (presets, templates) still work alongside external files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("""
name: bronze_layer
version: "1.0"
description: "Bronze layer preset"
""")
            
            # Create Python transform file
            transform_dir = project_root / "transformations"
            transform_dir.mkdir()
            transform_file = transform_dir / "clean.py"
            transform_file.write_text("def clean_data(df, spark, params): return df")
            
            # Create YAML file with both preset and external file references
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
actions:
  - name: clean_data
    type: transform
    transform_type: python
    source: v_raw_data
    module_path: "transformations/clean.py"
    function_name: "clean_data"
    target: v_clean_data
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find both preset dependency AND external file dependency
            assert "presets/bronze_layer.yaml" in dependencies
            assert dependencies["presets/bronze_layer.yaml"].type == "preset"
            
            assert "transformations/clean.py" in dependencies
            assert dependencies["transformations/clean.py"].type == "external_file"
            
            # Should have at least 2 dependencies (preset + external file)
            assert len(dependencies) >= 2

    def test_missing_external_file_handling(self):
        """Test graceful handling when external files don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create YAML file with reference to non-existent Python file
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: process_data
    type: transform
    transform_type: python
    source: v_raw_data
    module_path: "transformations/missing.py"
    function_name: "process"
    target: v_processed_data
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery - should handle missing files gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should not include the missing file in dependencies
            assert "transformations/missing.py" not in dependencies
            
            # Should still return successfully (no exceptions)
            assert isinstance(dependencies, dict)

    def test_file_change_detection_for_external_files(self):
        """Test that file content changes are detected for external files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create Python transform file
            transform_dir = project_root / "transformations"
            transform_dir.mkdir()
            transform_file = transform_dir / "process.py"
            original_content = "def process(df, spark, params): return df"
            transform_file.write_text(original_content)
            
            # Create YAML file
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: process_data
    type: transform
    transform_type: python
    source: v_raw_data
    module_path: "transformations/process.py"
    function_name: "process"
    target: v_processed_data
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Get initial dependencies
            dependencies1 = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert "transformations/process.py" in dependencies1
            initial_checksum = dependencies1["transformations/process.py"].checksum
            
            # Modify the Python file
            modified_content = "def process(df, spark, params): return df.select('*')"
            transform_file.write_text(modified_content)
            
            # Get dependencies again
            dependencies2 = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert "transformations/process.py" in dependencies2
            modified_checksum = dependencies2["transformations/process.py"].checksum
            
            # Checksums should be different (indicating file change detected)
            assert initial_checksum != modified_checksum

    def test_cloudfiles_schema_hints_file_dependency_tracking(self):
        """Test that cloudFiles.schemaHints external files are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create schema file
            schema_dir = project_root / "schemas" / "bronze"
            schema_dir.mkdir(parents=True)
            schema_file = schema_dir / "customer_schema.yaml"
            schema_file.write_text("""
name: customer_schema
version: "1.0"
columns:
  - name: customer_id
    type: BIGINT
  - name: name
    type: STRING
""")
            
            # Create YAML file with cloudFiles action
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: "/mnt/landing/customers"
      format: json
      options:
        cloudFiles.format: json
        cloudFiles.schemaHints: "schemas/bronze/customer_schema.yaml"
    target: v_customers_raw
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the schema file dependency
            assert "schemas/bronze/customer_schema.yaml" in dependencies
            assert dependencies["schemas/bronze/customer_schema.yaml"].type == "external_file"

    def test_cloudfiles_schema_hints_ddl_file_dependency_tracking(self):
        """Test that cloudFiles.schemaHints DDL files are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create DDL schema file
            schema_dir = project_root / "schemas"
            schema_dir.mkdir()
            schema_file = schema_dir / "product_schema.ddl"
            schema_file.write_text("product_id BIGINT, product_name STRING, price DECIMAL(10,2)")
            
            # Create YAML file with cloudFiles action using DDL
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_products
    type: load
    source:
      type: cloudfiles
      path: "/mnt/landing/products"
      format: csv
      options:
        cloudFiles.format: csv
        cloudFiles.schemaHints: "schemas/product_schema.ddl"
    target: v_products_raw
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the DDL schema file dependency
            assert "schemas/product_schema.ddl" in dependencies
            assert dependencies["schemas/product_schema.ddl"].type == "external_file"

    def test_transform_schema_file_dependency_tracking(self):
        """Test that transform schema_file is tracked as a dependency."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create schema file
            schema_dir = project_root / "schemas" / "transformations"
            schema_dir.mkdir(parents=True)
            schema_file = schema_dir / "enrichment_schema.yaml"
            schema_file.write_text("""
name: enrichment_schema
version: "1.0"
columns:
  - name: id
    type: BIGINT
  - name: enriched_field
    type: STRING
""")
            
            # Create YAML file with schema transform action
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: apply_schema
    type: transform
    transform_type: schema
    source: v_raw_data
    target: v_enriched_data
    schema_file: "schemas/transformations/enrichment_schema.yaml"
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the schema file dependency
            assert "schemas/transformations/enrichment_schema.yaml" in dependencies
            assert dependencies["schemas/transformations/enrichment_schema.yaml"].type == "external_file"

    def test_write_table_schema_file_dependency_tracking(self):
        """Test that write action table_schema files are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create table schema DDL file
            schema_dir = project_root / "schemas" / "bronze"
            schema_dir.mkdir(parents=True)
            schema_file = schema_dir / "customers_table.ddl"
            schema_file.write_text("customer_id BIGINT NOT NULL, name STRING, email STRING")
            
            # Create YAML file with streaming table action
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: write_customers
    type: write
    source: v_customers_raw
    write_target:
      type: streaming_table
      database: "bronze"
      table: "customers"
      table_schema: "schemas/bronze/customers_table.ddl"
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the table schema file dependency
            assert "schemas/bronze/customers_table.ddl" in dependencies
            assert dependencies["schemas/bronze/customers_table.ddl"].type == "external_file"

    def test_materialized_view_sql_path_dependency_tracking(self):
        """Test that materialized view sql_path files are tracked as dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create SQL file
            sql_dir = project_root / "sql" / "gold"
            sql_dir.mkdir(parents=True)
            sql_file = sql_dir / "customer_summary.sql"
            sql_file.write_text("""
SELECT customer_id, COUNT(*) as order_count
FROM bronze.orders
GROUP BY customer_id
""")
            
            # Create YAML file with materialized view action
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: create_customer_summary
    type: write
    write_target:
      type: materialized_view
      database: "gold"
      table: "customer_summary"
      sql_path: "sql/gold/customer_summary.sql"
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the SQL file dependency
            assert "sql/gold/customer_summary.sql" in dependencies
            assert dependencies["sql/gold/customer_summary.sql"].type == "external_file"

    def test_schema_files_with_expectations_file_dependency_tracking(self):
        """Test tracking schema files alongside expectations files in data quality actions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create expectations file
            expectations_dir = project_root / "expectations"
            expectations_dir.mkdir()
            expectations_file = expectations_dir / "data_quality.yaml"
            expectations_file.write_text("""
expectations:
  - name: valid_id
    constraint: "id IS NOT NULL"
  - name: positive_amount
    constraint: "amount > 0"
""")
            
            # Create schema file
            schema_dir = project_root / "schemas"
            schema_dir.mkdir()
            schema_file = schema_dir / "clean_schema.yaml"
            schema_file.write_text("""
name: clean_schema
columns:
  - name: id
    type: BIGINT
  - name: amount
    type: DECIMAL(10,2)
""")
            
            # Create YAML file with data quality and schema actions
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_data
    type: load
    source:
      type: cloudfiles
      path: "/data"
      format: json
      options:
        cloudFiles.format: json
        cloudFiles.schemaHints: "schemas/clean_schema.yaml"
    target: v_raw_data
  - name: apply_quality_checks
    type: transform
    transform_type: data_quality
    source: v_raw_data
    expectations_file: "expectations/data_quality.yaml"
    target: v_clean_data
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find both schema and expectations files
            assert "schemas/clean_schema.yaml" in dependencies
            assert "expectations/data_quality.yaml" in dependencies
            assert dependencies["schemas/clean_schema.yaml"].type == "external_file"
            assert dependencies["expectations/data_quality.yaml"].type == "external_file"

    def test_multiple_schema_file_types_in_single_flowgroup(self):
        """Test tracking multiple schema file types in a single flowgroup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create various schema files
            schema_dir = project_root / "schemas"
            schema_dir.mkdir()
            (schema_dir / "load_schema.yaml").write_text("name: load_schema")
            (schema_dir / "transform_schema.yaml").write_text("name: transform_schema")
            (schema_dir / "table_schema.ddl").write_text("id BIGINT, name STRING")
            
            sql_dir = project_root / "sql"
            sql_dir.mkdir()
            (sql_dir / "summary.sql").write_text("SELECT * FROM table")
            
            # Create YAML file with multiple schema file references
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_data
    type: load
    source:
      type: cloudfiles
      path: "/data"
      format: json
      options:
        cloudFiles.format: json
        cloudFiles.schemaHints: "schemas/load_schema.yaml"
    target: v_raw_data
  - name: transform_data
    type: transform
    transform_type: schema
    source: v_raw_data
    schema_file: "schemas/transform_schema.yaml"
    target: v_transformed_data
  - name: write_data
    type: write
    source: v_transformed_data
    write_target:
      type: streaming_table
      database: "bronze"
      table: "data_table"
      table_schema: "schemas/table_schema.ddl"
  - name: create_summary
    type: write
    write_target:
      type: materialized_view
      database: "gold"
      table: "summary"
      sql_path: "sql/summary.sql"
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find all schema files
            expected_files = [
                "schemas/load_schema.yaml",
                "schemas/transform_schema.yaml",
                "schemas/table_schema.ddl",
                "sql/summary.sql"
            ]
            
            for file_path in expected_files:
                assert file_path in dependencies, f"Missing dependency: {file_path}"
                assert dependencies[file_path].type == "external_file"