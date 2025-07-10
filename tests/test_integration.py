"""Integration tests for LakehousePlumber based on requirements."""

import pytest
import tempfile
import yaml
import json
from pathlib import Path
from click.testing import CliRunner

from lhp.cli.main import cli
from lhp.core.orchestrator import ActionOrchestrator
from lhp.parsers.yaml_parser import YAMLParser
from lhp.models.config import FlowGroup, Action, ActionType


class TestIntegrationCore:
    """Core integration tests based on requirements."""
    
    @pytest.fixture
    def temp_project(self):
        """Create a temporary project directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()
    
    def create_project_structure(self, project_path: Path):
        """Create standard LHP project structure."""
        # Create directories as per requirements
        directories = [
            'presets', 'templates', 'pipelines', 'substitutions',
            'schemas', 'expectations', 'generated'
        ]
        
        for dir_name in directories:
            (project_path / dir_name).mkdir(parents=True)
        
        # Create project config
        (project_path / "lhp.yaml").write_text("""
name: test_project
version: "1.0"
description: "Test LakehousePlumber project"
""")
        
        return project_path
    
    def test_bronze_ingestion_pattern(self, temp_project):
        """Test the bronze ingestion pattern from requirements.
        
        This test implements the example from the requirements:
        - CloudFiles source (Auto Loader)
        - Operational metadata addition
        - Write to streaming table
        """
        project_root = self.create_project_structure(temp_project)
        
        # Create bronze layer preset as per requirements
        (project_root / "presets" / "bronze_layer.yaml").write_text("""
name: bronze_layer
version: "1.0"
description: "Bronze layer preset for raw data ingestion"

defaults:
  operational_metadata: true
  
  write_actions:
    streaming_table:
      table_properties:
        delta.enableChangeDataFeed: "true"
        delta.autoOptimize.optimizeWrite: "true"
        quality: "bronze"
  
  load_actions:
    cloudfiles:
      schema_evolution_mode: "addNewColumns"
      rescue_data_column: "_rescued_data"
      options:
        cloudFiles.schemaHints: "true"
""")
        
        # Create substitutions for dev environment
        (project_root / "substitutions" / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  landing_path: /mnt/dev/landing
  checkpoint_path: /mnt/dev/checkpoints
""")
        
        # Create customer ingestion flowgroup
        pipeline_dir = project_root / "pipelines" / "sales_bronze"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "customer_ingestion.yaml").write_text("""
pipeline: sales_bronze
flowgroup: customer_ingestion
presets:
  - bronze_layer

actions:
  - name: load_customer_raw
    type: load
    target: v_customer_raw
    source:
      type: cloudfiles
      path: "{landing_path}/customer/*.json"
      format: json
    description: "Load raw customer data"
  
  - name: write_customer_bronze
    type: write
    source: v_customer_raw
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: customer_raw
      create_table: true
    description: "Write to bronze customer table"
""")
        
        # Generate pipeline
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("sales_bronze", "dev")
        
        # Verify generated code
        assert "customer_ingestion.py" in generated_files
        code = generated_files["customer_ingestion.py"]
        
        # Check for required elements from requirements
        assert "@dlt.view()" in code
        assert "spark.readStream" in code
        assert "cloudFiles" in code
        assert "/mnt/dev/landing/customer/*.json" in code
        assert "dlt.create_streaming_table(" in code  # Using append flow API
        assert "dev_catalog.bronze.customer_raw" in code
        
        # Check for operational metadata (enabled in preset)
        assert "_ingestion_timestamp" in code
        assert "F.current_timestamp()" in code
        assert "_pipeline_name" in code
        assert "from pyspark.sql import functions as F" in code
    
    def test_jdbc_source_with_secrets(self, temp_project):
        """Test JDBC source with secret management as per requirements."""
        project_root = self.create_project_structure(temp_project)
        
        # Create substitutions with secret configuration
        (project_root / "substitutions" / "prod.yaml").write_text("""
prod:
  catalog: prod_catalog
  bronze_schema: bronze

secrets:
  default_scope: prod_secrets
  scopes:
    database: prod_db_secrets
    apis: prod_api_secrets
""")
        
        # Create flowgroup with JDBC source using secrets
        pipeline_dir = project_root / "pipelines" / "customer_ingestion"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "external_customer_load.yaml").write_text("""
pipeline: customer_ingestion
flowgroup: external_customer_load

actions:
  - name: load_customers_from_postgres
    type: load
    source:
      type: jdbc
      url: "jdbc:postgresql://${secret:database/host}:5432/customers"
      user: "${secret:database/username}"
      password: "${secret:database/password}"
      driver: "org.postgresql.Driver"
      query: |
        SELECT 
          customer_id,
          customer_name,
          email,
          created_date
        FROM customers 
        WHERE updated_date >= current_date - interval '1 day'
    target: v_customers_raw
    
  - name: save_customers_bronze
    type: write
    source: v_customers_raw
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: customers_raw
      create_table: true
""")
        
        # Generate pipeline
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("customer_ingestion", "prod")
        
        # Verify generated code
        assert "external_customer_load.py" in generated_files
        code = generated_files["external_customer_load.py"]
        
        # Check for JDBC configuration
        assert "spark.read" in code
        assert ".format(\"jdbc\")" in code
        
        # Check for valid secret substitution - should be f-strings or direct dbutils calls
        assert "dbutils.secrets.get" in code
        assert 'scope="prod_db_secrets"' in code
        assert 'key="host"' in code or "key='host'" in code  # Either quote style is valid
        assert 'key="username"' in code or "key='username'" in code  # Either quote style is valid
        assert 'key="password"' in code or "key='password'" in code  # Either quote style is valid
        
        # Verify SQL query is included
        assert "SELECT" in code
        assert "customer_id" in code
        
        # Most importantly, verify the generated code is syntactically valid Python
        try:
            compile(code, '<string>', 'exec')
            # If compilation succeeds, the code is valid
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated integration code with secrets is not valid Python syntax: {e}")
    
    def test_cdc_silver_layer(self, temp_project):
        """Test CDC pattern for silver layer as per requirements."""
        project_root = self.create_project_structure(temp_project)
        
        # Create silver layer preset
        (project_root / "presets" / "silver_layer.yaml").write_text("""
name: silver_layer
version: "1.0"
description: "Silver layer preset for cleansed data"

defaults:
  write_actions:
    streaming_table:
      table_properties:
        delta.enableChangeDataFeed: "true"
        quality: "silver"
""")
        
        # Create substitutions
        (project_root / "substitutions" / "dev.yaml").write_text("""
dev:
  env: dev
  bronze_schema: bronze
  silver_schema: silver
""")
        
        # Create CDC flowgroup as per requirements example
        pipeline_dir = project_root / "pipelines" / "sales_silver"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "customer_dimensions.yaml").write_text("""
pipeline: sales_silver
flowgroup: customer_dimensions
presets:
  - silver_layer

actions:
  - name: load_customer_changes
    type: load
    source:
      type: delta
      database: "{env}_{bronze_schema}_sales"
      table: customer_raw
      read_change_feed: true
    target: v_customer_changes
    
  - name: cleanse_customer
    type: transform
    transform_type: sql
    source: v_customer_changes
    target: v_customer_cleansed
    sql: |
      SELECT
        customer_id,
        UPPER(TRIM(customer_name)) as customer_name,
        LOWER(TRIM(email)) as email,
        _change_type,
        _commit_timestamp
      FROM {source}
      
  - name: save_customer_dimension
    type: write
    source: v_customer_cleansed
    write_target:
      type: streaming_table
      mode: cdc
      database: "{env}_{silver_schema}_sales"
      table: dim_customer
      create_table: true
      cdc_config:
        keys: [customer_id]
        sequence_by: _commit_timestamp
        scd_type: 2
        track_history_columns: [customer_name, email]
""")
        
        # Generate pipeline
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("sales_silver", "dev")
        
        # Verify generated code
        assert "customer_dimensions.py" in generated_files
        code = generated_files["customer_dimensions.py"]
        
        # Check for Delta CDC source
        assert 'option("readChangeFeed", "true")' in code
        assert "dev_bronze_sales.customer_raw" in code
        
        # Check for SQL transformation
        assert "UPPER(TRIM(customer_name))" in code
        assert "LOWER(TRIM(email))" in code
        
        # Check for CDC write (auto_cdc)
        assert "dlt.create_streaming_table" in code  # Table must be created first
        assert 'name="dev_silver_sales.dim_customer"' in code
        assert "dlt.create_auto_cdc_flow" in code
        assert 'keys=["customer_id"]' in code
        assert 'sequence_by="_commit_timestamp"' in code
        assert "scd_type=2" in code
    
    def test_template_usage(self, temp_project):
        """Test template system as per requirements."""
        project_root = self.create_project_structure(temp_project)
        
        # Create bronze ingestion template as per requirements
        (project_root / "templates" / "bronze_ingestion.yaml").write_text("""
name: bronze_ingestion
version: "1.0"
description: "Standard bronze ingestion template"

parameters:
  - name: source_path
    required: true
  - name: file_format
    required: true
  - name: table_name
    required: true
  - name: schema
    required: true

actions:
  - name: load_{{ table_name }}_raw
    type: load
    source:
      type: cloudfiles
      path: "{{ source_path }}"
      format: "{{ file_format }}"
    target: v_{{ table_name }}_raw

  - name: add_{{ table_name }}_metadata
    type: transform
    transform_type: sql
    source: v_{{ table_name }}_raw
    target: v_{{ table_name }}_with_metadata
    sql: |
      SELECT 
        *,
        current_timestamp() as _ingestion_timestamp,
        input_file_name() as _source_file
      FROM {% raw %}{{ source }}{% endraw %}

  - name: save_{{ table_name }}_bronze
    type: write
    source: v_{{ table_name }}_with_metadata
    write_target:
      type: streaming_table
      mode: append_flow
      database: "{env}_bronze_{{ schema }}"
      table: "{{ table_name }}_raw"
      create_table: true
""")
        
        # Create substitutions
        (project_root / "substitutions" / "dev.yaml").write_text("""
dev:
  env: dev
""")
        
        # Create flowgroup using template
        pipeline_dir = project_root / "pipelines" / "orders_bronze"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "order_ingestion.yaml").write_text("""
pipeline: orders_bronze
flowgroup: order_ingestion

use_template: bronze_ingestion
template_parameters:
  source_path: "/mnt/landing/{env}/orders/*.json"
  file_format: json
  table_name: orders
  schema: sales
""")
        
        # Generate pipeline
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("orders_bronze", "dev")
        
        # Verify generated code
        assert "order_ingestion.py" in generated_files
        code = generated_files["order_ingestion.py"]
        
        # Check that template was expanded correctly
        assert "v_orders_raw" in code
        assert "v_orders_with_metadata" in code
        assert "/mnt/landing/dev/orders/*.json" in code
        assert "dev_bronze_sales.orders_raw" in code
        assert "_ingestion_timestamp" in code
        assert "_source_file" in code
    
    def test_data_quality_expectations(self, temp_project):
        """Test data quality expectations integration."""
        project_root = self.create_project_structure(temp_project)
        
        # Create expectations file as per requirements
        (project_root / "expectations" / "customer_quality.json").write_text(json.dumps({
            "version": "1.0",
            "expectations": [
                {
                    "name": "not_null_id",
                    "expression": "customer_id IS NOT NULL",
                    "failureAction": "fail"
                },
                {
                    "name": "valid_email",
                    "expression": "email RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$'",
                    "failureAction": "drop"
                },
                {
                    "name": "positive_amount",
                    "expression": "amount >= 0",
                    "failureAction": "warn"
                }
            ]
        }, indent=2))
        
        # Create flowgroup with data quality action
        pipeline_dir = project_root / "pipelines" / "customer_quality"
        pipeline_dir.mkdir(parents=True)
        
        (pipeline_dir / "customer_validation.yaml").write_text("""
pipeline: customer_quality
flowgroup: customer_validation

actions:
  - name: load_customers
    type: load
    source:
      type: sql
      sql: "SELECT * FROM raw_customers"
    target: v_customers_raw
    
  - name: validate_customers
    type: transform
    transform_type: data_quality
    source: v_customers_raw
    target: v_customers_validated
    expectations_file: "expectations/customer_quality.json"
    
  - name: save_validated_customers
    type: write
    source: v_customers_validated
    write_target:
      type: streaming_table
      database: "bronze"
      table: customers_validated
      create_table: true
""")
        
        # Generate pipeline
        orchestrator = ActionOrchestrator(project_root)
        generated_files = orchestrator.generate_pipeline("customer_quality", "dev")
        
        # Verify generated code
        code = generated_files["customer_validation.py"]
        
        # Check for DLT expectations
        assert "@dlt.expect_all_or_fail" in code
        assert '"customer_id IS NOT NULL"' in code
        
        assert "@dlt.expect_all_or_drop" in code
        assert "email RLIKE" in code
        
        assert "@dlt.expect_all" in code
        assert "amount >= 0" in code 