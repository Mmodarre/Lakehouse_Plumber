"""Integration tests for LakehousePlumber based on requirements."""

import json
import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.api import LakehousePlumberApplicationFacade
from lhp.cli.main import cli
from lhp.models import Action, ActionType, FlowGroup
from lhp.parsers.yaml_parser import YAMLParser
from tests.helpers import read_generated_pipeline


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
            "presets",
            "templates",
            "pipelines",
            "substitutions",
            "schemas",
            "expectations",
            "generated",
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
  operational_metadata: ["_ingestion_timestamp", "_pipeline_name"]
  
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
      path: "${landing_path}/customer/*.json"
      format: json
    description: "Load raw customer data"
  
  - name: write_customer_bronze
    type: write
    source: v_customer_raw
    write_target:
      type: streaming_table
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: customer_raw
      create_table: true
    description: "Write to bronze customer table"
""")

        # Generate pipeline
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        generated_files = read_generated_pipeline(
            facade,
            pipeline_field="sales_bronze",
            env="dev",
            output_dir=project_root / "generated",
        )

        # Verify generated code
        assert "customer_ingestion.py" in generated_files
        code = generated_files["customer_ingestion.py"]

        # Check for required elements from requirements
        assert "@dp.temporary_view()" in code
        assert "spark.readStream" in code
        assert "cloudFiles" in code
        assert "/mnt/dev/landing/customer/*.json" in code
        assert "dp.create_streaming_table(" in code  # Using append flow API
        assert "dev_catalog.bronze.customer_raw" in code

        # Check for operational metadata (enabled in preset)
        assert "_ingestion_timestamp" in code
        assert "F.current_timestamp()" in code
        assert "_pipeline_name" in code
        assert "from pyspark.sql import functions as F" in code

    def test_jdbc_source_with_secrets(self, temp_project):
        """Test JDBC source with secret management as per requirements.

        Pins the runtime-correct emission shape: bare ``dbutils.secrets.get(...)``
        calls for entire-value fields (user, password) and an f-string for
        the URL where the secret is embedded mid-literal. Explicitly checks
        that the wrapped-string regression form is absent — a string literal
        containing the call text is what broke JDBC auth in v0.8.7 §2.
        """
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
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: customers_raw
      create_table: true
""")

        # Generate pipeline
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        generated_files = read_generated_pipeline(
            facade,
            pipeline_field="customer_ingestion",
            env="prod",
            output_dir=project_root / "generated",
        )

        # Verify generated code
        assert "external_customer_load.py" in generated_files
        code = generated_files["external_customer_load.py"]

        # JDBC scaffolding still present
        assert "spark.read" in code
        assert '.format("jdbc")' in code

        # Entire-value secrets must be emitted as bare dbutils calls.
        # The wrapped-string form (which v0.8.7 §2 introduced and Option 1
        # reverts) would break JDBC auth at runtime — auth would receive
        # the literal call text instead of the resolved secret.
        # Wrap-tolerant: black may break the .option(...) call across lines
        # when the full single-line form is >88 chars (e.g. the password
        # variant comes out at 89 chars, one over the configured limit).
        # Normalise ALL whitespace to nothing so the wrapped form
        # ``.option(\n    "password",\n    dbutils...)`` collapses to the
        # same shape as the single-line form. The load-bearing distinction
        # (bare ``dbutils.secrets.get(...)`` vs string-wrapped
        # ``"dbutils.secrets.get..."``) is preserved by this normalisation
        # because the quote character is a structural difference, not a
        # whitespace one.
        import re

        compact_code = re.sub(r"\s+", "", code)

        def _compact(s: str) -> str:
            return re.sub(r"\s+", "", s)

        assert (
            _compact('.option("user", dbutils.secrets.get(scope="prod_db_secrets", key="username"))')
            in compact_code
        ), "Expected bare dbutils call for 'user'; got:\n" + code
        assert (
            _compact('.option("password", dbutils.secrets.get(scope="prod_db_secrets", key="password"))')
            in compact_code
        ), "Expected bare dbutils call for 'password'; got:\n" + code

        # Wrapped-string regression must not be present.
        for bad in (
            '.option("user", "dbutils.secrets.get',
            '.option("password", "dbutils.secrets.get',
            '.option("url", "dbutils.secrets.get',
        ):
            assert bad not in code, (
                f"Found wrapped-string regression ({bad!r}); code:\n" + code
            )

        # URL has the secret embedded mid-string, so the post-pass must
        # rewrite it as an f-string. Inside the f-expression the dbutils
        # call uses single quotes (no collision with the outer "...").
        assert (
            "f\"jdbc:postgresql://{dbutils.secrets.get(scope='prod_db_secrets', key='host')}:5432/customers\""
            in code
        ), "Expected f-string with embedded dbutils call for URL; got:\n" + code

        # Verify SQL query is included
        assert "SELECT" in code
        assert "customer_id" in code

        # Most importantly, verify the generated code is syntactically valid Python
        compile(code, "<string>", "exec")

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
  source: v_customer_changes
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
    readMode: stream
    source:
      type: delta
      catalog: "${env}"
      schema: "${bronze_schema}_sales"
      table: customer_raw
      options:
        readChangeFeed: "true"
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
      catalog: "${env}"
      schema: "${silver_schema}_sales"
      table: dim_customer
      create_table: true
      cdc_config:
        keys: [customer_id]
        sequence_by: _commit_timestamp
        scd_type: 2
        track_history_columns: [customer_name, email]
""")

        # Generate pipeline
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        generated_files = read_generated_pipeline(
            facade,
            pipeline_field="sales_silver",
            env="dev",
            output_dir=project_root / "generated",
        )

        # Verify generated code
        assert "customer_dimensions.py" in generated_files
        code = generated_files["customer_dimensions.py"]

        # Check for Delta CDC source
        assert 'option("readChangeFeed", "true")' in code
        assert "dev.bronze_sales.customer_raw" in code

        # Check for SQL transformation
        assert "UPPER(TRIM(customer_name))" in code
        assert "LOWER(TRIM(email))" in code

        # Check for CDC write (auto_cdc)
        assert "dp.create_streaming_table" in code  # Table must be created first
        assert 'name="dev.silver_sales.dim_customer"' in code
        assert "dp.create_auto_cdc_flow" in code
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
      FROM v_{{ table_name }}_raw

  - name: save_{{ table_name }}_bronze
    type: write
    source: v_{{ table_name }}_with_metadata
    write_target:
      type: streaming_table
      # mode defaults to "standard"
      catalog: "${env}"
      schema: "bronze_{{ schema }}"
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
  source_path: "/mnt/landing/${env}/orders/*.json"
  file_format: json
  table_name: orders
  schema: sales
""")

        # Generate pipeline
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        generated_files = read_generated_pipeline(
            facade,
            pipeline_field="orders_bronze",
            env="dev",
            output_dir=project_root / "generated",
        )

        # Verify generated code
        assert "order_ingestion.py" in generated_files
        code = generated_files["order_ingestion.py"]

        # Check that template was expanded correctly
        assert "v_orders_raw" in code
        assert "v_orders_with_metadata" in code
        assert "/mnt/landing/dev/orders/*.json" in code
        assert "dev.bronze_sales.orders_raw" in code
        assert "_ingestion_timestamp" in code
        assert "_source_file" in code

    def test_data_quality_expectations(self, temp_project):
        """Test data quality expectations integration."""
        project_root = self.create_project_structure(temp_project)

        # Create expectations file as per requirements
        (project_root / "expectations" / "customer_quality.json").write_text(
            json.dumps(
                {
                    "version": "1.0",
                    "expectations": [
                        {
                            "name": "not_null_id",
                            "expression": "customer_id IS NOT NULL",
                            "failureAction": "fail",
                        },
                        {
                            "name": "valid_email",
                            "expression": "email RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$'",
                            "failureAction": "drop",
                        },
                        {
                            "name": "positive_amount",
                            "expression": "amount >= 0",
                            "failureAction": "warn",
                        },
                    ],
                },
                indent=2,
            )
        )

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
      catalog: "test_cat"
      schema: "bronze"
      table: customers_validated
      create_table: true
""")

        # Generate pipeline
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        generated_files = read_generated_pipeline(
            facade,
            pipeline_field="customer_quality",
            env="dev",
            output_dir=project_root / "generated",
        )

        # Verify generated code
        code = generated_files["customer_validation.py"]

        # Check for DLT expectations
        assert "@dp.expect_all_or_fail" in code
        assert '"customer_id IS NOT NULL"' in code

        assert "@dp.expect_all_or_drop" in code
        assert "email RLIKE" in code

        assert "@dp.expect_all" in code
        assert "amount >= 0" in code
