"""Tests for substitution support in Python functions and SQL files."""

import tempfile
from pathlib import Path

import pytest

from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.generators.load.custom_datasource import CustomDataSourceLoadGenerator
from lhp.generators.load.sql import SQLLoadGenerator
from lhp.generators.transform.python import PythonTransformGenerator
from lhp.generators.transform.sql import SQLTransformGenerator
from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models import Action, ActionType, FlowGroup


class TestSnapshotCDCFunctionSubstitution:
    """Test the copy-and-import contract for snapshot CDC source functions.

    Snapshot CDC source functions are no longer inlined into the generated
    flowgroup file. The generator now copies the user's module into
    ``custom_python_functions/<stem>.py`` (once per pipeline) and imports it
    under a ``_snap_<stem>`` alias; ``source=`` references the alias-qualified
    function. Body-level token/secret substitution is the copier's job and is
    only performed when ``output_dir`` is a real path (covered by the
    copier-level tests). These generator tests run with ``output_dir=None``
    (dry-run): no file is written and the body is never substituted here, so
    they verify only the import/alias/no-inline contract.
    """

    def _snapshot_context(self, substitution_mgr):
        """Build a generation context for the copy-and-import dry-run path.

        ``flowgroup`` is required by ``copy_user_module_for_pipeline``;
        ``output_dir=None`` runs the copy in dry-run mode (leaf name resolved,
        no file written, no body substitution). A fresh per-pipeline signature
        cache is also supplied. Mirrors ``_make_context`` in
        ``tests/unit/test_snapshot_cdc_parameters.py``.
        """
        return {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            "output_dir": None,
            "source_function_signature_cache": {},
        }

    def test_snapshot_cdc_function_copy_and_import_contract(self):
        """A function file is imported (not inlined) under a _snap_ alias.

        Previously this test asserted that ``{catalog}``/``{bronze_schema}``
        tokens were substituted into the inlined function body. Body inlining
        is gone; the contract now is: the module is imported under
        ``_snap_<stem>`` and ``source=`` references that alias.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_snapshot_and_version(latest_snapshot_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    if latest_snapshot_version is None:
        df = spark.sql('''
            SELECT * FROM {catalog}.{bronze_schema}.part
            WHERE snapshot_id = (SELECT min(snapshot_id) FROM {catalog}.{bronze_schema}.part)
        ''')
        return (df, 1)

    return None
""")
            function_file = f.name

        stem = Path(function_file).stem
        alias = f"_snap_{stem}"

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {"catalog": "test_catalog", "bronze_schema": "test_bronze"}
        )

        action = Action(
            name="write_part_silver_snapshot",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "part_dim",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": function_file,
                        "function": "next_snapshot_and_version",
                    },
                    "keys": ["part_id"],
                    "stored_as_scd_type": 2,
                },
            },
        )

        context = self._snapshot_context(substitution_mgr)
        generator = StreamingTableWriteGenerator()

        try:
            code = generator.generate(action, context)

            # Module is imported under the _snap_ alias, not inlined.
            assert (
                f"import custom_python_functions.{stem} as {alias}"
                in generator._imports
            )
            # source= references the alias-qualified function.
            assert f"{alias}.next_snapshot_and_version," in code
            assert "dp.create_auto_cdc_from_snapshot_flow(" in code

            # The function body is never inlined into the generated flowgroup.
            assert "def next_snapshot_and_version" not in code
            # On the dry-run path the body is not processed, so its tokens are
            # never seen by the generator at all (no inline, no substitution).
            assert "test_catalog.test_bronze.part" not in code
            assert "spark.sql" not in code

        finally:
            Path(function_file).unlink()

    def test_snapshot_cdc_function_secret_body_not_inlined(self):
        """A function body containing ${secret:...} is imported, not inlined.

        Previously this test asserted ``__SECRET_..._`` placeholders appeared
        in the generated code (proving body substitution). On the dry-run
        copy path the body is not processed, so no body-derived placeholders
        or secret tracking occur here; that coverage now lives in the
        copier-level tests. This test verifies only that the body is imported,
        not inlined or substituted.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_snapshot_with_secrets(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    catalog = "${secret:db_config/catalog}"
    bronze_schema = "${secret:db_config/bronze_schema}"

    if latest_version is None:
        df = spark.sql(f'''
            SELECT * FROM {catalog}.{bronze_schema}.part
            WHERE snapshot_id = 1
        ''')
        return (df, 1)

    return None
""")
            function_file = f.name

        stem = Path(function_file).stem
        alias = f"_snap_{stem}"

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.default_secret_scope = "db_config"

        action = Action(
            name="write_part_with_secrets",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "part_dim",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": function_file,
                        "function": "next_snapshot_with_secrets",
                    },
                    "keys": ["part_id"],
                    "stored_as_scd_type": 2,
                },
            },
        )

        context = self._snapshot_context(substitution_mgr)
        generator = StreamingTableWriteGenerator()

        try:
            code = generator.generate(action, context)

            # Imported under alias; body absent from generated flowgroup.
            assert (
                f"import custom_python_functions.{stem} as {alias}"
                in generator._imports
            )
            assert f"{alias}.next_snapshot_with_secrets," in code
            assert "dp.create_auto_cdc_from_snapshot_flow(" in code
            assert "def next_snapshot_with_secrets" not in code

            # Body is not processed on the dry-run path: no inlined secret
            # placeholders and no body-derived secret tracking here. (Body
            # substitution/secret tracking is covered at the copier layer.)
            assert "__SECRET_db_config_catalog__" not in code
            assert "${secret:db_config/catalog}" not in code

        finally:
            Path(function_file).unlink()

    def test_snapshot_cdc_function_mixed_body_not_inlined(self):
        """A body mixing tokens and secrets is imported, not inlined.

        Previously asserted both token substitution and ``__SECRET_..._``
        placeholders in the body. Body processing no longer happens on this
        path; this test verifies the import/alias/no-inline contract.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_snapshot_mixed(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    api_key = "${secret:api/key}"

    if latest_version is None:
        df = spark.sql(f'''
            SELECT * FROM {catalog}.{bronze_schema}.part
            WHERE snapshot_id = 1
            AND source = '{environment}'
        ''')
        return (df, 1)

    return None
""")
            function_file = f.name

        stem = Path(function_file).stem
        alias = f"_snap_{stem}"

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {
                "catalog": "prod_catalog",
                "bronze_schema": "prod_bronze",
                "environment": "production",
            }
        )
        substitution_mgr.default_secret_scope = "api"

        action = Action(
            name="write_part_mixed",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "part_dim",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": function_file,
                        "function": "next_snapshot_mixed",
                    },
                    "keys": ["part_id"],
                    "stored_as_scd_type": 2,
                },
            },
        )

        context = self._snapshot_context(substitution_mgr)
        generator = StreamingTableWriteGenerator()

        try:
            code = generator.generate(action, context)

            assert (
                f"import custom_python_functions.{stem} as {alias}"
                in generator._imports
            )
            assert f"{alias}.next_snapshot_mixed," in code
            assert "dp.create_auto_cdc_from_snapshot_flow(" in code
            assert "def next_snapshot_mixed" not in code

            # Neither token substitution nor secret placeholders appear in the
            # generated code: the body is not processed on the dry-run path.
            assert "prod_catalog.prod_bronze.part" not in code
            assert "__SECRET_api_key__" not in code

        finally:
            Path(function_file).unlink()

    def test_snapshot_cdc_function_no_substitution_backward_compatibility(self):
        """A function with no substitution variables is imported, not inlined.

        Previously asserted the original body text survived inline. The body
        is no longer inlined; the contract is import-under-alias plus the
        snapshot flow call.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_snapshot_plain(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.sql('''
            SELECT * FROM raw.customer_snapshots
            WHERE snapshot_id = 1
        ''')
        return (df, 1)

    return None
""")
            function_file = f.name

        stem = Path(function_file).stem
        alias = f"_snap_{stem}"

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {"catalog": "test_catalog", "bronze_schema": "test_bronze"}
        )

        action = Action(
            name="write_part_plain",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "part_dim",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": function_file,
                        "function": "next_snapshot_plain",
                    },
                    "keys": ["part_id"],
                    "stored_as_scd_type": 2,
                },
            },
        )

        context = self._snapshot_context(substitution_mgr)
        generator = StreamingTableWriteGenerator()

        try:
            code = generator.generate(action, context)

            assert (
                f"import custom_python_functions.{stem} as {alias}"
                in generator._imports
            )
            assert f"{alias}.next_snapshot_plain," in code
            assert "dp.create_auto_cdc_from_snapshot_flow(" in code

            # Body is imported, not inlined.
            assert "def next_snapshot_plain" not in code
            assert "raw.customer_snapshots" not in code

        finally:
            Path(function_file).unlink()


class TestSQLFileSubstitution:
    """Test substitution in external SQL files."""

    def test_sql_load_generator_token_substitution(self):
        """Test {token} substitution in SQL load files."""
        # Create a temporary SQL file with substitution variables
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("""
            SELECT
                customer_id,
                customer_name,
                email,
                '{environment}' as source_env
            FROM {catalog}.{bronze_schema}.customers
            WHERE
                active = true
                AND created_date >= '{start_date}'
            """)
            sql_file = f.name

        # Create substitution manager with test values
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {
                "catalog": "test_catalog",
                "bronze_schema": "test_bronze",
                "environment": "dev",
                "start_date": "2024-01-01",
            }
        )

        # Create SQL load action
        action = Action(
            name="load_customers_sql",
            type=ActionType.LOAD,
            source={"type": "sql", "sql_path": sql_file},
            target="v_customers_filtered",
        )

        # Create context with substitution manager
        context = {
            "substitution_manager": substitution_mgr,
            "spec_dir": Path(sql_file).parent,
            "secret_references": set(),
        }

        generator = SQLLoadGenerator()

        try:
            # Generate code - this should apply substitutions
            code = generator.generate(action, context)

            # Verify substitutions were applied
            assert "test_catalog.test_bronze.customers" in code, (
                f"Expected substituted schema in: {code}"
            )
            assert "'dev' as source_env" in code, (
                f"Expected substituted environment in: {code}"
            )
            assert ">= '2024-01-01'" in code, f"Expected substituted date in: {code}"

            # Verify no unsubstituted tokens remain
            assert "{catalog}" not in code, (
                f"Unsubstituted {{catalog}} found in: {code}"
            )
            assert "{bronze_schema}" not in code, (
                f"Unsubstituted {{bronze_schema}} found in: {code}"
            )
            assert "{environment}" not in code, (
                f"Unsubstituted {{environment}} found in: {code}"
            )
            assert "{start_date}" not in code, (
                f"Unsubstituted {{start_date}} found in: {code}"
            )

            # Verify structure is preserved
            assert "@dp.temporary_view()" in code
            assert "def v_customers_filtered():" in code
            assert "spark.sql" in code

        finally:
            # Clean up temp file
            Path(sql_file).unlink()

    def test_sql_transform_generator_token_substitution(self):
        """Test {token} substitution in SQL transform files."""
        # Create a temporary SQL file with substitution variables
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("""
            SELECT
                c.customer_id,
                UPPER(TRIM(c.customer_name)) as customer_name,
                LOWER(TRIM(c.email)) as email,
                c.created_date,
                '{pipeline_version}' as version
            FROM {staging_view} c
            WHERE
                c.email IS NOT NULL
                AND c.created_date >= '{cutoff_date}'
            """)
            sql_file = f.name

        # Create substitution manager with test values
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {
                "staging_view": "v_customers_staging",
                "pipeline_version": "v2.1",
                "cutoff_date": "2023-12-01",
            }
        )

        # Create SQL transform action
        action = Action(
            name="transform_customers_clean",
            type=ActionType.TRANSFORM,
            source="v_customers_raw",
            target="v_customers_clean",
            sql_path=sql_file,
        )

        # Create context with substitution manager
        context = {
            "substitution_manager": substitution_mgr,
            "spec_dir": Path(sql_file).parent,
            "secret_references": set(),
        }

        generator = SQLTransformGenerator()

        try:
            # Generate code - this should apply substitutions
            code = generator.generate(action, context)

            # Verify substitutions were applied
            assert "FROM v_customers_staging c" in code, (
                f"Expected substituted staging view in: {code}"
            )
            assert "'v2.1' as version" in code, (
                f"Expected substituted version in: {code}"
            )
            assert ">= '2023-12-01'" in code, (
                f"Expected substituted cutoff date in: {code}"
            )

            # Verify no unsubstituted tokens remain
            assert "{staging_view}" not in code, (
                f"Unsubstituted {{staging_view}} found in: {code}"
            )
            assert "{pipeline_version}" not in code, (
                f"Unsubstituted {{pipeline_version}} found in: {code}"
            )
            assert "{cutoff_date}" not in code, (
                f"Unsubstituted {{cutoff_date}} found in: {code}"
            )

            # Verify structure is preserved
            assert "@dp.temporary_view(" in code  # Allow for comment parameter
            assert "def v_customers_clean():" in code
            assert "spark.sql" in code

        finally:
            # Clean up temp file
            Path(sql_file).unlink()

    def test_sql_file_secret_substitution(self):
        """Test ${secret:scope/key} substitution in SQL files.

        At the generator layer, secrets become ``__SECRET_scope_key__``
        placeholders. The post-pass (`SecretCodeGenerator`) decides on
        bare-call vs. f-string emission once the full flowgroup code is
        assembled.
        """
        # Create a temporary SQL file with secret references
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("""
            SELECT
                customer_id,
                customer_name,
                '${secret:env_config/environment}' as env_name,
                '${secret:build_info/version}' as build_version
            FROM {catalog}.bronze.customers
            WHERE api_key = '${secret:api_keys/customer_service}'
            """)
            sql_file = f.name

        # Create substitution manager with secret configuration
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({"catalog": "prod_catalog"})
        substitution_mgr.default_secret_scope = "env_config"

        # Create SQL load action
        action = Action(
            name="load_customers_with_secrets",
            type=ActionType.LOAD,
            source={"type": "sql", "sql_path": sql_file},
            target="v_customers_with_secrets",
        )

        # Create context with substitution manager
        context = {
            "substitution_manager": substitution_mgr,
            "spec_dir": Path(sql_file).parent,
            "secret_references": set(),
        }

        generator = SQLLoadGenerator()

        try:
            # Generate code - this applies substitution but not the post-pass
            code = generator.generate(action, context)

            # Verify token substitutions
            assert "prod_catalog.bronze.customers" in code, (
                f"Expected token substitution in: {code}"
            )
            assert "{catalog}" not in code, f"Unsubstituted token found in: {code}"

            # Placeholders are emitted at this layer.
            assert "__SECRET_env_config_environment__" in code, (
                f"Expected env placeholder in: {code}"
            )
            assert "__SECRET_build_info_version__" in code, (
                f"Expected build_info placeholder in: {code}"
            )
            assert "__SECRET_api_keys_customer_service__" in code, (
                f"Expected api_keys placeholder in: {code}"
            )

            # Raw ${secret:...} must be fully substituted.
            assert "${secret:env_config/environment}" not in code, (
                f"Unsubstituted secret found in: {code}"
            )
            assert "${secret:build_info/version}" not in code, (
                f"Unsubstituted secret found in: {code}"
            )
            assert "${secret:api_keys/customer_service}" not in code, (
                f"Unsubstituted secret found in: {code}"
            )

            # Bare dbutils calls only after the post-pass.
            assert "dbutils.secrets.get" not in code

            # Verify secret references were tracked
            assert len(substitution_mgr.secret_references) > 0, (
                "Expected secret references to be tracked"
            )

        finally:
            # Clean up temp file
            Path(sql_file).unlink()

    def test_sql_file_no_substitution_backward_compatibility(self):
        """Test that SQL files without substitution variables work unchanged."""
        # Create a SQL file without any substitution variables
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("""
            SELECT
                customer_id,
                customer_name,
                email,
                'production' as environment
            FROM raw.customers
            WHERE
                active = true
                AND created_date >= '2024-01-01'
            """)
            sql_file = f.name

        # Create substitution manager (but SQL doesn't use it)
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {"catalog": "test_catalog", "schema": "test_schema"}
        )

        # Create SQL load action
        action = Action(
            name="load_customers_plain",
            type=ActionType.LOAD,
            source={"type": "sql", "sql_path": sql_file},
            target="v_customers_plain",
        )

        # Create context with substitution manager
        context = {
            "substitution_manager": substitution_mgr,
            "spec_dir": Path(sql_file).parent,
            "secret_references": set(),
        }

        generator = SQLLoadGenerator()

        try:
            # Generate code
            code = generator.generate(action, context)

            # Verify original content is preserved exactly
            assert "raw.customers" in code, f"Expected original table name in: {code}"
            assert "'production' as environment" in code, (
                f"Expected original environment in: {code}"
            )
            assert ">= '2024-01-01'" in code, f"Expected original date in: {code}"

            # Verify structure is preserved
            assert "@dp.temporary_view()" in code
            assert "def v_customers_plain():" in code
            assert "spark.sql" in code

        finally:
            # Clean up temp file
            Path(sql_file).unlink()


class TestPythonFileSubstitution:
    """Test substitution in Python files (transform and custom datasource)."""

    def test_python_transform_file_token_substitution(self):
        """Test {token} substitution in Python transform files."""
        # Create a temporary Python file with substitution variables
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

def enrich_customers(df: DataFrame, spark, parameters) -> DataFrame:
    '''Enrich customer data with environment-specific values.'''

    # Add environment-specific columns
    enriched_df = df.withColumn("environment", lit("{environment}"))
    enriched_df = enriched_df.withColumn("catalog_name", lit("{catalog}"))
    enriched_df = enriched_df.withColumn("processing_date", lit("{processing_date}"))

    # Apply business rule based on environment
    if "{environment}" == "prod":
        enriched_df = enriched_df.withColumn("priority", lit("high"))
    else:
        enriched_df = enriched_df.withColumn("priority", lit("normal"))

    return enriched_df
""")
            python_file = f.name

        # Create substitution manager with test values
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {
                "environment": "dev",
                "catalog": "test_catalog",
                "processing_date": "2024-01-15",
            }
        )

        # Create Python transform action
        action = Action(
            name="transform_enrich_customers",
            type=ActionType.TRANSFORM,
            source="v_customers_raw",
            target="v_customers_enriched",
            module_path=python_file,
            function_name="enrich_customers",
            parameters={"param1": "value1"},
        )

        # Create context with substitution manager
        context = {
            "substitution_manager": substitution_mgr,
            "spec_dir": Path(python_file).parent,
            "secret_references": set(),
            "flowgroup": type(
                "FlowGroup",
                (),
                {"pipeline": "test_pipeline", "flowgroup": "test_flowgroup"},
            )(),
            "output_dir": Path(python_file).parent / "test_output",
        }

        generator = PythonTransformGenerator()

        try:
            # Generate code - this should apply substitutions to the copied file
            code = generator.generate(action, context)

            # The generated code should have the view function
            assert "def v_customers_enriched():" in code, (
                f"Expected view function in: {code}"
            )
            assert "enrich_customers(v_customers_raw_df, spark, parameters)" in code, (
                f"Expected function call in: {code}"
            )

            # Check that the copied file has substitutions applied
            copied_file = (
                context["output_dir"]
                / "custom_python_functions"
                / f"{Path(python_file).stem}.py"
            )

            # Verify the file was copied and substitutions applied
            assert copied_file.exists(), (
                f"Expected copied file to exist at: {copied_file}"
            )
            copied_content = copied_file.read_text()

            # Verify substitutions were applied in the copied file
            assert 'lit("dev")' in copied_content, (
                f"Expected substituted environment in copied file: {copied_content}"
            )
            assert 'lit("test_catalog")' in copied_content, (
                f"Expected substituted catalog in copied file: {copied_content}"
            )
            assert 'lit("2024-01-15")' in copied_content, (
                f"Expected substituted date in copied file: {copied_content}"
            )
            assert 'if "dev" ==' in copied_content, (
                f"Expected substituted condition in copied file: {copied_content}"
            )

            # Verify no unsubstituted tokens remain
            assert '"{environment}"' not in copied_content, (
                "Unsubstituted {environment} found in copied file"
            )
            assert '"{catalog}"' not in copied_content, (
                "Unsubstituted {catalog} found in copied file"
            )
            assert '"{processing_date}"' not in copied_content, (
                "Unsubstituted {processing_date} found in copied file"
            )

        finally:
            # Clean up temp files
            Path(python_file).unlink()
            # Clean up output directory if it exists
            if context["output_dir"].exists():
                import shutil

                shutil.rmtree(context["output_dir"])

    def test_custom_datasource_file_token_substitution(self, tmp_path):
        """Test {token} substitution in custom datasource files.

        Under the copy-and-import pattern, substitution is applied to the
        contents of the user file as it is copied into
        ``custom_python_functions/`` next to the generated pipeline file.
        Verify the resulting copied file has tokens resolved.
        """
        from lhp.models import FlowGroup

        # Create a custom datasource source file with substitution tokens.
        source_file = tmp_path / "api_source.py"
        source_file.write_text("""
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit

class APIDataSource:
    '''Custom data source for API integration.'''

    def __init__(self):
        self.api_endpoint = "{api_endpoint}"
        self.environment = "{environment}"
        self.catalog = "{catalog}"

    @classmethod
    def name(cls):
        return "api_source"

    def load(self, spark) -> DataFrame:
        data = [("c1", "John", self.environment)]
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("environment", StringType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("source_catalog", lit(self.catalog))
        return df
""")

        # Create substitution manager with test values
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {
                "api_endpoint": "https://test-api.example.com",
                "environment": "staging",
                "catalog": "staging_catalog",
            }
        )

        action = Action(
            name="load_api_customers",
            type=ActionType.LOAD,
            source={
                "type": "custom_datasource",
                "module_path": "api_source.py",
                "custom_datasource_class": "APIDataSource",
                "options": {"timeout": 30},
            },
            target="v_api_customers",
        )

        flowgroup = FlowGroup(pipeline="p_test", flowgroup="fg_test")
        output_dir = tmp_path / "generated"
        output_dir.mkdir()

        context = {
            "substitution_manager": substitution_mgr,
            "spec_dir": tmp_path,
            "output_dir": output_dir,
            "flowgroup": flowgroup,
            "secret_references": set(),
        }

        generator = CustomDataSourceLoadGenerator()
        code = generator.generate(action, context)

        # Generated pipeline file imports the class; doesn't inline it.
        assert "@dp.temporary_view()" in code
        assert "def v_api_customers():" in code
        assert "class APIDataSource" not in code

        # Substituted contents land in the COPIED file.
        copied = (output_dir / "custom_python_functions" / "api_source.py").read_text()
        assert '"https://test-api.example.com"' in copied
        assert '"staging"' in copied
        assert '"staging_catalog"' in copied

        # No unsubstituted tokens remain.
        assert '"{api_endpoint}"' not in copied
        assert '"{environment}"' not in copied
        assert '"{catalog}"' not in copied

        # Class structure preserved.
        assert "class APIDataSource:" in copied
