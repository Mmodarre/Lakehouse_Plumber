"""E2E golden test: SCD1 CDC write target that declares a PRIMARY KEY via ``table_schema``.

Exercises a streaming-table write target in ``mode: cdc`` / ``scd_type: 1`` whose
``table_schema`` contains a ``CONSTRAINT ... PRIMARY KEY`` and, correctly for Type 1, *no*
``__START_AT`` / ``__END_AT`` history columns. This is the reported case: ``table_schema`` is
the only way to register a PRIMARY KEY on a pipeline-managed table, but the schema validator
used to demand the SCD2 validity columns for every ``mode: cdc`` target, so an SCD1 dimension
could never declare a PK. Both the inline-DDL form (the reported reproduction) and the
file-reference form are covered — the latter proves the resolver path is gated too and the old
"tokens in the filename" workaround is no longer needed.

The pipeline and its schema are injected into an isolated *copy* of the shared fixture project
at runtime (never committed to the shared fixture), so this test does not perturb the
whole-project baselines other e2e suites compare against.
"""

import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli

# Inline DDL: surrogate-key PRIMARY KEY, business key, one attribute — and no
# __START_AT/__END_AT (those must not exist on an SCD Type 1 target).
_INLINE_SCHEMA = (
    "customer_sk STRING NOT NULL,\n"
    "customer_id BIGINT NOT NULL,\n"
    "c_name STRING,\n"
    "CONSTRAINT dim_customer_pk PRIMARY KEY (customer_sk)"
)

_INLINE_FLOWGROUP = """\
pipeline: 23_cdc_scd1_pk
flowgroup: cdc_scd1_pk_inline
actions:
  - name: load_customer_bronze
    type: load
    readMode: stream
    source:
      type: delta
      database: "{catalog}.{bronze_schema}"
      table: customer
    target: v_customer_bronze
    description: "Stream customer table from the bronze schema"

  - name: write_dim_customer
    type: write
    source: v_customer_bronze
    write_target:
      type: streaming_table
      database: "{catalog}.{silver_schema}"
      table: dim_customer_scd1_inline
      mode: cdc
      cdc_config:
        keys: ["customer_id"]
        sequence_by: "last_modified_dt"
        scd_type: 1
      table_schema: |
        customer_sk STRING NOT NULL,
        customer_id BIGINT NOT NULL,
        c_name STRING,
        CONSTRAINT dim_customer_pk PRIMARY KEY (customer_sk)
"""

# A ``.ddl`` sidecar with the same PK-bearing, validity-column-free schema.
_DDL_SCHEMA_FILE = _INLINE_SCHEMA + "\n"

_FILE_FLOWGROUP = """\
pipeline: 23_cdc_scd1_pk
flowgroup: cdc_scd1_pk_file
actions:
  - name: load_customer_bronze_file
    type: load
    readMode: stream
    source:
      type: delta
      database: "{catalog}.{bronze_schema}"
      table: customer
    target: v_customer_bronze_file
    description: "Stream customer table from the bronze schema"

  - name: write_dim_customer_file
    type: write
    source: v_customer_bronze_file
    write_target:
      type: streaming_table
      database: "{catalog}.{silver_schema}"
      table: dim_customer_scd1_file
      mode: cdc
      cdc_config:
        keys: ["customer_id"]
        sequence_by: "last_modified_dt"
        scd_type: 1
      table_schema: "schemas/dim_customer_scd1.ddl"
"""


@pytest.mark.e2e
class TestCdcScd1PrimaryKeyE2E:
    """E2E test for an SCD1 CDC write target declaring a PRIMARY KEY via ``table_schema``."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"

        self._init_bundle_project()

        # Inject the pipeline(s) + schema file under test into THIS copy only, so the
        # shared committed fixture (and every whole-project baseline) is untouched.
        pipeline_dir = self.project_root / "pipelines" / "23_cdc_scd1_pk"
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        (pipeline_dir / "cdc_scd1_pk_inline.yaml").write_text(_INLINE_FLOWGROUP)
        (pipeline_dir / "cdc_scd1_pk_file.yaml").write_text(_FILE_FLOWGROUP)
        (self.project_root / "schemas" / "dim_customer_scd1.ddl").write_text(
            _DDL_SCHEMA_FILE
        )

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def run_generate(self) -> tuple:
        """Run 'lhp generate --env dev' (no --include-tests)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        return result.exit_code, result.output

    def test_scd1_cdc_inline_schema_with_primary_key(self):
        """Inline table_schema with a PRIMARY KEY and no validity columns generates cleanly."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed:\n{output}"

        generated = self.generated_dir / "23_cdc_scd1_pk" / "cdc_scd1_pk_inline.py"
        assert generated.exists(), (
            "cdc_scd1_pk_inline.py should be generated under 23_cdc_scd1_pk/"
        )

        text = generated.read_text()
        assert "dp.create_streaming_table(" in text
        assert "dp.create_auto_cdc_flow(" in text
        assert "stored_as_scd_type=1" in text
        # The PRIMARY KEY constraint is preserved in the emitted schema.
        assert "PRIMARY KEY" in text
        # SCD Type 1 must not carry the SCD2 history/validity columns.
        assert "__START_AT" not in text
        assert "__END_AT" not in text

    def test_scd1_cdc_file_schema_with_primary_key(self):
        """A file-based (.ddl) table_schema with a PRIMARY KEY is resolved and gated too."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed:\n{output}"

        generated = self.generated_dir / "23_cdc_scd1_pk" / "cdc_scd1_pk_file.py"
        assert generated.exists(), (
            "cdc_scd1_pk_file.py should be generated under 23_cdc_scd1_pk/"
        )

        text = generated.read_text()
        assert "dp.create_streaming_table(" in text
        assert "dp.create_auto_cdc_flow(" in text
        assert "stored_as_scd_type=1" in text
        assert "PRIMARY KEY" in text
        assert "__START_AT" not in text
        assert "__END_AT" not in text
