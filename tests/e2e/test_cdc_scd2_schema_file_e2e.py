"""E2E golden test: SCD2 CDC write target with a file-based ``table_schema``.

Exercises a streaming-table write target in ``mode: cdc`` / ``scd_type: 2`` whose
``table_schema`` points at a YAML schema file that already contains the DLT-required
``__START_AT`` / ``__END_AT`` history columns — proving the full path from YAML through
validation to generated Python works.

The pipeline and its schema file are injected into an isolated *copy* of the shared
fixture project at runtime (never committed to the shared fixture), so this test does
not perturb the whole-project baselines other e2e suites compare against.
"""

import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli

_SCHEMA_FILE = """\
name: customer_scd2_dim
version: "1.0"
description: "Customer SCD2 dimension schema (with __START_AT/__END_AT)"
columns:
  - name: customer_id
    type: BIGINT
    nullable: false
    comment: "Customer key - CDC key"
  - name: c_name
    type: STRING
    nullable: true
  - name: c_address
    type: STRING
    nullable: true
  - name: last_modified_dt
    type: TIMESTAMP
    nullable: true
    comment: "CDC sequence_by column"
  - name: __START_AT
    type: TIMESTAMP
    nullable: true
    comment: "SCD2 history start (same type as sequence_by)"
  - name: __END_AT
    type: TIMESTAMP
    nullable: true
    comment: "SCD2 history end (same type as sequence_by)"
"""

_FLOWGROUP = """\
pipeline: 22_cdc_scd2_schema
flowgroup: cdc_scd2_schema_file
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

  - name: write_customer_scd2
    type: write
    source: v_customer_bronze
    write_target:
      type: streaming_table
      database: "{catalog}.{silver_schema}"
      table: customer_scd2_dim
      mode: cdc
      table_schema: "schemas/customer_scd2_dim.yaml"
      cdc_config:
        keys: ["customer_id"]
        sequence_by: "last_modified_dt"
        scd_type: 2
"""


@pytest.mark.e2e
class TestCdcScd2SchemaFileE2E:
    """E2E test for SCD2 CDC with a YAML-file ``table_schema``."""

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

        # Inject the pipeline + schema file under test into THIS copy only, so the
        # shared committed fixture (and every whole-project baseline) is untouched.
        (self.project_root / "schemas" / "customer_scd2_dim.yaml").write_text(
            _SCHEMA_FILE
        )
        pipeline_dir = self.project_root / "pipelines" / "22_cdc_scd2_schema"
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        (pipeline_dir / "cdc_scd2_schema_file.yaml").write_text(_FLOWGROUP)

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

    def test_scd2_cdc_with_yaml_table_schema_file(self):
        """SCD2 CDC write target whose table_schema is a YAML file reference generates cleanly."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed:\n{output}"

        generated = (
            self.generated_dir / "22_cdc_scd2_schema" / "cdc_scd2_schema_file.py"
        )
        assert generated.exists(), (
            "cdc_scd2_schema_file.py should be generated under 22_cdc_scd2_schema/"
        )

        # Constructive assertions: the file-based table_schema (incl. __START_AT /
        # __END_AT) is resolved and emitted, and the SCD2 CDC flow is generated.
        text = generated.read_text()
        assert "dp.create_streaming_table(" in text
        assert "dp.create_auto_cdc_flow(" in text
        assert "stored_as_scd_type=2" in text
        assert "__START_AT" in text
        assert "__END_AT" in text
