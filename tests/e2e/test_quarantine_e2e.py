"""
End-to-end tests for quarantine mode in data quality transforms.

Tests the complete quarantine workflow: YAML config → lhp generate → generated
Python code with DLQ pipeline components (clean view, DLQ sink, append flow,
output view with UNION of clean + recycled).
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestQuarantineE2E:
    """E2E tests for quarantine mode data quality transforms."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create isolated copy of fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working directories."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def run_generate(self) -> tuple:
        """Run 'lhp generate --env dev --force'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["generate", "--env", "dev", "--force"])
        return result.exit_code, result.output

    def run_validate(self) -> tuple:
        """Run 'lhp validate --env dev'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--env", "dev"])
        return result.exit_code, result.output

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""

    # ========================================================================
    # TEST CASES
    # ========================================================================

    def test_quarantine_non_cloudfiles_generation(self):
        """Verify quarantine non-CloudFiles generates expected output matching baseline."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Verify the quarantine file was generated
        generated_file = (
            self.generated_dir / "acmi_edw_bronze" / "quarantine_flow.py"
        )
        assert generated_file.exists(), "quarantine_flow.py should be generated"

        # Compare with baseline
        baseline_file = (
            self.project_root
            / "generated_baseline"
            / "dev"
            / "acmi_edw_bronze"
            / "quarantine_flow.py"
        )
        assert baseline_file.exists(), "Baseline file should exist"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Baseline mismatch: {hash_diff}"

    def test_quarantine_generated_code_structure(self):
        """Verify quarantine output has all required DLQ pipeline components."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = (
            self.generated_dir / "acmi_edw_bronze" / "quarantine_flow.py"
        )
        code = generated_file.read_text()

        # Verify quarantine constants
        assert "_EXPECTATIONS_v_orders_raw" in code
        assert "_INVERSE_FILTER_v_orders_raw" in code
        assert "_FAILED_RULE_EXPRS_v_orders_raw" in code
        assert "DLQ_TABLE_v_orders_raw" in code
        assert "DLQ_OUTBOX_TABLE_v_orders_raw" in code
        assert "universal_dlq_outbox" in code
        assert "SOURCE_TABLE_v_orders_raw" in code
        assert "_EXPECTATIONS_RECYCLED_v_orders_raw" in code

        # Verify clean view with expect_all_or_drop
        assert "@dp.expect_all_or_drop(_EXPECTATIONS_v_orders_raw)" in code
        assert "def _clean_v_orders_raw():" in code

        # Verify DLQ sink
        assert "@dp.foreach_batch_sink" in code
        assert "DeltaTable.forName" in code
        assert ".whenNotMatchedInsertAll()" in code

        # Verify append flow for quarantine
        assert "quarantine_flow_v_orders_raw" in code
        assert "_INVERSE_FILTER_v_orders_raw" in code

        # Verify recycle sink + flow (inbox → outbox dedup)
        assert "recycle_sink_v_orders_raw" in code
        assert "recycle_flow_v_orders_raw" in code
        assert "Window.partitionBy" in code
        assert "skipChangeCommits" in code

        # Verify recycled view (outbox → validated)
        assert "_recycled_v_orders_raw" in code
        assert "@dp.expect_all_or_drop(_EXPECTATIONS_RECYCLED_" in code

        # Verify output view reads from _recycled_ (no direct CDF in output)
        assert "def v_orders_validated():" in code
        assert 'spark.readStream.table("_recycled_v_orders_raw")' in code
        assert "clean.union(recycled)" in code
        assert "try_variant_get" in code

        # Verify native Spark rescued_data handling (no UDF)
        assert 'if "_rescued_data" in batch_df.columns:' in code
        assert "variant_cols" in code
        assert "map_zip_with" in code
        assert "map_filter" in code
        assert "from_json" in code

        # Verify imports
        assert "from delta.tables import DeltaTable" in code
        assert "from pyspark.sql import functions as F" in code
        assert "from pyspark.sql.types import MapType, StringType" in code
        assert "from pyspark.sql.window import Window" in code

        # Verify hash exclusion columns
        assert "_HASH_EXCLUDE_COLS_v_orders_raw" in code
        assert "hash_cols" in code

        # Verify DQ section structure (separate from TRANSFORMATION VIEWS)
        assert "DATA QUALITY & QUARANTINE" in code
        assert "TRANSFORMATION VIEWS" not in code  # DQ-only flowgroup
        assert "Quarantine: v_orders_raw" in code
        assert "# --- Rules & constants ---" in code
        assert "# --- Clean path (provides DQ metrics in event log) ---" in code
        assert "# --- Quarantine path (DLQ sink + routing) ---" in code
        assert "# --- Recycle path (dedup inbox" in code
        assert "# --- Recycled path (outbox" in code
        assert "# --- Validated output (clean + recycled) ---" in code

    def test_quarantine_validate_passes(self):
        """Verify 'lhp validate' passes on valid quarantine config."""
        exit_code, output = self.run_validate()
        assert exit_code == 0, f"Validation failed: {output}"

    def test_quarantine_validate_fails_missing_block(self):
        """Verify validation fails when mode=quarantine but quarantine block missing."""
        # Modify the quarantine flowgroup to remove the quarantine block
        flowgroup_file = (
            self.project_root
            / "pipelines"
            / "02_bronze"
            / "quarantine_flow.yaml"
        )
        content = yaml.safe_load(flowgroup_file.read_text())

        # Find the DQ action and remove quarantine block
        for action in content["actions"]:
            if action.get("mode") == "quarantine":
                del action["quarantine"]
                break

        flowgroup_file.write_text(yaml.dump(content, default_flow_style=False))

        exit_code, output = self.run_validate()
        assert exit_code != 0, "Validation should fail without quarantine block"
        assert "quarantine" in output.lower(), (
            f"Error should mention quarantine: {output}"
        )

    def test_quarantine_runtime_rescued_data_detection(self):
        """Verify quarantine generates runtime _rescued_data detection (both branches)."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = (
            self.generated_dir / "acmi_edw_bronze" / "quarantine_flow.py"
        )
        code = generated_file.read_text()

        # Runtime if/else for _rescued_data with native Spark functions
        assert 'if "_rescued_data" in batch_df.columns:' in code
        assert "variant_cols" in code
        assert "map_zip_with" in code
        assert "map_filter" in code

        # Both branches present
        assert "_dlq_rescued_data" in code
        assert 'F.lit(None).cast("string")' in code
