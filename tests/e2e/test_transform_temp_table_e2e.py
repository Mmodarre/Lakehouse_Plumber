"""E2E tests for LHP temp_table transform action (B5.3).

Covers the temp_table transform documented in
``docs/actions/transform_actions.rst:794-897``. A temp_table materializes an
intermediate staging view once so multiple downstream actions can read it
without recomputing the source.

LHP emits ``@dp.table(temporary=True)`` for temp_table — this matches the
documented Python output examples at lines 867-874 (simple passthrough) and
883-897 (with SQL). It is NOT the same shape as ``@dp.temporary_view()``,
which is used for non-materialized logical views.

The fixture chain (load → temp_table → sql transform → mv write) verifies
that:
  - The temp_table emits ``@dp.table(temporary=True)`` with the target name
    as the function name.
  - Downstream actions reference the temp table by its target name.
  - Section ordering remains SOURCE VIEWS → TRANSFORMATION VIEWS → TARGET
    TABLES across mixed action types.

Fixture flowgroup lives in ``tests/e2e/fixtures/testing_project/pipelines/16_temp_table/``.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestTransformTempTableE2E:
    """E2E test for LHP temp_table transform (intermediate staging table)."""

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
        self.temp_table_dir = self.generated_dir / "16_temp_table"
        self.temp_table_baseline_dir = (
            self.project_root / "generated_baseline" / "dev" / "16_temp_table"
        )
        self.resource_baseline = (
            self.project_root
            / "resources_baseline"
            / "lhp"
            / "16_temp_table.pipeline.yml"
        )

        self._init_bundle_project()

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
        """Run 'lhp generate --env dev --force' (no --include-tests)."""
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

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical, error string otherwise."""

        def get_hash(f: Path) -> str:
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""

    def test_temp_table_creates_staging_view_matches_baseline(self):
        """temp_table emits @dp.table(temporary=True) for an intermediate staging table.

        Verifies the full load → temp_table → sql → MV chain in
        ``16_temp_table/staging_chain.py`` matches the baseline, AND the
        pipeline resource YAML matches.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        filename = "staging_chain.py"
        generated = self.temp_table_dir / filename
        baseline = self.temp_table_baseline_dir / filename
        assert generated.exists(), (
            f"{filename} should be generated under 16_temp_table/"
        )
        assert baseline.exists(), f"Baseline {filename} should exist"

        diff = self._compare_file_hashes(generated, baseline)
        assert diff == "", f"Baseline mismatch for {filename}: {diff}"

        # Verify the pipeline resource YAML also matches its baseline.
        generated_resource = self.resources_dir / "16_temp_table.pipeline.yml"
        assert generated_resource.exists(), (
            "16_temp_table.pipeline.yml should be generated under resources/lhp/"
        )
        assert self.resource_baseline.exists(), (
            "Resource baseline 16_temp_table.pipeline.yml should exist"
        )
        resource_diff = self._compare_file_hashes(
            generated_resource, self.resource_baseline
        )
        assert resource_diff == "", (
            f"Resource baseline mismatch for 16_temp_table.pipeline.yml: {resource_diff}"
        )
