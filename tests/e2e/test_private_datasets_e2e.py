"""Golden E2E test for SDP `private=True` on materialized-view and streaming-table writes.

Databricks SDP (Lakeflow Declarative Pipelines) supports a `private` flag on the
dataset-defining APIs — `@dp.materialized_view(..., private=True)` and
`dp.create_streaming_table(..., private=True)`. A private table is persisted for the
lifetime of the pipeline and visible only inside the pipeline; it is NOT published to
the metastore.

This test is written **test-first (TDD)**. LHP accepts the `write_target.private: true`
YAML field (config surface only), but the generators/templates do not yet emit
`private=True`, so `lhp generate` currently produces Python WITHOUT the flag. The curated
baseline under ``generated_baseline/dev/private_datasets/`` DOES contain `private=True`,
so this test **fails today** (hash mismatch) and turns **green** once the generators and
templates render the flag.

Fixture flowgroups live in ``pipelines/22_private_datasets/`` (pipeline ``private_datasets``)
and cover the materialized view plus all three streaming-table modes (standard, cdc,
snapshot_cdc), exercising every ``dp.create_streaming_table(...)`` call site.
"""

import difflib
import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestPrivateDatasetsE2E:
    """Golden test: `private=True` must reach the generated MV / streaming-table writes."""

    # Files the `private_datasets` pipeline generates, and whether each is expected to
    # carry a `private=True` kwarg once the feature is implemented.
    _PRIVATE_FLAG_FILES = (
        "mv_private.py",
        "st_standard_private.py",
        "st_cdc_private.py",
        "st_snapshot_cdc_private.py",
    )

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Copy fixture to isolated temp dir, set up paths, init working dirs."""
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
        """Wipe and recreate working dirs (no-op on fresh fixture)."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def run_bundle_sync(self) -> tuple:
        """Run `lhp generate --env dev` with the fixture's pipeline config."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--verbose",
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        return result.exit_code, result.output

    def _compare_directory_hashes(
        self, generated_dir: Path, baseline_dir: Path
    ) -> list:
        """Recursively SHA-256 compare two dirs; return human-readable difference lines."""

        def get_file_hash(file_path: Path) -> str:
            with open(file_path, "rb") as f:
                return hashlib.sha256(f.read()).hexdigest()

        differences = []

        generated_files = {
            f.relative_to(generated_dir): f
            for f in generated_dir.rglob("*")
            if f.is_file() and "__pycache__" not in f.parts
        }
        baseline_files = {
            f.relative_to(baseline_dir): f
            for f in baseline_dir.rglob("*")
            if f.is_file() and "__pycache__" not in f.parts
        }

        for file_path in set(generated_files) - set(baseline_files):
            differences.append(f"Extra file in generated: {file_path}")
        for file_path in set(baseline_files) - set(generated_files):
            differences.append(f"Missing file from generated: {file_path}")

        for file_path in set(generated_files) & set(baseline_files):
            generated_hash = get_file_hash(generated_files[file_path])
            baseline_hash = get_file_hash(baseline_files[file_path])
            if generated_hash == baseline_hash:
                continue
            generated_lines = (
                generated_files[file_path].read_text().splitlines(keepends=True)
            )
            baseline_lines = (
                baseline_files[file_path].read_text().splitlines(keepends=True)
            )
            diff = list(
                difflib.unified_diff(
                    baseline_lines,
                    generated_lines,
                    fromfile=f"baseline/{file_path}",
                    tofile=f"generated/{file_path}",
                    lineterm="",
                    n=3,
                )
            )
            diff_output = "\n".join(diff[:50])
            if len(diff) > 50:
                diff_output += f"\n... ({len(diff) - 50} more lines omitted)"
            differences.append(
                f"Content differs (hash mismatch): {file_path}\n"
                f"    Generated: {generated_hash}\n"
                f"    Baseline:  {baseline_hash}\n"
                f"    Diff:\n{diff_output}"
            )

        return differences

    def test_private_flag_materialized_view_and_streaming_tables_match_baseline(self):
        """`lhp generate` must emit `private=True` for the private MV and all ST modes.

        RED until the generators/templates render the flag; GREEN once they do.
        """
        exit_code, output = self.run_bundle_sync()
        assert exit_code == 0, f"Generation should succeed: {output}"

        generated_pipeline = self.generated_dir / "private_datasets"
        baseline_pipeline = (
            self.project_root / "generated_baseline" / "dev" / "private_datasets"
        )
        assert generated_pipeline.is_dir(), (
            "private_datasets pipeline was not generated"
        )
        assert baseline_pipeline.is_dir(), "private_datasets baseline is missing"

        # Golden comparison: byte-identical to the curated baseline (which carries
        # `private=True`). This is the assertion that fails until the feature lands.
        differences = self._compare_directory_hashes(
            generated_pipeline, baseline_pipeline
        )
        assert not differences, (
            f"{len(differences)} difference(s) vs baseline — the generated output is "
            "missing `private=True`. Implement `private` in the MV and streaming-table "
            "generators/templates to turn this green.\n\n" + "\n".join(differences)
        )

        # Readable, feature-naming intent assertions (redundant with the hash check,
        # but they pinpoint the missing behaviour when the golden diff is large).
        for file_name in self._PRIVATE_FLAG_FILES:
            content = (generated_pipeline / file_name).read_text()
            assert "private=True" in content, (
                f"{file_name} should render `private=True` "
                "(SDP private dataset not published to the metastore)"
            )
