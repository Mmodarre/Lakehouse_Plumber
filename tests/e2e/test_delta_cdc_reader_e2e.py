"""E2E tests for LHP Delta CDC reader (B8).

Covers the Delta Change Data Feed and time-travel options documented in
``docs/actions/load_actions.rst:274-420``. LHP's Delta load generator at
``src/lhp/generators/load/delta.py:23-240`` enforces seven mutual-exclusion
guards at parse time (e.g. ``readChangeFeed`` vs ``skipChangeCommits``,
``versionAsOf`` vs ``timestampAsOf``); this test class verifies only the
**happy-path** generated code for four common option combinations.

Key SDP correctness invariants verified by the baselines:

  - ``readChangeFeed: "true"`` (stream mode) produces
    ``spark.readStream.option("readChangeFeed", "true").table(...)``.
  - ``versionAsOf`` (batch mode) produces ``spark.read.option("versionAsOf",
    "10").table(...)`` — NOT ``readStream``, since time travel is batch-only.
  - ``skipChangeCommits`` (stream mode) produces
    ``spark.readStream.option("skipChangeCommits", "true").table(...)`` with
    no ``readChangeFeed`` option (mutually exclusive per docs:328).
  - CDF metadata columns (``_change_type``, ``_commit_version``,
    ``_commit_timestamp``) are NOT explicitly projected — they appear
    naturally on the DataFrame per docs:370-395.

Fixture flowgroups live in ``tests/e2e/fixtures/testing_project/pipelines/17_delta_cdc/``.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestDeltaCDCReaderE2E:
    """E2E tests for LHP Delta CDC and time-travel reader options."""

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
        self.cdc_dir = self.generated_dir / "17_delta_cdc"
        self.cdc_baseline_dir = (
            self.project_root / "generated_baseline" / "dev" / "17_delta_cdc"
        )
        self.resource_baseline = (
            self.project_root
            / "resources_baseline"
            / "lhp"
            / "17_delta_cdc.pipeline.yml"
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

    def _assert_baseline_match(self, filename: str):
        """Generate, then assert the named CDC flowgroup file matches its baseline.

        Also verifies the generated pipeline resource YAML matches its baseline.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated = self.cdc_dir / filename
        baseline = self.cdc_baseline_dir / filename
        assert generated.exists(), f"{filename} should be generated under 17_delta_cdc/"
        assert baseline.exists(), f"Baseline {filename} should exist"

        diff = self._compare_file_hashes(generated, baseline)
        assert diff == "", f"Baseline mismatch for {filename}: {diff}"

        # Verify the pipeline resource YAML also matches its baseline.
        generated_resource = self.resources_dir / "17_delta_cdc.pipeline.yml"
        assert generated_resource.exists(), (
            "17_delta_cdc.pipeline.yml should be generated under resources/lhp/"
        )
        assert self.resource_baseline.exists(), (
            "Resource baseline 17_delta_cdc.pipeline.yml should exist"
        )
        resource_diff = self._compare_file_hashes(
            generated_resource, self.resource_baseline
        )
        assert resource_diff == "", (
            f"Resource baseline mismatch for 17_delta_cdc.pipeline.yml: {resource_diff}"
        )

    # ------------------------------------------------------------------
    # 4 CDC / time-travel variants — one method per option combination.
    # ------------------------------------------------------------------

    def test_delta_read_change_feed_matches_baseline(self):
        """readChangeFeed + startingVersion=0 (stream): full CDF from beginning."""
        self._assert_baseline_match("cdc_read_change_feed.py")

    def test_delta_starting_version_matches_baseline(self):
        """readChangeFeed + startingVersion=5 + ignoreDeletes (stream): CDF tail."""
        self._assert_baseline_match("cdc_starting_version.py")

    def test_delta_time_travel_matches_baseline(self):
        """versionAsOf=10 (batch): time-travel snapshot via spark.read, NOT readStream."""
        self._assert_baseline_match("cdc_time_travel.py")

    def test_delta_skip_change_commits_matches_baseline(self):
        """skipChangeCommits=true (stream): take latest state, skip change commits.

        Must NOT include readChangeFeed (mutually exclusive per docs:328).
        """
        self._assert_baseline_match("cdc_skip_change_commits.py")
