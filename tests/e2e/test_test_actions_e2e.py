"""E2E tests for LHP test action codegen across the 6 documented test action types.

With ``--include-tests`` off, LHP skips test-only flowgroups entirely, so they do
not pollute the standard ``generated_baseline/`` comparison.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestTestActionsE2E:
    __test__ = True

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.test_actions_dir = self.generated_dir / "12_test_actions"
        self.test_actions_baseline_dir = (
            self.project_root
            / "generated_baseline_with_tests"
            / "dev"
            / "12_test_actions"
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

    def run_generate_with_tests(self) -> tuple:
        """Run 'lhp generate --env dev --include-tests'."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--include-tests",
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
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        generated = self.test_actions_dir / filename
        baseline = self.test_actions_baseline_dir / filename
        assert generated.exists(), (
            f"{filename} should be generated under 12_test_actions/"
        )
        assert baseline.exists(), f"Baseline {filename} should exist"

        diff = self._compare_file_hashes(generated, baseline)
        assert diff == "", f"Baseline mismatch for {filename}: {diff}"

    def test_row_count_matches_baseline(self):
        """row_count: cross-product COUNT subqueries; expectation on abs(diff) <= tolerance."""
        self._assert_baseline_match("ta_row_count.py")

    def test_referential_integrity_matches_baseline(self):
        """referential_integrity: LEFT JOIN; expectation on ref_<col> IS NOT NULL."""
        self._assert_baseline_match("ta_referential_integrity.py")

    def test_schema_match_matches_baseline(self):
        """schema_match: information_schema diff with catalog-qualified FROM and three-part WHERE."""
        self._assert_baseline_match("ta_schema_match.py")

    def test_all_lookups_found_matches_baseline(self):
        """all_lookups_found: LEFT JOIN against lookup table; expectation lookup_<col> IS NOT NULL."""
        self._assert_baseline_match("ta_all_lookups_found.py")

    def test_custom_sql_matches_baseline(self):
        """custom_sql: pass-through user SQL with attached expectations."""
        self._assert_baseline_match("ta_custom_sql.py")

    def test_custom_expectations_matches_baseline(self):
        """custom_expectations: synthesized SELECT * with multi-expectation grouping."""
        self._assert_baseline_match("ta_custom_expectations.py")
