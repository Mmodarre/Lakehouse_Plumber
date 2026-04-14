"""
End-to-end tests for test result reporting feature.

Tests the complete workflow: lhp.yaml test_reporting config + test actions
with test_id → lhp generate --include-tests → _test_reporting_hook.py
generated in the pipeline output directory with provider module copy.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestTestReportingE2E:
    """E2E tests for test reporting hook generation."""

    __test__ = True

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

    def run_generate_with_tests(self) -> tuple:
        """Run 'lhp generate --env dev --force --include-tests'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["generate", "--env", "dev", "--force", "--include-tests"]
        )
        return result.exit_code, result.output

    def run_generate(self) -> tuple:
        """Run 'lhp generate --env dev --force'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["generate", "--env", "dev", "--force"])
        return result.exit_code, result.output

    def run_validate_with_tests(self) -> tuple:
        """Run 'lhp validate --env dev --include-tests'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--env", "dev", "--include-tests"])
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

    def test_hook_generation_matches_baseline(self):
        """Verify _test_reporting_hook.py matches baseline when generated with --include-tests."""
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = (
            self.generated_dir / "acmi_edw_bronze" / "_test_reporting_hook.py"
        )
        assert generated_file.exists(), "_test_reporting_hook.py should be generated"

        baseline_file = (
            self.project_root
            / "generated_baseline_with_tests"
            / "dev"
            / "acmi_edw_bronze"
            / "_test_reporting_hook.py"
        )
        assert baseline_file.exists(), "Baseline file should exist"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Baseline mismatch: {hash_diff}"

    def test_test_flowgroup_matches_baseline(self):
        """Verify tst_customer_dq.py matches baseline when generated with --include-tests."""
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = self.generated_dir / "acmi_edw_bronze" / "tst_customer_dq.py"
        assert generated_file.exists(), "tst_customer_dq.py should be generated"

        baseline_file = (
            self.project_root
            / "generated_baseline_with_tests"
            / "dev"
            / "acmi_edw_bronze"
            / "tst_customer_dq.py"
        )
        assert baseline_file.exists(), "Baseline file should exist"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Baseline mismatch: {hash_diff}"

    def test_provider_module_copied_matches_baseline(self):
        """Verify provider module copy matches baseline."""
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = (
            self.generated_dir
            / "acmi_edw_bronze"
            / "test_reporting_providers"
            / "test_reporting_publisher.py"
        )
        assert generated_file.exists(), "Provider module should be copied"

        baseline_file = (
            self.project_root
            / "generated_baseline_with_tests"
            / "dev"
            / "acmi_edw_bronze"
            / "test_reporting_providers"
            / "test_reporting_publisher.py"
        )
        assert baseline_file.exists(), "Provider baseline should exist"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Provider baseline mismatch: {hash_diff}"

    def test_no_hook_without_include_tests(self):
        """Without --include-tests, no hook file is generated."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        hook_file = self.generated_dir / "acmi_edw_bronze" / "_test_reporting_hook.py"
        assert (
            not hook_file.exists()
        ), "Hook file should NOT exist without --include-tests"

    def test_no_test_flowgroup_without_include_tests(self):
        """Without --include-tests, test flowgroup is not generated."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        tst_file = self.generated_dir / "acmi_edw_bronze" / "tst_customer_dq.py"
        assert (
            not tst_file.exists()
        ), "Test flowgroup should NOT exist without --include-tests"

    def test_existing_baselines_unaffected_without_include_tests(self):
        """Adding test_reporting config does NOT change existing baselines
        when generating without --include-tests."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Verify a known baseline still matches (uses standard baseline, not _with_tests)
        generated_file = self.generated_dir / "acmi_edw_bronze" / "customer_bronze.py"
        baseline_file = (
            self.project_root
            / "generated_baseline"
            / "dev"
            / "acmi_edw_bronze"
            / "customer_bronze.py"
        )

        if generated_file.exists() and baseline_file.exists():
            hash_diff = self._compare_file_hashes(generated_file, baseline_file)
            assert (
                hash_diff == ""
            ), f"Existing baseline affected by test_reporting config: {hash_diff}"

    def test_validate_with_include_tests(self):
        """Validate command succeeds with valid test reporting config."""
        exit_code, output = self.run_validate_with_tests()
        assert exit_code == 0, f"Validation failed: {output}"
        assert "Test reporting configuration is valid" in output
