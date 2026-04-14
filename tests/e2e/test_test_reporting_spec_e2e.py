"""Spec-derived E2E tests for Test Result Reporting feature.

Tests derived from docs/design/test_reporting_implementation_plan.md only.
No implementation code was read during test authoring.

Uses the testing_project fixture which includes:
- lhp.yaml with test_reporting section
- pipelines/02_bronze/tst_customer_dq.yaml with test_id fields
- py_functions/test_reporting_publisher.py provider stub
- generated_baseline_with_tests/dev/ for --include-tests baselines
"""

import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestTestReportingSpecE2E:
    """Spec-derived E2E tests for test result reporting feature."""

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

        self._init_working_dirs()

        yield
        os.chdir(self.original_cwd)

    def _init_working_dirs(self):
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

    def run_generate_with_tests(self) -> tuple:
        """Run 'lhp generate --env dev --force --include-tests'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["generate", "--env", "dev", "--force", "--include-tests"]
        )
        return result.exit_code, result.output

    def run_validate(self) -> tuple:
        """Run 'lhp validate --env dev'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--env", "dev"])
        return result.exit_code, result.output

    def run_validate_with_tests(self) -> tuple:
        """Run 'lhp validate --env dev --include-tests'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--env", "dev", "--include-tests"])
        return result.exit_code, result.output

    def _file_hash(self, path: Path) -> str:
        """Return SHA-256 hex digest of a file."""
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical."""
        h1 = self._file_hash(file1)
        h2 = self._file_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) "
                f"!= {file2.name} ({h2[:12]})"
            )
        return ""

    def _load_state_file(self) -> dict:
        """Load .lhp_state.json from project root."""
        state_file = self.project_root / ".lhp_state.json"
        if not state_file.exists():
            return {}
        with open(state_file, "r") as f:
            return json.load(f)

    def _remove_test_reporting_from_config(self):
        """Remove the test_reporting section from lhp.yaml."""
        lhp_yaml = self.project_root / "lhp.yaml"
        content = yaml.safe_load(lhp_yaml.read_text())
        content.pop("test_reporting", None)
        lhp_yaml.write_text(yaml.dump(content, default_flow_style=False))

    # ========================================================================
    # TEST CASES
    # ========================================================================

    # TC-07: test_id recognized by config field validator
    def test_tc07_test_id_recognized_by_validator(self):
        """TC-07: test_id in YAML does not trigger 'Unknown field' validation error."""
        exit_code, output = self.run_validate()
        assert (
            exit_code == 0
        ), f"Validation should pass with test_id in YAML. Output: {output}"
        assert (
            "unknown field" not in output.lower()
        ), f"test_id should be recognized, not flagged as unknown: {output}"

    # TC-17: Hook generation skipped when --include-tests not passed
    def test_tc17_no_hook_without_include_tests_flag(self):
        """TC-17: Without --include-tests, no hook file is generated."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation should succeed: {output}"

        hook_file = self.generated_dir / "acmi_edw_bronze" / "_test_reporting_hook.py"
        assert (
            not hook_file.exists()
        ), "Hook file should NOT exist without --include-tests"

    # TC-18: Hook generation skipped when no test_reporting in config
    def test_tc18_no_hook_without_test_reporting_config(self):
        """TC-18: Even with --include-tests, no hook when test_reporting absent."""
        self._remove_test_reporting_from_config()

        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation should succeed: {output}"

        hook_file = self.generated_dir / "acmi_edw_bronze" / "_test_reporting_hook.py"
        assert (
            not hook_file.exists()
        ), "Hook file should NOT exist without test_reporting config"

    # TC-19: Generated hook tracked in state file after generation
    def test_tc19_hook_tracked_in_state_file(self):
        """TC-19: After --include-tests generation, hook file appears in state."""
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        state = self._load_state_file()
        assert state, "State file should exist and be non-empty after generation"

        # The state file should track the generated hook file
        state_str = json.dumps(state)
        assert (
            "_test_reporting_hook" in state_str
        ), f"Hook file should be tracked in state. State keys: {list(state.keys())}"

    # TC-20: lhp validate --env dev --include-tests validates successfully
    def test_tc20_validate_with_include_tests_succeeds(self):
        """TC-20: lhp validate --env dev --include-tests succeeds with valid config."""
        exit_code, output = self.run_validate_with_tests()
        assert (
            exit_code == 0
        ), f"Validation with --include-tests should succeed. Output: {output}"

    # TC-21: lhp validate --env dev (no flag) still validates file existence
    def test_tc21_validate_without_flag_checks_file_existence(self):
        """TC-21: validate without --include-tests still checks module_path exists."""
        exit_code, output = self.run_validate()
        assert (
            exit_code == 0
        ), f"Validation should pass (provider file exists). Output: {output}"

    def test_tc21b_validate_fails_when_provider_missing(self):
        """TC-21b: validate fails when module_path file doesn't exist."""
        # Remove the provider file
        provider = self.project_root / "py_functions" / "test_reporting_publisher.py"
        if provider.exists():
            provider.unlink()

        exit_code, output = self.run_validate()
        assert (
            exit_code != 0
        ), f"Validation should fail when provider file is missing. Output: {output}"

    # TC-22: Full generation with --include-tests produces hook matching baseline
    def test_tc22_hook_matches_baseline(self):
        """TC-22: Generated hook file matches baseline via hash comparison."""
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_hook = (
            self.generated_dir / "acmi_edw_bronze" / "_test_reporting_hook.py"
        )
        baseline_hook = (
            self.project_root
            / "generated_baseline_with_tests"
            / "dev"
            / "acmi_edw_bronze"
            / "_test_reporting_hook.py"
        )

        assert generated_hook.exists(), "Generated hook file should exist"
        assert baseline_hook.exists(), "Baseline hook file should exist"

        diff = self._compare_file_hashes(generated_hook, baseline_hook)
        assert diff == "", f"Hook baseline mismatch: {diff}"

    def test_tc22b_test_flowgroup_matches_baseline(self):
        """TC-22b: Generated test flowgroup matches baseline via hash comparison."""
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_fg = self.generated_dir / "acmi_edw_bronze" / "tst_customer_dq.py"
        baseline_fg = (
            self.project_root
            / "generated_baseline_with_tests"
            / "dev"
            / "acmi_edw_bronze"
            / "tst_customer_dq.py"
        )

        assert generated_fg.exists(), "Generated test flowgroup file should exist"
        assert baseline_fg.exists(), "Baseline test flowgroup file should exist"

        diff = self._compare_file_hashes(generated_fg, baseline_fg)
        assert diff == "", f"Test flowgroup baseline mismatch: {diff}"

    def test_tc22c_provider_copy_matches_baseline(self):
        """TC-22c: Copied provider module matches baseline via hash comparison."""
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_provider = (
            self.generated_dir
            / "acmi_edw_bronze"
            / "test_reporting_providers"
            / "test_reporting_publisher.py"
        )
        baseline_provider = (
            self.project_root
            / "generated_baseline_with_tests"
            / "dev"
            / "acmi_edw_bronze"
            / "test_reporting_providers"
            / "test_reporting_publisher.py"
        )

        assert generated_provider.exists(), "Generated provider copy should exist"
        assert baseline_provider.exists(), "Baseline provider copy should exist"

        diff = self._compare_file_hashes(generated_provider, baseline_provider)
        assert diff == "", f"Provider copy baseline mismatch: {diff}"

    # TC-23: Existing (non-test) baselines unaffected by test_reporting config
    def test_tc23_existing_baselines_unaffected(self):
        """TC-23: Standard baselines are identical whether or not test_reporting is in config."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Check that each file in generated_baseline/dev/ matches generated/dev/
        baseline_dir = self.project_root / "generated_baseline" / "dev"
        for baseline_file in baseline_dir.rglob("*.py"):
            relative = baseline_file.relative_to(baseline_dir)
            generated_file = self.generated_dir / relative

            assert generated_file.exists(), f"Generated file {relative} should exist"

            diff = self._compare_file_hashes(generated_file, baseline_file)
            assert diff == "", f"Standard baseline mismatch for {relative}: {diff}"

    # TC-25: Hook generation ordering — state includes hook file
    def test_tc25_hook_in_state_proves_ordering(self):
        """TC-25: Hook file tracked in state proves it ran before state save.

        Per the plan, hook generation is inserted BEFORE state_manager.save()
        in the orchestrator finalize block. If the hook file appears in the
        state file, it was generated before the state was persisted.
        """
        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation failed: {output}"

        # Verify hook file exists
        hook_file = self.generated_dir / "acmi_edw_bronze" / "_test_reporting_hook.py"
        assert hook_file.exists(), "Hook file should be generated"

        # Verify state file includes the hook
        state = self._load_state_file()
        assert state, "State file should exist after generation"

        state_str = json.dumps(state)
        assert (
            "_test_reporting_hook" in state_str
        ), "Hook must appear in state file (proves generation before state save)"
