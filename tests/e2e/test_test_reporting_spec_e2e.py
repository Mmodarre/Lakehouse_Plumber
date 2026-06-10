"""E2E tests for test result reporting feature."""

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
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def run_generate(self) -> tuple:
        """Run 'lhp generate --env dev'. Returns (exit_code, output)."""
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

    def run_generate_with_tests(self) -> tuple:
        """Run 'lhp generate --env dev --include-tests'. Returns (exit_code, output)."""
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

    def run_validate(self) -> tuple:
        """Run 'lhp validate --env dev'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "validate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        return result.exit_code, result.output

    def run_validate_with_tests(self) -> tuple:
        """Run 'lhp validate --env dev --include-tests'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "validate",
                "--env",
                "dev",
                "--include-tests",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
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
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""

    def _load_state_file(self) -> dict:
        """Rebuild the legacy-shape state dict from the new sharded format.

        Reads ``.lhp_state/_global.json`` + per-pipeline shards and merges
        their ``environments`` slices into the old monolithic dict shape
        so existing assertions keep working.
        """
        state_dir = self.project_root / ".lhp_state"
        if not state_dir.exists():
            return {}

        result: dict = {"environments": {}}

        global_path = state_dir / "_global.json"
        if global_path.exists():
            with open(global_path, "r") as f:
                global_data = json.load(f)
            result["version"] = global_data.get("version", "1.0")
            result["last_updated"] = global_data.get("last_updated", "")
            result["global_dependencies"] = global_data.get("global_dependencies", {})
            result["last_generation_context"] = global_data.get(
                "last_generation_context", {}
            )

        for shard_path in sorted(state_dir.glob("*.json")):
            if shard_path.stem.startswith("_"):
                continue
            with open(shard_path, "r") as f:
                shard_data = json.load(f)
            for env_name, env_files in shard_data.get("environments", {}).items():
                result["environments"].setdefault(env_name, {}).update(env_files)

        return result

    def _remove_test_reporting_from_config(self):
        """Remove the test_reporting section from lhp.yaml."""
        lhp_yaml = self.project_root / "lhp.yaml"
        content = yaml.safe_load(lhp_yaml.read_text())
        content.pop("test_reporting", None)
        lhp_yaml.write_text(yaml.dump(content, default_flow_style=False))

    def test_tc07_test_id_recognized_by_validator(self):
        """TC-07: test_id in YAML does not trigger 'Unknown field' validation error."""
        exit_code, output = self.run_validate()
        assert exit_code == 0, (
            f"Validation should pass with test_id in YAML. Output: {output}"
        )
        assert "unknown field" not in output.lower(), (
            f"test_id should be recognized, not flagged as unknown: {output}"
        )

    def test_tc17_no_hook_without_include_tests_flag(self):
        """TC-17: Without --include-tests, no hook file is generated."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation should succeed: {output}"

        hook_file = self.generated_dir / "acmi_edw_bronze" / "_test_reporting_hook.py"
        assert not hook_file.exists(), (
            "Hook file should NOT exist without --include-tests"
        )

    def test_tc18_no_hook_without_test_reporting_config(self):
        """TC-18: Even with --include-tests, no hook when test_reporting absent."""
        self._remove_test_reporting_from_config()

        exit_code, output = self.run_generate_with_tests()
        assert exit_code == 0, f"Generation should succeed: {output}"

        hook_file = self.generated_dir / "acmi_edw_bronze" / "_test_reporting_hook.py"
        assert not hook_file.exists(), (
            "Hook file should NOT exist without test_reporting config"
        )

    def test_tc20_validate_with_include_tests_succeeds(self):
        """TC-20: lhp validate --env dev --include-tests succeeds with valid config."""
        exit_code, output = self.run_validate_with_tests()
        assert exit_code == 0, (
            f"Validation with --include-tests should succeed. Output: {output}"
        )

    def test_tc21_validate_without_flag_checks_file_existence(self):
        """TC-21: validate without --include-tests still checks module_path exists."""
        exit_code, output = self.run_validate()
        assert exit_code == 0, (
            f"Validation should pass (provider file exists). Output: {output}"
        )

    def test_tc21b_validate_fails_when_provider_missing(self):
        """TC-21b: validate fails when module_path file doesn't exist."""
        provider = self.project_root / "py_functions" / "test_reporting_publisher.py"
        if provider.exists():
            provider.unlink()

        exit_code, output = self.run_validate()
        assert exit_code != 0, (
            f"Validation should fail when provider file is missing. Output: {output}"
        )

    def test_tc22_generate_fails_when_provider_missing(self) -> None:
        """Constitution §9.24: generate and validate share one preflight, so LHP-CFG-032
        fires on ``lhp generate`` independent of ``--include-tests``."""
        # Remove the provider file (mirror test_tc21b's setup).
        provider = self.project_root / "py_functions" / "test_reporting_publisher.py"
        if provider.exists():
            provider.unlink()

        exit_code, output = self.run_generate()
        assert exit_code != 0, (
            "Generate should fail when the test_reporting provider file is "
            f"missing, even without --include-tests. Output: {output}"
        )
        assert "LHP-CFG-032" in output, (
            "Generate's test-reporting preflight must surface LHP-CFG-032 "
            f"independent of --include-tests. Output: {output}"
        )

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

    def test_tc23_existing_baselines_unaffected(self):
        """TC-23: Standard baselines are identical whether or not test_reporting is in config."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        baseline_dir = self.project_root / "generated_baseline" / "dev"
        for baseline_file in baseline_dir.rglob("*.py"):
            relative = baseline_file.relative_to(baseline_dir)
            generated_file = self.generated_dir / relative

            assert generated_file.exists(), f"Generated file {relative} should exist"

            diff = self._compare_file_hashes(generated_file, baseline_file)
            assert diff == "", f"Standard baseline mismatch for {relative}: {diff}"
