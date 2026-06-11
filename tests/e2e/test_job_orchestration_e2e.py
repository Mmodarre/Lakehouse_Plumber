"""
End-to-end integration tests for job orchestration file generation.

Tests the complete workflow of generating job orchestration files using
the `lhp dag -b` command with and without job configuration.
"""

import hashlib
import json
import os
import shutil
import warnings
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestJobOrchestrationE2E:
    """E2E tests for job orchestration file generation."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Set up fresh test project for each test method."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.resources_dir = self.project_root / "resources"
        self.resources_baseline_dir = self.project_root / "resources_baseline"

        yield

        os.chdir(self.original_cwd)

    def run_dag_command(self, *args) -> tuple:
        """Run lhp dag command and return (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["dag", *args])
        return result.exit_code, result.output

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare hashes of two files, return difference or empty string."""

        def get_file_hash(file_path: Path) -> str:
            with open(file_path, "rb") as f:
                return hashlib.sha256(f.read()).hexdigest()

        try:
            hash1 = get_file_hash(file1)
            hash2 = get_file_hash(file2)

            if hash1 != hash2:
                return f"Hash mismatch: {file1.name} vs {file2.name}"
            return ""
        except (OSError, IOError, UnicodeDecodeError) as e:
            return f"Error comparing files: {e}"

    def uncomment_job_names(self):
        """Uncomment all #job_name: lines in flowgroup YAML files.

        Covers both ``pipelines/`` (on-disk flowgroups) and ``blueprints/``
        (the per-spec ``#job_name:`` lines whose values propagate to the
        flowgroups expanded from each blueprint instance). Both must be
        uncommented for the project to be a consistent multi-job project,
        since blueprint-expanded flowgroups are validated alongside on-disk
        ones by ``validate_job_names`` (all-or-nothing).
        """
        scan_dirs = [self.project_root / "pipelines", self.project_root / "blueprints"]

        yaml_files = []
        for scan_dir in scan_dirs:
            if scan_dir.exists():
                yaml_files += list(scan_dir.rglob("*.yaml")) + list(
                    scan_dir.rglob("*.yml")
                )

        for yaml_file in yaml_files:
            content = yaml_file.read_text()
            modified_content = content.replace("#job_name:", "job_name:")
            yaml_file.write_text(modified_content)

        print(f"Uncommented job_name in {len(yaml_files)} YAML files")

    def uncomment_lines_in_file(self, file_path: Path, line_numbers: list):
        """Uncomment specific lines (1-indexed) by removing leading # while preserving indentation."""
        lines = file_path.read_text().splitlines(keepends=True)

        for line_num in line_numbers:
            idx = line_num - 1
            if idx < len(lines):
                line = lines[idx]
                # Find the # character and remove it along with one space after it
                if "#" in line:
                    leading_spaces = len(line) - len(line.lstrip())
                    content = line.lstrip()
                    if content.startswith("#"):
                        content = content[1:]
                        if content.startswith(" "):
                            content = content[1:]
                        lines[idx] = " " * leading_spaces + content
                        if not lines[idx].endswith("\n"):
                            lines[idx] += "\n"

        file_path.write_text("".join(lines))

    def test_deps_bundle_without_job_config(self):
        """Test lhp dag -b generates correct orchestration job without job config."""
        self.resources_dir.mkdir(parents=True, exist_ok=True)

        exit_code, output = self.run_dag_command("-b")

        assert exit_code == 0, f"Command should succeed: {output}"

        generated_file = self.resources_dir / "acme_edw_orchestration.job.yml"
        assert generated_file.exists(), f"Generated file should exist: {generated_file}"

        baseline_file = self.resources_baseline_dir / "acme_edw_orchestration.job.yml"
        assert baseline_file.exists(), f"Baseline file should exist: {baseline_file}"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Generated file should match baseline: {hash_diff}"

        print("✅ Job orchestration file (without job config) matches baseline")

    def test_deps_bundle_with_job_config(self):
        """Test lhp dag -b -jc generates correct orchestration job with job config applied."""
        self.resources_dir.mkdir(parents=True, exist_ok=True)

        exit_code, output = self.run_dag_command("-b", "-jc", "config/job_config.yaml")

        assert exit_code == 0, f"Command should succeed: {output}"

        generated_file = self.resources_dir / "acme_edw_orchestration.job.yml"
        assert generated_file.exists(), f"Generated file should exist: {generated_file}"

        baseline_file = (
            self.resources_baseline_dir / "acme_edw_orchestration-JC.job.yml"
        )
        assert baseline_file.exists(), f"Baseline file should exist: {baseline_file}"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", (
            f"Generated file should match baseline with job config: {hash_diff}"
        )

        print("✅ Job orchestration file (with job config) matches baseline")

    def test_multi_job_with_default_master(self):
        """Test multi-job generation with default master job name."""
        self.uncomment_job_names()

        self.resources_dir.mkdir(parents=True, exist_ok=True)

        exit_code, output = self.run_dag_command("-b", "-jc", "config/job_config.yaml")

        assert exit_code == 0, f"Command should succeed: {output}"

        expected_files = [
            "j_one.job.yml",
            "j_two.job.yml",
            "j_three.job.yml",
            "j_four.job.yml",
            "j_six.job.yml",
            "j_seven.job.yml",
            "j_nine.job.yml",
            # Feature-demo jobs, grouped by feature directory so the whole
            # fixture is a valid multi-job project.
            "j_blueprint.job.yml",
            "j_cdc.job.yml",
            "j_dep_bindings.job.yml",
            "j_helpers.job.yml",
            "j_jdbc.job.yml",
            "j_namespace.job.yml",
            "j_python_load.job.yml",
            "j_sinks.job.yml",
            "j_temp_table.job.yml",
            "j_test_actions.job.yml",
            "acme_edw_master.job.yml",  # Default master job name
        ]

        for filename in expected_files:
            generated_file = self.resources_dir / filename
            assert generated_file.exists(), (
                f"Generated file should exist: {generated_file}"
            )

        baseline_dir = self.resources_baseline_dir / "indvidual_jobs"
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            baseline_file = baseline_dir / filename

            assert baseline_file.exists(), (
                f"Baseline file should exist: {baseline_file}"
            )

            hash_diff = self._compare_file_hashes(generated_file, baseline_file)
            assert hash_diff == "", (
                f"File {filename} should match baseline: {hash_diff}"
            )

        print(
            "✅ Multi-job orchestration with default master job name matches baseline"
        )

    def test_multi_job_with_custom_master_name(self):
        """Test multi-job generation with custom master job name."""
        self.uncomment_job_names()

        # Uncomment lines 13-14 in job_config.yaml to set custom master job name.
        job_config_file = self.project_root / "config" / "job_config.yaml"
        self.uncomment_lines_in_file(job_config_file, [13, 14])

        self.resources_dir.mkdir(parents=True, exist_ok=True)

        exit_code, output = self.run_dag_command("-b", "-jc", "config/job_config.yaml")

        assert exit_code == 0, f"Command should succeed: {output}"

        expected_files = [
            "j_one.job.yml",
            "j_two.job.yml",
            "j_three.job.yml",
            "j_four.job.yml",
            "j_six.job.yml",
            "j_seven.job.yml",
            "j_nine.job.yml",
            # Feature-demo jobs (see test_multi_job_with_default_master).
            "j_blueprint.job.yml",
            "j_cdc.job.yml",
            "j_dep_bindings.job.yml",
            "j_helpers.job.yml",
            "j_jdbc.job.yml",
            "j_namespace.job.yml",
            "j_python_load.job.yml",
            "j_sinks.job.yml",
            "j_temp_table.job.yml",
            "j_test_actions.job.yml",
            "mehdi_master_job.job.yml",  # Custom master job name
        ]

        for filename in expected_files:
            generated_file = self.resources_dir / filename
            assert generated_file.exists(), (
                f"Generated file should exist: {generated_file}"
            )

        baseline_dir = self.resources_baseline_dir / "indvidual_jobs"
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            baseline_file = baseline_dir / filename

            assert baseline_file.exists(), (
                f"Baseline file should exist: {baseline_file}"
            )

            hash_diff = self._compare_file_hashes(generated_file, baseline_file)
            assert hash_diff == "", (
                f"File {filename} should match baseline: {hash_diff}"
            )

        print("✅ Multi-job orchestration with custom master job name matches baseline")

    def test_multi_job_without_master(self):
        """Test multi-job generation with master job disabled."""
        self.uncomment_job_names()

        # Uncomment line 15 in job_config.yaml to disable master job.
        job_config_file = self.project_root / "config" / "job_config.yaml"
        self.uncomment_lines_in_file(job_config_file, [15])

        self.resources_dir.mkdir(parents=True, exist_ok=True)

        exit_code, output = self.run_dag_command("-b", "-jc", "config/job_config.yaml")

        assert exit_code == 0, f"Command should succeed: {output}"

        expected_files = [
            "j_one.job.yml",
            "j_two.job.yml",
            "j_three.job.yml",
            "j_four.job.yml",
            "j_six.job.yml",
            "j_seven.job.yml",
            "j_nine.job.yml",
            # Feature-demo jobs (see test_multi_job_with_default_master).
            "j_blueprint.job.yml",
            "j_cdc.job.yml",
            "j_dep_bindings.job.yml",
            "j_helpers.job.yml",
            "j_jdbc.job.yml",
            "j_namespace.job.yml",
            "j_python_load.job.yml",
            "j_sinks.job.yml",
            "j_temp_table.job.yml",
            "j_test_actions.job.yml",
        ]

        for filename in expected_files:
            generated_file = self.resources_dir / filename
            assert generated_file.exists(), (
                f"Generated file should exist: {generated_file}"
            )

        master_file = self.resources_dir / "acme_edw_master.job.yml"
        assert not master_file.exists(), (
            f"Master job file should NOT exist: {master_file}"
        )

        baseline_dir = self.resources_baseline_dir / "indvidual_jobs"
        for filename in expected_files:
            generated_file = self.resources_dir / filename
            baseline_file = baseline_dir / filename

            assert baseline_file.exists(), (
                f"Baseline file should exist: {baseline_file}"
            )

            hash_diff = self._compare_file_hashes(generated_file, baseline_file)
            assert hash_diff == "", (
                f"File {filename} should match baseline: {hash_diff}"
            )

        print("✅ Multi-job orchestration without master job matches baseline")

    def test_deps_passes_through_file_arrival_trigger(self):
        """End-to-end: a `trigger.file_arrival` block in job_config.yaml must
        appear verbatim in the generated orchestration job YAML.

        This is the regression test for the bug report where users with
        file-arrival triggers saw no `trigger:` block in the output.
        """
        import yaml

        self.resources_dir.mkdir(parents=True, exist_ok=True)
        config_file = self.project_root / "config" / "job_config_trigger_e2e.yaml"
        config_file.write_text(
            "project_defaults:\n"
            "  max_concurrent_runs: 1\n"
            "  queue:\n"
            "    enabled: true\n"
            "  trigger:\n"
            "    file_arrival:\n"
            '      url: "s3://my-bucket/landing/"\n'
            "      min_time_between_triggers_seconds: 60\n"
            "      wait_after_last_change_seconds: 30\n"
            "    pause_status: UNPAUSED\n"
            "  continuous:\n"
            "    pause_status: PAUSED\n"
        )

        exit_code, output = self.run_dag_command(
            "-b", "-jc", "config/job_config_trigger_e2e.yaml"
        )
        assert exit_code == 0, f"lhp dag failed: {output}"

        generated_file = self.resources_dir / "acme_edw_orchestration.job.yml"
        assert generated_file.exists(), "orchestration job YAML was not generated"

        job_data = yaml.safe_load(generated_file.read_text())
        job = job_data["resources"]["jobs"]["acme_edw_orchestration"]

        assert "trigger" in job, (
            "trigger block missing from generated YAML — pass-through regressed"
        )
        assert job["trigger"]["file_arrival"]["url"] == "s3://my-bucket/landing/"
        assert job["trigger"]["file_arrival"]["min_time_between_triggers_seconds"] == 60
        assert job["trigger"]["file_arrival"]["wait_after_last_change_seconds"] == 30
        assert job["trigger"]["pause_status"] == "UNPAUSED"
        assert job["continuous"]["pause_status"] == "PAUSED"

    def test_deps_alias_still_works_with_deprecation_warning(self):
        """The hidden ``deps`` alias forwards to ``dag``: it must emit a
        ``DeprecationWarning`` and produce output identical to ``dag``.

        ``deps`` was renamed to ``dag`` in the CLI rebuild; ``deps`` survives
        as a hidden alias that forwards via ``ctx.forward`` with a deprecation
        notice. This is the single test guarding that backward-compat path.
        """
        dep_json = (
            self.project_root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
        )

        # 1. Canonical command.
        runner = CliRunner()
        dag_result = runner.invoke(cli, ["dag", "-b"])
        assert dag_result.exit_code == 0, f"lhp dag failed: {dag_result.output}"
        dag_data = json.loads(dep_json.read_text())

        # Reset the dependency artifacts so the alias run regenerates them.
        shutil.rmtree(self.project_root / ".lhp", ignore_errors=True)

        # 2. Deprecated alias -- capture the DeprecationWarning it emits.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            deps_result = runner.invoke(cli, ["deps", "-b"])

        assert deps_result.exit_code == 0, (
            f"hidden 'deps' alias failed: {deps_result.output}"
        )

        deprecations = [
            str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert any("deps" in m and "dag" in m for m in deprecations), (
            "'deps' alias must emit a DeprecationWarning pointing at 'dag'; "
            f"caught: {deprecations}"
        )

        # 3. Output parity with ``dag`` (stdout byte-identical; the dependency
        # JSON identical modulo the per-run ``generated_at`` timestamp). Compare
        # ``.stdout`` specifically: the alias additionally emits a one-line
        # deprecation notice to stderr (mirroring ``generate --force``), so the
        # combined ``.output`` stream intentionally differs from ``dag``.
        assert deps_result.stdout == dag_result.stdout, (
            "Deprecated 'deps' alias must produce the same stdout as 'dag'."
        )
        deps_data = json.loads(dep_json.read_text())
        for payload in (dag_data, deps_data):
            payload.get("generation_info", {}).pop("generated_at", None)
        assert deps_data == dag_data, (
            "Deprecated 'deps' alias must produce the same dependency analysis "
            "as 'dag' (ignoring the per-run timestamp)."
        )
