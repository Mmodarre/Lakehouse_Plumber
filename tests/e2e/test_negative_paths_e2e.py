"""End-to-end negative-path tests (B6).

Verifies that LHP returns non-zero exit codes and surfaces actionable
error messages for common failure modes:

- Invalid YAML in flowgroup files
- Unknown environment passed to --env
- Missing required --env flag (Click usage error)
- Undefined ${token} reference
- Undefined ${secret:scope/key} reference
- Unknown action type (LHP-ACT-001)
- Duplicate (pipeline, flowgroup) tuple across files (LHP-VAL-009)
- Python module-name collision across flowgroups (LHP-VAL-019)

Phase 2 invariant: all mutations target the deep-copied tmp project
(``self.project_root``); no permanent fixtures are added under
``tests/e2e/fixtures/testing_project/``.

As of v0.8.7, all negative-path tests are regular (non-xfail) asserts:
LHP surfaces structured ``LHP-<CATEGORY>-<CODE>`` errors with did-you-mean
suggestions for unknown action types, and ``generate`` fails fast on any
per-pipeline failure (returning a POSIX exit code mapped from the LHP
error category by ``cli_error_boundary``).
"""

import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestNegativePathsE2E:
    """E2E negative-path coverage for validate/generate."""

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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def run_validate(self, *args) -> tuple:
        """Run 'lhp validate' with the given args. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", *args])
        return result.exit_code, result.output

    def run_generate(self, *args) -> tuple:
        """Run 'lhp generate' with the given args. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["generate", *args])
        return result.exit_code, result.output

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_invalid_yaml_in_flowgroup_fails(self):
        """Malformed YAML in a flowgroup must surface a parse-time error
        and fail validation."""
        bad_file = (
            self.project_root
            / "pipelines"
            / "01_raw_ingestion"
            / "csv_ingestions"
            / "broken_yaml.yaml"
        )
        bad_file.write_text(
            "pipeline: acmi_edw_raw\n"
            "flowgroup: broken_yaml\n"
            "actions:\n"
            "  - name: bad_action\n"
            "    type: load\n"
            "    source: [unclosed_bracket\n"
        )

        exit_code, output = self.run_validate("--env", "dev")
        assert exit_code != 0, "Validate should fail on malformed YAML"
        lower = output.lower()
        assert (
            "yaml" in lower or "parse" in lower or "broken_yaml" in output
        ), f"Expected YAML/parse error, got:\n{output[-2000:]}"

    def test_unknown_environment_fails(self):
        """An unknown --env value must fail (no substitution file)."""
        exit_code, output = self.run_generate("--env", "xyz")
        assert exit_code != 0, "Generate should fail on unknown environment"
        lower = output.lower()
        assert (
            "xyz" in output or "environment" in lower or "substitution" in lower
        ), f"Expected env/substitution error, got:\n{output[-2000:]}"

    def test_missing_environment_flag_fails(self):
        """Generate requires --env; running without it must fail with a Click usage error."""
        exit_code, output = self.run_generate()
        assert exit_code != 0, "Generate should fail when --env is missing"
        lower = output.lower()
        assert (
            "--env" in output or "missing option" in lower or "required" in lower
        ), f"Expected required-flag error, got:\n{output[-2000:]}"

    def test_undefined_token_reference_fails(self):
        """Inserting ${nonexistent_token} into a flowgroup must surface a
        substitution error (LHP-CFG-010) during validation, with the
        token name visible in default (non-verbose) output."""
        bad_file = (
            self.project_root
            / "pipelines"
            / "01_raw_ingestion"
            / "csv_ingestions"
            / "undefined_token.yaml"
        )
        bad_file.write_text(
            "pipeline: acmi_edw_raw\n"
            "flowgroup: undefined_token_fg\n"
            "actions:\n"
            "  - name: load_undefined_token\n"
            "    type: load\n"
            "    readMode: stream\n"
            "    source:\n"
            "      type: delta\n"
            '      database: "${nonexistent_token}"\n'
            "      table: customer_raw\n"
            "    target: v_undefined_token\n"
            "  - name: write_undefined_token\n"
            "    type: write\n"
            "    source: v_undefined_token\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: undefined_token_raw\n"
        )

        exit_code, output = self.run_validate("--env", "dev")
        assert exit_code != 0, "Validate should fail on undefined token"
        assert "LHP-CFG-010" in output or "nonexistent_token" in output, (
            "Without --verbose, validate should still surface the "
            f"unresolved-token error code or token name. Got:\n{output[-2000:]}"
        )

    def test_undefined_secret_reference_fails(self):
        """``${secret:bad scope!/key}`` must surface a secret-validation
        error: the scope name contains characters Databricks does not allow.

        LHP's SecretValidator is syntax-only (it cannot contact the workspace
        to enumerate real scopes), so we test the syntactic rule it enforces.
        """
        bad_file = (
            self.project_root
            / "pipelines"
            / "01_raw_ingestion"
            / "csv_ingestions"
            / "undefined_secret.yaml"
        )
        bad_file.write_text(
            "pipeline: acmi_edw_raw\n"
            "flowgroup: undefined_secret_fg\n"
            "actions:\n"
            "  - name: load_undefined_secret\n"
            "    type: load\n"
            "    readMode: stream\n"
            "    source:\n"
            "      type: delta\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: customer_raw\n"
            "      options:\n"
            '        password: "${secret:bad scope!/key}"\n'
            "    target: v_undefined_secret\n"
            "  - name: write_undefined_secret\n"
            "    type: write\n"
            "    source: v_undefined_secret\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: undefined_secret_raw\n"
        )

        exit_code, output = self.run_validate("--env", "dev")
        assert exit_code != 0, "Validate should fail on invalid secret scope syntax"
        lower = output.lower()
        assert (
            "bad scope" in output or "secret" in lower or "scope" in lower
        ), f"Expected secret-scope error, got:\n{output[-2000:]}"

    def test_unknown_action_type_fails(self):
        """type: bogus_type must surface LHP-ACT-001 cleanly (not a
        silent parse-error skip)."""
        bad_file = (
            self.project_root
            / "pipelines"
            / "01_raw_ingestion"
            / "csv_ingestions"
            / "unknown_action.yaml"
        )
        bad_file.write_text(
            "pipeline: acmi_edw_raw\n"
            "flowgroup: unknown_action_fg\n"
            "actions:\n"
            "  - name: bogus_action\n"
            "    type: bogus_type\n"
            "    source:\n"
            "      type: delta\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: customer_raw\n"
            "    target: v_bogus\n"
        )

        exit_code, output = self.run_validate("--env", "dev")
        assert exit_code != 0, "Validate should fail on unknown action type"
        assert (
            "LHP-ACT-001" in output or "bogus_type" in output or "Unknown" in output
        ), f"Expected unknown-type error, got:\n{output[-2000:]}"

    def test_duplicate_flowgroup_id_fails(self):
        """Two flowgroup files with same (pipeline, flowgroup) tuple must
        surface LHP-VAL-009 (duplicate pipeline+flowgroup combination).

        Cross-file duplicate detection runs in the orchestrator's generate
        path (validate_duplicate_pipeline_flowgroup_combinations), so this
        test uses 'lhp generate' rather than 'lhp validate'.
        """
        original = (
            self.project_root
            / "pipelines"
            / "01_raw_ingestion"
            / "csv_ingestions"
            / "customer_ingestion_incremental.yaml"
        )
        duplicate = original.with_name("customer_ingestion_incremental_dup.yaml")
        duplicate.write_text(
            "pipeline: acmi_edw_raw\n"
            "flowgroup: customer_ingestion_incremental\n"
            "actions:\n"
            "  - name: dup_load\n"
            "    type: load\n"
            "    readMode: stream\n"
            "    source:\n"
            "      type: delta\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: customer_raw\n"
            "    target: v_dup\n"
            "  - name: dup_write\n"
            "    type: write\n"
            "    source: v_dup\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: dup_table\n"
        )

        exit_code, output = self.run_generate("--env", "dev")
        assert exit_code != 0, "Generate should fail on duplicate (pipeline, flowgroup)"
        lower = output.lower()
        assert (
            "VAL-009" in output or "duplicate" in lower
        ), f"Expected duplicate-flowgroup error, got:\n{output[-2000:]}"

    def test_python_import_collision_fails(self):
        """Two flowgroups referencing different module_paths whose stem
        is identical must surface LHP-VAL-019 from
        PythonFunctionConflictError AND cause generate to exit non-zero.
        """
        py_dir_a = self.project_root / "py_functions" / "collision_a"
        py_dir_b = self.project_root / "py_functions" / "collision_b"
        py_dir_a.mkdir(parents=True, exist_ok=True)
        py_dir_b.mkdir(parents=True, exist_ok=True)

        # Same destination stem ("shared_util"), different source paths,
        # different bodies — triggers PythonFunctionConflictError.
        (py_dir_a / "shared_util.py").write_text(
            "from pyspark.sql import DataFrame\n"
            "\n"
            "def transform_passthrough(df: DataFrame, spark, parameters) -> DataFrame:\n"
            "    return df.withColumn('source_marker', spark.sql(\"SELECT 'A'\").first()[0])\n"
        )
        (py_dir_b / "shared_util.py").write_text(
            "from pyspark.sql import DataFrame\n"
            "\n"
            "def transform_passthrough(df: DataFrame, spark, parameters) -> DataFrame:\n"
            "    return df.withColumn('source_marker', spark.sql(\"SELECT 'B'\").first()[0])\n"
        )

        flowgroup_a = (
            self.project_root / "pipelines" / "09_test_python" / "collision_a.yaml"
        )
        flowgroup_a.write_text(
            "pipeline: sample_python_func_pipeline\n"
            "flowgroup: collision_a\n"
            "actions:\n"
            "  - name: load_for_collision_a\n"
            "    type: load\n"
            "    readMode: stream\n"
            "    source:\n"
            "      type: delta\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: customer_raw\n"
            "    target: v_collision_a_raw\n"
            "  - name: transform_collision_a\n"
            "    type: transform\n"
            "    transform_type: python\n"
            "    source: v_collision_a_raw\n"
            '    module_path: "py_functions/collision_a/shared_util.py"\n'
            '    function_name: "transform_passthrough"\n'
            "    parameters:\n"
            "      spark: spark\n"
            "      parameters: {}\n"
            "    target: v_collision_a_out\n"
            "  - name: write_collision_a\n"
            "    type: write\n"
            "    source: v_collision_a_out\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            '      database: "${catalog}.${bronze_schema}"\n'
            "      table: collision_a_out\n"
        )
        flowgroup_b = (
            self.project_root / "pipelines" / "09_test_python" / "collision_b.yaml"
        )
        flowgroup_b.write_text(
            "pipeline: sample_python_func_pipeline\n"
            "flowgroup: collision_b\n"
            "actions:\n"
            "  - name: load_for_collision_b\n"
            "    type: load\n"
            "    readMode: stream\n"
            "    source:\n"
            "      type: delta\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: customer_raw\n"
            "    target: v_collision_b_raw\n"
            "  - name: transform_collision_b\n"
            "    type: transform\n"
            "    transform_type: python\n"
            "    source: v_collision_b_raw\n"
            '    module_path: "py_functions/collision_b/shared_util.py"\n'
            '    function_name: "transform_passthrough"\n'
            "    parameters:\n"
            "      spark: spark\n"
            "      parameters: {}\n"
            "    target: v_collision_b_out\n"
            "  - name: write_collision_b\n"
            "    type: write\n"
            "    source: v_collision_b_out\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            '      database: "${catalog}.${bronze_schema}"\n'
            "      table: collision_b_out\n"
        )

        exit_code, output = self.run_generate("--env", "dev")
        assert exit_code != 0, "Generate should fail on Python module collision"
        lower = output.lower()
        assert (
            "VAL-019" in output or "naming conflict" in lower
        ), f"Expected Python collision error, got:\n{output[-2000:]}"
