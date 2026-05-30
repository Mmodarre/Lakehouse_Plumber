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
        """Run 'lhp validate' with the given args. Returns (exit_code, output).

        v0.8.7: bundle-enabled projects (databricks.yml present) require
        ``--pipeline-config`` or preflight blocks with ``LHP-CFG-023``. If
        the caller did not supply ``--pipeline-config`` / ``-pc`` /
        ``--no-bundle`` and the fixture has a default
        ``config/pipeline_config.yaml``, inject ``-pc`` automatically so
        the negative-path assertions are not shadowed by the B-gate
        (mirrors ``run_generate``).
        """
        runner = CliRunner()
        argv = list(args)
        needs_pc = (
            "--pipeline-config" not in argv
            and "-pc" not in argv
            and "--no-bundle" not in argv
            and (self.project_root / "config" / "pipeline_config.yaml").exists()
        )
        if needs_pc:
            argv.extend(["--pipeline-config", "config/pipeline_config.yaml"])
        result = runner.invoke(cli, ["validate", *argv])
        return result.exit_code, result.output

    def run_generate(self, *args) -> tuple:
        """Run 'lhp generate' with the given args. Returns (exit_code, output).

        v0.8.7: bundle-enabled projects (databricks.yml present) require
        ``--pipeline-config`` or preflight blocks with ``LHP-CFG-023``. If
        the caller did not supply ``--pipeline-config`` / ``-pc`` /
        ``--no-bundle`` and the fixture has a default
        ``config/pipeline_config.yaml``, inject ``-pc`` automatically so
        the negative-path assertions are not shadowed by the B-gate.
        """
        runner = CliRunner()
        argv = list(args)
        needs_pc = (
            "--pipeline-config" not in argv
            and "-pc" not in argv
            and "--no-bundle" not in argv
            and (self.project_root / "config" / "pipeline_config.yaml").exists()
        )
        if needs_pc:
            argv.extend(["--pipeline-config", "config/pipeline_config.yaml"])
        result = runner.invoke(cli, ["generate", *argv])
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
        surface LHP-VAL-009 (duplicate pipeline+flowgroup combination) on
        the GENERATE path.

        Retained as a regression guard for the generate side of the shared
        cross-file duplicate check. Per CODING_CONSTITUTION §9.24 the same
        detection logic must not be duplicated across paths: as of Phase 3
        ``lhp validate`` runs the identical check and also surfaces
        LHP-VAL-009 (see the sibling
        ``test_duplicate_flowgroup_id_fails_via_validate``). This test keeps
        the generate path covered so both error-surfacing paths stay green.
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

    def test_duplicate_flowgroup_id_fails_via_validate(self) -> None:
        """Two flowgroup files with the same (pipeline, flowgroup) tuple must
        also surface LHP-VAL-009 on the VALIDATE path (N3).

        Per CODING_CONSTITUTION §9.24 the cross-file duplicate-(pipeline,
        flowgroup) check must not be duplicated across the generate and
        validate paths. As of Phase 3 ``lhp validate`` runs the same check
        ``lhp generate`` runs; the duplicate batch carries
        ``error_code="LHP-VAL-009"``, which validate counts as an error and
        maps to a non-zero exit (ExitCode.DATA_ERROR). This is the validate
        sibling of ``test_duplicate_flowgroup_id_fails`` (generate path).

        Negative test: it expects FAILURE, so it neither generates nor diffs
        any baseline.
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

        exit_code, output = self.run_validate("--env", "dev")
        assert (
            exit_code != 0
        ), "Validate should fail on duplicate (pipeline, flowgroup)"
        assert "LHP-VAL-009" in output, (
            "Validate must surface the LHP-VAL-009 duplicate-flowgroup error "
            f"code (shared §9.24 dup check). Got:\n{output[-2000:]}"
        )

    def test_validate_no_table_creator_fails(self) -> None:
        """A target table written to only by actions with ``create_table:
        false`` (i.e. NO creating action anywhere in the pipeline) must
        surface LHP-VAL-009 on the VALIDATE path (N1).

        Per CODING_CONSTITUTION §9.24 the cross-flowgroup table-creation
        check is single-sourced in ``TableCreationValidator``. As of the
        sibling §9.24 routing change, ``lhp validate`` runs that check via
        ``validate_cross_flowgroup`` and FOLDS the resulting
        ``table_creation_errors`` into a structured LHP-VAL-009 error
        (previously these were discarded on the validate path). The folded
        error makes validate report ``success=False`` and exit non-zero.

        Mechanism: ``TableCreationValidator`` groups every write action by
        its fully-qualified ``catalog.schema.table``. A table whose write
        actions all have ``create_table: false`` ends up with zero creators,
        which yields the "Table '...' has no creator" message. We introduce
        a brand-new flowgroup whose single write action targets a UNIQUE
        table (``no_creator_raw``) with ``create_table: false``, so no other
        fixture flowgroup creates it — guaranteeing the zero-creator branch.

        Negative test: it expects FAILURE, so it neither generates nor diffs
        any baseline.
        """
        bad_file = (
            self.project_root
            / "pipelines"
            / "01_raw_ingestion"
            / "csv_ingestions"
            / "no_table_creator.yaml"
        )
        bad_file.write_text(
            "pipeline: acmi_edw_raw\n"
            "flowgroup: no_table_creator_fg\n"
            "actions:\n"
            "  - name: load_no_creator\n"
            "    type: load\n"
            "    readMode: stream\n"
            "    source:\n"
            "      type: delta\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: customer_raw\n"
            "    target: v_no_creator\n"
            "  - name: write_no_creator\n"
            "    type: write\n"
            "    source: v_no_creator\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            '      catalog: "${catalog}"\n'
            '      schema: "${raw_schema}"\n'
            "      table: no_creator_raw\n"
            "      create_table: false\n"
        )

        exit_code, output = self.run_validate("--env", "dev")
        assert (
            exit_code != 0
        ), "Validate should fail when a target table has no creating action"
        assert "LHP-VAL-009" in output, (
            "Validate must surface the LHP-VAL-009 table-creation error code "
            "for a table with no creator (folded cross-flowgroup "
            f"table_creation_errors, §9.24). Got:\n{output[-2000:]}"
        )

    def test_validate_multiple_table_creators_fails(self) -> None:
        """A table created by MULTIPLE actions (each ``create_table: true``)
        must surface LHP-CFG-004 on the VALIDATE path (N2).

        Symmetry guard for N1: LHP-CFG-004 is already RAISED by
        ``TableCreationValidator`` when ``len(creators) > 1``. This test
        proves ``lhp validate`` catches that raise too — the §9.24 routing
        lets the ``LHPConfigError`` propagate out of
        ``validate_cross_flowgroup`` and the executor folds it into the
        structured ``lhp_errors`` (validate then exits non-zero), rather than
        only the generate path catching it.

        Mechanism: two write actions in a single new flowgroup target the
        SAME unique table (``multi_creator_raw``) with the default
        ``create_table: true``. ``TableCreationValidator`` records two
        creators for that table and raises LHP-CFG-004 ("Multiple table
        creators detected"). The unique table name keeps the defect isolated
        from existing fixtures.

        Negative test: it expects FAILURE, so it neither generates nor diffs
        any baseline.
        """
        bad_file = (
            self.project_root
            / "pipelines"
            / "01_raw_ingestion"
            / "csv_ingestions"
            / "multiple_table_creators.yaml"
        )
        bad_file.write_text(
            "pipeline: acmi_edw_raw\n"
            "flowgroup: multiple_table_creators_fg\n"
            "actions:\n"
            "  - name: load_multi_creator\n"
            "    type: load\n"
            "    readMode: stream\n"
            "    source:\n"
            "      type: delta\n"
            '      database: "${catalog}.${raw_schema}"\n'
            "      table: customer_raw\n"
            "    target: v_multi_creator_a\n"
            "  - name: write_multi_creator_a\n"
            "    type: write\n"
            "    source: v_multi_creator_a\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            '      catalog: "${catalog}"\n'
            '      schema: "${raw_schema}"\n'
            "      table: multi_creator_raw\n"
            "      create_table: true\n"
            "  - name: write_multi_creator_b\n"
            "    type: write\n"
            "    source: v_multi_creator_a\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            '      catalog: "${catalog}"\n'
            '      schema: "${raw_schema}"\n'
            "      table: multi_creator_raw\n"
            "      create_table: true\n"
        )

        exit_code, output = self.run_validate("--env", "dev")
        assert (
            exit_code != 0
        ), "Validate should fail when a table has multiple creating actions"
        assert "LHP-CFG-004" in output, (
            "Validate must surface the LHP-CFG-004 multiple-table-creators "
            "error code (raised by TableCreationValidator, caught and folded "
            f"on the validate path per §9.24). Got:\n{output[-2000:]}"
        )

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
