"""Tests for CLI --include-tests flag functionality."""

import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


class TestCLIIncludeTestsFlag:
    """Test CLI --include-tests flag functionality."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def temp_project(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            directories = [
                "presets",
                "templates",
                "pipelines",
                "substitutions",
                "schemas",
                "expectations",
                "generated",
            ]

            for dir_name in directories:
                (project_root / dir_name).mkdir(parents=True)

            (project_root / "lhp.yaml").write_text("""
name: test_cli_project
version: "1.0"
description: "Test project for CLI include-tests flag"
""")

            (project_root / "substitutions" / "test.yaml").write_text("""
test:
  env: test
  catalog: test_catalog
  bronze_schema: bronze
  silver_schema: silver

secrets:
  default_scope: test_secrets
""")

            pipeline_dir = project_root / "pipelines" / "test_pipeline"
            pipeline_dir.mkdir(parents=True)

            # mixed flowgroup: has both test and non-test actions
            (pipeline_dir / "mixed_flowgroup.yaml").write_text("""
pipeline: test_pipeline
flowgroup: mixed_flowgroup

actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT 1 as id"
    target: v_data

  - name: write_data
    type: write
    source: v_data
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: test_table
      create_table: true

  - name: test_data_quality
    type: test
    test_type: row_count
    source: ["v_data", "v_data"]
    tolerance: 0
    on_violation: fail
    description: "Test data quality"
""")

            test_pipeline_dir = project_root / "pipelines" / "test_only_pipeline"
            test_pipeline_dir.mkdir(parents=True)

            (test_pipeline_dir / "test_only_flowgroup.yaml").write_text("""
pipeline: test_only_pipeline  
flowgroup: test_only_flowgroup

actions:
  - name: test_data_quality_only
    type: test
    test_type: uniqueness
    source: some_table
    columns: ["id"]
    on_violation: fail
    description: "Test only pipeline"
""")

            yield project_root

    def test_cli_help_shows_include_tests_flag(self, runner):
        """Test that CLI help shows --include-tests flag."""
        result = runner.invoke(cli, ["generate", "--help"])

        assert "--include-tests" in result.output

    def test_cli_accepts_include_tests_flag(self, runner, temp_project):
        """Test that the diff command accepts the --include-tests flag.

        ``generate --dry-run`` was removed; ``lhp diff`` is its successor and
        carries the same ``--include-tests`` flag. A clean exit confirms the
        flag is still wired up (an unregistered flag is a Click usage error).
        """
        with runner.isolated_filesystem():
            import os

            os.chdir(str(temp_project))

            # Test that the flag is accepted without error
            result = runner.invoke(cli, ["diff", "--env", "test", "--include-tests"])

            # If the flag were unregistered, Click would exit with a
            # non-zero status before reaching the command body. A clean
            # exit confirms the flag is wired up correctly.
            assert result.exit_code == 0, f"CLI failed: {result.output}"

    def test_cli_default_behavior_skips_tests(self, runner, temp_project):
        """Test that diff skips test-only flowgroups by default (no flag)."""
        with runner.isolated_filesystem():
            import os

            os.chdir(str(temp_project))

            # Run without --include-tests flag (diff is the dry-run successor).
            result = runner.invoke(cli, ["diff", "--env", "test"])

            # Should succeed
            assert result.exit_code == 0, f"CLI failed: {result.output}"

            # Without --include-tests the test-only flowgroup contributes no
            # files, so only the mixed pipeline is planned. The mixed
            # flowgroup is the sole would-create entry, and the test-only
            # flowgroup never produces one. (The progress table still lists
            # ``test_only_pipeline`` at ``0 files`` on stderr — the
            # would-create line is the file-count contract that the flag
            # actually governs.) With the flag set (next test) the test-only
            # flowgroup's would-create line appears.
            assert "+ would-create  test_pipeline/mixed_flowgroup.py" in result.output
            assert "test_only_pipeline/test_only_flowgroup.py" not in result.output

    def test_cli_with_flag_includes_tests(self, runner, temp_project):
        """Test that diff includes test-only flowgroups with --include-tests."""
        with runner.isolated_filesystem():
            import os

            os.chdir(str(temp_project))

            # Run with --include-tests flag (diff is the dry-run successor).
            result = runner.invoke(
                cli,
                ["diff", "--env", "test", "--include-tests"],
            )

            # Should succeed
            assert result.exit_code == 0, f"CLI failed: {result.output}"

            # With --include-tests, the test-only flowgroup now contributes a
            # file and appears as a would-create entry in the diff body (the
            # discriminator vs the default run, whose progress table also
            # names ``test_only_pipeline`` but at ``0 files``).
            assert (
                "+ would-create  test_only_pipeline/test_only_flowgroup.py"
                in result.output
            )
