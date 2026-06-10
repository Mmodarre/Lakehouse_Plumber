"""Integration tests for the ``dag`` command wired into the top-level CLI group.

Successor to the old ``test_cli_deps_integration.py``: the ``deps`` command and
its backing ``DependenciesCommand`` class were removed in the CLI rebuild and
replaced by the ``dag`` command object (``lhp.cli.commands.dag_command``), with
``deps`` surviving only as a hidden, deprecation-warning alias. These tests
exercise ``dag`` *through* ``lhp.cli.main.cli`` — registration, the option
surface, Click's format-choice usage error, and the hidden alias — rather than
the command object directly (that level is covered by
``tests/cli/test_dag_command.py``).

Click 8.4 always captures stdout and stderr separately, so ``CliRunner()`` (no
``mix_stderr`` kwarg — removed in Click 8.2) exposes ``result.stdout`` /
``result.stderr`` distinctly. ``dag`` writes analysis to stderr and the written
output paths to stdout.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli

# A minimal but schema-valid project: one flowgroup with a SQL-source load and a
# streaming-table write, so dependency analysis runs cleanly end-to-end.
_FLOWGROUP_YAML = """\
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_seed
    type: load
    source:
      type: sql
      sql: "SELECT 1 AS id"
    target: v_seed
  - name: write_seed
    type: write
    source: v_seed
    write_target:
      type: streaming_table
      database: test_db
      table: output_table
"""


def _make_project(root: Path) -> None:
    """Write a minimal LHP project (``lhp.yaml`` + one flowgroup) under ``root``."""
    (root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")
    pipelines = root / "pipelines"
    pipelines.mkdir()
    (pipelines / "test.yaml").write_text(_FLOWGROUP_YAML)


class TestDagCommandRegistration:
    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_dag_command_registered_in_cli(self) -> None:
        """The ``dag`` command is listed in the top-level help."""
        result = self.runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "dag" in result.output

    def test_dag_command_help_lists_options(self) -> None:
        """``dag --help`` advertises the dependency-analysis option surface."""
        result = self.runner.invoke(cli, ["dag", "--help"])
        assert result.exit_code == 0
        assert "--format" in result.output
        assert "--output" in result.output
        assert "--job-name" in result.output

    def test_deps_alias_hidden_from_top_level_help(self) -> None:
        """The deprecated ``deps`` alias is hidden from the command listing."""
        result = self.runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        # Hidden commands are not advertised as their own help row.
        assert "  deps " not in result.output


class TestDagCommandExecution:
    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_dag_default_writes_outputs(self) -> None:
        """``dag`` with no options analyses deps and writes outputs (exit 0)."""
        with self.runner.isolated_filesystem():
            root = Path.cwd()
            _make_project(root)

            result = self.runner.invoke(cli, ["dag"], catch_exceptions=False)

            assert result.exit_code == 0, result.stderr
            # Default ``--format all`` writes the concrete formats under
            # .lhp/dependencies/; the written paths surface on stdout.
            dep_dir = root / ".lhp" / "dependencies"
            assert dep_dir.is_dir()
            assert any(dep_dir.iterdir())
            # The analysis view is diagnostic -> stderr.
            assert "Dependency analysis" in result.stderr

    def test_dag_format_json_writes_only_json(self) -> None:
        """``dag --format json`` writes the JSON output and reports its path."""
        with self.runner.isolated_filesystem():
            root = Path.cwd()
            _make_project(root)

            result = self.runner.invoke(
                cli, ["dag", "--format", "json"], catch_exceptions=False
            )

            assert result.exit_code == 0, result.stderr
            json_path = root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
            assert json_path.exists()
            # File path is the primary data stream -> stdout.
            assert "pipeline_dependencies.json" in result.stdout

    @pytest.mark.parametrize("fmt", ["dot", "json", "text", "all"])
    def test_dag_accepts_valid_format_choices(self, fmt: str) -> None:
        """Every documented ``--format`` token is accepted (exit 0)."""
        with self.runner.isolated_filesystem():
            _make_project(Path.cwd())

            result = self.runner.invoke(
                cli, ["dag", "--format", fmt], catch_exceptions=False
            )
            assert result.exit_code == 0, (
                f"format {fmt} should be valid: {result.stderr}"
            )

    def test_dag_invalid_format_is_usage_error(self) -> None:
        """A bad ``--format`` token is a Click usage error -> exit code 2."""
        with self.runner.isolated_filesystem():
            _make_project(Path.cwd())

            result = self.runner.invoke(cli, ["dag", "--format", "invalid_format"])

            # Click rejects values outside the accepted set before the command
            # body runs; the new 4-value scheme maps usage errors to 2.
            assert result.exit_code == 2

    def test_dag_outside_project_is_domain_error(self) -> None:
        """Running ``dag`` outside a project raises LHP-CFG-011 -> exit code 1."""
        with self.runner.isolated_filesystem():
            # No lhp.yaml anywhere: resolve_project_root raises an LHPError,
            # which the error boundary maps to ExitCode.ERROR (1).
            result = self.runner.invoke(cli, ["dag"])

            assert result.exit_code == 1


class TestDeprecatedDepsAlias:
    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_deps_alias_warns_and_still_works(self) -> None:
        """The hidden ``deps`` alias forwards to ``dag`` with a DeprecationWarning."""
        with self.runner.isolated_filesystem():
            root = Path.cwd()
            _make_project(root)

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = self.runner.invoke(
                    cli, ["deps", "--format", "json"], catch_exceptions=False
                )

            assert result.exit_code == 0, result.stderr

            messages = [
                str(w.message)
                for w in caught
                if issubclass(w.category, DeprecationWarning)
            ]
            assert any("deprecated" in m for m in messages), messages

            # Same observable behavior as ``dag``: JSON written, path on stdout.
            json_path = root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
            assert json_path.exists()
            assert "pipeline_dependencies.json" in result.stdout
