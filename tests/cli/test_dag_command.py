"""Acceptance tests for the ``dag`` command and its hidden ``deps`` alias.

Invokes the command objects directly (not through ``main.py``, which is
import-red until the CLI rebuild lands). Click 8.4 always captures stdout and
stderr separately, so ``CliRunner()`` (no ``mix_stderr`` kwarg — removed in
Click 8.2) gives ``result.stdout`` / ``result.stderr`` distinctly.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.commands.dag_command import dag, deps

# A minimal but schema-valid project: one flowgroup with a SQL-source load and
# a streaming-table write, so dependency analysis runs without warnings.
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


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_dag_json_writes_file_path_to_stdout_and_analysis_to_stderr(
    runner: CliRunner,
) -> None:
    """``dag --format json`` writes JSON under .lhp/dependencies/.

    The written path goes to STDOUT (pipeable); the stages/dependency view
    goes to STDERR (diagnostic).
    """
    with runner.isolated_filesystem():
        root = Path.cwd()
        _make_project(root)

        result = runner.invoke(dag, ["--format", "json"], catch_exceptions=False)

        assert result.exit_code == 0, result.stderr

        json_path = root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
        assert json_path.exists(), "expected JSON output under .lhp/dependencies/"

        # File path is the primary data stream -> stdout.
        assert "pipeline_dependencies.json" in result.stdout

        # Analysis view (stages, deps) is diagnostic -> stderr, NOT stdout.
        assert "Dependency analysis" in result.stderr
        assert "Execution order" in result.stderr
        assert "Dependency analysis" not in result.stdout


def test_dag_writes_only_requested_format(runner: CliRunner) -> None:
    """``--format json`` writes the JSON file and not the dot/text/job ones."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _make_project(root)

        result = runner.invoke(dag, ["--format", "json"], catch_exceptions=False)
        assert result.exit_code == 0, result.stderr

        dep_dir = root / ".lhp" / "dependencies"
        written = sorted(p.name for p in dep_dir.glob("*"))
        assert written == ["pipeline_dependencies.json"]


def test_dag_bogus_format_exits_2(runner: CliRunner) -> None:
    """A bad ``--format`` token is a usage error (exit code 2)."""
    with runner.isolated_filesystem():
        _make_project(Path.cwd())

        result = runner.invoke(dag, ["--format", "bogus"])

        assert result.exit_code == 2
        assert "invalid format" in result.stderr.lower()


def test_dag_format_all_writes_every_format(runner: CliRunner) -> None:
    """``--format all`` expands to dot/json/text (job needs job-name wiring)."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _make_project(root)

        result = runner.invoke(dag, ["--format", "all"], catch_exceptions=False)
        assert result.exit_code == 0, result.stderr

        dep_dir = root / ".lhp" / "dependencies"
        written = {p.suffix for p in dep_dir.glob("*")}
        # dot, json, and text formats all produce files.
        assert {".dot", ".json", ".txt"}.issubset(written)


def test_deps_alias_warns_and_forwards(runner: CliRunner) -> None:
    """``deps`` emits a DeprecationWarning and produces the same output as ``dag``."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _make_project(root)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = runner.invoke(deps, ["--format", "json"], catch_exceptions=False)

        assert result.exit_code == 0, result.stderr

        messages = [
            str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert any("deprecated" in m for m in messages), messages

        # Same observable output as `dag`: path on stdout, analysis on stderr.
        json_path = root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
        assert json_path.exists()
        assert "pipeline_dependencies.json" in result.stdout
        assert "Dependency analysis" in result.stderr


def test_deps_is_hidden(runner: CliRunner) -> None:
    """The ``deps`` alias is hidden from help listings."""
    assert deps.hidden is True


def test_dag_pipeline_filter_scopes_analysis(runner: CliRunner) -> None:
    """``--pipeline`` restricts the graph to the named pipeline."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _make_project(root)
        (root / "pipelines" / "other.yaml").write_text(
            _FLOWGROUP_YAML.replace("test_pipeline", "other_pipeline").replace(
                "test_flowgroup", "other_flowgroup"
            )
        )

        result = runner.invoke(
            dag,
            ["--format", "json", "--pipeline", "test_pipeline"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.stderr
        assert "test_pipeline" in result.stderr
        assert "other_pipeline" not in result.stderr


def test_dag_pipeline_filter_skips_job_format_with_warning(
    runner: CliRunner,
) -> None:
    """Job files are whole-project artifacts: filtered runs skip them."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _make_project(root)

        result = runner.invoke(
            dag,
            ["--format", "all", "--pipeline", "test_pipeline"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.stderr

        dep_dir = root / ".lhp" / "dependencies"
        assert not list(dep_dir.glob("*.job.yml"))
        assert not list(dep_dir.glob("*job*"))


def test_dag_trust_depends_on_flag_accepted(runner: CliRunner) -> None:
    """``--trust-depends-on`` runs the analysis in trust mode (exit 0)."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _make_project(root)

        result = runner.invoke(
            dag,
            ["--format", "json", "--trust-depends-on"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.stderr
        json_path = root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
        assert json_path.exists()
