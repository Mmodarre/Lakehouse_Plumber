"""Acceptance tests for the ``lhp validate`` command (D2).

Invokes the ``validate_command`` Click object directly with a bare
``CliRunner()`` (NOT through ``main.py``, which is import-red until the CLI
assembly task). The autouse ``_isolate_lhp_console`` fixture in
``tests/conftest.py`` swaps the module-level Rich consoles for deterministic
plain-text sinks: the status/summary lands in ``result.stderr`` and any data on
``result.stdout``.

Validate REPORTS findings — every validation issue is folded into the terminal
``BatchValidationResponse``; ``renderer_factory.render`` then merges those into
``RunOutcome.failures`` so the exit code and the per-failure attribution line
(pipeline / flowgroup / file / CODE) are correct.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from conftest import strip_ansi

from lhp.cli.commands.validate_command import validate_command
from tests.helpers import (
    assert_no_names_in_values,
    last_cli_command_line,
    parse_last_cli_command,
    project_names,
)

pytestmark = pytest.mark.unit

FIXTURE_PROJECT = (
    Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"
)

# A flowgroup with two actions sharing the same name — ConfigValidator REPORTS
# this (LHP-VAL-007, folded into the terminal response) rather than raising.
_DUP_ACTION_FG = """pipeline: p_err
flowgroup: fg_err
actions:
  - name: dup_name
    type: load
    source: {type: sql, sql: "SELECT 1 AS id"}
    target: v_a
  - name: dup_name
    type: transform
    transform_type: sql
    sql: "SELECT * FROM v_a"
    target: v_b
  - name: write_b
    type: write
    source: v_b
    write_target: {type: streaming_table, database: c.s, table: t_out}
"""

# A valid flowgroup that uses the deprecated bare ``{token}`` syntax — emits a
# non-fatal LHP-DEPR warning (no error).
_WARNING_FG = """pipeline: p_warn
flowgroup: fg_warn
actions:
  - name: load_a
    type: load
    source: {type: sql, sql: "SELECT 1 AS id"}
    target: v_a
  - name: write_a
    type: write
    source: v_a
    write_target:
      type: streaming_table
      database: "{catalog}.{schema}"
      table: t_warn
"""


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _project(root: Path, *, pipeline_dir: str, flowgroup_yaml: str) -> None:
    """Minimal on-disk project with one flowgroup, no databricks.yml."""
    _write(root / "lhp.yaml", 'name: validate_cmd_test\nversion: "1.0"\n')
    _write(root / "substitutions" / "dev.yaml", "dev:\n  catalog: c\n  schema: s\n")
    for sub in ("presets", "templates"):
        (root / sub).mkdir(exist_ok=True)
    _write(root / "pipelines" / pipeline_dir / "fg.yaml", flowgroup_yaml)


def test_clean_project_exits_zero_and_validates_pipelines(monkeypatch):
    """Clean validate on the e2e fixture -> exit 0, with pipelines actually
    validated (guards against an empty-worklist no-op). ``--no-bundle`` because
    the fixture ships a databricks.yml (otherwise CFG-023 would fold in).
    """
    monkeypatch.chdir(FIXTURE_PROJECT)
    # Runs in-place in the tracked fixture: the parse cache must not write
    # .lhp/cache/ shards into it.
    monkeypatch.setenv("LHP_NO_CACHE", "1")
    runner = CliRunner()
    result = runner.invoke(
        validate_command, ["--env", "dev", "--no-bundle"], catch_exceptions=False
    )

    assert result.exit_code == 0, result.stderr
    # The counts banner proves a non-empty worklist ran (not "0 validated").
    # Word-boundary match: a bare substring check would false-positive on
    # multiples of ten ("20 validated" contains "0 validated").
    counts = re.search(r"(\d+) validated", result.stderr)
    assert counts is not None, result.stderr
    assert int(counts.group(1)) > 0, result.stderr


def test_known_validation_error_exits_one_with_attribution():
    """A reported validation error -> exit 1 with a failure line carrying the
    pipeline, flowgroup, file, and LHP code (sec 6.6 attribution).
    """
    runner = CliRunner()
    with runner.isolated_filesystem() as fs:
        root = Path(fs)
        _project(root, pipeline_dir="p_err", flowgroup_yaml=_DUP_ACTION_FG)
        result = runner.invoke(validate_command, ["--env", "dev", "--no-bundle"])

    assert result.exit_code == 1, result.stderr
    err = result.stderr
    # Full attribution: pipeline / flowgroup / file / CODE all present.
    assert "p_err" in err
    assert "fg_err" in err
    assert "fg.yaml" in err
    assert "LHP-VAL-007" in err


def test_strict_escalates_warning_to_failure():
    """A non-fatal warning passes (exit 0) normally but fails (exit 1) under
    ``--strict`` — proving ``--strict`` is what flips the outcome.
    """
    runner = CliRunner()
    with runner.isolated_filesystem() as fs:
        root = Path(fs)
        _project(root, pipeline_dir="p_warn", flowgroup_yaml=_WARNING_FG)
        lenient = runner.invoke(validate_command, ["--env", "dev", "--no-bundle"])
        strict = runner.invoke(
            validate_command, ["--env", "dev", "--no-bundle", "--strict"]
        )

    assert lenient.exit_code == 0, lenient.stderr
    # The warning is present in both runs; only --strict changes the exit code.
    assert "warning" in lenient.stderr.lower()
    assert strict.exit_code == 1, strict.stderr


def test_pipeline_filter_validates_named_pipeline():
    """``-p <name>`` drives the single-pipeline worklist path and still REPORTS
    the folded error (exit 1).
    """
    runner = CliRunner()
    with runner.isolated_filesystem() as fs:
        root = Path(fs)
        _project(root, pipeline_dir="p_err", flowgroup_yaml=_DUP_ACTION_FG)
        result = runner.invoke(
            validate_command, ["--env", "dev", "--no-bundle", "-p", "p_err"]
        )

    assert result.exit_code == 1, result.stderr
    assert "LHP-VAL-007" in result.stderr


def _mock_built_facade(monkeypatch: pytest.MonkeyPatch) -> "MagicMock":
    """Patch ``build_facade`` in the command module with a MagicMock facade.

    ``validation.validate_pipelines`` returns an empty event stream — ``drive``
    tolerates it (no events, empty outcome, exit 0) — so the test can assert on
    the exact kwargs the CLI forwarded without running a real validate.
    """
    facade = MagicMock()
    facade.validation.validate_pipelines.return_value = iter(())
    monkeypatch.setattr(
        "lhp.cli.commands.validate_command.build_facade", lambda *a, **k: facade
    )
    return facade


@pytest.mark.parametrize("pipeline_flag", ["-p", "--pipeline"])
def test_sandbox_and_pipeline_are_mutually_exclusive(pipeline_flag: str) -> None:
    """``--sandbox`` with ``-p``/``--pipeline`` is a Click usage error (exit 2):
    sandbox scope comes from the profile, never from a CLI filter. The check
    fires before any facade/project work, so no project dir is needed."""
    result = CliRunner().invoke(
        validate_command, ["--env", "dev", "--sandbox", pipeline_flag, "some_pipeline"]
    )

    assert result.exit_code == 2
    # rich-click colorizes the UsageError panel under GITHUB_ACTIONS (set in
    # CI), so strip ANSI and flatten the panel before matching the message.
    flat_stderr = " ".join(strip_ansi(result.stderr).replace("│", " ").split())
    assert "--sandbox cannot be combined with -p/--pipeline" in flat_stderr


def test_sandbox_flag_forwards_sandbox_true_to_facade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--sandbox`` alone reaches the facade as ``sandbox=True`` — the CLI's
    only job (§9.11); profile loading happens behind the facade."""
    facade = _mock_built_facade(monkeypatch)
    _write(tmp_path / "lhp.yaml", 'name: sandbox_cli_test\nversion: "1.0"\n')
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(
        validate_command,
        ["--env", "dev", "--no-bundle", "--no-progress", "--sandbox"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.stderr
    kwargs = facade.validation.validate_pipelines.call_args.kwargs
    assert kwargs["sandbox"] is True


def test_default_run_forwards_sandbox_false_to_facade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without the flag the facade receives ``sandbox=False`` (never auto-on)."""
    facade = _mock_built_facade(monkeypatch)
    _write(tmp_path / "lhp.yaml", 'name: sandbox_cli_test\nversion: "1.0"\n')
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(
        validate_command,
        ["--env", "dev", "--no-bundle", "--no-progress"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.stderr
    kwargs = facade.validation.validate_pipelines.call_args.kwargs
    assert kwargs["sandbox"] is False


def test_help_documents_sandbox_flag() -> None:
    """``lhp validate --help`` lists ``--sandbox`` (cheap drift regression)."""
    result = CliRunner().invoke(validate_command, ["--help"])

    assert result.exit_code == 0
    assert "--sandbox" in result.output


def test_validate_records_one_cli_command_event_in_log_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    telemetry_log_mode: Path,
    generous_shape_budget: None,
) -> None:
    """A clean validate emits exactly one ``cli.command`` envelope and no names.

    Runs on a deep copy of the fixture so the parse cache never lands in the
    tracked tree. Validate writes no files, so ``files_written`` is null.
    """
    project_dir = tmp_path / "testing_project"
    shutil.copytree(FIXTURE_PROJECT, project_dir)
    monkeypatch.chdir(project_dir)
    result = CliRunner().invoke(
        validate_command, ["--env", "dev", "--no-bundle"], catch_exceptions=False
    )
    assert result.exit_code == 0, result.stderr

    envelope = parse_last_cli_command(result.stderr)
    props = envelope["props"]
    assert props["command"] == "validate"
    assert props["exit_code"] == 0
    assert props["flags"] == ["env", "no_bundle"]
    assert props["env_class"] == "production"
    assert props["bundle_enabled"] is False
    assert props["cache_used"] is True
    assert props["files_written"] is None
    assert props["project"]["flowgroups"] > 0

    assert result.stderr.count(last_cli_command_line(result.stderr)) == 1
    assert_no_names_in_values(envelope, project_names(project_dir))
