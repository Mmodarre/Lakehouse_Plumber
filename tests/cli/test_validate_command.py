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
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.commands.validate_command import validate_command

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
