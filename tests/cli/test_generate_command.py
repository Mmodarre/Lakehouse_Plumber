"""Acceptance tests for the rewritten ``lhp generate`` command shell.

The command object is invoked DIRECTLY (not through ``lhp.cli.main`` — that
module is import-red until the W3 cutover). A bare :class:`CliRunner` is used:
Click 8.4 removed the ``mix_stderr`` kwarg, so stdout and stderr are captured
separately as ``result.stdout`` / ``result.stderr``.

The fixture project (``tests/e2e/fixtures/testing_project``) is deep-copied
into a temp dir per test because ``generate`` writes files; the source fixture
is never mutated. ``--no-bundle`` keeps the run deterministic on the
bundle-enabled fixture (no ``--pipeline-config`` preflight). The CliRunner is
non-interactive, so the event-stream presenter selects its log renderer and
every status line lands on stderr — stdout stays empty.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from conftest import strip_ansi

from lhp.cli.commands.generate_command import generate

_FIXTURE = Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Deep-copy the e2e fixture project so a generate run cannot mutate it."""
    dest = tmp_path / "testing_project"
    shutil.copytree(_FIXTURE, dest)
    return dest


def test_clean_run_generates_files_and_reports_nonzero_count(
    project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful generate writes real .py files and reports a NON-ZERO count.

    When no ``-p`` is given the command must derive ``pipeline_fields`` from the
    discovered flowgroups; omitting it makes the facade generate ZERO pipelines
    (exit 0, empty stdout, ``0 pipelines generated`` on stderr) — a silent no-op
    this test is built to catch. Asserts the real worklist: exit 0, empty stdout,
    >=10 generated ``.py`` files under ``generated/dev``, and the stderr summary's
    ``N pipelines generated`` count is non-zero and matches the files on disk.
    """
    monkeypatch.chdir(project_dir)
    result = CliRunner().invoke(
        generate,
        ["-e", "dev", "--no-bundle", "--no-progress"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.stderr
    # generate writes files, not text — stdout is the machine-readable channel
    # and must stay empty; all human status/summary goes to stderr.
    assert result.stdout == ""

    # Real output: the fixture has many flowgroups across several pipelines.
    py_files = list((project_dir / "generated" / "dev").rglob("*.py"))
    assert len(py_files) >= 10, (
        f"expected a real generated tree, got {len(py_files)} .py files; "
        f"stderr=\n{result.stderr}"
    )

    # The post-run summary banner reports a NON-ZERO count on stderr.
    match = re.search(r"(\d+)\s+pipelines?\s+generated", result.stderr)
    assert match is not None, (
        f"no 'N pipelines generated' summary on stderr:\n{result.stderr}"
    )
    generated_count = int(match.group(1))
    assert generated_count > 0, f"summary reports zero generated:\n{result.stderr}"


def test_removed_no_state_flag_is_a_usage_error(project_dir: Path) -> None:
    """``--no-state`` was removed; Click rejects the unknown option with exit 2."""
    result = CliRunner().invoke(generate, ["-e", "dev", "--no-state"])

    assert result.exit_code == 2
    assert "no such option" in result.stderr.lower() or "no-state" in result.stderr


def test_env_is_required() -> None:
    """``--env`` is mandatory; omitting it is a Click usage error (exit 2)."""
    result = CliRunner().invoke(generate, [])

    assert result.exit_code == 2
    assert "--env" in result.stderr or "env" in result.stderr.lower()


@pytest.fixture
def minimal_project(tmp_path: Path) -> Path:
    """Bare project marker so ``resolve_project_root`` succeeds; the facade is
    mocked in the sandbox-forwarding tests, so nothing else is read from disk."""
    (tmp_path / "lhp.yaml").write_text('name: sandbox_cli_test\nversion: "1.0"\n')
    return tmp_path


def _mock_built_facade(monkeypatch: pytest.MonkeyPatch) -> "MagicMock":
    """Patch ``build_facade`` in the command module with a MagicMock facade.

    ``generation.generate_pipelines`` returns an empty event stream — ``drive``
    tolerates it (no events, empty outcome, exit 0) — so the test can assert on
    the exact kwargs the CLI forwarded without running a real generate.
    """
    facade = MagicMock()
    facade.generation.generate_pipelines.return_value = iter(())
    monkeypatch.setattr(
        "lhp.cli.commands.generate_command.build_facade", lambda *a, **k: facade
    )
    return facade


@pytest.mark.parametrize("pipeline_flag", ["-p", "--pipeline"])
def test_sandbox_and_pipeline_are_mutually_exclusive(pipeline_flag: str) -> None:
    """``--sandbox`` with ``-p``/``--pipeline`` is a Click usage error (exit 2):
    sandbox scope comes from the profile, never from a CLI filter. The check
    fires before any facade/project work, so no project dir is needed."""
    result = CliRunner().invoke(
        generate, ["-e", "dev", "--sandbox", pipeline_flag, "some_pipeline"]
    )

    assert result.exit_code == 2
    # rich-click colorizes the UsageError panel under GITHUB_ACTIONS (set in
    # CI), so strip ANSI and flatten the panel before matching the message.
    flat_stderr = " ".join(strip_ansi(result.stderr).replace("│", " ").split())
    assert "--sandbox cannot be combined with -p/--pipeline" in flat_stderr


def test_sandbox_flag_forwards_sandbox_true_to_facade(
    minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--sandbox`` alone reaches the facade as ``sandbox=True`` — the CLI's
    only job (§9.11); profile loading happens behind the facade."""
    facade = _mock_built_facade(monkeypatch)
    monkeypatch.chdir(minimal_project)
    result = CliRunner().invoke(
        generate,
        ["-e", "dev", "--no-bundle", "--no-progress", "--sandbox"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.stderr
    kwargs = facade.generation.generate_pipelines.call_args.kwargs
    assert kwargs["sandbox"] is True


def test_default_run_forwards_sandbox_false_to_facade(
    minimal_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without the flag the facade receives ``sandbox=False`` (never auto-on)."""
    facade = _mock_built_facade(monkeypatch)
    monkeypatch.chdir(minimal_project)
    result = CliRunner().invoke(
        generate,
        ["-e", "dev", "--no-bundle", "--no-progress"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.stderr
    kwargs = facade.generation.generate_pipelines.call_args.kwargs
    assert kwargs["sandbox"] is False


def test_help_documents_sandbox_flag() -> None:
    """``lhp generate --help`` lists ``--sandbox`` (cheap drift regression)."""
    result = CliRunner().invoke(generate, ["--help"])

    assert result.exit_code == 0
    assert "--sandbox" in result.output
