"""Unit tests for the generated-code formatting utilities.

Formatting is performed by a single terminal ``ruff format`` pass
(:func:`format_generated_tree`); the in-worker step only asserts the
generated source parses (:func:`assert_generated_python_valid`).

The ``format_generated_tree`` tests MOCK the ruff subprocess (constitution
§8.8 — mock the external boundary, not LHP code): they assert the command LHP
*builds* and how it reacts to ruff's exit code, without invoking real ruff.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.core.codegen.formatter import (
    _RUFF_FORMAT_BASE_ARGS,
    _ruff_exe,
    assert_generated_python_valid,
    format_generated_tree,
)
from lhp.errors import LHPConfigError


@pytest.mark.unit
def test_assert_generated_python_valid_raises_cfg_031_with_flowgroup():
    """Invalid source raises LHP-CFG-031 with the flowgroup name in the message."""
    with pytest.raises(LHPConfigError) as exc_info:
        assert_generated_python_valid("def (:", "my_flowgroup")
    assert exc_info.value.code_number == "031"
    assert "my_flowgroup" in str(exc_info.value)


@pytest.mark.unit
def test_assert_generated_python_valid_noop_on_valid():
    """Valid source is a no-op returning None (microsecond ast.parse)."""
    assert assert_generated_python_valid("x = 1\n", "fg") is None


@pytest.mark.unit
def test_ruff_exe_returns_existing_path():
    """``_ruff_exe`` resolves the bundled ruff to a path that exists.

    ruff is a runtime dependency (and installed in this dev env), so the
    resolver must return a concrete, on-disk executable path — not a bare
    name that relies on PATH resolution at call time.
    """
    resolved = _ruff_exe()
    assert Path(resolved).exists(), f"ruff path does not exist: {resolved}"


# These mock ``subprocess.run`` so real ruff is never invoked; they assert
# the COMMAND LHP builds (deterministic flags + whole output dir) and the
# exit-code handling (non-zero -> actionable LHP-CFG-033, not a silent pass).


class _FakeCompleted:
    """Minimal stand-in for ``subprocess.CompletedProcess``."""

    def __init__(self, returncode: int, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.mark.unit
def test_format_generated_tree_invokes_ruff_with_deterministic_flags(tmp_path):
    """The pass shells out to ``ruff format`` with the pinned flags + output dir.

    Asserts the exact argv: the resolved ruff executable, ``format``, the
    isolation + explicit ``target-version`` / ``line-length`` overrides (so it
    ignores any user ``pyproject.toml``), and the whole output directory as the
    sole path argument.
    """
    output_dir = tmp_path / "generated" / "dev"
    output_dir.mkdir(parents=True)

    with (
        patch(
            "lhp.core.codegen.formatter.subprocess.run",
            return_value=_FakeCompleted(0),
        ) as mock_run,
        patch(
            "lhp.core.codegen.formatter._ruff_exe",
            return_value="/fake/bin/ruff",
        ),
    ):
        format_generated_tree(output_dir)

    mock_run.assert_called_once()
    argv = mock_run.call_args.args[0]
    assert argv == [
        "/fake/bin/ruff",
        "format",
        "--isolated",
        "--config",
        "target-version='py311'",
        "--config",
        "line-length=88",
        str(output_dir),
    ]
    # The deterministic flags are single-sourced and do NOT pin to the user's
    # config: ``--isolated`` must be present so the user's pyproject is ignored.
    assert "--isolated" in _RUFF_FORMAT_BASE_ARGS
    # check=False so we inspect the return code ourselves (no silent CalledProcessError).
    assert mock_run.call_args.kwargs["check"] is False
    assert mock_run.call_args.kwargs["capture_output"] is True


@pytest.mark.unit
def test_format_generated_tree_targets_the_output_dir(tmp_path):
    """The command targets the WHOLE output dir (last argv element)."""
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    with (
        patch(
            "lhp.core.codegen.formatter.subprocess.run",
            return_value=_FakeCompleted(0),
        ) as mock_run,
        patch(
            "lhp.core.codegen.formatter._ruff_exe",
            return_value="/fake/bin/ruff",
        ),
    ):
        format_generated_tree(output_dir)

    argv = mock_run.call_args.args[0]
    assert argv[-1] == str(output_dir)


@pytest.mark.unit
def test_format_generated_tree_nonzero_exit_raises_actionable_error(tmp_path):
    """A non-zero ruff exit raises LHP-CFG-033 carrying ruff's stderr (no silent pass)."""
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    with (
        patch(
            "lhp.core.codegen.formatter.subprocess.run",
            return_value=_FakeCompleted(
                2, stdout="", stderr="error: Failed to parse generated.py"
            ),
        ),
        patch(
            "lhp.core.codegen.formatter._ruff_exe",
            return_value="/fake/bin/ruff",
        ),
    ):
        with pytest.raises(LHPConfigError) as exc_info:
            format_generated_tree(output_dir)

    err = exc_info.value
    assert err.code_number == "033"
    # ruff's stderr must surface in the error so the failure is diagnosable.
    assert "Failed to parse generated.py" in str(err)


@pytest.mark.unit
def test_format_generated_tree_zero_exit_does_not_raise(tmp_path):
    """A clean ruff run (exit 0) returns None and does not raise."""
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    with (
        patch(
            "lhp.core.codegen.formatter.subprocess.run",
            return_value=_FakeCompleted(0),
        ),
        patch(
            "lhp.core.codegen.formatter._ruff_exe",
            return_value="/fake/bin/ruff",
        ),
    ):
        assert format_generated_tree(output_dir) is None
