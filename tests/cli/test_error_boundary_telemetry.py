"""Every path through the error boundary reaches ``finish`` exactly once.

The boundary owns the process exit code, so each test pins two things at
once: the exit code the caller observes is unchanged, and the telemetry hook
saw that same code with the matching error code and exception class. The
hook is replaced by a recorder here; its own behaviour is covered in
``test_telemetry_hook.py``.
"""

from __future__ import annotations

import sys
from typing import Any, Dict, List

import click
import pytest

from lhp.cli import _telemetry_hook
from lhp.cli.error_boundary import cli_error_boundary
from lhp.cli.exit_codes import ExitCode
from lhp.errors import ErrorCategory, LHPError

pytestmark = pytest.mark.unit

Calls = Dict[str, List[Any]]


@pytest.fixture
def hook(monkeypatch: pytest.MonkeyPatch) -> Calls:
    """Replace ``begin``/``finish`` with recorders and return their call logs."""
    calls: Calls = {"begin": [], "finish": [], "order": []}

    def begin(operation: str) -> None:
        calls["begin"].append(operation)
        calls["order"].append("begin")

    def finish(**kwargs: Any) -> None:
        calls["finish"].append(kwargs)
        calls["order"].append("finish")

    monkeypatch.setattr(_telemetry_hook, "begin", begin)
    monkeypatch.setattr(_telemetry_hook, "finish", finish)
    return calls


def _lhp_error() -> LHPError:
    return LHPError(
        category=ErrorCategory.CONFIG,
        code_number="001",
        title="Config error",
        details="Bad config",
    )


def _only_finish(calls: Calls) -> Dict[str, Any]:
    assert len(calls["finish"]) == 1, calls["finish"]
    return calls["finish"][0]


def test_normal_return_finishes_with_zero_and_returns_the_value(hook: Calls) -> None:
    @cli_error_boundary("list presets")
    def command() -> str:
        hook["order"].append("body")
        return "value"

    assert command() == "value"
    assert hook["begin"] == ["list presets"]
    assert hook["order"] == ["begin", "body", "finish"]
    assert _only_finish(hook) == {
        "exit_code": 0,
        "error_code": None,
        "exception_class": None,
    }


def test_lhp_error_exits_one_with_its_code(hook: Calls) -> None:
    @cli_error_boundary("generate")
    def command() -> None:
        raise _lhp_error()

    with pytest.raises(SystemExit) as raised:
        command()

    assert raised.value.code == ExitCode.ERROR
    assert _only_finish(hook) == {
        "exit_code": ExitCode.ERROR,
        "error_code": "LHP-CFG-001",
        "exception_class": "LHPError",
    }


def test_unexpected_exception_exits_three_with_the_original_class(hook: Calls) -> None:
    @cli_error_boundary("generate")
    def command() -> None:
        raise ValueError("boom")

    with pytest.raises(SystemExit) as raised:
        command()

    assert raised.value.code == ExitCode.INTERNAL_ERROR
    assert _only_finish(hook) == {
        "exit_code": ExitCode.INTERNAL_ERROR,
        "error_code": "LHP-GEN-902",
        "exception_class": "ValueError",
    }


def test_sys_exit_passes_through_with_its_code(hook: Calls) -> None:
    @cli_error_boundary("generate")
    def command() -> None:
        sys.exit(2)

    with pytest.raises(SystemExit) as raised:
        command()

    assert raised.value.code == 2
    assert _only_finish(hook) == {
        "exit_code": 2,
        "error_code": None,
        "exception_class": None,
    }


def test_outcome_exit_records_the_enum_as_a_plain_int(hook: Calls) -> None:
    """``exit_for_outcome`` raises ``SystemExit(ExitCode.ERROR)``; telemetry sees 1."""

    @cli_error_boundary("validate")
    def command() -> None:
        sys.exit(ExitCode.ERROR)

    with pytest.raises(SystemExit) as raised:
        command()

    assert raised.value.code == ExitCode.ERROR
    recorded = _only_finish(hook)["exit_code"]
    assert recorded == 1 and type(recorded) is int


def test_usage_error_propagates_natively_and_records_two(hook: Calls) -> None:
    @cli_error_boundary("generate")
    def command() -> None:
        raise click.UsageError("--sandbox cannot be combined with -p")

    with pytest.raises(click.UsageError):
        command()

    assert _only_finish(hook) == {
        "exit_code": ExitCode.USAGE_ERROR,
        "error_code": None,
        "exception_class": "UsageError",
    }


def test_keyboard_interrupt_is_re_raised_and_records_130(hook: Calls) -> None:
    @cli_error_boundary("generate")
    def command() -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        command()

    assert _only_finish(hook) == {
        "exit_code": 130,
        "error_code": None,
        "exception_class": "KeyboardInterrupt",
    }


def test_finish_runs_before_the_process_exit(hook: Calls) -> None:
    """The join budget must be spent inside the boundary, before ``sys.exit``."""

    @cli_error_boundary("generate")
    def command() -> None:
        raise _lhp_error()

    with pytest.raises(SystemExit):
        command()

    assert hook["order"] == ["begin", "finish"]
