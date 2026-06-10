"""Acceptance tests for the top-level ``lhp`` command group (``cli/main.py``).

Drives the group through ``CliRunner()`` (Click 8.4: bare constructor, no
``mix_stderr``; stdout and stderr are captured separately). Asserts the four
contract points of the rebuilt entry point:

* ``--version`` prints a version string and exits 0,
* ``-h`` leads with the most-used commands and hides the retired ones,
* an unknown subcommand is a usage error (exit 2),
* importing ``lhp.cli.main`` leaves ``click.Group`` as the vanilla Click class
  (the old global ``import rich_click as click`` monkeypatch is gone).
"""

from __future__ import annotations

import re

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_version_exits_zero_with_version_string(runner: CliRunner) -> None:
    """``lhp --version`` prints a dotted version and exits 0."""
    result = runner.invoke(cli, ["--version"])

    assert result.exit_code == 0, result.output
    assert re.search(r"\d+\.\d+", result.output), result.output


def test_help_lists_active_commands(runner: CliRunner) -> None:
    """``-h`` lists every registered command."""
    result = runner.invoke(cli, ["-h"])

    assert result.exit_code == 0, result.output
    for command in (
        "generate",
        "validate",
        "dag",
        "list",
        "substitutions",
        "diff",
        "init",
        "skill",
    ):
        assert command in result.output, f"{command!r} missing from help"


def test_retired_commands_not_registered(runner: CliRunner) -> None:
    """show / stats / info are not registered and not invocable.

    The registry is the authoritative source for "is X a command"; the rendered
    help text is not, because option prose legitimately contains words like
    "Show ..." that a substring scan would false-positive on.
    """
    registered = set(cli.commands)
    for retired in ("show", "stats", "info", "doctor"):
        assert retired not in registered, f"{retired!r} unexpectedly registered"
        result = runner.invoke(cli, [retired])
        assert result.exit_code == 2, f"{retired!r} should be an unknown command"


def test_unknown_subcommand_is_usage_error(runner: CliRunner) -> None:
    """An unknown subcommand is a Click usage error (exit code 2)."""
    result = runner.invoke(cli, ["bogus-command"])

    assert result.exit_code == 2


def test_import_does_not_monkeypatch_click_group() -> None:
    """Importing the entry point leaves ``click.Group`` as vanilla Click.

    Guards against regressing to the old global ``import rich_click as click``
    swap: the group uses ``cls=RichGroup`` explicitly instead.
    """
    import click

    import lhp.cli.main

    assert click.Group.__module__.startswith("click"), click.Group.__module__
