"""Targeted tests for BaseCommand presentation helpers.

The log-file path is announced whenever a file was written, independent of
``--verbose``: the ``--log-file`` opt-in flag writes a file even without ``-v``,
so the user must always be told where it landed.
"""

import click
from click.testing import CliRunner

from lhp.cli.commands.base_command import BaseCommand


def _run(verbose, log_file):
    """Invoke announce_log_file inside a Click context, return captured stdout."""

    @click.command()
    def _cmd():
        cmd = BaseCommand()
        cmd.verbose = verbose
        cmd.log_file = log_file
        cmd.announce_log_file()

    result = CliRunner().invoke(_cmd, [])
    assert result.exit_code == 0, result.output
    return result.output


def test_announces_path_when_log_file_set_without_verbose():
    """log_file set + verbose=False -> path emitted exactly once."""
    out = _run(verbose=False, log_file="/tmp/lhp/run.log")
    assert "Detailed logs: /tmp/lhp/run.log" in out
    assert out.count("Detailed logs:") == 1


def test_announces_path_when_log_file_set_with_verbose():
    """log_file set + verbose=True -> still exactly once, not gated on verbose."""
    out = _run(verbose=True, log_file="/tmp/lhp/run.log")
    assert "Detailed logs: /tmp/lhp/run.log" in out
    assert out.count("Detailed logs:") == 1


def test_no_announcement_when_log_file_none_not_verbose():
    """log_file None + verbose=False -> no log-path line."""
    out = _run(verbose=False, log_file=None)
    assert "Detailed logs:" not in out


def test_no_announcement_when_log_file_none_verbose():
    """log_file None + verbose=True -> still no log-path line."""
    out = _run(verbose=True, log_file=None)
    assert "Detailed logs:" not in out
