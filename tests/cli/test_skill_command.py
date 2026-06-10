"""Tests for the ``lhp skill`` command group.

The group and its subcommands are invoked directly via ``CliRunner`` (not
through ``main.py``, which is import-red until the entrypoint is rewired).
File-writing subcommands run inside ``isolated_filesystem`` so the install
directory resolves under a throwaway cwd.
"""

from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli import _skill_files as sf
from lhp.cli.commands.skill_command import install, status, uninstall, update

SKILL_DIR = Path(".claude") / "skills" / "lhp"


@pytest.fixture
def runner() -> CliRunner:
    # Click >=8.2 dropped ``mix_stderr``; stderr is captured separately by
    # default and read back via ``result.stderr``.
    return CliRunner()


def test_install_creates_files_under_claude_skills(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(install, [])
        assert result.exit_code == 0, result.output
        assert (SKILL_DIR / "SKILL.md").is_file()
        assert (SKILL_DIR / sf.MARKER_FILE).is_file()
        # The marker records the current version.
        assert (SKILL_DIR / sf.MARKER_FILE).read_text().strip() == sf.current_version()


def test_reinstall_without_force_exits_one(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        first = runner.invoke(install, [])
        assert first.exit_code == 0, first.output

        second = runner.invoke(install, [])
        assert second.exit_code == 1
        # The already-installed guard surfaces via the error boundary on stderr.
        assert "already installed" in second.stderr.lower()


def test_force_overwrites_existing_install(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(install, []).exit_code == 0
        result = runner.invoke(install, ["--force"])
        assert result.exit_code == 0, result.output
        assert (SKILL_DIR / "SKILL.md").is_file()


def test_status_reports_up_to_date_after_install(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(install, []).exit_code == 0
        result = runner.invoke(status, [])
        assert result.exit_code == 0, result.output
        assert "up-to-date" in result.output


def test_status_when_not_installed_reports_missing(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(status, [])
        assert result.exit_code == 0, result.output
        assert "Not installed" in result.output


def test_update_without_prior_install_exits_one(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(update, [])
        assert result.exit_code == 1
        assert "not installed" in result.stderr.lower()


def test_update_refreshes_existing_install(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(install, []).exit_code == 0
        result = runner.invoke(update, [])
        assert result.exit_code == 0, result.output
        assert (SKILL_DIR / sf.MARKER_FILE).is_file()


def test_uninstall_with_force_removes_install(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(install, []).exit_code == 0
        result = runner.invoke(uninstall, ["--force"])
        assert result.exit_code == 0, result.output
        assert not SKILL_DIR.exists()


def test_uninstall_when_absent_is_noop_success(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(uninstall, ["--force"])
        assert result.exit_code == 0, result.output
        assert "Nothing to remove" in result.output


CLAUDE_MD = Path("CLAUDE.md")


def test_install_writes_routing_block_to_claude_md(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(install, [])
        assert result.exit_code == 0, result.output
        assert CLAUDE_MD.is_file()
        assert "lhp:routing:start" in CLAUDE_MD.read_text()


def test_install_preserves_existing_claude_md(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        CLAUDE_MD.write_text("# My rules\n\nDo the thing.\n", encoding="utf-8")
        assert runner.invoke(install, []).exit_code == 0
        text = CLAUDE_MD.read_text()
        # The user's content survives and the block is appended.
        assert "Do the thing." in text
        assert "lhp:routing:start" in text


def test_user_install_does_not_write_project_claude_md(
    runner: CliRunner, tmp_path: Path
) -> None:
    # --user targets ~/.claude; HOME is redirected so the test stays hermetic.
    with runner.isolated_filesystem():
        result = runner.invoke(install, ["--user"], env={"HOME": str(tmp_path)})
        assert result.exit_code == 0, result.output
        # No project-level CLAUDE.md is written for a global install.
        assert not CLAUDE_MD.exists()


def test_update_refreshes_routing_block(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(install, []).exit_code == 0
        # Corrupt the block; update should restore it.
        CLAUDE_MD.write_text(
            "<!-- lhp:routing:start -->\nstale\n<!-- lhp:routing:end -->\n",
            encoding="utf-8",
        )
        assert runner.invoke(update, []).exit_code == 0
        text = CLAUDE_MD.read_text()
        assert "stale" not in text
        assert "Lakehouse Plumber project" in text


def test_uninstall_removes_routing_block(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        assert runner.invoke(install, []).exit_code == 0
        assert CLAUDE_MD.is_file()
        assert runner.invoke(uninstall, ["--force"]).exit_code == 0
        # LHP created the block-only file, so it is removed entirely.
        assert not CLAUDE_MD.exists()


def test_uninstall_keeps_user_content_in_claude_md(runner: CliRunner) -> None:
    with runner.isolated_filesystem():
        CLAUDE_MD.write_text("# My rules\n\nKeep me.\n", encoding="utf-8")
        assert runner.invoke(install, []).exit_code == 0
        assert runner.invoke(uninstall, ["--force"]).exit_code == 0
        # The file persists with the user's content; only the block is stripped.
        assert CLAUDE_MD.is_file()
        text = CLAUDE_MD.read_text()
        assert "Keep me." in text
        assert "lhp:routing:start" not in text
