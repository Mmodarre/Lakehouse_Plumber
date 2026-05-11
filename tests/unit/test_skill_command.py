"""Unit tests for ``lhp skill`` CLI subcommand group."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.commands.skill_command import MARKER_FILE
from lhp.cli.main import cli

pytestmark = pytest.mark.unit


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def fake_version(monkeypatch: pytest.MonkeyPatch) -> str:
    """Pin the LHP version reported by ``importlib.metadata.version``."""
    target = "9.9.9"
    monkeypatch.setattr(
        "lhp.cli.commands.skill_command.version",
        lambda _name: target,
    )
    return target


def _install_dir(home: bool = False) -> Path:
    return (Path.home() if home else Path.cwd()) / ".claude" / "skills" / "lhp"


def _expected_relative_files() -> list[str]:
    """Files that should land in the installed skill directory."""
    return [
        "SKILL.md",
        "references/actions-load.md",
        "references/actions-test.md",
        "references/actions-transform.md",
        "references/actions-write.md",
        "references/advanced.md",
        "references/best-practices.md",
        "references/cdc-patterns.md",
        "references/errors.md",
        "references/monitoring.md",
        "references/project-config.md",
        "references/templates-presets.md",
    ]


def _list_relative_files(root: Path) -> list[str]:
    return sorted(
        p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
    )


def test_install_creates_files_and_marker(runner: CliRunner, fake_version: str) -> None:
    """A fresh install copies all skill files and writes the marker."""
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["skill", "install"])
        assert result.exit_code == 0, result.output

        install_dir = _install_dir()
        assert install_dir.is_dir()

        files = _list_relative_files(install_dir)
        expected = sorted([*_expected_relative_files(), MARKER_FILE])
        assert files == expected

        # No __init__.py leak from the package layout
        assert "__init__.py" not in files
        assert not any("__pycache__" in f for f in files)

        marker = (install_dir / MARKER_FILE).read_text(encoding="utf-8").strip()
        assert marker == fake_version


def test_install_errors_if_marker_exists(runner: CliRunner, fake_version: str) -> None:
    """A second install fails with a clear ``--force`` hint."""
    with runner.isolated_filesystem():
        first = runner.invoke(cli, ["skill", "install"])
        assert first.exit_code == 0, first.output

        result = runner.invoke(cli, ["skill", "install"])
        assert result.exit_code != 0
        assert "already installed" in result.output.lower()
        assert "--force" in result.output


def test_install_force_overwrites(runner: CliRunner, fake_version: str) -> None:
    """``--force`` succeeds when an install already exists."""
    with runner.isolated_filesystem():
        first = runner.invoke(cli, ["skill", "install"])
        assert first.exit_code == 0, first.output

        # Mutate a file to verify it gets restored
        skill_md = _install_dir() / "SKILL.md"
        skill_md.write_text("tampered", encoding="utf-8")

        result = runner.invoke(cli, ["skill", "install", "--force"])
        assert result.exit_code == 0, result.output
        assert skill_md.read_text(encoding="utf-8") != "tampered"


def test_install_user_flag(
    runner: CliRunner,
    fake_version: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """``--user`` writes to ``~/.claude/skills/lhp/``."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["skill", "install", "--user"])
        assert result.exit_code == 0, result.output

        target = tmp_path / ".claude" / "skills" / "lhp"
        assert (target / "SKILL.md").is_file()
        assert (target / MARKER_FILE).read_text(encoding="utf-8").strip() == (
            fake_version
        )

        # Make sure CWD did not get an install
        assert not _install_dir().exists()


def test_update_errors_if_marker_missing(runner: CliRunner, fake_version: str) -> None:
    """Without a marker, ``update`` refuses and points to ``install``."""
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["skill", "update"])
        assert result.exit_code != 0
        assert "not installed" in result.output.lower()
        assert "lhp skill install" in result.output


def test_update_replaces_files_and_updates_marker(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``update`` replaces stale files and rewrites the marker version."""
    versions = iter(["1.0.0", "2.0.0"])

    def _version(_name: str) -> str:
        return next(versions)

    monkeypatch.setattr("lhp.cli.commands.skill_command.version", _version)

    with runner.isolated_filesystem():
        first = runner.invoke(cli, ["skill", "install"])
        assert first.exit_code == 0, first.output
        assert (_install_dir() / MARKER_FILE).read_text(
            encoding="utf-8"
        ).strip() == "1.0.0"

        # User edits a file — update should overwrite it
        skill_md = _install_dir() / "SKILL.md"
        skill_md.write_text("user-modified", encoding="utf-8")

        result = runner.invoke(cli, ["skill", "update"])
        assert result.exit_code == 0, result.output
        assert skill_md.read_text(encoding="utf-8") != "user-modified"
        assert (_install_dir() / MARKER_FILE).read_text(
            encoding="utf-8"
        ).strip() == "2.0.0"


def test_update_downgrade_warns_requires_yes(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the install is newer than the CLI, ``update`` requires confirmation."""
    versions = iter(["9.9.9", "1.0.0", "1.0.0"])

    def _version(_name: str) -> str:
        return next(versions)

    monkeypatch.setattr("lhp.cli.commands.skill_command.version", _version)

    with runner.isolated_filesystem():
        first = runner.invoke(cli, ["skill", "install"])
        assert first.exit_code == 0, first.output
        # Now CLI version returns "1.0.0" but installed is "9.9.9"

        # Refuse the prompt
        aborted = runner.invoke(cli, ["skill", "update"], input="n\n")
        assert aborted.exit_code == 0
        assert "newer" in aborted.output.lower()
        assert "Aborted" in aborted.output
        # Marker still pinned to original
        assert (_install_dir() / MARKER_FILE).read_text(
            encoding="utf-8"
        ).strip() == "9.9.9"

        # Accept via --yes
        accepted = runner.invoke(cli, ["skill", "update", "--yes"])
        assert accepted.exit_code == 0, accepted.output
        assert (_install_dir() / MARKER_FILE).read_text(
            encoding="utf-8"
        ).strip() == "1.0.0"


def test_status_not_installed(runner: CliRunner, fake_version: str) -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["skill", "status"])
        assert result.exit_code == 0, result.output
        assert "Not installed" in result.output


def test_status_up_to_date(runner: CliRunner, fake_version: str) -> None:
    with runner.isolated_filesystem():
        runner.invoke(cli, ["skill", "install"])
        result = runner.invoke(cli, ["skill", "status"])
        assert result.exit_code == 0, result.output
        assert "up-to-date" in result.output
        assert fake_version in result.output


def test_status_older_install(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Installed version older than CLI -> suggests `lhp skill update`."""
    versions = iter(["1.0.0", "2.0.0", "2.0.0"])

    def _version(_name: str) -> str:
        return next(versions)

    monkeypatch.setattr("lhp.cli.commands.skill_command.version", _version)

    with runner.isolated_filesystem():
        runner.invoke(cli, ["skill", "install"])
        result = runner.invoke(cli, ["skill", "status"])
        assert result.exit_code == 0, result.output
        assert "Update available" in result.output
        assert "lhp skill update" in result.output


def test_status_newer_install(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Installed version newer than CLI -> suggests `pip install -U`."""
    versions = iter(["9.9.9", "1.0.0", "1.0.0"])

    def _version(_name: str) -> str:
        return next(versions)

    monkeypatch.setattr("lhp.cli.commands.skill_command.version", _version)

    with runner.isolated_filesystem():
        runner.invoke(cli, ["skill", "install"])
        result = runner.invoke(cli, ["skill", "status"])
        assert result.exit_code == 0, result.output
        assert "newer" in result.output.lower()
        assert "pip install -U lakehouse-plumber" in result.output


def test_status_missing_marker(runner: CliRunner, fake_version: str) -> None:
    """Install dir exists without marker -> foreign-install message."""
    with runner.isolated_filesystem():
        runner.invoke(cli, ["skill", "install"])
        marker = _install_dir() / MARKER_FILE
        marker.unlink()

        result = runner.invoke(cli, ["skill", "status"])
        assert result.exit_code == 0, result.output
        assert "Foreign install" in result.output
        assert "--force" in result.output


def test_uninstall_removes_dir_with_confirm(
    runner: CliRunner, fake_version: str
) -> None:
    """Confirmation prompt: ``y`` removes the directory."""
    with runner.isolated_filesystem():
        runner.invoke(cli, ["skill", "install"])
        assert _install_dir().exists()

        result = runner.invoke(cli, ["skill", "uninstall"], input="y\n")
        assert result.exit_code == 0, result.output
        assert not _install_dir().exists()


def test_uninstall_force_skips_prompt(runner: CliRunner, fake_version: str) -> None:
    """``--force`` removes without prompting."""
    with runner.isolated_filesystem():
        runner.invoke(cli, ["skill", "install"])
        assert _install_dir().exists()

        result = runner.invoke(cli, ["skill", "uninstall", "--force"])
        assert result.exit_code == 0, result.output
        assert not _install_dir().exists()
