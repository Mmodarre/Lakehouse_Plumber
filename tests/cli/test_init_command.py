"""Tests for the ``lhp init`` command (``lhp.cli.commands.init_command``).

Uses ``CliRunner()`` (Click 8.4 removed the ``mix_stderr`` kwarg) so the
success report (stdout) and the error panel (stderr) can be asserted
independently.
"""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from lhp.cli.commands.init_command import init


def test_init_fresh_dir_succeeds_and_shows_tree() -> None:
    """Fresh cwd -> exit 0, project scaffolded, created tree reported."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        # Project was actually scaffolded into the cwd.
        assert Path("lhp.yaml").exists()
        # Success report names the project and the created structure.
        assert "Initialized LHP" in result.stdout
        assert "lhp.yaml" in result.stdout
        # Next-step guidance is present.
        assert "lhp validate" in result.stdout
        assert "lhp generate" in result.stdout


def test_init_default_is_bundle_and_shows_deploy_step() -> None:
    """Default (no --no-bundle) scaffolds a bundle project incl. databricks.yml."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        assert Path("databricks.yml").exists()
        assert "databricks bundle deploy" in result.stdout


def test_init_no_bundle_skips_bundle_files() -> None:
    """--no-bundle scaffolds a plain project without databricks.yml."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["--no-bundle", "demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        assert Path("lhp.yaml").exists()
        assert not Path("databricks.yml").exists()
        assert "databricks bundle deploy" not in result.stdout


def test_init_existing_lhp_yaml_raises_io_007() -> None:
    """Pre-existing lhp.yaml -> exit 1 with LHP-IO-007 surfaced on stderr."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("lhp.yaml").write_text("name: existing\n")

        result = runner.invoke(init, ["demo_project"])

        assert result.exit_code == 1, (
            f"exit {result.exit_code}; stdout:\n{result.stdout}"
        )
        assert "LHP-IO-007" in result.stderr
