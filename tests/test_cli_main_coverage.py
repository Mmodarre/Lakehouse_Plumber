"""Unit tests targeting uncovered lines in lhp/cli/main.py.

Focuses on helper functions, edge-case branches, and command routing
that existing CLI tests do not exercise.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from lhp.cli.main import (
    _find_project_root,
    cli,
    get_version,
)

# ============================================================================
# get_version edge cases (lines 50, 52-53)
# ============================================================================


@pytest.mark.unit
class TestGetVersionEdgeCases:
    """Cover fallback paths inside get_version()."""

    def test_get_version_pyproject_no_version_match(self):
        """Line 50: pyproject.toml exists but contains no version = '...' line."""
        with patch("lhp.cli.main.version", side_effect=Exception("not installed")):
            with tempfile.TemporaryDirectory() as tmpdir:
                # Create a pyproject.toml without a version field
                pyproject = Path(tmpdir) / "pyproject.toml"
                pyproject.write_text("[tool.poetry]\nname = 'pkg'\n")

                import lhp.cli.main as mod

                orig = mod.__file__
                try:
                    mod.__file__ = str(Path(tmpdir) / "cli" / "main.py")
                    result = get_version()
                    # Falls through to final fallback
                    assert result == "0.2.11"
                finally:
                    mod.__file__ = orig

    def test_get_version_pyproject_read_raises(self):
        """Lines 52-53: exception while reading pyproject.toml triggers
        the inner except block and logs a debug message."""
        with patch("lhp.cli.main.version", side_effect=Exception("not installed")):
            with tempfile.TemporaryDirectory() as tmpdir:
                # Create a pyproject.toml that exists (so the open() call is
                # reached), then make builtins.open raise when invoked.
                pyproject = Path(tmpdir) / "pyproject.toml"
                pyproject.write_text("version = '9.9.9'\n")

                import lhp.cli.main as mod

                orig = mod.__file__
                try:
                    mod.__file__ = str(Path(tmpdir) / "cli" / "main.py")
                    # Patch builtins.open to blow up, triggering except on line 52
                    with patch("builtins.open", side_effect=PermissionError("denied")):
                        result = get_version()
                    assert result == "0.2.11"
                finally:
                    mod.__file__ = orig


# ============================================================================
# CLI group --perf flag (lines 217-219)
# ============================================================================


@pytest.mark.unit
class TestCliPerfFlag:
    """Cover the hidden --perf flag branch."""

    def test_perf_flag_enables_timing(self):
        """Lines 217-219: --perf calls enable_perf_timing."""
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None):
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch(
                    "lhp.utils.performance_timer.enable_perf_timing"
                ) as mock_perf:
                    # Invoke with --perf and --help to avoid needing a subcommand
                    # that requires a project root
                    runner.invoke(cli, ["--perf", "--help"])
                    # --help exits before subcommand, but cli() group callback
                    # still runs, so perf should have been called.
                    # Actually --help short-circuits. We need a real subcommand.
                    # Let's use init which doesn't need project root.

        # Try again with a subcommand that triggers the group callback + perf
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None):
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch(
                    "lhp.utils.performance_timer.enable_perf_timing"
                ) as mock_perf:
                    with patch("lhp.cli.commands.init_command.InitCommand.execute"):
                        runner.invoke(cli, ["--perf", "init", "test_proj"])
                        mock_perf.assert_called_once_with(None)


# ============================================================================
# CLI group --log-file flag wiring (flag -> callback -> configure_logging)
# ============================================================================


@pytest.mark.unit
class TestCliLogFileFlag:
    """Cover the --log-file flag-wiring seam (§8.1).

    The unit tests for configure_logging call it directly, bypassing Click.
    These exercise the real group callback so a regression that ignores the
    flag (or hardcodes log_to_file) is caught.
    """

    def test_log_file_flag_sets_log_to_file_true(self):
        """--log-file routes through to configure_logging(log_to_file=True)."""
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None) as mock_cfg:
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch("lhp.cli.commands.init_command.InitCommand.execute"):
                    runner.invoke(cli, ["--log-file", "init", "test_proj"])
        mock_cfg.assert_called_once()
        assert mock_cfg.call_args.kwargs["log_to_file"] is True

    def test_no_log_file_flag_sets_log_to_file_false(self):
        """Without --log-file, configure_logging gets log_to_file=False (default)."""
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None) as mock_cfg:
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch("lhp.cli.commands.init_command.InitCommand.execute"):
                    runner.invoke(cli, ["init", "test_proj"])
        mock_cfg.assert_called_once()
        assert mock_cfg.call_args.kwargs["log_to_file"] is False


# ============================================================================
# Command routing - state, substitutions, deps (lines 338-340, 387-389, 436-438)
# ============================================================================


@pytest.mark.unit
class TestCommandRouting:
    """Cover command body lines for state, substitutions, and deps commands.

    These commands delegate to their respective Command classes.  We mock the
    command classes to exercise the routing code without needing a real project.
    """

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def _patch_cli_group(self):
        """Context manager that patches the cli group callback to be a no-op."""
        return patch("lhp.cli.main.configure_logging", return_value=None)

    def test_substitutions_command_routing(self, runner):
        """Lines 387-389: substitutions command routes to ShowCommand.show_substitutions."""
        with self._patch_cli_group():
            with patch("lhp.cli.main._find_project_root", return_value=Path("/fake")):
                with patch(
                    "lhp.cli.commands.show_command.ShowCommand.show_substitutions"
                ) as mock_subs:
                    result = runner.invoke(cli, ["substitutions", "--env", "prod"])
                    mock_subs.assert_called_once_with("prod")
                    assert result.exit_code == 0

    def test_deps_command_routing(self, runner):
        """Lines 436-438: deps command routes to DependenciesCommand.execute."""
        with self._patch_cli_group():
            with patch("lhp.cli.main._find_project_root", return_value=Path("/fake")):
                with patch(
                    "lhp.cli.commands.dependencies_command.DependenciesCommand.execute"
                ) as mock_exec:
                    result = runner.invoke(
                        cli, ["deps", "--format", "json", "--pipeline", "p1"]
                    )
                    mock_exec.assert_called_once_with(
                        "json",
                        None,
                        "p1",
                        None,
                        None,
                        False,
                        False,
                        expand_blueprints=False,
                        blueprint_filter=None,
                    )
                    assert result.exit_code == 0

    def test_deps_command_all_options(self, runner):
        """Deps command with all options exercised."""
        with self._patch_cli_group():
            with patch("lhp.cli.main._find_project_root", return_value=Path("/fake")):
                with patch(
                    "lhp.cli.commands.dependencies_command.DependenciesCommand.execute"
                ) as mock_exec:
                    result = runner.invoke(
                        cli,
                        [
                            "deps",
                            "--format",
                            "job",
                            "--output",
                            "/tmp/out",
                            "--pipeline",
                            "mypipe",
                            "--job-name",
                            "my_job",
                            "--job-config",
                            "cfg.yaml",
                            "--bundle-output",
                            "--verbose",
                        ],
                    )
                    mock_exec.assert_called_once_with(
                        "job",
                        "/tmp/out",
                        "mypipe",
                        "my_job",
                        "cfg.yaml",
                        True,
                        True,
                        expand_blueprints=False,
                        blueprint_filter=None,
                    )
                    assert result.exit_code == 0


# ============================================================================
# _find_project_root (lines 112-119)
# ============================================================================


@pytest.mark.unit
class TestFindProjectRoot:
    """Cover _find_project_root traversal."""

    def test_find_project_root_in_current_dir(self, tmp_path):
        """Line 116-117: finds lhp.yaml in current directory."""
        (tmp_path / "lhp.yaml").write_text("name: test\n")
        with patch("lhp.cli._project_root.Path.cwd", return_value=tmp_path):
            result = _find_project_root()
        assert result == tmp_path

    def test_find_project_root_in_parent(self, tmp_path):
        """Lines 115-117: finds lhp.yaml in a parent directory."""
        (tmp_path / "lhp.yaml").write_text("name: test\n")
        child = tmp_path / "sub" / "deep"
        child.mkdir(parents=True)
        with patch("lhp.cli._project_root.Path.cwd", return_value=child):
            result = _find_project_root()
        assert result == tmp_path

    def test_find_project_root_not_found(self, tmp_path):
        """Line 119: returns None when no lhp.yaml anywhere."""
        child = tmp_path / "no_project"
        child.mkdir()
        with patch("lhp.cli._project_root.Path.cwd", return_value=child):
            result = _find_project_root()
        assert result is None
