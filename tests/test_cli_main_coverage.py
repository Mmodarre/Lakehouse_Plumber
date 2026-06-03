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


@pytest.mark.unit
class TestGetVersionEdgeCases:
    def test_get_version_pyproject_no_version_match(self):
        with patch("lhp.cli.main.version", side_effect=Exception("not installed")):
            with tempfile.TemporaryDirectory() as tmpdir:
                pyproject = Path(tmpdir) / "pyproject.toml"
                pyproject.write_text("[tool.poetry]\nname = 'pkg'\n")

                import lhp.cli.main as mod

                orig = mod.__file__
                try:
                    mod.__file__ = str(Path(tmpdir) / "cli" / "main.py")
                    result = get_version()
                    assert result == "0.2.11"
                finally:
                    mod.__file__ = orig

    def test_get_version_pyproject_read_raises(self):
        with patch("lhp.cli.main.version", side_effect=Exception("not installed")):
            with tempfile.TemporaryDirectory() as tmpdir:
                pyproject = Path(tmpdir) / "pyproject.toml"
                pyproject.write_text("version = '9.9.9'\n")

                import lhp.cli.main as mod

                orig = mod.__file__
                try:
                    mod.__file__ = str(Path(tmpdir) / "cli" / "main.py")
                    with patch("builtins.open", side_effect=PermissionError("denied")):
                        result = get_version()
                    assert result == "0.2.11"
                finally:
                    mod.__file__ = orig


@pytest.mark.unit
class TestCliPerfFlag:
    def test_perf_flag_enables_timing(self):
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None):
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch(
                    "lhp.utils.performance_timer.enable_perf_timing"
                ) as mock_perf:
                    runner.invoke(cli, ["--perf", "--help"])

        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None):
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch(
                    "lhp.utils.performance_timer.enable_perf_timing"
                ) as mock_perf:
                    with patch("lhp.cli.commands.init_command.InitCommand.execute"):
                        runner.invoke(cli, ["--perf", "init", "test_proj"])
                        mock_perf.assert_called_once_with(None)


@pytest.mark.unit
class TestCliLogFileFlag:
    """§8.1: exercises the real group callback so a regression that ignores the flag (or hardcodes log_to_file) is caught."""

    def test_log_file_flag_sets_log_to_file_true(self):
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None) as mock_cfg:
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch("lhp.cli.commands.init_command.InitCommand.execute"):
                    runner.invoke(cli, ["--log-file", "init", "test_proj"])
        mock_cfg.assert_called_once()
        assert mock_cfg.call_args.kwargs["log_to_file"] is True

    def test_no_log_file_flag_sets_log_to_file_false(self):
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None) as mock_cfg:
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch("lhp.cli.commands.init_command.InitCommand.execute"):
                    runner.invoke(cli, ["init", "test_proj"])
        mock_cfg.assert_called_once()
        assert mock_cfg.call_args.kwargs["log_to_file"] is False


@pytest.mark.unit
class TestCommandRouting:
    """Mock command classes to exercise routing without needing a real project."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def _patch_cli_group(self):
        return patch("lhp.cli.main.configure_logging", return_value=None)

    def test_substitutions_command_routing(self, runner):
        with self._patch_cli_group():
            with patch("lhp.cli.main._find_project_root", return_value=Path("/fake")):
                with patch(
                    "lhp.cli.commands.show_command.ShowCommand.show_substitutions"
                ) as mock_subs:
                    result = runner.invoke(cli, ["substitutions", "--env", "prod"])
                    mock_subs.assert_called_once_with("prod")
                    assert result.exit_code == 0

    def test_deps_command_routing(self, runner):
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


@pytest.mark.unit
class TestFindProjectRoot:
    def test_find_project_root_in_current_dir(self, tmp_path):
        (tmp_path / "lhp.yaml").write_text("name: test\n")
        with patch("lhp.cli._project_root.Path.cwd", return_value=tmp_path):
            result = _find_project_root()
        assert result == tmp_path

    def test_find_project_root_in_parent(self, tmp_path):
        (tmp_path / "lhp.yaml").write_text("name: test\n")
        child = tmp_path / "sub" / "deep"
        child.mkdir(parents=True)
        with patch("lhp.cli._project_root.Path.cwd", return_value=child):
            result = _find_project_root()
        assert result == tmp_path

    def test_find_project_root_not_found(self, tmp_path):
        child = tmp_path / "no_project"
        child.mkdir()
        with patch("lhp.cli._project_root.Path.cwd", return_value=child):
            result = _find_project_root()
        assert result is None
