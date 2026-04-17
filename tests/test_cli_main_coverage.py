"""Unit tests targeting uncovered lines in lhp/cli/main.py.

Focuses on helper functions, edge-case branches, and command routing
that existing CLI tests do not exercise.
"""

import logging
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from lhp.cli.main import (
    _discover_yaml_files_with_include,
    _ensure_project_root,
    _find_project_root,
    _get_include_patterns,
    _load_project_config,
    cleanup_logging,
    cli,
    configure_logging,
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
# _ensure_project_root (lines 124-139)
# ============================================================================


@pytest.mark.unit
class TestEnsureProjectRoot:
    """Cover _ensure_project_root raising LHPError when no lhp.yaml found."""

    def test_ensure_project_root_no_project(self, tmp_path):
        """Lines 124-139: raises LHPError when _find_project_root returns None."""
        from lhp.utils.error_formatter import LHPError

        with patch("lhp.cli.main._find_project_root", return_value=None):
            with pytest.raises(LHPError) as exc_info:
                _ensure_project_root()
            assert "Not in a LakehousePlumber project directory" in exc_info.value.title

    def test_ensure_project_root_found(self, tmp_path):
        """Happy path: returns the project root when found."""
        with patch("lhp.cli.main._find_project_root", return_value=tmp_path):
            result = _ensure_project_root()
            assert result == tmp_path


# ============================================================================
# _load_project_config (lines 144-152)
# ============================================================================


@pytest.mark.unit
class TestLoadProjectConfig:
    """Cover _load_project_config branches."""

    def test_load_project_config_no_file(self, tmp_path):
        """Line 148: returns {} when lhp.yaml does not exist."""
        result = _load_project_config(tmp_path)
        assert result == {}

    def test_load_project_config_with_file(self, tmp_path):
        """Lines 150-157: loads YAML via safe_load_yaml_with_fallback."""
        (tmp_path / "lhp.yaml").write_text("name: test_proj\nversion: '1.0'\n")
        # Patch at origin since _load_project_config uses a lazy import
        with patch(
            "lhp.utils.yaml_loader.safe_load_yaml_with_fallback",
            return_value={"name": "test_proj"},
        ) as mock_load:
            result = _load_project_config(tmp_path)
            assert result == {"name": "test_proj"}
            mock_load.assert_called_once()


# ============================================================================
# _get_include_patterns (lines 162-176)
# ============================================================================


@pytest.mark.unit
class TestGetIncludePatterns:
    """Cover _get_include_patterns branches."""

    def test_get_include_patterns_with_patterns(self, tmp_path):
        """Lines 162-169: returns patterns when project config has include."""
        mock_config = MagicMock()
        mock_config.include = ["pipelines/bronze/**"]

        mock_loader = MagicMock()
        mock_loader.load_project_config.return_value = mock_config

        with patch(
            "lhp.core.project_config_loader.ProjectConfigLoader",
            return_value=mock_loader,
        ):
            result = _get_include_patterns(tmp_path)

        assert result == ["pipelines/bronze/**"]

    def test_get_include_patterns_no_include(self, tmp_path):
        """Lines 170-171: returns [] when project config has no include."""
        mock_config = MagicMock()
        mock_config.include = None

        mock_loader = MagicMock()
        mock_loader.load_project_config.return_value = mock_config

        with patch(
            "lhp.core.project_config_loader.ProjectConfigLoader",
            return_value=mock_loader,
        ):
            result = _get_include_patterns(tmp_path)
        assert result == []

    def test_get_include_patterns_no_config(self, tmp_path):
        """Line 170: returns [] when load_project_config returns None."""
        mock_loader = MagicMock()
        mock_loader.load_project_config.return_value = None

        with patch(
            "lhp.core.project_config_loader.ProjectConfigLoader",
            return_value=mock_loader,
        ):
            result = _get_include_patterns(tmp_path)
        assert result == []

    def test_get_include_patterns_exception(self, tmp_path):
        """Lines 172-176: returns [] and logs warning on exception."""
        with patch(
            "lhp.core.project_config_loader.ProjectConfigLoader",
            side_effect=Exception("config error"),
        ):
            result = _get_include_patterns(tmp_path)
        assert result == []


# ============================================================================
# _discover_yaml_files_with_include (lines 183-191)
# ============================================================================


@pytest.mark.unit
class TestDiscoverYamlFilesWithInclude:
    """Cover _discover_yaml_files_with_include branches."""

    def test_discover_with_include_patterns(self, tmp_path):
        """Lines 183-186: delegates to discover_files_with_patterns."""
        sentinel = [tmp_path / "a.yaml"]
        with patch(
            "lhp.utils.file_pattern_matcher.discover_files_with_patterns",
            return_value=sentinel,
        ) as mock_discover:
            result = _discover_yaml_files_with_include(
                tmp_path, include_patterns=["*.yaml"]
            )
        assert result == sentinel
        mock_discover.assert_called_once_with(tmp_path, ["*.yaml"])

    def test_discover_without_include_patterns(self, tmp_path):
        """Lines 188-191: collects *.yaml and *.yml via rglob."""
        (tmp_path / "a.yaml").write_text("a: 1\n")
        (tmp_path / "b.yml").write_text("b: 2\n")
        (tmp_path / "c.txt").write_text("not yaml\n")

        result = _discover_yaml_files_with_include(tmp_path, include_patterns=None)
        names = sorted(p.name for p in result)
        assert "a.yaml" in names
        assert "b.yml" in names
        assert "c.txt" not in names

    def test_discover_without_include_patterns_empty(self, tmp_path):
        """Lines 188-191: empty list also means no patterns."""
        (tmp_path / "a.yaml").write_text("x: 1\n")
        result = _discover_yaml_files_with_include(tmp_path, include_patterns=[])
        names = [p.name for p in result]
        assert "a.yaml" in names


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

    def test_state_command_routing(self, runner):
        """Lines 338-340: state command instantiates StateCommand and calls execute."""
        with self._patch_cli_group():
            with patch("lhp.cli.main._find_project_root", return_value=Path("/fake")):
                with patch(
                    "lhp.cli.commands.state_command.StateCommand.execute"
                ) as mock_exec:
                    result = runner.invoke(cli, ["state", "--env", "dev"])
                    mock_exec.assert_called_once_with(
                        "dev", None, False, False, False, False, False, False
                    )
                    assert result.exit_code == 0

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
                        "json", None, "p1", None, None, False, False
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
                    )
                    assert result.exit_code == 0

    def test_state_command_all_flags(self, runner):
        """State command with all boolean flags set."""
        with self._patch_cli_group():
            with patch("lhp.cli.main._find_project_root", return_value=Path("/fake")):
                with patch(
                    "lhp.cli.commands.state_command.StateCommand.execute"
                ) as mock_exec:
                    runner.invoke(
                        cli,
                        [
                            "state",
                            "--env",
                            "prod",
                            "--pipeline",
                            "p1",
                            "--orphaned",
                            "--stale",
                            "--new",
                            "--dry-run",
                            "--cleanup",
                            "--regen",
                        ],
                    )
                    mock_exec.assert_called_once_with(
                        "prod", "p1", True, True, True, True, True, True
                    )


# ============================================================================
# configure_logging with project_root (lines 84-94, 98)
# ============================================================================


@pytest.mark.unit
class TestConfigureLogging:
    """Cover configure_logging branches including file handler setup."""

    def test_configure_logging_with_project_root(self, tmp_path):
        """Lines 84-94, 98: creates file handler when project_root is given."""
        log_file = configure_logging(verbose=False, project_root=tmp_path)
        assert log_file is not None
        assert "lhp.log" in log_file
        assert (tmp_path / ".lhp" / "logs" / "lhp.log").exists()
        # Cleanup
        cleanup_logging()

    def test_configure_logging_verbose_with_project_root(self, tmp_path):
        """Verbose mode sets DEBUG level on console and root logger."""
        log_file = configure_logging(verbose=True, project_root=tmp_path)
        assert log_file is not None

        root = logging.getLogger()
        assert root.level == logging.DEBUG
        # Cleanup
        cleanup_logging()

    def test_configure_logging_no_project_root(self):
        """Line 98: returns None when no project_root."""
        result = configure_logging(verbose=False, project_root=None)
        assert result is None
        cleanup_logging()


# ============================================================================
# cleanup_logging with existing handlers (line 107)
# ============================================================================


@pytest.mark.unit
class TestCleanupLogging:
    """Cover cleanup_logging removing handlers."""

    def test_cleanup_removes_existing_handlers(self):
        """Lines 105-107: removes and closes all root logger handlers."""
        root = logging.getLogger()
        handler = logging.StreamHandler()
        root.addHandler(handler)
        count_before = len(root.handlers)
        assert count_before > 0

        cleanup_logging()
        assert len(root.handlers) == 0


# ============================================================================
# _find_project_root (lines 112-119)
# ============================================================================


@pytest.mark.unit
class TestFindProjectRoot:
    """Cover _find_project_root traversal."""

    def test_find_project_root_in_current_dir(self, tmp_path):
        """Line 116-117: finds lhp.yaml in current directory."""
        (tmp_path / "lhp.yaml").write_text("name: test\n")
        with patch("lhp.cli.main.Path.cwd", return_value=tmp_path):
            result = _find_project_root()
        assert result == tmp_path

    def test_find_project_root_in_parent(self, tmp_path):
        """Lines 115-117: finds lhp.yaml in a parent directory."""
        (tmp_path / "lhp.yaml").write_text("name: test\n")
        child = tmp_path / "sub" / "deep"
        child.mkdir(parents=True)
        with patch("lhp.cli.main.Path.cwd", return_value=child):
            result = _find_project_root()
        assert result == tmp_path

    def test_find_project_root_not_found(self, tmp_path):
        """Line 119: returns None when no lhp.yaml anywhere."""
        child = tmp_path / "no_project"
        child.mkdir()
        with patch("lhp.cli.main.Path.cwd", return_value=child):
            result = _find_project_root()
        assert result is None
