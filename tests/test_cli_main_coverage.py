"""Unit tests targeting uncovered lines in lhp/cli/main.py.

Focuses on helper functions, edge-case branches, and command routing
that existing CLI tests do not exercise.
"""

import tempfile
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from lhp.cli.main import (
    _find_project_root,
    cli,
    get_version,
)


@contextmanager
def _patched_init():
    """Stub out ``init``'s scaffolding so the group callback can be exercised.

    ``init`` is now a module-level click object that delegates to
    ``LakehousePlumberBootstrap.init_project`` and renders the result (the old
    ``InitCommand`` class is gone). These tests only care about the group
    callback, so the bootstrap and presenter are no-oped.
    """
    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "lhp.api.LakehousePlumberBootstrap.init_project",
                return_value=SimpleNamespace(
                    success=True, error_code=None, error_message=None
                ),
            )
        )
        stack.enter_context(patch("lhp.cli.commands.init_command.render_init_result"))
        yield


@pytest.mark.unit
class TestGetVersionEdgeCases:
    # ``get_version`` now lives in ``lhp.cli._version`` (re-exported through
    # ``lhp.cli.main``). It reads the version from the installed distribution
    # and falls back to parsing ``pyproject.toml`` relative to its own module
    # file, so the patch target and the ``__file__`` it walks from are both the
    # ``_version`` module.
    def test_get_version_pyproject_no_version_match(self):
        with patch("lhp.cli._version.version", side_effect=Exception("not installed")):
            with tempfile.TemporaryDirectory() as tmpdir:
                pyproject = Path(tmpdir) / "pyproject.toml"
                pyproject.write_text("[tool.poetry]\nname = 'pkg'\n")

                import lhp.cli._version as mod

                orig = mod.__file__
                try:
                    mod.__file__ = str(Path(tmpdir) / "cli" / "_version.py")
                    result = get_version()
                    assert result == "0.2.11"
                finally:
                    mod.__file__ = orig

    def test_get_version_pyproject_read_raises(self):
        with patch("lhp.cli._version.version", side_effect=Exception("not installed")):
            with tempfile.TemporaryDirectory() as tmpdir:
                pyproject = Path(tmpdir) / "pyproject.toml"
                pyproject.write_text("version = '9.9.9'\n")

                import lhp.cli._version as mod

                orig = mod.__file__
                try:
                    mod.__file__ = str(Path(tmpdir) / "cli" / "_version.py")
                    with patch("builtins.open", side_effect=PermissionError("denied")):
                        result = get_version()
                    assert result == "0.2.11"
                finally:
                    mod.__file__ = orig


@pytest.mark.unit
class TestCliPerfFlag:
    """``--perf`` is guarded on a resolvable project root.

    The rebuilt group callback enables timing only when BOTH ``--perf`` is set
    AND a project root is found: ``if perf and project_root is not None:
    enable_perf_timing(project_root)``. ``enable_perf_timing(None)`` flips the
    in-memory flag but installs no file handler, so perf.log can never be
    written outside a project — the old unconditional ``enable_perf_timing(None)``
    was a latent no-op. These tests pin the guard from both sides.
    """

    def test_perf_flag_enables_timing_with_project_root(self):
        # ``--perf`` WITH a discoverable project root → enable_perf_timing is
        # called with that root. ``init`` is a module-level click object (the
        # InitCommand class is gone); patch the bootstrap it delegates to so the
        # subcommand body runs without scaffolding a project.
        fake_root = Path("/tmp/fake_lhp_project")
        runner = CliRunner()
        # ``enable_perf_timing`` is lazy-imported inside the group callback
        # (``from ..utils.performance_timer import enable_perf_timing``), so it
        # is not a module attribute of ``lhp.cli.main``; patch it at the source.
        with patch("lhp.cli.main.configure_logging", return_value=None):
            with patch("lhp.cli.main._find_project_root", return_value=fake_root):
                with patch(
                    "lhp.utils.performance_timer.enable_perf_timing"
                ) as mock_perf:
                    with patch(
                        "lhp.api.LakehousePlumberBootstrap.init_project",
                        return_value=SimpleNamespace(
                            success=True, error_code=None, error_message=None
                        ),
                    ):
                        with patch("lhp.cli.commands.init_command.render_init_result"):
                            runner.invoke(cli, ["--perf", "init", "test_proj"])
        mock_perf.assert_called_once_with(fake_root)

    def test_perf_flag_outside_project_does_not_enable_timing(self):
        # ``--perf`` WITHOUT a project root → enable_perf_timing must NOT be
        # called (and the invocation must not crash). perf.log needs a root.
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None):
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with patch(
                    "lhp.utils.performance_timer.enable_perf_timing"
                ) as mock_perf:
                    result = runner.invoke(cli, ["--perf", "--help"])
        assert result.exit_code == 0
        mock_perf.assert_not_called()


@pytest.mark.unit
class TestCliLogFileFlag:
    """§8.1: exercises the real group callback so a regression that ignores the flag (or hardcodes log_to_file) is caught."""

    def test_log_file_flag_sets_log_to_file_true(self):
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None) as mock_cfg:
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with _patched_init():
                    runner.invoke(cli, ["--log-file", "init", "test_proj"])
        mock_cfg.assert_called_once()
        assert mock_cfg.call_args.kwargs["log_to_file"] is True

    def test_no_log_file_flag_sets_log_to_file_false(self):
        runner = CliRunner()
        with patch("lhp.cli.main.configure_logging", return_value=None) as mock_cfg:
            with patch("lhp.cli.main._find_project_root", return_value=None):
                with _patched_init():
                    runner.invoke(cli, ["init", "test_proj"])
        mock_cfg.assert_called_once()
        assert mock_cfg.call_args.kwargs["log_to_file"] is False


@pytest.mark.unit
class TestCommandRouting:
    """Route a subcommand through the ``cli`` group and assert it invokes the
    facade (§9.11: the command bodies are thin shells over ``lhp.api``).

    The old command CLASSES (ShowCommand, DependenciesCommand) are gone, so
    these tests invoke through ``cli`` against a real isolated project (so the
    facade actually builds) and patch the matching ``InspectionFacade`` read.
    ``deps`` is now a hidden alias that forwards to ``dag``.
    """

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @staticmethod
    def _make_project(root: Path) -> None:
        (root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")
        pipelines = root / "pipelines"
        pipelines.mkdir()
        (pipelines / "test.yaml").write_text(
            "pipeline: p\n"
            "flowgroup: fg\n"
            "actions:\n"
            "  - name: load_seed\n"
            "    type: load\n"
            "    source: {type: sql, sql: 'SELECT 1 AS id'}\n"
            "    target: v_seed\n"
            "  - name: write_seed\n"
            "    type: write\n"
            "    source: v_seed\n"
            "    write_target: {type: streaming_table, database: db, table: t}\n"
        )
        subs = root / "substitutions"
        subs.mkdir()
        (subs / "prod.yaml").write_text("prod:\n  env: prod\n")

    def test_substitutions_command_routing(self, runner):
        from lhp.api.views import SubstitutionView

        with runner.isolated_filesystem():
            self._make_project(Path.cwd())
            with patch(
                "lhp.api._inspection_facade.InspectionFacade.build_substitution_view",
                return_value=SubstitutionView(env="prod", tokens={}, raw_mappings={}),
            ) as mock_subs:
                result = runner.invoke(cli, ["substitutions", "--env", "prod"])
                mock_subs.assert_called_once_with("prod")
                assert result.exit_code == 0

    def test_deps_command_routing(self, runner):
        # ``deps`` forwards to ``dag``; ``dag`` calls the facade twice
        # (analyze + write). ``--pipeline`` is no longer a dag option.
        with runner.isolated_filesystem():
            self._make_project(Path.cwd())
            with patch(
                "lhp.api._inspection_facade.InspectionFacade.analyze_dependencies"
            ) as mock_analyze:
                with patch(
                    "lhp.api._inspection_facade.InspectionFacade.save_dependency_outputs"
                ) as mock_save:
                    result = runner.invoke(
                        cli, ["deps", "--format", "json"], catch_exceptions=False
                    )
                    assert result.exit_code == 0, result.stderr
                    mock_analyze.assert_called_once_with(
                        expand_blueprints=False, blueprint_filter=None
                    )
                    assert mock_save.call_count == 1
                    assert mock_save.call_args.kwargs["formats"] == ["json"]

    def test_deps_command_all_options(self, runner):
        # The dag option set replaces the old deps flags: ``--pipeline`` is
        # gone and ``--verbose`` is a global option (not a dag option).
        with runner.isolated_filesystem():
            self._make_project(Path.cwd())
            Path("cfg.yaml").write_text("{}\n")
            with patch(
                "lhp.api._inspection_facade.InspectionFacade.analyze_dependencies"
            ) as mock_analyze:
                with patch(
                    "lhp.api._inspection_facade.InspectionFacade.save_dependency_outputs"
                ) as mock_save:
                    result = runner.invoke(
                        cli,
                        [
                            "dag",
                            "--format",
                            "job",
                            "--output",
                            "out_dir",
                            "--job-name",
                            "my_job",
                            "--job-config",
                            "cfg.yaml",
                            "--bundle-output",
                            "--expand-blueprints",
                            "--blueprint",
                            "mybp",
                        ],
                        catch_exceptions=False,
                    )
                    assert result.exit_code == 0, result.stderr
                    mock_analyze.assert_called_once_with(
                        expand_blueprints=True, blueprint_filter="mybp"
                    )
                    save_kwargs = mock_save.call_args.kwargs
                    assert save_kwargs["formats"] == ["job"]
                    assert save_kwargs["output_dir"] == Path("out_dir")
                    assert save_kwargs["job_name"] == "my_job"
                    assert save_kwargs["job_config_path"] == "cfg.yaml"
                    assert save_kwargs["bundle_output"] is True
                    assert save_kwargs["expand_blueprints"] is True
                    assert save_kwargs["blueprint_filter"] == "mybp"


@pytest.mark.unit
class TestFindProjectRoot:
    def test_find_project_root_in_current_dir(self, tmp_path):
        (tmp_path / "lhp.yaml").write_text("name: test\n")
        with patch("lhp.cli.main.Path.cwd", return_value=tmp_path):
            result = _find_project_root()
        assert result == tmp_path

    def test_find_project_root_in_parent(self, tmp_path):
        (tmp_path / "lhp.yaml").write_text("name: test\n")
        child = tmp_path / "sub" / "deep"
        child.mkdir(parents=True)
        with patch("lhp.cli.main.Path.cwd", return_value=child):
            result = _find_project_root()
        assert result == tmp_path

    def test_find_project_root_not_found(self, tmp_path):
        child = tmp_path / "no_project"
        child.mkdir()
        with patch("lhp.cli.main.Path.cwd", return_value=child):
            result = _find_project_root()
        assert result is None
