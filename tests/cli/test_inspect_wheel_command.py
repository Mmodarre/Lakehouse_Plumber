"""Acceptance tests for the ``lhp inspect-wheel`` command.

Invokes the command object directly via ``CliRunner`` (not through
``main.py``). The command resolves the project root and builds the facade, so
every test runs inside a minimal real LHP project (``lhp.yaml`` present) created
under ``runner.isolated_filesystem()``. Real wheels are built with
:class:`PipelinePackager` using the same hand-built ``FlowgroupOutcome`` pattern
as ``tests/api/test_wheel_facade.py`` / ``tests/core/packaging/test_wheel_reader.py``.

Success-path output (the listing / extraction table) is rendered to the
``console`` singleton, captured via ``capture_lhp_console``. Error-path output
(the error panel) is rendered to ``err_console`` (stderr); under Click 8.4
``CliRunner`` separates streams, so error-code assertions read ``result.stderr``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner
from conftest import capture_lhp_console

from lhp.cli.commands.inspect_wheel_command import inspect_wheel_command
from lhp.core.packaging import PipelinePackager
from lhp.models.processing import CopiedModuleRecord, FlowgroupOutcome

_PIPELINE = "15_python_load"
_ENV = "preprod"
_VERSION = "0.9.0"
_IMPORT_PKG = "p_15_python_load"
_FLOWGROUP_NAME = "customers"
_CPF_SUBMODULE = "util"

_FLOWGROUP_CODE = "# LHP flowgroup module\nFLOWGROUP = 'customers'\n"
_HELPER_CODE = "# LHP-SOURCE: pipelines/helpers/util.py\nHELPER = 'util'\n"
_INIT_CODE = "# LHP-SOURCE: pipelines/helpers/__init__.py\n"

# Every ``.py`` member a real built wheel carries, in sorted order.
_EXPECTED_PY_ARCS = sorted(
    [
        f"{_IMPORT_PKG}/{_FLOWGROUP_NAME}.py",
        f"{_IMPORT_PKG}/__init__.py",
        "custom_python_functions/__init__.py",
        f"custom_python_functions/{_CPF_SUBMODULE}.py",
    ]
)


def _write_project(root: Path) -> None:
    """Write a minimal LHP project marker so the facade can be built."""
    (root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")


def _outcome(output_dir: Path) -> FlowgroupOutcome:
    """A success outcome: one flowgroup module + a ``custom_python_functions``
    closure (a ``<sub>.py`` and its package ``__init__.py``)."""
    cpf_dir = output_dir / "custom_python_functions"
    return FlowgroupOutcome.ok(
        _PIPELINE,
        _FLOWGROUP_NAME,
        formatted_code=_FLOWGROUP_CODE,
        copy_records=[
            CopiedModuleRecord(
                source_path="pipelines/helpers/util.py",
                dest_path=cpf_dir / f"{_CPF_SUBMODULE}.py",
                content=_HELPER_CODE,
                module_path="pipelines/helpers/util.py",
                custom_functions_dir=cpf_dir,
            ),
            CopiedModuleRecord(
                source_path="pipelines/helpers/__init__.py",
                dest_path=cpf_dir / "__init__.py",
                content=_INIT_CODE,
                module_path="pipelines/helpers/util.py",
                custom_functions_dir=cpf_dir,
            ),
        ],
    )


def _build_wheel_file(dest_dir: Path) -> Path:
    """Package the hand-built outcome into a real ``.whl`` under ``dest_dir``."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    result = PipelinePackager().package(
        [_outcome(dest_dir)],
        output_dir=dest_dir,
        pipeline=_PIPELINE,
        env=_ENV,
        version=_VERSION,
    )
    wheel_path = dest_dir / result.wheel_filename
    wheel_path.write_bytes(result.wheel_bytes)
    return wheel_path


def _pipeline_dist_dir(project_root: Path) -> Path:
    """The dir the facade globs for the pipeline's built wheel(s)."""
    d = project_root / "generated" / _ENV / "_wheels" / _PIPELINE / "dist"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.mark.unit
def test_list_by_path(runner: CliRunner) -> None:
    """A wheel path lists the packaged ``.py`` modules to stdout (exit 0)."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _write_project(root)
        wheel = _build_wheel_file(root / "build")

        with capture_lhp_console(width=200) as buf:
            result = runner.invoke(
                inspect_wheel_command, [str(wheel)], catch_exceptions=False
            )
        output = buf.getvalue()

        assert result.exit_code == 0, result.stderr
        for arc in _EXPECTED_PY_ARCS:
            assert arc in output, output


@pytest.mark.unit
def test_list_by_pipeline(runner: CliRunner) -> None:
    """A pipeline name + ``-e`` resolves the built wheel and lists it (exit 0)."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _write_project(root)
        _build_wheel_file(_pipeline_dist_dir(root))

        with capture_lhp_console(width=200) as buf:
            result = runner.invoke(
                inspect_wheel_command,
                [_PIPELINE, "-e", _ENV],
                catch_exceptions=False,
            )
        output = buf.getvalue()

        assert result.exit_code == 0, result.stderr
        for arc in _EXPECTED_PY_ARCS:
            assert arc in output, output


@pytest.mark.unit
def test_extract_writes_files(runner: CliRunner) -> None:
    """``--extract`` writes the ``.py`` members and lists their paths (exit 0)."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _write_project(root)
        wheel = _build_wheel_file(root / "build")
        out = root / "extracted"

        with capture_lhp_console(width=200) as buf:
            result = runner.invoke(
                inspect_wheel_command,
                [str(wheel), "--extract", str(out)],
                catch_exceptions=False,
            )
        output = buf.getvalue()

        assert result.exit_code == 0, result.stderr
        written_rel = sorted(p.relative_to(out).as_posix() for p in out.rglob("*.py"))
        assert written_rel == _EXPECTED_PY_ARCS
        # The extracted path table reaches stdout.
        assert str(out) in output, output


@pytest.mark.unit
def test_missing_env_in_pipeline_mode_exits_2(runner: CliRunner) -> None:
    """A bare pipeline name (no ``-e``) is a usage error (exit 2)."""
    with runner.isolated_filesystem():
        _write_project(Path.cwd())

        result = runner.invoke(inspect_wheel_command, ["some_pipeline"])

        assert result.exit_code == 2, result.output
        # Click renders the UsageError message to stderr.
        assert "env" in result.stderr.lower()


@pytest.mark.unit
def test_missing_file_path_mode_exits_1_with_io_022(runner: CliRunner) -> None:
    """A nonexistent ``.whl`` path raises ``LHP-IO-022`` (exit 1)."""
    with runner.isolated_filesystem():
        _write_project(Path.cwd())

        result = runner.invoke(inspect_wheel_command, ["does_not_exist.whl"])

        assert result.exit_code == 1, result.output
        assert "LHP-IO-022" in result.stderr


@pytest.mark.unit
def test_corrupt_wheel_exits_1_with_io_024(runner: CliRunner) -> None:
    """A non-zip ``.whl`` raises ``LHP-IO-024`` (exit 1)."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _write_project(root)
        bad = root / "broken.whl"
        bad.write_bytes(b"not a zip archive")

        result = runner.invoke(inspect_wheel_command, [str(bad)])

        assert result.exit_code == 1, result.output
        assert "LHP-IO-024" in result.stderr


@pytest.mark.unit
def test_zero_wheels_pipeline_mode_exits_1_with_gen_001(runner: CliRunner) -> None:
    """An empty pipeline dist dir raises ``LHP-GEN-001`` (exit 1)."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _write_project(root)
        _pipeline_dist_dir(root)  # empty: zero wheels

        result = runner.invoke(inspect_wheel_command, [_PIPELINE, "-e", _ENV])

        assert result.exit_code == 1, result.output
        assert "LHP-GEN-001" in result.stderr


@pytest.mark.unit
def test_many_wheels_pipeline_mode_exits_1_with_gen_001(runner: CliRunner) -> None:
    """Two wheels in the dist dir raise ``LHP-GEN-001`` (exit 1)."""
    with runner.isolated_filesystem():
        root = Path.cwd()
        _write_project(root)
        dist = _pipeline_dist_dir(root)
        (dist / "a-0.9.0-py3-none-any.whl").write_bytes(b"PK\x03\x04")
        (dist / "b-0.9.0-py3-none-any.whl").write_bytes(b"PK\x03\x04")

        result = runner.invoke(inspect_wheel_command, [_PIPELINE, "-e", _ENV])

        assert result.exit_code == 1, result.output
        assert "LHP-GEN-001" in result.stderr
