"""Contract for the public :class:`WheelFacade` (``facade.wheel.*``).

Exercises the sub-facade against REAL wheels built via
:class:`PipelinePackager` (the same hand-built ``FlowgroupOutcome`` pattern
as ``tests/core/packaging/test_wheel_reader.py``). Locks:

  * ``list_modules(wheel_path=...)`` — path-mode returns the expected
    :class:`WheelContentsView` (``pipeline`` / ``env`` are ``None``);
  * ``list_modules(pipeline=..., env=...)`` — pipeline-mode resolves the
    wheel under ``generated/<env>/_wheels/<pipeline>/dist/`` and returns a
    view carrying that ``pipeline`` / ``env`` context;
  * ``extract_modules(output_dir, wheel_path=...)`` — writes the ``.py``
    members and returns a correct :class:`WheelExtractionResult`;
  * pipeline-mode 0 / >1 wheels surface ``LHP-GEN-001``;
  * selector misuse (both / neither / pipeline-without-env) raises
    :class:`ValueError`;
  * the facade is wired onto :class:`LakehousePlumberApplicationFacade`
    as the ``wheel`` attribute and shares the orchestrator.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from lhp.api import (
    LakehousePlumberApplicationFacade,
    WheelContentsView,
    WheelExtractionResult,
    WheelFacade,
    WheelModuleView,
)
from lhp.core.packaging import PipelinePackager
from lhp.errors import LHPError
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


class _StubOrchestrator:
    """Minimal stand-in exposing only what :class:`WheelFacade` reads.

    ``WheelFacade`` touches the orchestrator solely via ``project_root``
    (for pipeline-mode wheel resolution), so a one-attribute stub keeps
    the test hermetic — no ``lhp.yaml`` / service-graph composition.
    """

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root


def _outcome(output_dir: Path) -> FlowgroupOutcome:
    """A success outcome: one flowgroup module + a ``custom_python_functions``
    closure (a ``<sub>.py`` and its package ``__init__.py``), top-level (R6)."""
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
    """Package the hand-built outcome and write the real ``.whl`` into
    ``dest_dir``; return the written wheel path."""
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
    """The directory ``locate_pipeline_wheel`` globs for built wheels."""
    d = project_root / "generated" / _ENV / "_wheels" / _PIPELINE / "dist"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _facade(project_root: Path) -> WheelFacade:
    return WheelFacade(_StubOrchestrator(project_root))


@pytest.mark.unit
class TestListModulesPathMode:
    def test_returns_expected_contents_view(self, tmp_path: Path) -> None:
        wheel = _build_wheel_file(tmp_path / "build")
        view = _facade(tmp_path).list_modules(wheel_path=wheel)

        assert isinstance(view, WheelContentsView)
        assert view.wheel_path == wheel
        # Path-mode: no pipeline/env context.
        assert view.pipeline is None
        assert view.env is None
        assert view.module_count == len(_EXPECTED_PY_ARCS)
        assert view.module_count == len(view.modules)
        assert all(isinstance(m, WheelModuleView) for m in view.modules)
        # Modules are the sorted .py members, each with a real positive size.
        assert [m.arcname for m in view.modules] == _EXPECTED_PY_ARCS
        assert all(m.size_bytes > 0 for m in view.modules)


@pytest.mark.unit
class TestListModulesPipelineMode:
    def test_resolves_and_returns_view_with_context(self, tmp_path: Path) -> None:
        dist = _pipeline_dist_dir(tmp_path)
        wheel = _build_wheel_file(dist)

        view = _facade(tmp_path).list_modules(pipeline=_PIPELINE, env=_ENV)

        assert view.wheel_path == wheel
        # Pipeline-mode: the selector context is carried onto the view.
        assert view.pipeline == _PIPELINE
        assert view.env == _ENV
        assert [m.arcname for m in view.modules] == _EXPECTED_PY_ARCS

    def test_zero_wheels_raises_gen_001(self, tmp_path: Path) -> None:
        _pipeline_dist_dir(tmp_path)  # empty dist dir
        with pytest.raises(LHPError) as exc:
            _facade(tmp_path).list_modules(pipeline=_PIPELINE, env=_ENV)
        assert exc.value.code == "LHP-GEN-001"

    def test_many_wheels_raises_gen_001(self, tmp_path: Path) -> None:
        dist = _pipeline_dist_dir(tmp_path)
        (dist / "a-0.9.0-py3-none-any.whl").write_bytes(b"PK\x03\x04")
        (dist / "b-0.9.0-py3-none-any.whl").write_bytes(b"PK\x03\x04")
        with pytest.raises(LHPError) as exc:
            _facade(tmp_path).list_modules(pipeline=_PIPELINE, env=_ENV)
        assert exc.value.code == "LHP-GEN-001"


@pytest.mark.unit
class TestExtractModules:
    def test_writes_files_and_returns_result(self, tmp_path: Path) -> None:
        wheel = _build_wheel_file(tmp_path / "build")
        out = tmp_path / "extracted" / "nested"  # does not exist yet
        assert not out.exists()

        result = _facade(tmp_path).extract_modules(out, wheel_path=wheel)

        assert isinstance(result, WheelExtractionResult)
        assert result.wheel_path == wheel
        assert result.output_dir == out
        assert result.written_count == len(_EXPECTED_PY_ARCS)
        assert result.written_count == len(result.written_paths)
        # Files actually landed, structure preserved.
        written_rel = sorted(
            p.relative_to(out).as_posix() for p in result.written_paths
        )
        assert written_rel == _EXPECTED_PY_ARCS
        for path in result.written_paths:
            assert path.is_file()
        # The flowgroup module's on-disk bytes round-trip from the wheel.
        with zipfile.ZipFile(wheel) as zf:
            expected = zf.read(f"{_IMPORT_PKG}/{_FLOWGROUP_NAME}.py")
        assert (out / _IMPORT_PKG / f"{_FLOWGROUP_NAME}.py").read_bytes() == expected

    def test_pipeline_mode_resolves(self, tmp_path: Path) -> None:
        dist = _pipeline_dist_dir(tmp_path)
        wheel = _build_wheel_file(dist)
        out = tmp_path / "out"

        result = _facade(tmp_path).extract_modules(out, pipeline=_PIPELINE, env=_ENV)

        assert result.wheel_path == wheel
        assert result.written_count == len(_EXPECTED_PY_ARCS)


@pytest.mark.unit
class TestSelectorMisuse:
    def test_both_selectors_raises_value_error(self, tmp_path: Path) -> None:
        wheel = _build_wheel_file(tmp_path / "build")
        with pytest.raises(ValueError):
            _facade(tmp_path).list_modules(wheel_path=wheel, pipeline=_PIPELINE)

    def test_neither_selector_raises_value_error(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            _facade(tmp_path).list_modules()

    def test_pipeline_without_env_raises_value_error(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            _facade(tmp_path).list_modules(pipeline=_PIPELINE)

    def test_extract_neither_selector_raises_value_error(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            _facade(tmp_path).extract_modules(tmp_path / "out")

    def test_extract_pipeline_without_env_raises_value_error(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(ValueError):
            _facade(tmp_path).extract_modules(tmp_path / "out", pipeline=_PIPELINE)


@pytest.mark.unit
class TestApplicationFacadeWiring:
    def test_wheel_subfacade_is_wired_and_shares_orchestrator(
        self, tmp_path: Path
    ) -> None:
        orchestrator = _StubOrchestrator(tmp_path)
        app = LakehousePlumberApplicationFacade(orchestrator)

        assert isinstance(app.wheel, WheelFacade)
        # The sub-facade receives the SAME orchestrator the app holds.
        assert app.wheel._orchestrator is orchestrator

    def test_wheel_subfacade_lists_through_app(self, tmp_path: Path) -> None:
        wheel = _build_wheel_file(tmp_path / "build")
        app = LakehousePlumberApplicationFacade(_StubOrchestrator(tmp_path))

        view = app.wheel.list_modules(wheel_path=wheel)
        assert [m.arcname for m in view.modules] == _EXPECTED_PY_ARCS
