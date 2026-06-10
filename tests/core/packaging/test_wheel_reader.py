"""Contract for the domain-aware wheel reader (``core/packaging/wheel_reader``).

Builds REAL wheels via :class:`PipelinePackager` (the same hand-built outcome
pattern as ``test_wheel_contents.py``) and locks the reader's three public
functions plus the four error paths it owns:

  * ``list_wheel_py_modules`` — sorted ``.py``-only members, ``.dist-info``
    excluded, ``(arcname, uncompressed_size)`` pairs;
  * ``extract_wheel_py_modules`` — structure-preserving extraction into a
    freshly-created output dir; written files match the listed ``.py`` arcnames;
  * ``locate_pipeline_wheel`` — exactly-1 resolves, 0/>1 raise ``LHP-GEN-001``;
  * the validate/open helper's IO-022 (missing) / IO-023 (not a wheel) /
    IO-024 (corrupt) paths, and a zero-``.py`` foreign wheel (no raise).
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from lhp.core.packaging import (
    PipelinePackager,
    extract_wheel_py_modules,
    list_wheel_py_modules,
    locate_pipeline_wheel,
)
from lhp.errors import LHPError, LHPFileError
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


def _build_wheel_file(tmp_path: Path) -> Path:
    """Package the hand-built outcome and write the real ``.whl`` to disk."""
    result = PipelinePackager().package(
        [_outcome(tmp_path)],
        output_dir=tmp_path,
        pipeline=_PIPELINE,
        env=_ENV,
        version=_VERSION,
    )
    wheel_path = tmp_path / result.wheel_filename
    wheel_path.write_bytes(result.wheel_bytes)
    return wheel_path


# Every ``.py`` member a real built wheel carries, in sorted order.
_EXPECTED_PY_ARCS = sorted(
    [
        f"{_IMPORT_PKG}/{_FLOWGROUP_NAME}.py",
        f"{_IMPORT_PKG}/__init__.py",
        "custom_python_functions/__init__.py",
        f"custom_python_functions/{_CPF_SUBMODULE}.py",
    ]
)


@pytest.mark.unit
class TestListWheelPyModules:
    def test_lists_sorted_py_only_members(self, tmp_path: Path) -> None:
        """Returns every ``.py`` member, sorted by arcname, ``.dist-info``
        excluded; each pair carries the uncompressed size."""
        wheel = _build_wheel_file(tmp_path)

        members = list_wheel_py_modules(wheel)
        arcs = [arc for arc, _ in members]

        assert arcs == _EXPECTED_PY_ARCS
        assert arcs == sorted(arcs)
        # No .dist-info member leaked in (none end in .py).
        assert not any(".dist-info" in arc for arc in arcs)
        # Sizes are the real uncompressed byte sizes.
        sizes = dict(members)
        with zipfile.ZipFile(wheel) as zf:
            for info in zf.infolist():
                if info.filename in sizes:
                    assert sizes[info.filename] == info.file_size
        assert all(size > 0 for _, size in members)


@pytest.mark.unit
class TestExtractWheelPyModules:
    def test_extracts_preserving_structure_into_new_dir(self, tmp_path: Path) -> None:
        """Creates a missing output dir, preserves in-wheel structure, and the
        written files match the listed ``.py`` arcnames."""
        wheel = _build_wheel_file(tmp_path)
        out = tmp_path / "extracted" / "nested"  # does not exist yet
        assert not out.exists()

        written = extract_wheel_py_modules(wheel, out)

        assert out.is_dir()
        written_rel = sorted(p.relative_to(out).as_posix() for p in written)
        assert written_rel == _EXPECTED_PY_ARCS
        for path in written:
            assert path.is_file()
        # The flowgroup module's on-disk bytes round-trip from the wheel.
        with zipfile.ZipFile(wheel) as zf:
            expected = zf.read(f"{_IMPORT_PKG}/{_FLOWGROUP_NAME}.py")
        assert (out / _IMPORT_PKG / f"{_FLOWGROUP_NAME}.py").read_bytes() == expected


@pytest.mark.unit
class TestLocatePipelineWheel:
    def _dist_dir(self, root: Path) -> Path:
        d = root / "generated" / _ENV / "_wheels" / _PIPELINE / "dist"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def test_exactly_one_resolves(self, tmp_path: Path) -> None:
        dist = self._dist_dir(tmp_path)
        wheel = dist / "lhp_pkg-0.9.0-py3-none-any.whl"
        wheel.write_bytes(b"PK\x03\x04")  # content irrelevant to locate
        assert locate_pipeline_wheel(tmp_path, _PIPELINE, _ENV) == wheel

    def test_zero_wheels_raises_gen_001(self, tmp_path: Path) -> None:
        self._dist_dir(tmp_path)  # empty dist dir
        with pytest.raises(LHPError) as exc:
            locate_pipeline_wheel(tmp_path, _PIPELINE, _ENV)
        assert exc.value.code == "LHP-GEN-001"

    def test_more_than_one_wheel_raises_gen_001(self, tmp_path: Path) -> None:
        dist = self._dist_dir(tmp_path)
        (dist / "a-0.9.0-py3-none-any.whl").write_bytes(b"PK\x03\x04")
        (dist / "b-0.9.0-py3-none-any.whl").write_bytes(b"PK\x03\x04")
        with pytest.raises(LHPError) as exc:
            locate_pipeline_wheel(tmp_path, _PIPELINE, _ENV)
        assert exc.value.code == "LHP-GEN-001"


@pytest.mark.unit
class TestReaderErrorPaths:
    def test_missing_file_raises_io_022(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.whl"
        with pytest.raises(LHPFileError) as exc:
            list_wheel_py_modules(missing)
        assert exc.value.code == "LHP-IO-022"

    def test_non_whl_suffix_raises_io_023(self, tmp_path: Path) -> None:
        not_a_wheel = tmp_path / "data.txt"
        not_a_wheel.write_text("hello")
        with pytest.raises(LHPFileError) as exc:
            list_wheel_py_modules(not_a_wheel)
        assert exc.value.code == "LHP-IO-023"

    def test_directory_raises_io_023(self, tmp_path: Path) -> None:
        a_dir = tmp_path / "looks_like.whl"
        a_dir.mkdir()
        with pytest.raises(LHPFileError) as exc:
            list_wheel_py_modules(a_dir)
        assert exc.value.code == "LHP-IO-023"

    def test_corrupt_zip_raises_io_024(self, tmp_path: Path) -> None:
        corrupt = tmp_path / "broken.whl"
        corrupt.write_bytes(b"this is not a zip archive at all")
        with pytest.raises(LHPFileError) as exc:
            list_wheel_py_modules(corrupt)
        assert exc.value.code == "LHP-IO-024"


def _foreign_wheel(tmp_path: Path) -> Path:
    """A trivial real zip with only a ``.dist-info/METADATA`` member — a valid
    archive that carries zero ``.py`` modules."""
    wheel = tmp_path / "foreign-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as zf:
        zf.writestr("foreign-1.0.dist-info/METADATA", "Metadata-Version: 2.1\n")
    return wheel


@pytest.mark.unit
class TestForeignWheel:
    def test_list_returns_empty_for_zero_py(self, tmp_path: Path) -> None:
        assert list_wheel_py_modules(_foreign_wheel(tmp_path)) == ()

    def test_extract_writes_nothing_for_zero_py(self, tmp_path: Path) -> None:
        out = tmp_path / "out"
        written = extract_wheel_py_modules(_foreign_wheel(tmp_path), out)
        assert written == ()
        # The output dir is created but no .py files are written.
        assert out.is_dir()
        assert list(out.rglob("*.py")) == []
