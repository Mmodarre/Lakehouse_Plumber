"""Wheel-internals contract: open a BUILT ``.whl`` and verify its structure
plus that it is importable after extraction.

These pins are the consumer-side complement to ``test_packager.py``: rather
than asserting the ``arcname -> bytes`` member map the packager *builds*, they
open ``result.wheel_bytes`` as a real archive and lock the two invariants that
matter to whatever imports the wheel at runtime:

  * R6 — ``custom_python_functions`` ships at the wheel TOP-LEVEL (its own
    ``__init__.py`` plus each copied submodule), NOT nested under the flowgroup
    import package, so ``import custom_python_functions.<sub>`` resolves; and
  * R4 — every flowgroup ``.py``'s in-wheel bytes are BYTE-IDENTICAL to
    ``normalize_content(formatted_code).encode("utf-8")``, i.e. exactly what a
    ``--no-format`` source-mode on-disk write (``write_normalized``) produces
    (WHEEL_PACKAGING_SPEC §6.4).

The flowgroup and custom-function sources here are intentionally trivial (plain
module-level assignments, no ``dlt`` / ``spark`` top-level side effects) so the
extract-and-import check is hermetic and needs no Databricks runtime.
"""

from __future__ import annotations

import importlib
import io
import pkgutil
import sys
import zipfile
from pathlib import Path

import pytest

from lhp.core.packaging import PipelinePackager
from lhp.models.processing import CopiedModuleRecord, FlowgroupOutcome
from lhp.utils.file_header import normalize_content

# ``import_package_name`` lowercases, maps non-identifier chars to ``_``, and
# prefixes a leading digit with ``p_`` -> ``p_15_python_load``.
_PIPELINE = "15_python_load"
_ENV = "preprod"
_VERSION = "0.9.0"
_IMPORT_PKG = "p_15_python_load"

_FLOWGROUP_NAME = "customers"
_CPF_SUBMODULE = "util"

# Trivially importable sources: module-level constants only, no ``dlt`` /
# ``spark`` references, so importing them after extraction is hermetic.
_FLOWGROUP_CODE = "# LHP flowgroup module\nFLOWGROUP = 'customers'\n"
_HELPER_CODE = "# LHP-SOURCE: pipelines/helpers/util.py\nHELPER = 'util'\n\n\ndef helper():\n    return HELPER\n"
_INIT_CODE = "# LHP-SOURCE: pipelines/helpers/__init__.py\n"


def _outcome(output_dir: Path) -> FlowgroupOutcome:
    """A success outcome with one flowgroup module + a ``custom_python_functions``
    closure (a ``<sub>.py`` entry and its package ``__init__.py``).

    Mirrors the ``FlowgroupOutcome.ok(...)`` + ``CopiedModuleRecord`` helper in
    ``tests/core/packaging/test_packager.py::_outcome`` — each copy record's
    ``dest_path`` is ``output_dir / "custom_python_functions" / <name>.py`` so it
    lands TOP-LEVEL in the wheel (R6).
    """
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


def _build_wheel_bytes(tmp_path: Path) -> bytes:
    """Package the hand-built outcome and return the ``.whl`` bytes."""
    result = PipelinePackager().package(
        [_outcome(tmp_path)],
        output_dir=tmp_path,
        pipeline=_PIPELINE,
        env=_ENV,
        version=_VERSION,
    )
    return result.wheel_bytes


@pytest.mark.unit
class TestWheelStructure:
    def test_custom_python_functions_ships_top_level(self, tmp_path: Path) -> None:
        """R6: ``custom_python_functions`` (its ``__init__`` + the submodule)
        sits at the wheel TOP-LEVEL, never under the import package."""
        with zipfile.ZipFile(io.BytesIO(_build_wheel_bytes(tmp_path))) as zf:
            names = set(zf.namelist())

        assert "custom_python_functions/__init__.py" in names
        assert f"custom_python_functions/{_CPF_SUBMODULE}.py" in names

        # Must NOT be nested under the flowgroup import package.
        assert f"{_IMPORT_PKG}/custom_python_functions/__init__.py" not in names
        assert f"{_IMPORT_PKG}/custom_python_functions/{_CPF_SUBMODULE}.py" not in names

    def test_flowgroup_package_present(self, tmp_path: Path) -> None:
        """The synthesized flowgroup-package ``__init__`` and the flowgroup
        module both live under ``<import_pkg>/``."""
        with zipfile.ZipFile(io.BytesIO(_build_wheel_bytes(tmp_path))) as zf:
            names = set(zf.namelist())

        assert f"{_IMPORT_PKG}/__init__.py" in names  # synthesized package init
        assert f"{_IMPORT_PKG}/{_FLOWGROUP_NAME}.py" in names

    def test_flowgroup_bytes_match_no_format_source_build(self, tmp_path: Path) -> None:
        """R4: the flowgroup module's in-wheel bytes equal
        ``normalize_content(formatted_code).encode('utf-8')`` — exactly what a
        ``--no-format`` source-mode on-disk write produces."""
        with zipfile.ZipFile(io.BytesIO(_build_wheel_bytes(tmp_path))) as zf:
            in_wheel = zf.read(f"{_IMPORT_PKG}/{_FLOWGROUP_NAME}.py")

        assert in_wheel == normalize_content(_FLOWGROUP_CODE).encode("utf-8")


@pytest.mark.unit
class TestWheelImportable:
    def test_extracted_wheel_is_importable(self, tmp_path: Path) -> None:
        """Extract the wheel and confirm the top-level ``custom_python_functions``
        package, the flowgroup import package, and the flowgroup submodule all
        import after the extraction dir is placed on ``sys.path``.

        Hermetic: the packaged sources are plain module-level assignments (no
        ``dlt`` / ``spark`` side effects), so no Databricks runtime is needed.
        """
        wheel_bytes = _build_wheel_bytes(tmp_path)

        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as zf:
            zf.extractall(extract_dir)

        # Sanity: the members landed at the wheel top-level on disk too.
        assert (extract_dir / "custom_python_functions" / "__init__.py").is_file()
        assert (
            extract_dir / "custom_python_functions" / f"{_CPF_SUBMODULE}.py"
        ).is_file()
        assert (extract_dir / _IMPORT_PKG / "__init__.py").is_file()
        assert (extract_dir / _IMPORT_PKG / f"{_FLOWGROUP_NAME}.py").is_file()

        # Import from the extraction dir without leaking modules/sys.path into
        # the rest of the suite.
        added_path = str(extract_dir)
        sys.path.insert(0, added_path)
        try:
            cpf = importlib.import_module(f"custom_python_functions.{_CPF_SUBMODULE}")
            assert cpf.helper() == "util"

            import_pkg = importlib.import_module(_IMPORT_PKG)
            # The flowgroup submodule is enumerable on the regular package.
            submodules = {
                info.name
                for info in pkgutil.iter_modules(
                    import_pkg.__path__, prefix=f"{_IMPORT_PKG}."
                )
            }
            assert f"{_IMPORT_PKG}.{_FLOWGROUP_NAME}" in submodules

            flowgroup_mod = importlib.import_module(f"{_IMPORT_PKG}.{_FLOWGROUP_NAME}")
            assert flowgroup_mod.FLOWGROUP == "customers"
        finally:
            if added_path in sys.path:
                sys.path.remove(added_path)
            for name in [
                f"custom_python_functions.{_CPF_SUBMODULE}",
                "custom_python_functions",
                f"{_IMPORT_PKG}.{_FLOWGROUP_NAME}",
                _IMPORT_PKG,
            ]:
                sys.modules.pop(name, None)
