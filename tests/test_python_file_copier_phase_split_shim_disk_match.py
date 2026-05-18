"""Byte-for-byte equivalence: shim vs split-phase PythonFileCopier output.

The legacy single-call ``copy_user_module_for_pipeline`` shim path (no
Phase A collector active) and the new split mechanism (Phase A captures
a :class:`CopiedModuleRecord`, Phase B replays it on the main thread)
MUST produce identical on-disk file contents when given the same input.

This test writes two output paths to DIFFERENT destination directories
and compares the on-disk file contents BYTE-FOR-BYTE (no string-replace
munging of filenames). If the implementer drifts the two code paths'
output (e.g. different header encoding, different newline handling,
different substitution timing) this test fails.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pytest

from lhp.generators.python_file_copier import (
    CopiedModuleRecord,
    PythonFileCopier,
    copy_user_module_for_pipeline,
)


def _build_context(
    *,
    output_dir: Path,
    spec_dir: Path,
    python_copier: PythonFileCopier,
    flowgroup: Any,
) -> Dict[str, Any]:
    """Build the shared context dict consumed by copy_user_module_for_pipeline.

    No substitution manager (we want the raw source content to flow through
    unchanged), no state manager (we are comparing disk content only).
    """
    return {
        "spec_dir": spec_dir,
        "output_dir": output_dir,
        "python_file_copier": python_copier,
        "flowgroup": flowgroup,
        "substitution_manager": None,
        "secret_references": None,
        "source_yaml": None,
        "state_manager": None,
        "environment": "dev",
    }


@pytest.mark.unit
class TestShimDiskOutputMatchesSplit:
    """Plan task D: shim path output == split-phase path output (byte-for-byte)."""

    def test_shim_disk_output_matches_split(self, tmp_path: Path) -> None:
        # Source file the user wants copied. Identical content fed to
        # both paths; the only difference is the code path that copies it.
        spec_dir = tmp_path / "spec"
        spec_dir.mkdir()
        (spec_dir / "my_module.py").write_text(
            "import math\n"
            "\n"
            "def square(x):\n"
            "    return x * x\n"
            "\n"
            "PI = math.pi\n"
        )

        module_path = "my_module.py"  # user-visible relative path

        # Minimal fake flowgroup carrying just the surface area
        # copy_user_module_for_pipeline reads via getattr.
        class _StubFlowgroup:
            pipeline = "pipeline_test"
            flowgroup = "fg_test"
            _auxiliary_files: Dict[str, str] = {}

        # ==== Path 1: shim (no Phase A records bucket in context) =========
        # copy_user_module_for_pipeline takes the shim branch when the
        # context does NOT carry a ``phase_a_records`` list.
        output_dir_shim = tmp_path / "out_shim"
        output_dir_shim.mkdir()
        shim_copier = PythonFileCopier()
        shim_flowgroup = _StubFlowgroup()
        shim_ctx = _build_context(
            output_dir=output_dir_shim,
            spec_dir=spec_dir,
            python_copier=shim_copier,
            flowgroup=shim_flowgroup,
        )
        # Explicitly do NOT set phase_a_records in this context.
        assert "phase_a_records" not in shim_ctx
        shim_stem = copy_user_module_for_pipeline(
            module_path,
            shim_ctx,
            component_label="Python load action",
        )
        assert shim_stem == "my_module"

        # ==== Path 2: split mechanism (Phase A collect + Phase B apply) ===
        # Set up the Phase A records bucket on the context, invoke
        # copy_user_module_for_pipeline (which now follows the collect
        # branch — no disk write yet), then apply the captured
        # CopiedModuleRecord on the main thread (Phase B).
        output_dir_split = tmp_path / "out_split"
        output_dir_split.mkdir()
        split_copier = PythonFileCopier()
        split_flowgroup = _StubFlowgroup()
        split_ctx = _build_context(
            output_dir=output_dir_split,
            spec_dir=spec_dir,
            python_copier=split_copier,
            flowgroup=split_flowgroup,
        )

        records: List[CopiedModuleRecord] = []
        split_ctx["phase_a_records"] = records
        split_stem = copy_user_module_for_pipeline(
            module_path,
            split_ctx,
            component_label="Python load action",
        )

        assert split_stem == "my_module"
        assert len(records) == 1, (
            "Phase A collector should have captured exactly one "
            f"CopiedModuleRecord; got {len(records)}: {records!r}"
        )

        # Phase B replay on the main thread. This is the code path the
        # parallel executor uses to actually write the file. ``state_manager``
        # is None so we skip state tracking and focus on disk content.
        split_copier.apply_copy_record(
            records[0],
            state_manager=None,
            source_yaml=None,
            flowgroup=split_flowgroup,
            env="dev",
        )

        # ===== Byte-for-byte comparison ====================================
        shim_output_file = output_dir_shim / "custom_python_functions" / "my_module.py"
        split_output_file = (
            output_dir_split / "custom_python_functions" / "my_module.py"
        )

        assert (
            shim_output_file.exists()
        ), f"Shim path did not write the expected file: {shim_output_file}"
        assert (
            split_output_file.exists()
        ), f"Split path did not write the expected file: {split_output_file}"

        shim_bytes = shim_output_file.read_bytes()
        split_bytes = split_output_file.read_bytes()
        assert shim_bytes == split_bytes, (
            "Shim and split-phase on-disk output differ.\n"
            f"  shim path: {shim_output_file}\n"
            f"  split path: {split_output_file}\n"
            f"  shim bytes ({len(shim_bytes)}): {shim_bytes!r}\n"
            f"  split bytes ({len(split_bytes)}): {split_bytes!r}"
        )
