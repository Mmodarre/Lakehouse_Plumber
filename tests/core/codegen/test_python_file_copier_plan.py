"""Unit tests for ``PythonFileCopier.plan`` — the no-write dedup planner.

``plan`` runs the exact dedup + conflict-detection rule used by the write
path (``apply_copy_record`` -> ``ensure_init_file`` / ``copy_python_file``)
over a sequence of ``CopiedModuleRecord`` objects, but performs zero disk
writes and never mutates the copier's own registry. These tests pin three
guarantees:

  1. A cross-source destination collision raises ``PythonFunctionConflictError``
     (code 019) — identical to the write path.
  2. Identical (source, dest) records are deduped to a single planned record.
  3. Planning touches the filesystem zero times and leaves the copier's
     ``get_copied_files`` registry empty.
"""

from pathlib import Path

import pytest

from lhp.core.codegen import PythonFileCopier
from lhp.errors import PythonFunctionConflictError
from lhp.models.processing import CopiedModuleRecord


def _record(source_path: str, custom_dir: Path, *, stem: str) -> CopiedModuleRecord:
    """Build a record whose dest is ``custom_dir/<stem>.py`` (mirrors
    ``compute_copy_record``'s flat layout) without touching disk."""
    return CopiedModuleRecord(
        source_path=source_path,
        dest_path=custom_dir / f"{stem}.py",
        content=f"# header for {source_path}\n",
        module_path=source_path,
        custom_functions_dir=custom_dir,
    )


@pytest.mark.unit
class TestPlanConflict:
    """A cross-source collision must raise 019, same as the write path."""

    def test_different_sources_same_destination_raises_019(self, tmp_path):
        custom_dir = tmp_path / "custom_python_functions"
        records = [
            _record("pkg_a/timestamp.py", custom_dir, stem="timestamp"),
            _record("pkg_b/timestamp.py", custom_dir, stem="timestamp"),
        ]

        copier = PythonFileCopier()
        with pytest.raises(PythonFunctionConflictError) as exc_info:
            copier.plan(records)

        # Surface the canonical LHP-VAL-019 contract.
        assert exc_info.value.code_number == "019"
        assert exc_info.value.existing_source == "pkg_a/timestamp.py"
        assert exc_info.value.new_source == "pkg_b/timestamp.py"

    def test_conflict_raises_without_writing_anything(self, tmp_path):
        """Even on the raising path, no files are created."""
        custom_dir = tmp_path / "custom_python_functions"
        records = [
            _record("pkg_a/util.py", custom_dir, stem="util"),
            _record("pkg_b/util.py", custom_dir, stem="util"),
        ]

        copier = PythonFileCopier()
        with pytest.raises(PythonFunctionConflictError):
            copier.plan(records)

        assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
class TestPlanDedup:
    """Identical (source, dest) records collapse to one planned record."""

    def test_identical_records_dedup_to_one(self, tmp_path):
        custom_dir = tmp_path / "custom_python_functions"
        record = _record("pkg/loader.py", custom_dir, stem="loader")

        copier = PythonFileCopier()
        planned = copier.plan([record, record])

        assert planned == (record,)

    def test_dedup_matches_on_normalized_source(self, tmp_path):
        """Backslash vs forward-slash sources are the *same* source, so the
        second record dedups rather than conflicts (mirrors copy_python_file's
        path normalization)."""
        custom_dir = tmp_path / "custom_python_functions"
        records = [
            _record("pkg\\loader.py", custom_dir, stem="loader"),
            _record("pkg/loader.py", custom_dir, stem="loader"),
        ]

        copier = PythonFileCopier()
        planned = copier.plan(records)

        assert len(planned) == 1

    def test_distinct_destinations_all_planned_in_input_order(self, tmp_path):
        custom_dir = tmp_path / "custom_python_functions"
        records = [
            _record("pkg/a.py", custom_dir, stem="a"),
            _record("pkg/b.py", custom_dir, stem="b"),
            _record("pkg/c.py", custom_dir, stem="c"),
        ]

        copier = PythonFileCopier()
        planned = copier.plan(records)

        assert [r.dest_path.stem for r in planned] == ["a", "b", "c"]


@pytest.mark.unit
class TestPlanNoSideEffects:
    """plan() must be I/O-free and must not touch the copier's registry."""

    def test_plan_writes_nothing_to_disk(self, tmp_path):
        custom_dir = tmp_path / "custom_python_functions"
        records = [
            _record("pkg/a.py", custom_dir, stem="a"),
            _record("pkg/b.py", custom_dir, stem="b"),
            _record("pkg/a.py", custom_dir, stem="a"),  # dedup
        ]

        copier = PythonFileCopier()
        copier.plan(records)

        # No module files, no __init__.py, no custom_python_functions dir.
        assert list(tmp_path.iterdir()) == []

    def test_plan_does_not_mutate_instance_registry(self, tmp_path):
        custom_dir = tmp_path / "custom_python_functions"
        records = [_record("pkg/a.py", custom_dir, stem="a")]

        copier = PythonFileCopier()
        copier.plan(records)

        # Planning uses a throwaway registry; the instance stays pristine so
        # the same copier can later be used for the real write pass.
        assert copier.get_copied_files() == {}

    def test_plan_is_deterministic(self, tmp_path):
        custom_dir = tmp_path / "custom_python_functions"
        records = [
            _record("pkg/a.py", custom_dir, stem="a"),
            _record("pkg/b.py", custom_dir, stem="b"),
            _record("pkg/a.py", custom_dir, stem="a"),
        ]

        first = PythonFileCopier().plan(records)
        second = PythonFileCopier().plan(records)

        assert first == second


@pytest.mark.unit
class TestPlanInitStemEdgeCase:
    """A module whose stem is literally ``__init__`` collides with the
    package-init key the write path registers first — plan must reproduce
    that 019, since apply_copy_record registers the init key before the copy."""

    def test_module_named_init_conflicts_with_package_init(self, tmp_path):
        custom_dir = tmp_path / "custom_python_functions"
        record = _record("pkg/__init__.py", custom_dir, stem="__init__")

        copier = PythonFileCopier()
        with pytest.raises(PythonFunctionConflictError) as exc_info:
            copier.plan([record])

        assert exc_info.value.code_number == "019"
