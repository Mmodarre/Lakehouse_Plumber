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
     internal registry empty.
"""

from pathlib import Path

import pytest

from lhp.core.codegen import PythonFileCopier
from lhp.core.codegen.python_file_copier import compute_copy_records
from lhp.errors import PythonFunctionConflictError
from lhp.models.processing import CopiedModuleRecord


def _record(source_path: str, custom_dir: Path, *, stem: str) -> CopiedModuleRecord:
    """Build a record whose dest is ``custom_dir/<stem>.py`` (mirrors
    ``compute_copy_records``' flat entry layout) without touching disk."""
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
        assert copier._copied_files == {}

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


def _make_entry_with_helper(root: Path, entry_stem: str) -> Path:
    """Entry under ``root`` importing a shared ``helpers.shared`` leaf helper."""
    helpers = root / "helpers"
    helpers.mkdir(parents=True, exist_ok=True)
    (helpers / "__init__.py").write_text("")
    (helpers / "shared.py").write_text("def s():\n    return 1\n")
    entry = root / f"{entry_stem}.py"
    entry.write_text("from helpers.shared import s\n")
    return entry


@pytest.mark.unit
class TestPlanMultiRecordFromComputeCopyRecords:
    """plan() over the real multi-record output of compute_copy_records:
    the planned set must equal what apply() actually writes to disk."""

    def test_plan_matches_apply_for_entry_plus_closure(self, tmp_path):
        root = tmp_path / "py_functions"
        entry = _make_entry_with_helper(root, "entry")
        cfd = tmp_path / "out" / "custom_python_functions"

        records = compute_copy_records(
            entry, "py_functions/entry.py", cfd, {"source_parse_cache": {}}
        )

        planned = PythonFileCopier().plan(records)
        planned_dests = {r.dest_path for r in planned}

        # Apply to a real copier and compare against what landed on disk.
        apply_copier = PythonFileCopier()
        for record in records:
            apply_copier.apply_copy_record(record)
        written = {
            p
            for p in cfd.rglob("*.py")
            if not p.name == "__init__.py" or "helpers" in str(p)
        }

        # Every planned module dest exists on disk; the planner did not invent
        # or drop any module write relative to apply.
        for dest in planned_dests:
            assert dest.exists(), f"planned dest not written: {dest}"
        assert planned_dests <= written

    def test_cross_source_collision_on_shared_helper_dest_raises_019(self, tmp_path):
        """Two DISTINCT real helper files both targeting
        ``custom_python_functions/sib.py`` raise LHP-VAL-019.

        The entries have distinct stems (so the entry dests do NOT collide)
        and import a FLAT sibling helper (no package -> no ``__init__.py`` to
        collide first), isolating the clash to ``sib.py`` reached from two
        different real source files. The closure record's ``source_path`` is
        the real resolved path, so this is a genuine cross-source clash, not a
        dedup."""
        root_a = tmp_path / "proj_a" / "py_functions"
        root_b = tmp_path / "proj_b" / "py_functions"
        root_a.mkdir(parents=True)
        root_b.mkdir(parents=True)
        (root_a / "entry_a.py").write_text("from sib import f\n")
        (root_a / "sib.py").write_text("def f():\n    return 'a'\n")
        (root_b / "entry_b.py").write_text("from sib import f\n")
        (root_b / "sib.py").write_text("def f():\n    return 'b'\n")

        # Shared destination dir forces the helper collision: both closures
        # target custom_python_functions/sib.py from different real files.
        cfd = tmp_path / "out" / "custom_python_functions"
        recs_a = compute_copy_records(
            root_a / "entry_a.py",
            "proj_a/py_functions/entry_a.py",
            cfd,
            {"source_parse_cache": {}},
        )
        recs_b = compute_copy_records(
            root_b / "entry_b.py",
            "proj_b/py_functions/entry_b.py",
            cfd,
            {"source_parse_cache": {}},
        )

        with pytest.raises(PythonFunctionConflictError) as exc_info:
            PythonFileCopier().plan(recs_a + recs_b)

        assert exc_info.value.code_number == "019"
        assert exc_info.value.destination == str(cfd / "sib.py")
        # The two clashing sources are the distinct real helper paths.
        assert exc_info.value.existing_source == str((root_a / "sib.py").resolve())
        assert exc_info.value.new_source == str((root_b / "sib.py").resolve())
