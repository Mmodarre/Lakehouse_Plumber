"""Unit tests for the Phase A / Phase B split of copy_user_module.

The split was introduced so Phase A workers can run inside a worker
process pool (``ProcessPoolExecutor`` under ``spawn``) without touching
the lock-bearing, main-thread-only state manager or PythonFileCopier:

  - compute_copy_record (Phase A, worker process): pure compute. Reads
    the source, applies substitutions, prepends header. NO disk writes,
    NO state_manager calls. ``ThreadPoolExecutor`` is used inside the
    tests below only to stress the function's reentrancy — the function
    itself is process-safe by virtue of having no shared state.

  - apply_copy_record (Phase B, main thread): performs the actual copy
    (lock-protected via existing PythonFileCopier semantics for the
    case where Phase B itself ever becomes multi-threaded) and tracks
    the resulting files with the state manager.

The legacy copy_user_module shim must be equivalent to compute + apply.
"""

import shutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lhp.generators.python_file_copier import (
    CopiedModuleRecord,
    PythonFileCopier,
    compute_copy_record,
)


@pytest.fixture
def temp_dir():
    td = tempfile.mkdtemp()
    yield Path(td)
    shutil.rmtree(td)


@pytest.fixture
def flowgroup():
    from lhp.models.config import FlowGroup

    return FlowGroup(pipeline="p_test", flowgroup="fg_test")


@pytest.mark.unit
class TestComputeCopyRecord:
    """Phase A pure compute — must NEVER touch the filesystem (writes) or
    the state manager. The single permitted I/O is reading the source file."""

    def test_returns_record_with_expected_fields(self, temp_dir):
        source_file = temp_dir / "user_module.py"
        source_file.write_text("def hello():\n    return 'world'\n")
        custom_dir = temp_dir / "custom_python_functions"

        record = compute_copy_record(source_file, "user_module.py", custom_dir, {})

        assert isinstance(record, CopiedModuleRecord)
        assert record.source_path == "user_module.py"
        assert record.module_path == "user_module.py"
        assert record.custom_functions_dir == custom_dir
        assert record.dest_path == custom_dir / "user_module.py"
        assert "user_module.py" in record.content  # header carries source ref
        assert "def hello()" in record.content

    def test_does_not_write_destination_file(self, temp_dir):
        """The defining contract: compute must NOT write anything."""
        source_file = temp_dir / "user_module.py"
        source_file.write_text("X = 1\n")
        custom_dir = temp_dir / "custom_python_functions"

        compute_copy_record(source_file, "user_module.py", custom_dir, {})

        assert not custom_dir.exists()  # no directory was created
        assert not (custom_dir / "user_module.py").exists()
        assert not (custom_dir / "__init__.py").exists()

    def test_does_not_call_state_manager(self, temp_dir):
        """Phase A must NEVER call state_manager. Even if one is in context."""
        source_file = temp_dir / "user_module.py"
        source_file.write_text("X = 1\n")
        custom_dir = temp_dir / "custom_python_functions"

        from lhp.models.config import FlowGroup

        state_mgr = MagicMock()
        compute_copy_record(
            source_file,
            "user_module.py",
            custom_dir,
            {
                "state_manager": state_mgr,
                "source_yaml": temp_dir / "fg.yaml",
                "flowgroup": FlowGroup(pipeline="p", flowgroup="fg"),
                "environment": "dev",
            },
        )

        state_mgr.track_generated_file.assert_not_called()

    def test_inline_source_skips_disk_read(self, temp_dir):
        """When inline_source is supplied, no disk read happens."""
        custom_dir = temp_dir / "custom_python_functions"
        record = compute_copy_record(
            None,
            "synthetic_module.py",
            custom_dir,
            {},
            inline_source="SYNTH = 1\n",
        )
        assert "SYNTH = 1" in record.content

    def test_substitution_applied_in_phase_a(self, temp_dir):
        class FakeSubMgr:
            secret_references: set = set()

            def _process_string(self, s):
                return s.replace("${name}", "world")

        source_file = temp_dir / "user_module.py"
        source_file.write_text("X = '${name}'\n")
        custom_dir = temp_dir / "custom_python_functions"

        record = compute_copy_record(
            source_file,
            "user_module.py",
            custom_dir,
            {"substitution_manager": FakeSubMgr()},
        )
        assert "X = 'world'" in record.content
        assert "${name}" not in record.content

    def test_safe_to_call_concurrently_from_workers(self, temp_dir):
        """Many threads computing distinct records do not interfere."""
        custom_dir = temp_dir / "custom_python_functions"
        records: list = []
        lock = threading.Lock()

        def worker(i: int):
            src = temp_dir / f"mod_{i}.py"
            src.write_text(f"VALUE = {i}\n")
            rec = compute_copy_record(src, f"mod_{i}.py", custom_dir, {})
            with lock:
                records.append(rec)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(50)))

        assert len(records) == 50
        # destinations distinct
        dests = {r.dest_path for r in records}
        assert len(dests) == 50
        # NO disk write happened
        assert not custom_dir.exists()


@pytest.mark.unit
class TestApplyCopyRecord:
    """Phase B replay — uses existing lock-protected primitives plus tracks
    state. Equivalent disk + state outcome as the legacy single-call path."""

    def test_writes_module_and_init_file(self, temp_dir, flowgroup):
        custom_dir = temp_dir / "custom_python_functions"
        record = CopiedModuleRecord(
            source_path="user_module.py",
            dest_path=custom_dir / "user_module.py",
            content="# header\nX = 1\n",
            module_path="user_module.py",
            custom_functions_dir=custom_dir,
        )

        copier = PythonFileCopier()
        result = copier.apply_copy_record(record)

        assert result is True
        assert (custom_dir / "user_module.py").exists()
        assert (custom_dir / "__init__.py").exists()
        assert (custom_dir / "user_module.py").read_text() == "# header\nX = 1\n"

    def test_state_manager_receives_two_track_calls(self, temp_dir, flowgroup):
        custom_dir = temp_dir / "custom_python_functions"
        record = CopiedModuleRecord(
            source_path="user_module.py",
            dest_path=custom_dir / "user_module.py",
            content="# header\nX = 1\n",
            module_path="user_module.py",
            custom_functions_dir=custom_dir,
        )

        class FakeStateMgr:
            def __init__(self):
                self.tracked: list = []

            def track_generated_file(
                self, generated_path, source_yaml, environment, pipeline, flowgroup
            ):
                self.tracked.append(generated_path.name)

        state_mgr = FakeStateMgr()
        source_yaml = temp_dir / "fg.yaml"
        source_yaml.write_text("# yaml")

        PythonFileCopier().apply_copy_record(
            record,
            state_manager=state_mgr,
            source_yaml=source_yaml,
            flowgroup=flowgroup,
            env="dev",
        )

        assert sorted(state_mgr.tracked) == ["__init__.py", "user_module.py"]

    def test_dedup_returns_false_on_second_apply(self, temp_dir, flowgroup):
        """Same record applied twice: second call dedupes, returns False."""
        custom_dir = temp_dir / "custom_python_functions"
        record = CopiedModuleRecord(
            source_path="user_module.py",
            dest_path=custom_dir / "user_module.py",
            content="X = 1\n",
            module_path="user_module.py",
            custom_functions_dir=custom_dir,
        )

        copier = PythonFileCopier()
        first = copier.apply_copy_record(record)
        second = copier.apply_copy_record(record)

        assert first is True
        assert second is False

    def test_no_state_tracking_when_state_mgr_is_none(self, temp_dir, flowgroup):
        """File is still copied; just no state_manager calls."""
        custom_dir = temp_dir / "custom_python_functions"
        record = CopiedModuleRecord(
            source_path="user_module.py",
            dest_path=custom_dir / "user_module.py",
            content="X = 1\n",
            module_path="user_module.py",
            custom_functions_dir=custom_dir,
        )

        copier = PythonFileCopier()
        assert copier.apply_copy_record(record, state_manager=None) is True
        assert (custom_dir / "user_module.py").exists()


@pytest.mark.unit
class TestShimEquivalence:
    """copy_user_module shim must produce the exact same disk + state
    outcomes as compute_copy_record + apply_copy_record called separately."""

    def test_shim_disk_output_matches_split(self, temp_dir, flowgroup):
        """Same disk state when going through shim vs split."""
        src_a = temp_dir / "a.py"
        src_a.write_text("A = 1\n")
        src_b = temp_dir / "b.py"
        src_b.write_text("A = 1\n")
        dir_shim = temp_dir / "shim_out"
        dir_split = temp_dir / "split_out"

        # Shim path
        PythonFileCopier().copy_user_module(
            src_a, "a.py", dir_shim, {"flowgroup": flowgroup}
        )

        # Split path: compute, then apply
        copier_split = PythonFileCopier()
        record = compute_copy_record(src_b, "b.py", dir_split, {})
        copier_split.apply_copy_record(record, flowgroup=flowgroup)

        # Module contents identical except filename
        assert (dir_shim / "a.py").read_text().replace("a.py", "MODULE") == (
            dir_split / "b.py"
        ).read_text().replace("b.py", "MODULE")
        # Both have __init__.py
        assert (dir_shim / "__init__.py").exists()
        assert (dir_split / "__init__.py").exists()

    def test_shim_state_tracking_matches_split(self, temp_dir, flowgroup):
        """State manager receives the same track calls via both paths."""

        class FakeStateMgr:
            def __init__(self):
                self.tracked: list = []

            def track_generated_file(
                self, generated_path, source_yaml, environment, pipeline, flowgroup
            ):
                self.tracked.append((generated_path.name, environment, pipeline))

        source_file = temp_dir / "user_module.py"
        source_file.write_text("X = 1\n")
        custom_dir = temp_dir / "custom_python_functions"
        source_yaml = temp_dir / "fg.yaml"
        source_yaml.write_text("# yaml")

        # Shim path
        state_shim = FakeStateMgr()
        PythonFileCopier().copy_user_module(
            source_file,
            "user_module.py",
            custom_dir / "shim",
            {
                "flowgroup": flowgroup,
                "state_manager": state_shim,
                "source_yaml": source_yaml,
                "environment": "dev",
            },
        )

        # Split path
        state_split = FakeStateMgr()
        record = compute_copy_record(
            source_file, "user_module.py", custom_dir / "split", {}
        )
        PythonFileCopier().apply_copy_record(
            record,
            state_manager=state_split,
            source_yaml=source_yaml,
            flowgroup=flowgroup,
            env="dev",
        )

        assert sorted(state_shim.tracked) == sorted(state_split.tracked)
