"""Unit tests for the Phase A / Phase B split of copy_user_module.

The split was introduced so Phase A workers can run inside a worker
process pool (``ProcessPoolExecutor`` under ``spawn``) without touching
the lock-bearing, main-thread-only state manager or PythonFileCopier:

  - compute_copy_records (Phase A, worker process): pure compute. Reads
    the source(s), applies substitutions, prepends header. NO disk writes,
    NO state_manager calls. ``ThreadPoolExecutor`` is used inside the
    tests below only to stress the function's reentrancy — the function
    itself is process-safe by virtue of having no shared state. With no
    local-helper imports the list it returns holds just the entry record.

  - apply_copy_record (Phase B, main thread): performs the actual copy
    (lock-protected via existing PythonFileCopier semantics for the
    case where Phase B itself ever becomes multi-threaded) and tracks
    the resulting files with the state manager.

The legacy ``copy_user_module`` shim was removed; the only production path
is ``compute_copy_records`` + ``apply_copy_record``.
"""

import shutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lhp.core.codegen import (
    PythonFileCopier,
    compute_copy_records,
)
from lhp.models.processing import CopiedModuleRecord


def _only_record(records):
    """Assert a single-record (entry-only, no-closure) result and return it."""
    assert isinstance(records, list)
    assert len(records) == 1, f"expected entry-only result, got {len(records)} records"
    return records[0]


@pytest.fixture
def temp_dir():
    td = tempfile.mkdtemp()
    yield Path(td)
    shutil.rmtree(td)


@pytest.fixture
def flowgroup():
    from lhp.models import FlowGroup

    return FlowGroup(pipeline="p_test", flowgroup="fg_test")


@pytest.mark.unit
class TestComputeCopyRecords:
    """Phase A pure compute — must NEVER touch the filesystem (writes) or
    the state manager. The single permitted I/O is reading the source file(s).
    These cases use entry modules with no local-helper imports, so each call
    returns a one-element list (the entry record)."""

    def test_returns_record_with_expected_fields(self, temp_dir):
        source_file = temp_dir / "user_module.py"
        source_file.write_text("def hello():\n    return 'world'\n")
        custom_dir = temp_dir / "custom_python_functions"

        record = _only_record(
            compute_copy_records(source_file, "user_module.py", custom_dir, {})
        )

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

        compute_copy_records(source_file, "user_module.py", custom_dir, {})

        assert not custom_dir.exists()  # no directory was created
        assert not (custom_dir / "user_module.py").exists()
        assert not (custom_dir / "__init__.py").exists()

    def test_does_not_call_state_manager(self, temp_dir):
        """Phase A must NEVER call state_manager. Even if one is in context."""
        source_file = temp_dir / "user_module.py"
        source_file.write_text("X = 1\n")
        custom_dir = temp_dir / "custom_python_functions"

        from lhp.models import FlowGroup

        state_mgr = MagicMock()
        compute_copy_records(
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
        record = _only_record(
            compute_copy_records(
                None,
                "synthetic_module.py",
                custom_dir,
                {},
                inline_source="SYNTH = 1\n",
            )
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

        record = _only_record(
            compute_copy_records(
                source_file,
                "user_module.py",
                custom_dir,
                {"substitution_manager": FakeSubMgr()},
            )
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
            rec = _only_record(compute_copy_records(src, f"mod_{i}.py", custom_dir, {}))
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
        copier.apply_copy_record(record)

        # Both the package __init__ and the module file should be on disk.
        assert (custom_dir / "user_module.py").exists()
        assert (custom_dir / "__init__.py").exists()
        assert (custom_dir / "user_module.py").read_text() == "# header\nX = 1\n"

    def test_dedup_on_second_apply_is_no_op(self, temp_dir, flowgroup):
        """Same record applied twice: second call dedupes (no-op).

        The second apply must not overwrite the on-disk module: its content
        must be byte-identical to what the first apply wrote.
        """
        custom_dir = temp_dir / "custom_python_functions"
        record = CopiedModuleRecord(
            source_path="user_module.py",
            dest_path=custom_dir / "user_module.py",
            content="X = 1\n",
            module_path="user_module.py",
            custom_functions_dir=custom_dir,
        )

        copier = PythonFileCopier()
        copier.apply_copy_record(record)
        first_content = (custom_dir / "user_module.py").read_text()

        copier.apply_copy_record(record)
        second_content = (custom_dir / "user_module.py").read_text()

        assert first_content == second_content
        assert (custom_dir / "user_module.py").exists()

    def test_apply_writes_file(self, temp_dir, flowgroup):
        """File is copied to disk and both entries are tracked."""
        custom_dir = temp_dir / "custom_python_functions"
        record = CopiedModuleRecord(
            source_path="user_module.py",
            dest_path=custom_dir / "user_module.py",
            content="X = 1\n",
            module_path="user_module.py",
            custom_functions_dir=custom_dir,
        )

        copier = PythonFileCopier()
        copier.apply_copy_record(record)
        assert (custom_dir / "user_module.py").exists()
        assert (custom_dir / "__init__.py").exists()


def _write_helper_project(root: Path) -> Path:
    """Build an entry importing a helper sub-package with a relative import.

    Layout under ``root`` (the import root ``R``):
        entry.py                 -> imports os, shared.util (local), pyspark
        shared/__init__.py       -> real on-disk package marker
        shared/util.py           -> imports a sibling via ``from .math_helper``
        shared/math_helper.py    -> leaf helper

    Returns the entry file path.
    """
    (root / "shared").mkdir(parents=True)
    (root / "entry.py").write_text(
        "import os\n"
        "from shared.util import compute\n"
        "from pyspark.sql import functions as F\n"
        "def go():\n    return compute(os.getcwd())\n"
    )
    (root / "shared" / "__init__.py").write_text("# shared package\n")
    (root / "shared" / "util.py").write_text(
        "from .math_helper import double\ndef compute(x):\n    return double(len(x))\n"
    )
    (root / "shared" / "math_helper.py").write_text(
        "def double(n):\n    return n * 2\n"
    )
    return root / "entry.py"


@pytest.mark.unit
class TestComputeCopyRecordsClosure:
    """compute_copy_records emits the entry PLUS its transitive local closure,
    with structure-preserving dests, prefix-rewritten absolute-local imports,
    and preserved relative/external imports."""

    def test_entry_plus_closure_dests_and_source_paths(self, tmp_path):
        root = tmp_path / "py_functions"
        entry = _write_helper_project(root)
        cfd = tmp_path / "out" / "custom_python_functions"

        records = compute_copy_records(
            entry, "py_functions/entry.py", cfd, {"source_parse_cache": {}}
        )

        dests = {r.dest_path for r in records}
        assert dests == {
            cfd / "entry.py",  # entry stays flat
            cfd / "shared" / "__init__.py",
            cfd / "shared" / "util.py",
            cfd / "shared" / "math_helper.py",
        }

        by_dest = {r.dest_path: r for r in records}
        # Entry record keeps the user-facing module_path as its source identity.
        assert by_dest[cfd / "entry.py"].source_path == "py_functions/entry.py"
        # Helper records carry the REAL resolved on-disk path (drives dedup/019).
        util = by_dest[cfd / "shared" / "util.py"]
        assert util.source_path == str((root / "shared" / "util.py").resolve())

    def test_entry_absolute_local_import_is_prefix_rewritten(self, tmp_path):
        root = tmp_path / "py_functions"
        entry = _write_helper_project(root)
        cfd = tmp_path / "out" / "custom_python_functions"

        records = compute_copy_records(
            entry, "py_functions/entry.py", cfd, {"source_parse_cache": {}}
        )
        entry_rec = next(r for r in records if r.dest_path == cfd / "entry.py")

        # Absolute-local import prefixed; stdlib + third-party untouched.
        assert (
            "from custom_python_functions.shared.util import compute"
            in entry_rec.content
        )
        assert "import os" in entry_rec.content
        assert "from pyspark.sql import functions as F" in entry_rec.content
        # Project-root-relative header (module_path), not the absolute disk path.
        assert "# LHP-SOURCE: py_functions/entry.py" in entry_rec.content

    def test_helper_relative_import_preserved_and_header_project_relative(
        self, tmp_path
    ):
        root = tmp_path / "py_functions"
        entry = _write_helper_project(root)
        cfd = tmp_path / "out" / "custom_python_functions"

        records = compute_copy_records(
            entry, "py_functions/entry.py", cfd, {"source_parse_cache": {}}
        )
        util_rec = next(r for r in records if r.dest_path == cfd / "shared" / "util.py")

        # Intra-package relative import is first-class: preserved verbatim.
        assert "from .math_helper import double" in util_rec.content
        assert "custom_python_functions" not in util_rec.content.replace(
            "# LHP-SOURCE", ""
        )
        # Helper header path is project-root-relative (module_path dir + rel).
        assert "# LHP-SOURCE: py_functions/shared/util.py" in util_rec.content

    def test_real_on_disk_init_carries_body(self, tmp_path):
        root = tmp_path / "py_functions"
        entry = _write_helper_project(root)
        cfd = tmp_path / "out" / "custom_python_functions"

        records = compute_copy_records(
            entry, "py_functions/entry.py", cfd, {"source_parse_cache": {}}
        )
        init_rec = next(
            r for r in records if r.dest_path == cfd / "shared" / "__init__.py"
        )
        # On-disk __init__.py is copied (not synthesized): body present.
        assert "# shared package" in init_rec.content
        assert "# LHP-SOURCE: py_functions/shared/__init__.py" in init_rec.content

    def test_synthesized_namespace_init_is_header_only(self, tmp_path):
        """A closure dir WITHOUT an on-disk __init__.py (PEP 420 namespace)
        gets a synthesized header-only __init__.py record."""
        root = tmp_path / "py_functions"
        (root / "ns").mkdir(parents=True)
        # No ns/__init__.py on disk -> namespace package.
        (root / "entry.py").write_text("from ns.leaf import v\n")
        (root / "ns" / "leaf.py").write_text("v = 1\n")
        cfd = tmp_path / "out" / "custom_python_functions"

        records = compute_copy_records(
            root / "entry.py",
            "py_functions/entry.py",
            cfd,
            {"source_parse_cache": {}},
        )
        init_rec = next(r for r in records if r.dest_path == cfd / "ns" / "__init__.py")
        header_only = (
            "# LHP-SOURCE: py_functions/ns/__init__.py\n"
            "# Generated by LakehousePlumber - DO NOT EDIT\n"
            "# Changes will be overwritten on next generation\n"
            "\n"
        )
        assert init_rec.content == header_only  # header + empty body

    def test_no_disk_write_during_closure_compute(self, tmp_path):
        root = tmp_path / "py_functions"
        entry = _write_helper_project(root)
        cfd = tmp_path / "out" / "custom_python_functions"

        compute_copy_records(
            entry, "py_functions/entry.py", cfd, {"source_parse_cache": {}}
        )
        assert not cfd.exists()  # pure compute: nothing written

    def test_shared_helper_across_two_entries_dedups_to_one_write(self, tmp_path):
        """A helper imported by TWO entries dedups to a single planned write
        because both records carry the same real source_path."""
        root = tmp_path / "py_functions"
        (root / "shared").mkdir(parents=True)
        (root / "shared" / "__init__.py").write_text("")
        (root / "shared" / "util.py").write_text("def u():\n    return 1\n")
        (root / "entry_a.py").write_text("from shared.util import u\n")
        (root / "entry_b.py").write_text("from shared.util import u\n")
        cfd = tmp_path / "out" / "custom_python_functions"

        cache: dict = {}
        recs_a = compute_copy_records(
            root / "entry_a.py",
            "py_functions/entry_a.py",
            cfd,
            {"source_parse_cache": cache},
        )
        recs_b = compute_copy_records(
            root / "entry_b.py",
            "py_functions/entry_b.py",
            cfd,
            {"source_parse_cache": cache},
        )

        planned = PythonFileCopier().plan(recs_a + recs_b)
        util_writes = [r for r in planned if r.dest_path == cfd / "shared" / "util.py"]
        assert len(util_writes) == 1  # shared helper written exactly once
        # Both entries are distinct dests, both planned.
        assert cfd / "entry_a.py" in {r.dest_path for r in planned}
        assert cfd / "entry_b.py" in {r.dest_path for r in planned}

    def test_closure_resolves_with_symlinked_root(self):
        """tempfile.mkdtemp() yields a symlinked path on macOS (/tmp ->
        /private/tmp). The resolver resolves discovered helpers, so the import
        root must be resolved too or ``relative_to`` would raise. This locks
        in compute_copy_records resolving the root before delegating."""
        td = Path(tempfile.mkdtemp())
        try:
            root = td / "py_functions"
            entry = _write_helper_project(root)
            cfd = td / "out" / "custom_python_functions"
            records = compute_copy_records(
                entry, "py_functions/entry.py", cfd, {"source_parse_cache": {}}
            )
            dests = {r.dest_path for r in records}
            assert cfd / "shared" / "util.py" in dests
            assert cfd / "shared" / "math_helper.py" in dests
        finally:
            shutil.rmtree(td, ignore_errors=True)
