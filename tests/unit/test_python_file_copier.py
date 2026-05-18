"""Unit tests for thread-safe Python file copier."""

import shutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pytest

from lhp.generators.python_file_copier import (
    PythonFileCopier,
    PythonFunctionConflictError,
)


class TestPythonFileCopier:
    """Test suite for PythonFileCopier class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        temp = tempfile.mkdtemp()
        yield Path(temp)
        shutil.rmtree(temp, ignore_errors=True)

    @pytest.fixture
    def copier(self):
        """Create a PythonFileCopier instance."""
        return PythonFileCopier()

    def test_single_file_copy(self, copier, temp_dir):
        """Test copying a single file."""
        dest_path = temp_dir / "test_module.py"
        source_path = "py_functions/test_module.py"
        content = "# Test content\ndef test_func():\n    pass"

        result = copier.copy_python_file(source_path, dest_path, content)

        assert result is True
        assert dest_path.exists()
        assert dest_path.read_text() == content

    def test_duplicate_same_source(self, copier, temp_dir):
        """Test that copying the same source twice skips the second copy."""
        dest_path = temp_dir / "test_module.py"
        source_path = "py_functions/test_module.py"
        content = "# Test content\ndef test_func():\n    pass"

        # First copy
        result1 = copier.copy_python_file(source_path, dest_path, content)
        assert result1 is True

        # Second copy (should skip)
        result2 = copier.copy_python_file(source_path, dest_path, content)
        assert result2 is False

    def test_conflict_different_sources(self, copier, temp_dir):
        """Test that different sources targeting same destination raises error."""
        dest_path = temp_dir / "test_module.py"
        source_path1 = "py_functions/test_module.py"
        source_path2 = "other_functions/test_module.py"
        content = "# Test content"

        # First copy
        copier.copy_python_file(source_path1, dest_path, content)

        # Second copy from different source should raise error
        with pytest.raises(PythonFunctionConflictError) as exc_info:
            copier.copy_python_file(source_path2, dest_path, content)

        assert source_path1 in str(exc_info.value)
        assert source_path2 in str(exc_info.value)

    def test_concurrent_same_source(self, copier, temp_dir):
        """Test concurrent access from multiple threads with same source."""
        dest_path = temp_dir / "test_module.py"
        source_path = "py_functions/test_module.py"
        content = "# Test content\ndef test_func():\n    pass"

        results = []

        def copy_file():
            return copier.copy_python_file(source_path, dest_path, content)

        # Run 10 concurrent copies
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(copy_file) for _ in range(10)]
            for future in as_completed(futures):
                results.append(future.result())

        # Exactly one should return True (actual copy), rest should return False (skipped)
        assert sum(results) == 1
        assert len(results) == 10
        assert dest_path.exists()
        assert dest_path.read_text() == content

    def test_concurrent_different_files(self, copier, temp_dir):
        """Test concurrent copying of different files."""
        files = [
            ("py_functions/module1.py", temp_dir / "module1.py", "# Module 1"),
            ("py_functions/module2.py", temp_dir / "module2.py", "# Module 2"),
            ("py_functions/module3.py", temp_dir / "module3.py", "# Module 3"),
        ]

        def copy_file(source, dest, content):
            return copier.copy_python_file(source, dest, content)

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(copy_file, source, dest, content)
                for source, dest, content in files
            ]
            results = [future.result() for future in as_completed(futures)]

        # All should succeed
        assert all(results)
        assert all(dest.exists() for _, dest, _ in files)

    def test_ensure_init_file(self, copier, temp_dir):
        """Test creating __init__.py file."""
        custom_dir = temp_dir / "custom_python_functions"

        copier.ensure_init_file(custom_dir)

        assert custom_dir.exists()
        assert (custom_dir / "__init__.py").exists()
        assert "Generated package" in (custom_dir / "__init__.py").read_text()

    def test_ensure_init_file_concurrent(self, copier, temp_dir):
        """Test concurrent __init__.py creation."""
        custom_dir = temp_dir / "custom_python_functions"

        def create_init():
            copier.ensure_init_file(custom_dir)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(create_init) for _ in range(5)]
            for future in as_completed(futures):
                future.result()  # Wait for completion

        # Should only create once
        assert custom_dir.exists()
        assert (custom_dir / "__init__.py").exists()

    def test_get_copied_files(self, copier, temp_dir):
        """Test retrieving copied files registry."""
        dest1 = temp_dir / "module1.py"
        dest2 = temp_dir / "module2.py"
        source1 = "py_functions/module1.py"
        source2 = "py_functions/module2.py"

        copier.copy_python_file(source1, dest1, "# Module 1")
        copier.copy_python_file(source2, dest2, "# Module 2")

        copied = copier.get_copied_files()

        assert len(copied) == 2
        assert str(dest1) in copied
        assert str(dest2) in copied
        assert copied[str(dest1)] == source1
        assert copied[str(dest2)] == source2

    def test_error_attributes(self, copier, temp_dir):
        """Test PythonFunctionConflictError has correct attributes."""
        dest_path = temp_dir / "test_module.py"
        source1 = "py_functions/test_module.py"
        source2 = "other_functions/test_module.py"
        content = "# Test"

        copier.copy_python_file(source1, dest_path, content)

        try:
            copier.copy_python_file(source2, dest_path, content)
            assert False, "Should have raised error"
        except PythonFunctionConflictError as e:
            assert e.destination == str(dest_path)
            assert e.existing_source == source1
            assert e.new_source == source2

    def test_thread_safety_stress(self, copier, temp_dir):
        """Stress test with many concurrent operations."""
        # Create 20 files, each copied by 5 threads
        num_files = 20
        copies_per_file = 5

        results = []
        errors = []

        def copy_file(file_id, thread_id):
            try:
                dest = temp_dir / f"module_{file_id}.py"
                source = f"py_functions/module_{file_id}.py"
                content = f"# Module {file_id} from thread {thread_id}"
                return copier.copy_python_file(source, dest, content)
            except Exception as e:
                errors.append(e)
                return False

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for file_id in range(num_files):
                for thread_id in range(copies_per_file):
                    futures.append(executor.submit(copy_file, file_id, thread_id))

            for future in as_completed(futures):
                results.append(future.result())

        # No errors should occur
        assert len(errors) == 0

        # Each file should be copied exactly once
        copied = copier.get_copied_files()
        assert len(copied) == num_files

        # Exactly one True per file (actual copy), rest False (skipped)
        true_count = sum(results)
        assert true_count == num_files

    def test_parent_directory_creation(self, copier, temp_dir):
        """Test that parent directories are created if needed."""
        dest_path = temp_dir / "nested" / "deep" / "test_module.py"
        source_path = "py_functions/test_module.py"
        content = "# Test content"

        copier.copy_python_file(source_path, dest_path, content)

        assert dest_path.exists()
        assert dest_path.parent.exists()
        assert dest_path.read_text() == content

    def test_path_separator_normalization_windows(self, copier, temp_dir):
        """Test that forward and backslash paths are treated as equal (cross-platform compatibility)."""
        dest_path = temp_dir / "test_module.py"
        content = "# Test content"

        # First copy with forward slashes (Unix-style)
        result1 = copier.copy_python_file(
            "py_functions/test_module.py", dest_path, content
        )
        assert result1 is True

        # Second copy with backslashes (Windows-style) - should skip, not raise error
        result2 = copier.copy_python_file(
            "py_functions\\test_module.py", dest_path, content
        )
        assert result2 is False  # Should skip as duplicate

        # Verify no error was raised (would have raised PythonFunctionConflictError before fix)
        assert dest_path.exists()
        assert dest_path.read_text() == content


class TestCopyUserModule:
    """Test PythonFileCopier.copy_user_module — the high-level copy helper.

    This is the shared entry point used by python transform, custom_datasource,
    and custom_sink generators. It composes the lower-level primitives
    (substitution, header prepending, ``ensure_init_file``, ``copy_python_file``,
    state-manager tracking) into a single call.
    """

    @pytest.fixture
    def temp_dir(self):
        td = tempfile.mkdtemp()
        yield Path(td)
        shutil.rmtree(td)

    @pytest.fixture
    def copier(self):
        return PythonFileCopier()

    @pytest.fixture
    def flowgroup(self):
        from lhp.models.config import FlowGroup

        return FlowGroup(pipeline="p_test", flowgroup="fg_test")

    def test_happy_path_copies_file_with_header(self, temp_dir, copier, flowgroup):
        """Copies file, prepends LHP-SOURCE header, creates __init__.py."""
        source_file = temp_dir / "user_module.py"
        source_file.write_text("def hello():\n    return 'world'\n")

        custom_dir = temp_dir / "custom_python_functions"

        module_name = copier.copy_user_module(
            source_file, "user_module.py", custom_dir, {"flowgroup": flowgroup}
        )

        assert module_name == "user_module"
        copied = custom_dir / "user_module.py"
        assert copied.exists()
        # Verify header was prepended.
        copied_content = copied.read_text()
        assert "user_module.py" in copied_content  # source path appears in header
        assert "def hello()" in copied_content
        # __init__.py was created.
        assert (custom_dir / "__init__.py").exists()

    def test_substitution_applied_to_content(self, temp_dir, copier, flowgroup):
        """Substitution manager processes the file content during copy."""

        class FakeSubstitutionMgr:
            secret_references: set = set()

            def _process_string(self, s):
                return s.replace("${greeting}", "hello")

        source_file = temp_dir / "user_module.py"
        source_file.write_text("MESSAGE = '${greeting}'\n")

        custom_dir = temp_dir / "custom_python_functions"
        copier.copy_user_module(
            source_file,
            "user_module.py",
            custom_dir,
            {"substitution_manager": FakeSubstitutionMgr(), "flowgroup": flowgroup},
        )

        copied = (custom_dir / "user_module.py").read_text()
        assert "MESSAGE = 'hello'" in copied
        assert "${greeting}" not in copied

    def test_state_manager_tracks_both_files(self, temp_dir, copier, flowgroup):
        """Both ``__init__.py`` and the copied module register with the state manager."""

        class FakeStateMgr:
            def __init__(self):
                self.tracked = []

            def track_generated_file(
                self, generated_path, source_yaml, environment, pipeline, flowgroup
            ):
                self.tracked.append(generated_path)

        source_file = temp_dir / "user_module.py"
        source_file.write_text("X = 1\n")
        custom_dir = temp_dir / "custom_python_functions"

        state_mgr = FakeStateMgr()
        source_yaml = temp_dir / "fg.yaml"
        source_yaml.write_text("# yaml")

        copier.copy_user_module(
            source_file,
            "user_module.py",
            custom_dir,
            {
                "flowgroup": flowgroup,
                "state_manager": state_mgr,
                "source_yaml": source_yaml,
                "environment": "dev",
            },
        )

        tracked_names = {p.name for p in state_mgr.tracked}
        assert tracked_names == {"__init__.py", "user_module.py"}

    def test_idempotent_on_second_call_with_same_source(
        self, temp_dir, copier, flowgroup
    ):
        """Calling copy_user_module twice with the same source file is safe (dedup)."""
        source_file = temp_dir / "user_module.py"
        source_file.write_text("X = 1\n")
        custom_dir = temp_dir / "custom_python_functions"

        # First call writes the file.
        name_1 = copier.copy_user_module(
            source_file, "user_module.py", custom_dir, {"flowgroup": flowgroup}
        )
        # Second call with the SAME source must not raise — it dedupes via
        # PythonFileCopier's internal registry.
        name_2 = copier.copy_user_module(
            source_file, "user_module.py", custom_dir, {"flowgroup": flowgroup}
        )

        assert name_1 == name_2 == "user_module"
        assert (custom_dir / "user_module.py").exists()
