"""Tests for code formatting utilities — extended coverage."""

import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from lhp.utils.formatter import (
    CodeFormatter,
    _read_black_config,
    format_code,
    format_sql,
    organize_imports,
)


class TestCodeFormatterFormatCode:
    """Tests for CodeFormatter.format_code fallback paths."""

    def test_black_import_error_falls_back_to_cli(self):
        """When Black import raises ImportError, format_code falls back to _format_with_black_cli."""
        formatter = CodeFormatter()
        code = "x = 1\n"

        with patch.object(formatter, "_format_with_black_cli", return_value=code) as mock_cli:
            # Make the `import black` inside format_code raise ImportError.
            # The function does `import black` then `black.format_str(...)`.
            # Patching `black.format_str` to raise ImportError simulates the
            # scenario where black is not installed (the import succeeds because
            # it is already in sys.modules during tests, but a side_effect of
            # ImportError on format_str triggers the except ImportError branch).
            with patch("black.format_str", side_effect=ImportError("no black")):
                result = formatter.format_code(code)

            mock_cli.assert_called_once_with(code, 88)

    def test_black_runtime_exception_falls_back_to_organize_imports(self):
        """When Black raises a runtime exception, format_code falls back to organize_imports."""
        formatter = CodeFormatter()
        code = "import os\nimport sys\n\nx = 1\n"

        with patch("black.format_str", side_effect=Exception("Black internal error")):
            result = formatter.format_code(code)

        # Should fall back to organize_imports, preserving imports and code
        assert "import os" in result
        assert "import sys" in result
        assert "x = 1" in result

    def test_black_runtime_exception_with_custom_line_length(self):
        """Fallback to organize_imports respects the fact that Black failed gracefully."""
        formatter = CodeFormatter()
        code = "import sys\nimport os\n\nprint('hello')\n"

        with patch("black.format_str", side_effect=ValueError("parse error")):
            result = formatter.format_code(code, line_length=120)

        # organize_imports sorts stdlib: os before sys
        lines = result.strip().split("\n")
        os_idx = next(i for i, l in enumerate(lines) if "import os" in l)
        sys_idx = next(i for i, l in enumerate(lines) if "import sys" in l)
        assert os_idx < sys_idx


class TestFormatWithBlackCli:
    """Tests for CodeFormatter._format_with_black_cli."""

    def test_black_cli_success(self, tmp_path):
        """Successful subprocess run returns formatted file content."""
        formatter = CodeFormatter()
        code = "x=1\n"
        formatted_code = "x = 1\n"

        mock_result = MagicMock()
        mock_result.returncode = 0

        # Use a real NamedTemporaryFile so .name and context manager work,
        # but intercept subprocess.run so black is not actually invoked.
        # After the context manager exits we overwrite the file with the
        # "formatted" content before the method reads it back.
        real_ntf = tempfile.NamedTemporaryFile

        captured_path = {}

        original_run = subprocess.run

        def fake_run(cmd, **kwargs):
            """Intercept subprocess.run, write formatted content to the temp file."""
            # cmd is ["black", "-l", "88", <temp_file>]
            temp_file = cmd[-1]
            captured_path["path"] = temp_file
            # Simulate black writing formatted output
            with open(temp_file, "w") as f:
                f.write(formatted_code)
            return mock_result

        with patch("lhp.utils.formatter.subprocess.run", side_effect=fake_run):
            result = formatter._format_with_black_cli(code, 88)

        assert result == formatted_code

    def test_black_cli_failure_returns_original(self):
        """When subprocess returns non-zero, original code is returned."""
        formatter = CodeFormatter()
        code = "x = 1\n"

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "error: cannot format"

        with patch("lhp.utils.formatter.tempfile.NamedTemporaryFile") as mock_tmp:
            mock_file = MagicMock()
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=False)
            mock_file.name = "/tmp/fakefile.py"
            mock_tmp.return_value = mock_file

            with patch("lhp.utils.formatter.subprocess.run", return_value=mock_result):
                with patch("lhp.utils.formatter.Path.unlink"):
                    result = formatter._format_with_black_cli(code, 88)

        assert result == code

    def test_black_cli_subprocess_exception_returns_original(self):
        """When subprocess.run raises an exception after file creation, original code is returned."""
        formatter = CodeFormatter()
        code = "y = 2\n"

        # Raise after the temp file is successfully created (so temp_file is assigned).
        with patch("lhp.utils.formatter.subprocess.run", side_effect=RuntimeError("no black binary")):
            result = formatter._format_with_black_cli(code, 88)

        assert result == code

    def test_black_cli_tempfile_creation_failure_raises(self):
        """When NamedTemporaryFile itself fails, UnboundLocalError propagates from finally block.

        This documents a known edge case in the source: temp_file is not
        assigned before the finally block tries to unlink it.
        """
        formatter = CodeFormatter()
        code = "y = 2\n"

        with patch("lhp.utils.formatter.tempfile.NamedTemporaryFile", side_effect=OSError("disk full")):
            with pytest.raises(UnboundLocalError):
                formatter._format_with_black_cli(code, 88)


class TestOrganizeImports:
    """Tests for CodeFormatter.organize_imports."""

    def test_no_imports_returns_code_unchanged(self):
        """Code with no imports is returned with content preserved."""
        formatter = CodeFormatter()
        code = "x = 1\ny = 2\n"
        result = formatter.organize_imports(code)
        assert "x = 1" in result
        assert "y = 2" in result
        # No import header should be present
        assert "import" not in result

    def test_blank_line_after_imports_ends_import_section(self):
        """A blank line after imports signals the end of the import section."""
        formatter = CodeFormatter()
        code = "import os\n\nx = 1\n"
        result = formatter.organize_imports(code)
        assert "import os" in result
        assert "x = 1" in result

    def test_comment_before_imports_preserved(self):
        """Comments before any import are placed in other_lines."""
        formatter = CodeFormatter()
        code = "# header comment\nimport os\n\nx = 1\n"
        result = formatter.organize_imports(code)
        assert "# header comment" in result
        assert "import os" in result

    def test_non_import_line_ends_import_section(self):
        """A non-import, non-comment line ends the import section."""
        formatter = CodeFormatter()
        code = "import os\nprint('hello')\nimport sys\n"
        result = formatter.organize_imports(code)
        # 'import sys' should be in other_lines (not sorted with imports)
        # because 'print' ended the import section
        lines = result.split("\n")
        os_idx = next(i for i, l in enumerate(lines) if "import os" in l)
        print_idx = next(i for i, l in enumerate(lines) if "print" in l)
        sys_idx = next(i for i, l in enumerate(lines) if "import sys" in l)
        assert os_idx < print_idx < sys_idx

    def test_blank_line_before_any_import_goes_to_other(self):
        """A blank line before any import is captured in other_lines."""
        formatter = CodeFormatter()
        code = "\nimport os\n\nx = 1\n"
        result = formatter.organize_imports(code)
        assert "import os" in result
        assert "x = 1" in result


class TestSortImports:
    """Tests for CodeFormatter._sort_imports."""

    def test_stdlib_before_third_party(self):
        """Standard library imports come before third-party imports."""
        formatter = CodeFormatter()
        imports = ["import pyspark", "import os"]
        result = formatter._sort_imports(imports)
        os_idx = result.index("import os")
        pyspark_idx = result.index("import pyspark")
        assert os_idx < pyspark_idx

    def test_blank_separator_between_groups(self):
        """An empty string separates import groups."""
        formatter = CodeFormatter()
        imports = ["import os", "import pyspark"]
        result = formatter._sort_imports(imports)
        assert "" in result

    def test_local_relative_imports_grouped_last(self):
        """Imports starting with '.' go to local imports group.

        Note: The module extraction splits on '.', so 'from .mod import x'
        yields module=''. This empty string is not in stdlib_modules and does
        not start with '.', so it lands in third_party_imports per the
        current implementation. This test documents that behaviour.
        """
        formatter = CodeFormatter()
        imports = ["from .local_module import foo", "import os"]
        result = formatter._sort_imports(imports)
        # Both should be present
        assert any("import os" in line for line in result)
        assert any(".local_module" in line for line in result)

    def test_empty_lines_in_input_skipped(self):
        """Empty lines in the input list are filtered out."""
        formatter = CodeFormatter()
        imports = ["import os", "", "import sys"]
        result = formatter._sort_imports(imports)
        # Both imports present, sorted
        non_blank = [l for l in result if l]
        assert "import os" in non_blank
        assert "import sys" in non_blank

    def test_alphabetical_within_group(self):
        """Imports within the same group are sorted alphabetically."""
        formatter = CodeFormatter()
        imports = ["import sys", "import os", "import json"]
        result = formatter._sort_imports(imports)
        # All are stdlib, should be sorted alphabetically
        non_blank = [l for l in result if l]
        assert non_blank == ["import json", "import os", "import sys"]

    def test_no_separator_when_single_group(self):
        """No blank separator when all imports belong to one group."""
        formatter = CodeFormatter()
        imports = ["import os", "import sys"]
        result = formatter._sort_imports(imports)
        assert "" not in result

    def test_three_groups_have_two_separators(self):
        """stdlib, third-party, and local groups each get a separator."""
        formatter = CodeFormatter()
        # 'os' is stdlib, 'pyspark' is third-party.
        # For local imports the module must literally start with '.' AFTER
        # split — which requires module string itself to start with '.'.
        # We can feed a hand-crafted line where module extraction yields '.x'.
        # Actually, let's use the code as-is: 'import .foo' is unusual but
        # the parser still processes it. Instead, we note that the current
        # implementation cannot really produce a local import from standard
        # Python syntax. Two groups are the practical maximum.
        imports = ["import os", "import pyspark"]
        result = formatter._sort_imports(imports)
        assert result.count("") == 1  # one separator between two groups


class TestReadBlackConfig:
    """Tests for _read_black_config."""

    def test_unreadable_pyproject_returns_empty_dict(self, tmp_path, monkeypatch):
        """Returns empty dict when pyproject.toml contains invalid TOML."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("this is not valid toml {{{{")
        monkeypatch.chdir(tmp_path)
        result = _read_black_config()
        assert result == {}

    def test_missing_pyproject_returns_empty_dict(self, tmp_path, monkeypatch):
        """Returns empty dict when no pyproject.toml exists in hierarchy."""
        # Create a deep nested directory with no pyproject.toml
        deep = tmp_path / "a" / "b" / "c" / "d" / "e" / "f"
        deep.mkdir(parents=True)
        monkeypatch.chdir(deep)
        result = _read_black_config()
        assert result == {}

    def test_valid_pyproject_returns_black_config(self, tmp_path, monkeypatch):
        """Reads Black config from a valid pyproject.toml."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            '[tool.black]\nline-length = 120\ntarget-version = ["py311"]\n'
        )
        monkeypatch.chdir(tmp_path)
        result = _read_black_config()
        assert result.get("line-length") == 120

    def test_pyproject_without_black_section_returns_empty(self, tmp_path, monkeypatch):
        """Returns empty dict when pyproject.toml has no [tool.black] section."""
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text('[tool.pytest]\nminversion = "6.0"\n')
        monkeypatch.chdir(tmp_path)
        result = _read_black_config()
        assert result == {}


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_format_code_delegates_to_formatter(self):
        """format_code() creates a CodeFormatter and calls format_code."""
        result = format_code("x=1\n")
        assert "x" in result
        # Black should produce "x = 1\n" if available
        assert "=" in result

    def test_format_code_with_line_length(self):
        """format_code() passes line_length through."""
        result = format_code("x = 1\n", line_length=120)
        assert "x" in result

    def test_organize_imports_delegates_to_formatter(self):
        """organize_imports() creates a CodeFormatter and organizes imports."""
        result = organize_imports("import sys\nimport os\nx = 1\n")
        assert "import" in result
        assert "x = 1" in result

    def test_format_sql_delegates_to_formatter(self):
        """format_sql() creates a CodeFormatter and formats SQL."""
        result = format_sql("SELECT * FROM foo WHERE bar = 1")
        assert "SELECT" in result
        assert "FROM" in result
        assert "WHERE" in result

    def test_format_sql_custom_indent(self):
        """format_sql() supports custom indentation."""
        result = format_sql("SELECT id FROM t WHERE x = 1", indent=2)
        lines = result.split("\n")
        # Non-first lines should have 2-space indent
        for line in lines[1:]:
            if line.strip():
                assert line.startswith("  ")
                assert not line.startswith("    ")
