"""Unit tests for the snapshot-CDC source-function loader.

These tests cover the module-level helpers in
``lhp.generators.write.snapshot_cdc_source_function`` directly, without going
through the StreamingTableWriteGenerator. End-to-end coverage of the
loader through the generator lives in
``tests/unit/test_snapshot_cdc_parameters.py``.
"""

import ast
import tempfile
from pathlib import Path

import pytest

from lhp.errors import LHPError
from lhp.generators.write.snapshot_cdc_source_function import (
    SourceFunctionResult,
    _extract_function_code,
    _validate_function_parameters,
    load_source_function,
)


class TestLoadSourceFunctionConfigValidation:
    """Configuration-level validation in load_source_function."""

    def test_missing_file_raises_config_002(self):
        """A source_function config without 'file' raises LHPError CONFIG/002."""
        with pytest.raises(LHPError) as exc_info:
            load_source_function({"function": "my_func"})
        # CONFIG/002 is the incomplete-config code.
        assert exc_info.value.code_number == "002"
        assert "Incomplete source_function configuration" in str(exc_info.value)

    def test_missing_function_raises_config_002(self):
        """A source_function config without 'function' raises LHPError CONFIG/002."""
        with pytest.raises(LHPError) as exc_info:
            load_source_function({"file": "funcs.py"})
        assert exc_info.value.code_number == "002"


class TestLoadSourceFunctionFileNotFound:
    """File-not-found resolution delegates to external_file_loader."""

    def test_missing_file_raises_lhp_error_with_file_type(self):
        """A non-existent file raises an LHPError mentioning the file_type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(LHPError) as exc_info:
                load_source_function(
                    {"file": "does_not_exist.py", "function": "my_func"},
                    context={"project_root": Path(tmpdir)},
                )
            # The external_file_loader uses ``file_type`` in its error title.
            assert "snapshot source function file" in str(exc_info.value)


class TestValidateFunctionParameters:
    """Unit tests for _validate_function_parameters."""

    def test_keyword_only_args_accepted(self):
        """A function with `*` and matching keyword-only args validates cleanly."""
        source = "def my_func(latest, *, catalog, schema):\n" "    return None\n"
        tree = ast.parse(source)
        func_node = tree.body[0]
        # Should not raise
        _validate_function_parameters(
            func_node, "my_func", {"catalog": "c", "schema": "s"}
        )

    def test_kwargs_function_skips_name_validation(self):
        """A function with **kwargs bypasses keyword-only name validation."""
        source = "def flexible(latest, **kwargs):\n" "    return None\n"
        tree = ast.parse(source)
        func_node = tree.body[0]
        # 'anything' isn't declared explicitly — should still pass.
        _validate_function_parameters(func_node, "flexible", {"anything": "value"})


class TestExtractFunctionCode:
    """Unit tests for _extract_function_code."""

    def test_extracts_function_and_imports(self):
        """Extracted code includes top-level imports and the function body."""
        source = (
            "from typing import Optional\n"
            "\n"
            "def my_func(x: Optional[int]):\n"
            "    return x\n"
        )
        tree = ast.parse(source)

        extracted = _extract_function_code(source, tree, "my_func")

        assert "from typing import Optional" in extracted
        assert "def my_func(x: Optional[int]):" in extracted
        assert "return x" in extracted


class TestSourceFunctionResultExport:
    """The SourceFunctionResult NamedTuple is exported by the loader module."""

    def test_namedtuple_fields(self):
        result = SourceFunctionResult("code", "name", {"k": "v"})
        assert result.code == "code"
        assert result.name == "name"
        assert result.parameters == {"k": "v"}

    def test_namedtuple_optional_parameters(self):
        result = SourceFunctionResult("code", "name")
        assert result.parameters is None
