"""Tests for source_function parameters support in snapshot CDC."""

import tempfile
from pathlib import Path

import pytest

from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.validators import SnapshotCdcConfigValidator
from lhp.errors import LHPError
from lhp.generators.write.streaming_table import (
    SourceFunctionResult,
    StreamingTableWriteGenerator,
)
from lhp.models import Action, ActionType, FlowGroup


@pytest.fixture
def generator():
    return StreamingTableWriteGenerator()


@pytest.fixture
def validator():
    return SnapshotCdcConfigValidator()


def _write_function_file(code: str) -> str:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False)
    f.write(code)
    f.flush()
    f.close()
    return f.name


def _make_action(function_file: str, function_name: str, parameters=None):
    source_function = {
        "file": function_file,
        "function": function_name,
    }
    if parameters is not None:
        source_function["parameters"] = parameters

    return Action(
        name="write_snapshot_with_params",
        type=ActionType.WRITE,
        source="v_source",
        write_target={
            "type": "streaming_table",
            "database": "silver",
            "table": "target_dim",
            "mode": "snapshot_cdc",
            "snapshot_cdc_config": {
                "source_function": source_function,
                "keys": ["id"],
                "stored_as_scd_type": 2,
            },
        },
    )


def _make_context(substitution_mgr=None):
    """``output_dir=None`` triggers dry-run copy: import lines are rendered without writing files."""
    ctx = {
        "secret_references": set(),
        "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
        "output_dir": None,
        "source_function_signature_cache": {},
    }
    if substitution_mgr:
        ctx["substitution_manager"] = substitution_mgr
    return ctx


def _alias_for(function_file: str) -> str:
    return f"_snap_{Path(function_file).stem}"


FUNC_WITH_KW_ONLY = """\
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_delta_snapshot(
    latest_version: Optional[int],
    *,
    catalog: str,
    schema: str,
    table: str,
) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.read.table(f"{catalog}.{schema}.{table}")
        return (df, 1)
    return None
"""

FUNC_WITH_KWARGS = """\
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def flexible_snapshot(
    latest_version: Optional[int],
    **kwargs,
) -> Optional[Tuple[DataFrame, int]]:
    catalog = kwargs.get("catalog", "default")
    if latest_version is None:
        df = spark.read.table(f"{catalog}.raw.items")
        return (df, 1)
    return None
"""

FUNC_NO_EXTRA_PARAMS = """\
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def simple_snapshot(
    latest_version: Optional[int],
) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.read.table("raw.items")
        return (df, 1)
    return None
"""

FUNC_WITH_REGULAR_ARGS = """\
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def snapshot_with_regular_args(
    latest_version: Optional[int],
    catalog: str = "default",
    schema: str = "raw",
) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.read.table(f"{catalog}.{schema}.items")
        return (df, 1)
    return None
"""

FUNC_WITH_SUBSTITUTION_TOKENS = """\
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_delta_snapshot(
    latest_version: Optional[int],
    *,
    catalog: str,
    schema: str,
) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.read.table(f"{catalog}.{schema}.items")
        return (df, 1)
    return None
"""


class TestParameterRendering:
    def test_string_parameters_produce_partial(self, generator):
        """String parameters render as partial(alias.func, k='v') and the
        copied module is imported + spark/dbutils injected (copy-and-import
        model); no inlined ``def`` body is emitted."""
        fn = _write_function_file(FUNC_WITH_KW_ONLY)
        alias = _alias_for(fn)
        try:
            action = _make_action(
                fn,
                "next_delta_snapshot",
                {"catalog": "prod", "schema": "silver", "table": "customers"},
            )
            code = generator.generate(action, _make_context())

            # Import is collected by the generator (assembled at pipeline level)
            assert "from functools import partial" in generator._imports
            assert (
                f"import custom_python_functions.{Path(fn).stem} as {alias}"
                in generator._imports
            )
            # Both spark/dbutils injection statements are pre-pipeline.
            pre = generator.get_pre_pipeline_statements()
            assert f"{alias}.spark = spark" in pre
            assert f"{alias}.dbutils = dbutils" in pre

            assert "partial(" in code
            # source= references the alias-qualified function, not a bare name.
            assert f"{alias}.next_delta_snapshot," in code
            assert "catalog='prod'" in code
            assert "schema='silver'" in code
            assert "table='customers'" in code
            # The function body is never inlined into the generated flowgroup.
            assert "def next_delta_snapshot" not in code
        finally:
            Path(fn).unlink()

    def test_mixed_types_produce_correct_repr(self, generator):
        """Int, bool, list, dict values use repr() for correct Python literals."""
        func_code = """\
from typing import Optional, Tuple, List, Dict, Any
from pyspark.sql import DataFrame

def typed_snapshot(
    latest_version: Optional[int],
    *,
    limit: int,
    enabled: bool,
    tags: List[str],
    options: Dict[str, Any],
) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        return (spark.read.table("t"), 1)
    return None
"""
        fn = _write_function_file(func_code)
        try:
            action = _make_action(
                fn,
                "typed_snapshot",
                {
                    "limit": 100,
                    "enabled": True,
                    "tags": ["a", "b"],
                    "options": {"x": 1},
                },
            )
            code = generator.generate(action, _make_context())

            assert "limit=100" in code
            assert "enabled=True" in code
            assert "tags=['a', 'b']" in code
            assert "options={'x': 1}" in code
            # Body is imported, not inlined.
            assert "def typed_snapshot" not in code
        finally:
            Path(fn).unlink()

    def test_no_parameters_produces_bare_function_name(self, generator):
        """Without parameters, source= uses the alias-qualified function
        reference (no partial)."""
        fn = _write_function_file(FUNC_NO_EXTRA_PARAMS)
        alias = _alias_for(fn)
        try:
            action = _make_action(fn, "simple_snapshot")
            code = generator.generate(action, _make_context())

            assert f"source={alias}.simple_snapshot," in code
            assert "partial" not in code
            assert "functools" not in code
            # Import + injection still emitted even without parameters.
            assert (
                f"import custom_python_functions.{Path(fn).stem} as {alias}"
                in generator._imports
            )
            pre = generator.get_pre_pipeline_statements()
            assert f"{alias}.spark = spark" in pre
            assert f"{alias}.dbutils = dbutils" in pre
            # No inlined body.
            assert "def simple_snapshot" not in code
        finally:
            Path(fn).unlink()

    def test_empty_parameters_treated_as_no_parameters(self, generator):
        """Empty parameters dict {} is treated as no parameters."""
        fn = _write_function_file(FUNC_NO_EXTRA_PARAMS)
        alias = _alias_for(fn)
        try:
            action = _make_action(fn, "simple_snapshot", {})
            code = generator.generate(action, _make_context())

            assert f"source={alias}.simple_snapshot," in code
            assert "partial" not in code
        finally:
            Path(fn).unlink()

    def test_functools_import_only_when_parameters_present(self, generator):
        fn = _write_function_file(FUNC_NO_EXTRA_PARAMS)
        try:
            action = _make_action(fn, "simple_snapshot")
            generator.generate(action, _make_context())
            assert not any("functools" in imp for imp in generator._imports)
        finally:
            Path(fn).unlink()


class TestASTValidation:
    def test_valid_keyword_only_args_accepted(self, generator):
        fn = _write_function_file(FUNC_WITH_KW_ONLY)
        try:
            action = _make_action(
                fn,
                "next_delta_snapshot",
                {"catalog": "prod", "schema": "silver", "table": "items"},
            )
            code = generator.generate(action, _make_context())
            assert "partial(" in code
        finally:
            Path(fn).unlink()

    def test_unknown_parameter_names_rejected(self, generator):
        fn = _write_function_file(FUNC_WITH_KW_ONLY)
        try:
            action = _make_action(
                fn,
                "next_delta_snapshot",
                {"catalog": "prod", "unknown_param": "bad"},
            )
            with pytest.raises(LHPError, match="Unknown parameters"):
                generator.generate(action, _make_context())
        finally:
            Path(fn).unlink()

    def test_kwargs_bypasses_validation(self, generator):
        fn = _write_function_file(FUNC_WITH_KWARGS)
        try:
            action = _make_action(
                fn,
                "flexible_snapshot",
                {"catalog": "prod", "anything_goes": "yes"},
            )
            code = generator.generate(action, _make_context())
            assert "partial(" in code
        finally:
            Path(fn).unlink()

    def test_regular_args_with_defaults_rejected(self, generator):
        fn = _write_function_file(FUNC_WITH_REGULAR_ARGS)
        try:
            action = _make_action(
                fn,
                "snapshot_with_regular_args",
                {"catalog": "prod", "schema": "silver"},
            )
            with pytest.raises(LHPError, match="Unknown parameters"):
                generator.generate(action, _make_context())
        finally:
            Path(fn).unlink()


class TestTypeGuard:
    def test_supported_types_accepted(self, generator):
        func_code = """\
from typing import Optional, Tuple, Any
from pyspark.sql import DataFrame

def multi_type_func(
    latest_version: Optional[int],
    *,
    s: str,
    i: int,
    f: float,
    b: bool,
    l: Any,
    d: Any,
    n: Any,
) -> Optional[Tuple[DataFrame, int]]:
    return None
"""
        fn = _write_function_file(func_code)
        try:
            action = _make_action(
                fn,
                "multi_type_func",
                {
                    "s": "hello",
                    "i": 42,
                    "f": 3.14,
                    "b": False,
                    "l": [1, 2],
                    "d": {"a": 1},
                    "n": None,
                },
            )
            code = generator.generate(action, _make_context())
            assert "partial(" in code
        finally:
            Path(fn).unlink()

    def test_unsupported_type_rejected(self, generator):
        fn = _write_function_file(FUNC_WITH_KW_ONLY)
        try:
            action = _make_action(
                fn,
                "next_delta_snapshot",
                {"catalog": {1, 2, 3}},  # set is unsupported
            )
            with pytest.raises(LHPError, match="Unsupported parameter type"):
                generator.generate(action, _make_context())
        finally:
            Path(fn).unlink()


class TestParameterSubstitution:
    def test_tokens_in_parameters_resolved(self, generator):
        fn = _write_function_file(FUNC_WITH_SUBSTITUTION_TOKENS)
        try:
            action = _make_action(
                fn,
                "next_delta_snapshot",
                {"catalog": "{catalog}", "schema": "{silver_schema}"},
            )

            sub_mgr = EnhancedSubstitutionManager()
            sub_mgr.mappings.update(
                {"catalog": "prod_catalog", "silver_schema": "silver"}
            )
            ctx = _make_context(sub_mgr)

            code = generator.generate(action, ctx)

            assert "catalog='prod_catalog'" in code
            assert "schema='silver'" in code
        finally:
            Path(fn).unlink()

    def test_secret_references_in_parameters_tracked(self, generator):
        """``EnhancedSubstitutionManager.secret_references`` is canonical; the per-context accumulator is a mirror."""
        fn = _write_function_file(FUNC_WITH_SUBSTITUTION_TOKENS)
        try:
            action = _make_action(
                fn,
                "next_delta_snapshot",
                {
                    "catalog": "${secret:config/catalog}",
                    "schema": "silver",
                },
            )

            sub_mgr = EnhancedSubstitutionManager()
            ctx = _make_context(sub_mgr)

            generator.generate(action, ctx)

            assert len(sub_mgr.secret_references) > 0
        finally:
            Path(fn).unlink()


class TestValidatorParameters:
    def test_valid_parameters_dict_accepted(self, validator):
        config = {
            "source_function": {
                "file": "funcs.py",
                "function": "my_func",
                "parameters": {"catalog": "prod", "schema": "silver"},
            },
            "keys": ["id"],
        }
        errors = validator._validate_source_configuration(config, "test_flowgroup")
        assert errors == []

    def test_non_dict_parameters_rejected(self, validator):
        for bad_value in ["not_a_dict", ["a", "b"], 42]:
            config = {
                "source_function": {
                    "file": "funcs.py",
                    "function": "my_func",
                    "parameters": bad_value,
                },
                "keys": ["id"],
            }
            errors = validator._validate_source_configuration(config, "test_flowgroup")
            assert any("'parameters' must be a dictionary" in e for e in errors), (
                f"Expected validation error for parameters={bad_value!r}"
            )

    def test_no_parameters_still_valid(self, validator):
        config = {
            "source_function": {
                "file": "funcs.py",
                "function": "my_func",
            },
            "keys": ["id"],
        }
        errors = validator._validate_source_configuration(config, "test_flowgroup")
        assert errors == []


class TestSourceFunctionResult:
    """Test the NamedTuple return type (shape ``(name, parameters)``)."""

    def test_result_with_parameters(self):
        result = SourceFunctionResult("name", {"k": "v"})
        assert result.name == "name"
        assert result.parameters == {"k": "v"}
        assert not hasattr(result, "code")
        assert result._fields == ("name", "parameters")

    def test_result_without_parameters(self):
        result = SourceFunctionResult("name")
        assert result.name == "name"
        assert result.parameters is None

    def test_result_unpacking(self):
        result = SourceFunctionResult("name", {"k": "v"})
        name, params = result
        assert name == "name"
        assert params == {"k": "v"}
