"""Unit tests for the per-shape parameter-binding rule builders.

Direct (§8.2 mirror) coverage of :mod:`lhp.core.dependencies._binding_rules`:
each builder mirrors its generator/template call convention exactly —
``${token}`` parameter bytes preserved verbatim — and any shape codegen would
reject binds nothing (``None``, never a guess). Indirect coverage through
``SourceParser`` lives in ``test_source_parsing.py`` and stays there.
"""

from __future__ import annotations

import pytest

from lhp.core.dependencies._binding_rules import (
    python_load_bindings,
    snapshot_cdc_bindings,
    transform_bindings,
)
from lhp.core.dependencies._bindings import DictValue, ParameterBindings
from lhp.models import Action, ActionType


def _transform_action(**overrides) -> Action:
    config = {
        "name": "t_act",
        "type": ActionType.TRANSFORM,
        "transform_type": "python",
        "module_path": "transforms/t.py",
        "function_name": "do_it",
        "source": "v_in",
        "target": "v_out",
        "parameters": {"table": "${env}.sch.t"},
    }
    config.update(overrides)
    return Action(**config)


@pytest.mark.unit
class TestTransformBindings:
    """Parameters dict is POSITIONAL: index 2 with source views, 1 without."""

    def test_source_view_binds_dict_positionally_at_index_2(self):
        assert transform_bindings(_transform_action()) == ParameterBindings(
            function_name="do_it",
            dict_arg_index=2,
            dict_value=DictValue({"table": frozenset({"${env}.sch.t"})}),
        )

    def test_no_source_views_bind_dict_at_index_1(self):
        binding = transform_bindings(_transform_action(source=[]))

        assert binding == ParameterBindings(
            function_name="do_it",
            dict_arg_index=1,
            dict_value=DictValue({"table": frozenset({"${env}.sch.t"})}),
        )

    def test_missing_function_name_binds_nothing(self):
        assert transform_bindings(_transform_action(function_name=None)) is None

    def test_non_python_transform_binds_nothing(self):
        assert transform_bindings(_transform_action(transform_type="sql")) is None

    def test_dict_source_shape_binds_nothing(self):
        """Codegen raises VAL-014 on a dict source — no runtime call to mirror."""
        action = _transform_action(source={"database": "bronze"})

        assert transform_bindings(action) is None


@pytest.mark.unit
class TestPythonLoadBindings:
    """Parameters dict is POSITIONAL at index 1: fn(spark, parameters)."""

    def test_explicit_function_name_binds_dict_at_index_1(self):
        source = {
            "type": "python",
            "module_path": "loaders/l.py",
            "function_name": "load_data",
            "parameters": {"table": "${catalog}.sch.orders"},
        }

        assert python_load_bindings(source) == ParameterBindings(
            function_name="load_data",
            dict_arg_index=1,
            dict_value=DictValue({"table": frozenset({"${catalog}.sch.orders"})}),
        )

    def test_missing_function_name_defaults_to_get_df(self):
        binding = python_load_bindings({"type": "python", "module_path": "l.py"})

        assert binding == ParameterBindings(
            function_name="get_df", dict_arg_index=1, dict_value=DictValue({})
        )

    def test_non_str_function_name_binds_nothing(self):
        assert python_load_bindings({"function_name": 123}) is None
        assert python_load_bindings({"function_name": ""}) is None


@pytest.mark.unit
class TestSnapshotCdcBindings:
    """source_function parameters bind KWONLY (functools.partial in codegen)."""

    def test_parameters_bind_kwonly_with_token_bytes(self):
        source_function = {
            "file": "cdc/source.py",
            "function": "next_snapshot",
            "parameters": {"table_name": "${catalog}.bronze.orders"},
        }

        assert snapshot_cdc_bindings(source_function) == ParameterBindings(
            function_name="next_snapshot",
            kwonly=DictValue({"table_name": frozenset({"${catalog}.bronze.orders"})}),
        )

    def test_missing_parameters_bind_empty_kwonly(self):
        """No params: codegen emits the bare function reference — no kwargs."""
        binding = snapshot_cdc_bindings({"function": "next_snapshot"})

        assert binding == ParameterBindings(
            function_name="next_snapshot", kwonly=DictValue({})
        )

    def test_missing_function_binds_nothing(self):
        assert snapshot_cdc_bindings({"file": "cdc/source.py"}) is None

    def test_non_dict_parameters_bind_nothing(self):
        source_function = {"function": "next_snapshot", "parameters": ["a", "b"]}

        assert snapshot_cdc_bindings(source_function) is None
