"""Unit tests for the YAML parameter-binding leaf module.

Covers :func:`bound_from_yaml` conversion rules (str byte fidelity, list/dict
shapes, unbindable scalars), :class:`ParameterBindings` style exclusivity, and
frozen-ness of the dataclasses. Token bytes like ``${env}`` must round-trip
verbatim — this layer never resolves substitutions.
"""

from __future__ import annotations

import dataclasses

import pytest

from lhp.core.dependencies._bindings import (
    DictValue,
    ListValue,
    ParameterBindings,
    bound_from_yaml,
)


@pytest.mark.unit
class TestBoundFromYamlStr:
    def test_plain_string_round_trip(self):
        assert bound_from_yaml("cat.sch.tbl") == frozenset({"cat.sch.tbl"})

    def test_substitution_token_bytes_preserved_verbatim(self):
        bound = bound_from_yaml("cat.sch.tbl${suffix}")
        assert bound == frozenset({"cat.sch.tbl${suffix}"})

    def test_secret_token_bytes_preserved_verbatim(self):
        raw = "${secret:scope/key}"
        assert bound_from_yaml(raw) == frozenset({raw})

    def test_whitespace_and_case_untouched(self):
        raw = "  MyCat.MySchema.MyTable  "
        assert bound_from_yaml(raw) == frozenset({raw})


@pytest.mark.unit
class TestBoundFromYamlList:
    def test_all_str_list_is_ordered_list_value(self):
        assert bound_from_yaml(["b", "a", "c"]) == ListValue(("b", "a", "c"))

    def test_mixed_list_is_unbindable(self):
        assert bound_from_yaml(["a", 1, "b"]) is None

    def test_list_with_bool_is_unbindable(self):
        assert bound_from_yaml(["a", True]) is None

    def test_empty_list_binds_as_empty_list_value(self):
        assert bound_from_yaml([]) == ListValue(())

    def test_nested_list_is_unbindable(self):
        assert bound_from_yaml([["a"], "b"]) is None


@pytest.mark.unit
class TestBoundFromYamlDict:
    def test_flat_dict_of_strings(self):
        assert bound_from_yaml({"table": "cat.sch.tbl"}) == DictValue(
            {"table": frozenset({"cat.sch.tbl"})}
        )

    def test_nested_dict_recurses(self):
        bound = bound_from_yaml({"outer": {"inner": "v", "names": ["x", "y"]}})
        assert bound == DictValue(
            {
                "outer": DictValue(
                    {"inner": frozenset({"v"}), "names": ListValue(("x", "y"))}
                )
            }
        )

    def test_unconvertible_entry_dropped_rest_kept(self):
        bound = bound_from_yaml({"keep": "v", "drop": 42})
        assert bound == DictValue({"keep": frozenset({"v"})})

    def test_non_str_key_entry_dropped(self):
        bound = bound_from_yaml({1: "v", "keep": "w"})
        assert bound == DictValue({"keep": frozenset({"w"})})

    def test_all_entries_dropped_still_binds_empty(self):
        assert bound_from_yaml({"a": None, "b": 1}) == DictValue({})


@pytest.mark.unit
class TestBoundFromYamlUnbindableScalars:
    @pytest.mark.parametrize("value", [1, 1.5, True, False, None, ("a",), object()])
    def test_scalar_is_unbindable(self, value):
        assert bound_from_yaml(value) is None


@pytest.mark.unit
class TestParameterBindingsStyleExclusivity:
    def test_valid_kwonly_style(self):
        pb = ParameterBindings("fn", kwonly=DictValue({"k": frozenset({"v"})}))
        assert pb.kwonly == DictValue({"k": frozenset({"v"})})
        assert pb.dict_arg_index is None
        assert pb.dict_value is None

    def test_valid_dict_style(self):
        pb = ParameterBindings("fn", dict_arg_index=0, dict_value=DictValue({}))
        assert pb.kwonly is None
        assert pb.dict_arg_index == 0
        assert pb.dict_value == DictValue({})

    def test_both_styles_raises(self):
        with pytest.raises(ValueError):
            ParameterBindings(
                "fn", kwonly=DictValue({}), dict_arg_index=0, dict_value=DictValue({})
            )

    def test_neither_style_raises(self):
        with pytest.raises(ValueError):
            ParameterBindings("fn")

    def test_partial_dict_style_raises(self):
        with pytest.raises(ValueError):
            ParameterBindings("fn", dict_arg_index=0)
        with pytest.raises(ValueError):
            ParameterBindings("fn", dict_value=DictValue({}))

    def test_kwonly_plus_partial_dict_raises(self):
        with pytest.raises(ValueError):
            ParameterBindings("fn", kwonly=DictValue({}), dict_arg_index=0)


@pytest.mark.unit
class TestFrozenness:
    def test_list_value_is_frozen(self):
        lv = ListValue(("a",))
        with pytest.raises(dataclasses.FrozenInstanceError):
            lv.items = ("b",)

    def test_dict_value_is_frozen(self):
        dv = DictValue({})
        with pytest.raises(dataclasses.FrozenInstanceError):
            dv.entries = {}

    def test_parameter_bindings_is_frozen(self):
        pb = ParameterBindings("fn", kwonly=DictValue({}))
        with pytest.raises(dataclasses.FrozenInstanceError):
            pb.function_name = "other"
