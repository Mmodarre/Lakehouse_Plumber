"""Unit tests for binding-aware static (AST-level) string resolution.

Covers :func:`resolve_static_string_values`, :func:`resolve_static_list` and
:func:`render_f_string` against the ``Bound`` shapes from ``_bindings``:
string sets, ordered lists (:class:`ListValue`) and string-keyed dicts
(:class:`DictValue`). Token byte fidelity is asserted throughout — ``${token}``
substrings must flow through every resolution path as exact bytes, never
resolved or altered.
"""

from __future__ import annotations

import ast
from typing import Callable, Optional

import pytest

from lhp.core.dependencies._bindings import Bound, DictValue, ListValue
from lhp.core.dependencies._static_resolution import (
    render_f_string,
    resolve_static_bound,
    resolve_static_dict,
    resolve_static_list,
    resolve_static_string_values,
)


def _expr(code: str) -> ast.expr:
    """Parse ``code`` as a single expression and return its AST node."""
    return ast.parse(code, mode="eval").body


def _resolver(**bindings: Bound) -> Callable[[str], Optional[Bound]]:
    """Build a name resolver backed by a simple mapping of bindings."""

    def resolve(name: str) -> Optional[Bound]:
        return bindings.get(name)

    return resolve


_PARAMS = DictValue(
    {
        "tbl": frozenset({"cat.sch.t${suffix}"}),
        "cols": ListValue(("c1", "c2", "c3")),
        "nested": DictValue({"inner": frozenset({"x.y.z"})}),
    }
)


@pytest.mark.unit
class TestStringContextBasics:
    """Plain string-context resolution, with and without a resolver."""

    def test_constant_string(self):
        assert resolve_static_string_values(_expr('"cat.sch.t"')) == frozenset(
            {"cat.sch.t"}
        )

    def test_non_string_constant_unresolved(self):
        assert resolve_static_string_values(_expr("42")) == frozenset()

    def test_name_resolves_through_frozenset_bound(self):
        resolver = _resolver(tbl=frozenset({"cat.sch.t"}))
        assert resolve_static_string_values(_expr("tbl"), resolver) == frozenset(
            {"cat.sch.t"}
        )

    def test_name_token_bytes_preserved_verbatim(self):
        resolver = _resolver(tbl=frozenset({"cat.sch.t${suffix}"}))
        assert resolve_static_string_values(_expr("tbl"), resolver) == frozenset(
            {"cat.sch.t${suffix}"}
        )

    def test_name_bound_to_list_value_is_not_a_string(self):
        resolver = _resolver(cols=ListValue(("a", "b")))
        assert resolve_static_string_values(_expr("cols"), resolver) == frozenset()

    def test_name_bound_to_dict_value_is_not_a_string(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(_expr("params"), resolver) == frozenset()

    def test_name_without_resolver_unresolved(self):
        assert resolve_static_string_values(_expr("tbl")) == frozenset()

    def test_unbound_name_unresolved(self):
        assert resolve_static_string_values(_expr("tbl"), _resolver()) == frozenset()

    def test_binop_concat_cartesian_product(self):
        resolver = _resolver(a=frozenset({"x", "y"}), b=frozenset({"1", "2"}))
        assert resolve_static_string_values(_expr("a + b"), resolver) == frozenset(
            {"x1", "x2", "y1", "y2"}
        )

    def test_binop_with_unresolved_side_unresolved(self):
        resolver = _resolver(a=frozenset({"x"}))
        assert (
            resolve_static_string_values(_expr("a + missing"), resolver) == frozenset()
        )

    def test_format_call_positional(self):
        node = _expr('"{}.{}.t".format("cat", "sch")')
        assert resolve_static_string_values(node) == frozenset({"cat.sch.t"})

    def test_format_call_with_multivalued_arg_unresolved(self):
        resolver = _resolver(a=frozenset({"x", "y"}))
        node = _expr('"{}.t".format(a)')
        assert resolve_static_string_values(node, resolver) == frozenset()


@pytest.mark.unit
class TestSubscriptResolution:
    """``name["key"]`` lookups into a dict-bound name (string context)."""

    def test_const_key_resolves_with_exact_token_bytes(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(
            _expr('params["tbl"]'), resolver
        ) == frozenset({"cat.sch.t${suffix}"})

    def test_missing_key_unresolved(self):
        resolver = _resolver(params=_PARAMS)
        assert (
            resolve_static_string_values(_expr('params["nope"]'), resolver)
            == frozenset()
        )

    def test_non_constant_slice_unresolved(self):
        resolver = _resolver(params=_PARAMS, key=frozenset({"tbl"}))
        assert (
            resolve_static_string_values(_expr("params[key]"), resolver) == frozenset()
        )

    def test_non_string_constant_slice_unresolved(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(_expr("params[0]"), resolver) == frozenset()

    def test_non_dict_base_unresolved(self):
        resolver = _resolver(tbl=frozenset({"cat.sch.t"}))
        assert resolve_static_string_values(_expr('tbl["k"]'), resolver) == frozenset()

    def test_list_value_entry_is_not_a_string(self):
        resolver = _resolver(params=_PARAMS)
        assert (
            resolve_static_string_values(_expr('params["cols"]'), resolver)
            == frozenset()
        )

    def test_nested_dict_subscript_chain(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(
            _expr('params["nested"]["inner"]'), resolver
        ) == frozenset({"x.y.z"})


@pytest.mark.unit
class TestGetCalls:
    """``name.get("key"[, default])`` on a dict-bound name."""

    def test_get_present_key_exact_token_bytes(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(
            _expr('params.get("tbl")'), resolver
        ) == frozenset({"cat.sch.t${suffix}"})

    def test_get_present_key_ignores_default(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(
            _expr('params.get("tbl", "other")'), resolver
        ) == frozenset({"cat.sch.t${suffix}"})

    def test_get_missing_key_with_constant_default(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(
            _expr('params.get("nope", "cat.sch.d${env}")'), resolver
        ) == frozenset({"cat.sch.d${env}"})

    def test_get_missing_key_with_bound_name_default(self):
        resolver = _resolver(params=_PARAMS, fallback=frozenset({"cat.sch.f"}))
        assert resolve_static_string_values(
            _expr('params.get("nope", fallback)'), resolver
        ) == frozenset({"cat.sch.f"})

    def test_get_missing_key_without_default_unresolved(self):
        # A runtime ``.get`` miss yields None — not a string.
        resolver = _resolver(params=_PARAMS)
        assert (
            resolve_static_string_values(_expr('params.get("nope")'), resolver)
            == frozenset()
        )

    def test_get_missing_key_with_unresolvable_default_unresolved(self):
        resolver = _resolver(params=_PARAMS)
        assert (
            resolve_static_string_values(_expr('params.get("nope", unknown)'), resolver)
            == frozenset()
        )

    def test_get_non_constant_key_unresolved(self):
        resolver = _resolver(params=_PARAMS, key=frozenset({"tbl"}))
        assert (
            resolve_static_string_values(_expr("params.get(key)"), resolver)
            == frozenset()
        )

    def test_get_on_non_dict_bound_unresolved(self):
        resolver = _resolver(tbl=frozenset({"cat.sch.t"}))
        assert (
            resolve_static_string_values(_expr('tbl.get("k")'), resolver) == frozenset()
        )

    def test_get_with_keyword_argument_unresolved(self):
        resolver = _resolver(params=_PARAMS)
        assert (
            resolve_static_string_values(
                _expr('params.get("tbl", default="x")'), resolver
            )
            == frozenset()
        )


@pytest.mark.unit
class TestStringMethods:
    """Element-wise string methods over statically-resolved receivers."""

    def test_replace_on_constant(self):
        node = _expr('"cat.sch.t_raw".replace("_raw", "")')
        assert resolve_static_string_values(node) == frozenset({"cat.sch.t"})

    def test_replace_token_bytes_flow_through(self):
        resolver = _resolver(
            params=DictValue({"tbl": frozenset({"sch.t_raw${suffix}"})})
        )
        node = _expr('params["tbl"].replace("_raw", "")')
        assert resolve_static_string_values(node, resolver) == frozenset(
            {"sch.t${suffix}"}
        )

    def test_replace_cartesian_over_receiver_and_args(self):
        resolver = _resolver(
            recv=frozenset({"a_x", "b_x"}),
            new=frozenset({"1", "2"}),
        )
        node = _expr('recv.replace("_x", new)')
        assert resolve_static_string_values(node, resolver) == frozenset(
            {"a1", "a2", "b1", "b2"}
        )

    def test_replace_wrong_arity_unresolved(self):
        assert resolve_static_string_values(_expr('"a_b".replace("_")')) == frozenset()
        assert (
            resolve_static_string_values(_expr('"a_b".replace("_", ".", 1)'))
            == frozenset()
        )

    def test_replace_unresolvable_arg_unresolved(self):
        assert (
            resolve_static_string_values(_expr('"a_b".replace(unknown, ".")'))
            == frozenset()
        )

    def test_replace_keyword_argument_unresolved(self):
        assert (
            resolve_static_string_values(_expr('"a_b".replace("_", new=".")'))
            == frozenset()
        )

    def test_upper(self):
        resolver = _resolver(tbl=frozenset({"cat.sch.t"}))
        assert resolve_static_string_values(
            _expr("tbl.upper()"), resolver
        ) == frozenset({"CAT.SCH.T"})

    def test_lower(self):
        assert resolve_static_string_values(_expr('"CaT".lower()')) == frozenset(
            {"cat"}
        )

    def test_upper_with_argument_unresolved(self):
        assert resolve_static_string_values(_expr('"x".upper("y")')) == frozenset()

    def test_strip_default_whitespace(self):
        assert resolve_static_string_values(
            _expr('"  cat.sch.t  ".strip()')
        ) == frozenset({"cat.sch.t"})

    def test_lstrip_and_rstrip(self):
        assert resolve_static_string_values(_expr('"  x ".lstrip()')) == frozenset(
            {"x "}
        )
        assert resolve_static_string_values(_expr('"  x ".rstrip()')) == frozenset(
            {"  x"}
        )

    def test_strip_with_const_chars(self):
        assert resolve_static_string_values(
            _expr('"xxcat.sch.txx".strip("x")')
        ) == frozenset({"cat.sch.t"})

    def test_strip_cartesian_over_multivalued_receiver(self):
        resolver = _resolver(raw=frozenset({" a ", " b "}))
        assert resolve_static_string_values(
            _expr("raw.strip()"), resolver
        ) == frozenset({"a", "b"})

    def test_strip_with_unresolvable_arg_unresolved(self):
        assert resolve_static_string_values(_expr('"x".strip(unknown)')) == frozenset()

    def test_strip_too_many_args_unresolved(self):
        assert resolve_static_string_values(_expr('"x".strip("a", "b")')) == frozenset()

    def test_methods_compose_with_f_string_receiver(self):
        resolver = _resolver(env=frozenset({"dev"}))
        node = _expr('f"{env}.sch.t".upper()')
        assert resolve_static_string_values(node, resolver) == frozenset({"DEV.SCH.T"})

    def test_unknown_method_unresolved(self):
        assert resolve_static_string_values(_expr('"cat".title()')) == frozenset()


@pytest.mark.unit
class TestJoin:
    """``sep.join(items)`` with statically-resolved ordered lists."""

    def test_join_list_literal(self):
        node = _expr('".".join(["cat", "sch", "t"])')
        assert resolve_static_string_values(node) == frozenset({"cat.sch.t"})

    def test_join_name_bound_to_list_value(self):
        resolver = _resolver(parts=ListValue(("cat", "sch", "t")))
        node = _expr('".".join(parts)')
        assert resolve_static_string_values(node, resolver) == frozenset({"cat.sch.t"})

    def test_join_subscript_list_entry(self):
        resolver = _resolver(params=_PARAMS)
        node = _expr('"_".join(params["cols"])')
        assert resolve_static_string_values(node, resolver) == frozenset({"c1_c2_c3"})

    def test_join_get_list_entry(self):
        resolver = _resolver(params=_PARAMS)
        node = _expr('"-".join(params.get("cols"))')
        assert resolve_static_string_values(node, resolver) == frozenset({"c1-c2-c3"})

    def test_join_multivalued_separator_cartesian(self):
        resolver = _resolver(sep=frozenset({".", "_"}))
        node = _expr('sep.join(["a", "b"])')
        assert resolve_static_string_values(node, resolver) == frozenset({"a.b", "a_b"})

    def test_join_token_bytes_preserved(self):
        node = _expr('".".join(["${catalog}", "sch", "t"])')
        assert resolve_static_string_values(node) == frozenset({"${catalog}.sch.t"})

    def test_join_unresolvable_items_unresolved(self):
        assert resolve_static_string_values(_expr('".".join(unknown)')) == frozenset()

    def test_join_unresolved_separator_unresolved(self):
        assert (
            resolve_static_string_values(_expr('sep.join(["a", "b"])')) == frozenset()
        )

    def test_join_wrong_arity_unresolved(self):
        assert resolve_static_string_values(_expr('".".join()')) == frozenset()
        assert (
            resolve_static_string_values(_expr('".".join(["a"], ["b"])')) == frozenset()
        )


@pytest.mark.unit
class TestResolveStaticList:
    """Ordered-list resolution: literals, names, and dict lookups."""

    def test_list_literal_preserves_order(self):
        assert resolve_static_list(_expr('["b", "a", "c"]')) == ListValue(
            ("b", "a", "c")
        )

    def test_tuple_literal(self):
        assert resolve_static_list(_expr('("a", "b")')) == ListValue(("a", "b"))

    def test_empty_list_literal(self):
        assert resolve_static_list(_expr("[]")) == ListValue(())

    def test_elements_resolve_through_resolver(self):
        resolver = _resolver(x=frozenset({"b"}))
        assert resolve_static_list(_expr('["a", x]'), resolver) == ListValue(("a", "b"))

    def test_multivalued_element_makes_whole_list_unresolved(self):
        # PIN: a multi-valued element does NOT expand into a cartesian set of
        # candidate lists — the whole list is unresolved instead.
        resolver = _resolver(x=frozenset({"b", "c"}))
        assert resolve_static_list(_expr('["a", x]'), resolver) is None

    def test_unresolvable_element_unresolved(self):
        assert resolve_static_list(_expr('["a", unknown]')) is None

    def test_starred_element_unresolved(self):
        assert resolve_static_list(_expr('[*xs, "a"]')) is None

    def test_non_string_element_unresolved(self):
        assert resolve_static_list(_expr('["a", 1]')) is None

    def test_name_bound_to_list_value(self):
        resolver = _resolver(cols=ListValue(("a", "b")))
        assert resolve_static_list(_expr("cols"), resolver) == ListValue(("a", "b"))

    def test_name_bound_to_frozenset_is_not_a_list(self):
        resolver = _resolver(tbl=frozenset({"a"}))
        assert resolve_static_list(_expr("tbl"), resolver) is None

    def test_name_bound_to_dict_is_not_a_list(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_list(_expr("params"), resolver) is None

    def test_subscript_entry_list_value(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_list(_expr('params["cols"]'), resolver) == ListValue(
            ("c1", "c2", "c3")
        )

    def test_get_entry_list_value(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_list(_expr('params.get("cols")'), resolver) == ListValue(
            ("c1", "c2", "c3")
        )

    def test_get_missing_key_with_list_literal_default(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_list(
            _expr('params.get("nope", ["a", "b"])'), resolver
        ) == ListValue(("a", "b"))

    def test_constant_string_is_not_a_list(self):
        assert resolve_static_list(_expr('"a,b"')) is None

    def test_token_bytes_preserved_in_items(self):
        assert resolve_static_list(_expr('["${catalog}.sch.t", "b"]')) == ListValue(
            ("${catalog}.sch.t", "b")
        )


@pytest.mark.unit
class TestFStrings:
    """Resolver-aware f-string resolution (no fabricated ``{var}`` marker)."""

    def test_constant_only_f_string(self):
        assert resolve_static_string_values(_expr('f"a.b.c"')) == frozenset({"a.b.c"})

    def test_interpolated_bound_name_uses_real_bytes(self):
        resolver = _resolver(env=frozenset({"dev"}))
        assert resolve_static_string_values(
            _expr('f"{env}.sch.t"'), resolver
        ) == frozenset({"dev.sch.t"})

    def test_token_bytes_through_interpolation(self):
        resolver = _resolver(cat=frozenset({"${catalog}"}))
        assert resolve_static_string_values(
            _expr('f"{cat}.sch.t"'), resolver
        ) == frozenset({"${catalog}.sch.t"})

    def test_multivalued_interpolations_cartesian(self):
        resolver = _resolver(
            env=frozenset({"dev", "prod"}),
            tbl=frozenset({"a", "b"}),
        )
        assert resolve_static_string_values(
            _expr('f"{env}.sch.{tbl}"'), resolver
        ) == frozenset({"dev.sch.a", "dev.sch.b", "prod.sch.a", "prod.sch.b"})

    def test_subscript_inside_f_string(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(
            _expr("""f"{params['tbl']}.x\""""), resolver
        ) == frozenset({"cat.sch.t${suffix}.x"})

    def test_get_inside_f_string(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_string_values(
            _expr("""f"{params.get('nope', 'dflt')}.x\""""), resolver
        ) == frozenset({"dflt.x"})

    def test_method_inside_f_string(self):
        resolver = _resolver(env=frozenset({"dev"}))
        assert resolve_static_string_values(
            _expr('f"{env.upper()}.sch.t"'), resolver
        ) == frozenset({"DEV.sch.t"})

    def test_known_placeholder_kept_without_resolver(self):
        assert resolve_static_string_values(
            _expr('f"{catalog}.silver.t"')
        ) == frozenset({"{catalog}.silver.t"})

    def test_known_placeholder_resolution_wins_over_retention(self):
        resolver = _resolver(catalog=frozenset({"main"}))
        assert resolve_static_string_values(
            _expr('f"{catalog}.silver.t"'), resolver
        ) == frozenset({"main.silver.t"})

    def test_unknown_name_unresolved_no_var_junk(self):
        result = resolve_static_string_values(_expr('f"{unknown}.silver.t"'))
        assert result == frozenset()
        assert not any("{var}" in value for value in result)

    def test_mixed_known_and_unknown_whole_f_string_unresolved(self):
        assert (
            resolve_static_string_values(_expr('f"{catalog}.{unknown}.t"'))
            == frozenset()
        )

    def test_non_name_unresolvable_expression_unresolved(self):
        assert resolve_static_string_values(_expr('f"{obj.attr}.t"')) == frozenset()

    def test_conversion_specifier_unresolved(self):
        resolver = _resolver(env=frozenset({"dev"}))
        assert (
            resolve_static_string_values(_expr('f"{env!r}.t"'), resolver) == frozenset()
        )

    def test_format_spec_unresolved(self):
        resolver = _resolver(env=frozenset({"dev"}))
        assert (
            resolve_static_string_values(_expr('f"{env:>10}.t"'), resolver)
            == frozenset()
        )

    def test_render_f_string_direct(self):
        resolver = _resolver(env=frozenset({"dev", "prod"}))
        node = _expr('f"{env}.sch.{table}"')
        assert isinstance(node, ast.JoinedStr)
        assert render_f_string(node, resolver) == frozenset(
            {"dev.sch.{table}", "prod.sch.{table}"}
        )


@pytest.mark.unit
class TestListSubscriptResolution:
    """``list_bound[index]`` lookups into a name bound to a ``ListValue``."""

    def test_constant_index_returns_element(self):
        resolver = _resolver(cols=ListValue(("a", "b", "c")))
        assert resolve_static_string_values(_expr("cols[0]"), resolver) == frozenset(
            {"a"}
        )

    def test_negative_index_returns_element(self):
        resolver = _resolver(cols=ListValue(("a", "b", "c")))
        assert resolve_static_string_values(_expr("cols[-1]"), resolver) == frozenset(
            {"c"}
        )

    def test_out_of_range_index_unresolved(self):
        resolver = _resolver(cols=ListValue(("a", "b")))
        assert resolve_static_string_values(_expr("cols[9]"), resolver) == frozenset()

    def test_dynamic_index_unions_all_elements(self):
        resolver = _resolver(cols=ListValue(("a", "b", "c")), i=frozenset({"0"}))
        assert resolve_static_string_values(_expr("cols[i]"), resolver) == frozenset(
            {"a", "b", "c"}
        )

    def test_slice_unions_all_elements(self):
        resolver = _resolver(cols=ListValue(("a", "b", "c")))
        assert resolve_static_string_values(_expr("cols[1:2]"), resolver) == frozenset(
            {"a", "b", "c"}
        )

    def test_index_token_bytes_preserved(self):
        resolver = _resolver(cols=ListValue(("${catalog}.sch.t", "b")))
        assert resolve_static_string_values(_expr("cols[0]"), resolver) == frozenset(
            {"${catalog}.sch.t"}
        )


@pytest.mark.unit
class TestResolveStaticDict:
    """``resolve_static_dict``: dict literals and dict-bound names → DictValue."""

    def test_dict_literal_binds_string_entries(self):
        assert resolve_static_dict(_expr('{"o": "cat.sch.o", "p": "cat.sch.p"}')) == (
            DictValue({"o": frozenset({"cat.sch.o"}), "p": frozenset({"cat.sch.p"})})
        )

    def test_nested_container_values(self):
        node = _expr('{"d": {"inner": "x.y.z"}, "l": ["c1", "c2"]}')
        assert resolve_static_dict(node) == DictValue(
            {
                "d": DictValue({"inner": frozenset({"x.y.z"})}),
                "l": ListValue(("c1", "c2")),
            }
        )

    def test_unbindable_value_dropped_rest_still_bind(self):
        # Mirrors bound_from_yaml: an entry whose value is not statically known
        # is dropped; the static entries still bind.
        node = _expr('{"o": "cat.sch.o", "d": get_name()}')
        assert resolve_static_dict(node) == DictValue({"o": frozenset({"cat.sch.o"})})

    def test_non_constant_and_non_string_keys_dropped(self):
        resolver = _resolver(k=frozenset({"o"}))
        node = _expr('{k: "cat.sch.a", 0: "cat.sch.b", "o": "cat.sch.o"}')
        assert resolve_static_dict(node, resolver) == DictValue(
            {"o": frozenset({"cat.sch.o"})}
        )

    def test_spread_entry_dropped(self):
        node = _expr('{"o": "cat.sch.o", **other}')
        assert resolve_static_dict(node) == DictValue({"o": frozenset({"cat.sch.o"})})

    def test_token_bytes_preserved(self):
        assert resolve_static_dict(_expr('{"o": "cat.sch.t${suffix}"}')) == DictValue(
            {"o": frozenset({"cat.sch.t${suffix}"})}
        )

    def test_name_bound_to_dict_value(self):
        resolver = _resolver(params=_PARAMS)
        assert resolve_static_dict(_expr("params"), resolver) == _PARAMS

    def test_non_dict_node_unresolved(self):
        assert resolve_static_dict(_expr('"cat.sch.t"')) is None
        assert resolve_static_dict(_expr('["a", "b"]')) is None


@pytest.mark.unit
class TestResolveStaticBound:
    """``resolve_static_bound``: the assignment-RHS dispatcher (any shape)."""

    def test_dict_literal_becomes_dict_value(self):
        assert resolve_static_bound(_expr('{"o": "cat.sch.o"}')) == DictValue(
            {"o": frozenset({"cat.sch.o"})}
        )

    def test_list_literal_becomes_list_value(self):
        assert resolve_static_bound(_expr('["a", "b"]')) == ListValue(("a", "b"))

    def test_tuple_literal_becomes_list_value(self):
        assert resolve_static_bound(_expr('("a", "b")')) == ListValue(("a", "b"))

    def test_string_expression_becomes_frozenset(self):
        assert resolve_static_bound(_expr('"cat.sch.t"')) == frozenset({"cat.sch.t"})

    def test_multivalued_list_is_unbindable(self):
        # A list with a multi-valued element is all-or-nothing (None), matching
        # resolve_static_list — never expand into candidate lists.
        resolver = _resolver(x=frozenset({"a", "b"}))
        assert resolve_static_bound(_expr('["a", x]'), resolver) is None

    def test_name_carries_its_existing_bound(self):
        resolver = _resolver(params=_PARAMS, cols=ListValue(("a",)))
        assert resolve_static_bound(_expr("params"), resolver) == _PARAMS
        assert resolve_static_bound(_expr("cols"), resolver) == ListValue(("a",))

    def test_unresolvable_expression_is_none(self):
        assert resolve_static_bound(_expr("get_name()")) is None
        assert resolve_static_bound(_expr("tbl"), _resolver()) is None
