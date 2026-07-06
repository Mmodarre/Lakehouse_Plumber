"""Unit tests for the inter-procedural call-resolution engine.

Covers :func:`compute_seed_bindings` (YAML seed lookup: module-depth,
first-match, kwonly-leftovers-to-``**kwargs``, dict-index out-of-range binds
nothing) and :class:`CallResolutionEngine`'s two oracle questions — parameter
value (union across every bare-name call site + seed + signature default; any
unmappable / dynamic contribution poisons) and call-return folding (union of
callee returns, bare/``None`` returns skipped, any other unresolvable return
poisons). Cycle and cardinality guards degrade to ``None`` rather than hang or
blow up, while a resolution diamond still resolves cleanly.
"""

from __future__ import annotations

import ast
import textwrap

import pytest

from lhp.core.dependencies._bindings import (
    DictValue,
    ListValue,
    ParameterBindings,
)
from lhp.core.dependencies._call_resolution import (
    MAX_VALUE_SET_SIZE,
    CallResolutionEngine,
    compute_seed_bindings,
)
from lhp.core.dependencies._function_index import FunctionIndex, FunctionNode


def _engine(code: str, bindings=None):
    tree = ast.parse(textwrap.dedent(code))
    index = FunctionIndex(tree)
    target, seeded = compute_seed_bindings(tree, bindings)
    engine = CallResolutionEngine(tree, index, seed_target=target, seed_bindings=seeded)
    return tree, index, engine


def _func(tree: ast.Module, name: str) -> FunctionNode:
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]
    assert len(matches) == 1, f"expected one def named {name}, got {len(matches)}"
    return matches[0]


def _call(source: str) -> ast.Call:
    node = ast.parse(source, mode="eval").body
    assert isinstance(node, ast.Call)
    return node


@pytest.mark.unit
class TestResolveParam:
    """Parameter value = union of call-site arguments + seed + default."""

    def test_union_across_two_call_sites(self):
        tree, _, engine = _engine(
            """
            def helper(t):
                return t

            helper("cat.a.x")
            helper("cat.a.y")
            """
        )
        assert engine.resolve_param(_func(tree, "helper"), "t") == frozenset(
            {"cat.a.x", "cat.a.y"}
        )

    def test_one_dynamic_site_poisons_to_none(self):
        tree, _, engine = _engine(
            """
            def helper(t):
                return t

            helper("cat.a.x")
            helper(dynamic_thing)
            """
        )
        assert engine.resolve_param(_func(tree, "helper"), "t") is None

    def test_keyword_argument_site(self):
        tree, _, engine = _engine(
            """
            def helper(a, t):
                return t

            helper("ignored", t="cat.a.kw")
            """
        )
        assert engine.resolve_param(_func(tree, "helper"), "t") == frozenset(
            {"cat.a.kw"}
        )

    def test_signature_default_used_when_site_omits_param(self):
        tree, _, engine = _engine(
            """
            def helper(a, t="cat.a.def"):
                return t

            helper("x")
            """
        )
        assert engine.resolve_param(_func(tree, "helper"), "t") == frozenset(
            {"cat.a.def"}
        )

    def test_starred_arg_at_site_poisons(self):
        tree, _, engine = _engine(
            """
            def helper(t):
                return t

            helper(*args)
            """
        )
        assert engine.resolve_param(_func(tree, "helper"), "t") is None

    def test_uncalled_param_without_default_is_none(self):
        tree, _, engine = _engine(
            """
            def helper(t):
                return t
            """
        )
        assert engine.resolve_param(_func(tree, "helper"), "t") is None

    def test_unknown_param_name_is_none(self):
        tree, _, engine = _engine(
            """
            def helper(t):
                return t

            helper("cat.a.x")
            """
        )
        assert engine.resolve_param(_func(tree, "helper"), "not_a_param") is None

    def test_seed_contributes_to_param_union(self):
        tree, _, engine = _engine(
            """
            def transform(df, parameters):
                return parameters
            """,
            bindings=ParameterBindings(
                function_name="transform",
                dict_arg_index=1,
                dict_value=DictValue({"k": frozenset({"v"})}),
            ),
        )
        assert engine.resolve_param(_func(tree, "transform"), "parameters") == (
            DictValue({"k": frozenset({"v"})})
        )


@pytest.mark.unit
class TestResolveCallReturn:
    """Return folding = union of callee returns in the callee's own env."""

    def test_single_literal_return(self):
        tree, _, engine = _engine(
            """
            def helper():
                return "cat.a.ret"
            """
        )
        assert engine.resolve_call_return(_call("helper()"), ()) == frozenset(
            {"cat.a.ret"}
        )

    def test_union_of_two_returns(self):
        tree, _, engine = _engine(
            """
            def helper(flag):
                if flag:
                    return "cat.a.r1"
                return "cat.a.r2"
            """
        )
        assert engine.resolve_call_return(_call("helper(x)"), ()) == frozenset(
            {"cat.a.r1", "cat.a.r2"}
        )

    def test_return_none_and_bare_return_skipped(self):
        tree, _, engine = _engine(
            """
            def helper(flag, other):
                if flag:
                    return None
                if other:
                    return
                return "cat.a.only"
            """
        )
        assert engine.resolve_call_return(_call("helper(a, b)"), ()) == frozenset(
            {"cat.a.only"}
        )

    def test_only_none_returns_is_none(self):
        tree, _, engine = _engine(
            """
            def helper():
                return None
            """
        )
        assert engine.resolve_call_return(_call("helper()"), ()) is None

    def test_dynamic_return_branch_poisons(self):
        tree, _, engine = _engine(
            """
            def helper(flag):
                if flag:
                    return "cat.a.r1"
                return some_dynamic()
            """
        )
        assert engine.resolve_call_return(_call("helper(x)"), ()) is None

    def test_unknown_callee_is_none(self):
        tree, _, engine = _engine(
            """
            def helper():
                return "cat.a.ret"
            """
        )
        assert engine.resolve_call_return(_call("not_defined()"), ()) is None


@pytest.mark.unit
class TestCycleAndCardinalityGuards:
    """Recursion, and value sets past the cap, degrade to ``None`` (no hang)."""

    def test_self_recursive_param_resolves_to_none(self):
        tree, _, engine = _engine(
            """
            def f(x):
                return f(x)
            """
        )
        assert engine.resolve_param(_func(tree, "f"), "x") is None

    def test_self_recursive_return_resolves_to_none(self):
        tree, _, engine = _engine(
            """
            def f(x):
                return f(x)
            """
        )
        assert engine.resolve_call_return(_call("f('a')"), ()) is None

    def test_value_set_over_cap_degrades_to_none(self):
        # A module name rebound past the cardinality cap folds (via merge_bound
        # union) to an oversized frozenset; a call site passing it degrades the
        # parameter to "not statically known" instead of expanding further.
        assigns = "\n".join(f'x = "a{i}"' for i in range(MAX_VALUE_SET_SIZE + 5))
        code = assigns + "\ndef helper(t):\n    return t\nhelper(x)\n"
        tree, _, engine = _engine(code)
        assert engine.resolve_param(_func(tree, "helper"), "t") is None


@pytest.mark.unit
class TestDiamondResolution:
    """A shared callee reached via two paths still resolves correctly."""

    def test_diamond_return_resolves(self):
        tree, _, engine = _engine(
            """
            def d():
                return "cat.d.t"
            def b():
                return d()
            def c():
                return d()
            def a(flag):
                if flag:
                    return b()
                return c()
            """
        )
        assert engine.resolve_call_return(_call("a(x)"), ()) == frozenset({"cat.d.t"})

    def test_diamond_param_unions_both_callers(self):
        tree, _, engine = _engine(
            """
            def base(t):
                return spark.table(t)
            def left():
                base("cat.left")
            def right():
                base("cat.right")
            """
        )
        assert engine.resolve_param(_func(tree, "base"), "t") == frozenset(
            {"cat.left", "cat.right"}
        )


@pytest.mark.unit
class TestComputeSeedBindings:
    """YAML seed lookup mirrors codegen's function-node resolution."""

    def test_kwonly_leftovers_bind_to_kwargs(self):
        tree = ast.parse(
            textwrap.dedent(
                """
                def snap(*, catalog, schema, **kwargs):
                    return catalog
                """
            )
        )
        bindings = ParameterBindings(
            function_name="snap",
            kwonly=DictValue(
                {
                    "catalog": frozenset({"c"}),
                    "schema": frozenset({"s"}),
                    "extra": frozenset({"e"}),
                }
            ),
        )
        target, seeded = compute_seed_bindings(tree, bindings)
        assert target is not None and target.name == "snap"
        assert seeded == {
            "catalog": frozenset({"c"}),
            "schema": frozenset({"s"}),
            "kwargs": DictValue({"extra": frozenset({"e"})}),
        }

    def test_kwonly_leftovers_without_kwargs_silently_dropped(self):
        tree = ast.parse(
            textwrap.dedent(
                """
                def snap(*, catalog):
                    return catalog
                """
            )
        )
        bindings = ParameterBindings(
            function_name="snap",
            kwonly=DictValue({"catalog": frozenset({"c"}), "extra": frozenset({"e"})}),
        )
        target, seeded = compute_seed_bindings(tree, bindings)
        assert target is not None
        assert seeded == {"catalog": frozenset({"c"})}

    def test_dict_style_valid_index_binds_positional(self):
        tree = ast.parse(
            textwrap.dedent(
                """
                def transform(df, parameters):
                    return df
                """
            )
        )
        bindings = ParameterBindings(
            function_name="transform",
            dict_arg_index=1,
            dict_value=DictValue({"k": frozenset({"v"})}),
        )
        target, seeded = compute_seed_bindings(tree, bindings)
        assert target is not None and target.name == "transform"
        assert seeded == {"parameters": DictValue({"k": frozenset({"v"})})}

    def test_dict_style_out_of_range_index_binds_nothing_but_returns_target(self):
        tree = ast.parse(
            textwrap.dedent(
                """
                def transform(df, parameters):
                    return df
                """
            )
        )
        bindings = ParameterBindings(
            function_name="transform",
            dict_arg_index=5,
            dict_value=DictValue({"k": frozenset({"v"})}),
        )
        target, seeded = compute_seed_bindings(tree, bindings)
        # Signature mismatch: never guess — no binding, but the target function
        # is still located (its body will still be walked by the visitor).
        assert target is not None and target.name == "transform"
        assert seeded == {}

    def test_no_matching_function_name(self):
        tree = ast.parse(
            textwrap.dedent(
                """
                def transform(df, parameters):
                    return df
                """
            )
        )
        bindings = ParameterBindings(
            function_name="nomatch",
            dict_arg_index=1,
            dict_value=DictValue({"k": frozenset({"v"})}),
        )
        target, seeded = compute_seed_bindings(tree, bindings)
        assert target is None
        assert seeded == {}

    def test_none_bindings_returns_empty(self):
        tree = ast.parse("def f():\n    return 1\n")
        assert compute_seed_bindings(tree, None) == (None, {})

    def test_first_module_level_match_wins(self):
        tree = ast.parse(
            textwrap.dedent(
                """
                def transform(df, parameters):
                    return df

                def transform(df, parameters):
                    return df
                """
            )
        )
        bindings = ParameterBindings(
            function_name="transform",
            dict_arg_index=1,
            dict_value=DictValue({"k": frozenset({"v"})}),
        )
        target, _ = compute_seed_bindings(tree, bindings)
        first = next(
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "transform"
        )
        assert target is first

    def test_nested_def_is_not_a_seed_target(self):
        tree = ast.parse(
            textwrap.dedent(
                """
                def outer():
                    def transform(df, parameters):
                        return df
                """
            )
        )
        bindings = ParameterBindings(
            function_name="transform",
            dict_arg_index=1,
            dict_value=DictValue({"k": frozenset({"v"})}),
        )
        # Only direct children of tree.body are candidates.
        assert compute_seed_bindings(tree, bindings) == (None, {})


@pytest.mark.unit
class TestListValueSeedThreading:
    """A seeded ``ListValue`` parameter is returned verbatim by resolve_param."""

    def test_listvalue_param_seed(self):
        tree, _, engine = _engine(
            """
            def transform(df, parameters):
                return parameters
            """,
            bindings=ParameterBindings(
                function_name="transform",
                dict_arg_index=1,
                dict_value=DictValue({"tables": ListValue(("a", "b"))}),
            ),
        )
        assert engine.resolve_param(_func(tree, "transform"), "parameters") == (
            DictValue({"tables": ListValue(("a", "b"))})
        )
