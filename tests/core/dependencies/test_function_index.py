"""Unit tests for the structural function index.

Covers :class:`FunctionInfo` / :class:`CallSite` / :class:`FunctionIndex`:
one structural walk records every ``FunctionDef`` (including nested closures)
with its enclosing-function chain, signature shape (positional / kwonly params,
vararg / kwarg names, right-aligned defaults), and own-body return expressions
(nested defs excluded, ``None`` for a bare ``return``). Bare-name call sites are
recorded with their enclosing chain; :meth:`FunctionIndex.resolve_callee`
applies Python's lexical innermost-wins rule and refuses to guess between
same-level duplicates.
"""

from __future__ import annotations

import ast
import textwrap
from typing import List

import pytest

from lhp.core.dependencies._function_index import (
    FunctionIndex,
    FunctionNode,
)


def _index(code: str) -> tuple[ast.Module, FunctionIndex]:
    tree = ast.parse(textwrap.dedent(code))
    return tree, FunctionIndex(tree)


def _defs(tree: ast.Module, name: str) -> List[FunctionNode]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]


def _the_def(tree: ast.Module, name: str) -> FunctionNode:
    matches = _defs(tree, name)
    assert len(matches) == 1, f"expected one def named {name}, got {len(matches)}"
    return matches[0]


def _enclosing_names(index: FunctionIndex, node: FunctionNode) -> tuple[str, ...]:
    info = index.info_for(node)
    assert info is not None
    return tuple(e.name for e in info.enclosing)


def _default_reprs(index: FunctionIndex, node: FunctionNode) -> dict[str, str]:
    info = index.info_for(node)
    assert info is not None
    return {name: ast.unparse(expr) for name, expr in info.defaults.items()}


def _return_reprs(index: FunctionIndex, node: FunctionNode) -> tuple:
    info = index.info_for(node)
    assert info is not None
    return tuple(None if r is None else ast.unparse(r) for r in info.return_exprs)


@pytest.mark.unit
class TestScopeChains:
    """Enclosing chains for module functions and nested closures."""

    def test_module_and_nested_closure_chains(self):
        tree, index = _index(
            """
            def outer(a):
                def inner(b):
                    def deepest(c):
                        return c
                    return deepest(b)
                return inner(a)

            def sibling():
                return 1
            """
        )
        assert _enclosing_names(index, _the_def(tree, "outer")) == ()
        assert _enclosing_names(index, _the_def(tree, "inner")) == ("outer",)
        assert _enclosing_names(index, _the_def(tree, "deepest")) == ("outer", "inner")
        assert _enclosing_names(index, _the_def(tree, "sibling")) == ()

    def test_all_nested_defs_are_indexed(self):
        tree, index = _index(
            """
            def outer():
                def inner():
                    return 1
            """
        )
        assert index.info_for(_the_def(tree, "outer")) is not None
        assert index.info_for(_the_def(tree, "inner")) is not None

    def test_bare_name_visible_true_for_module_and_closure_defs(self):
        tree, index = _index(
            """
            def outer():
                def inner():
                    return 1
            """
        )
        assert index.info_for(_the_def(tree, "outer")).bare_name_visible is True
        assert index.info_for(_the_def(tree, "inner")).bare_name_visible is True


@pytest.mark.unit
class TestClassBodyScoping:
    """Class bodies never enter the enclosing chain and hide their methods."""

    def test_method_in_class_body_not_bare_name_visible(self):
        tree, index = _index(
            """
            class C:
                def method(self, p):
                    return p
            """
        )
        info = index.info_for(_the_def(tree, "method"))
        assert info is not None
        # A class body contributes no lexical scope, so the enclosing chain is
        # empty; but the def lives in the class namespace, so it is not
        # reachable by a bare name.
        assert info.enclosing == ()
        assert info.bare_name_visible is False

    def test_def_nested_inside_method_is_visible_with_method_enclosing(self):
        tree, index = _index(
            """
            class C:
                def method(self, p):
                    def helper(q):
                        return q
                    return helper(p)
            """
        )
        method = _the_def(tree, "method")
        helper_info = index.info_for(_the_def(tree, "helper"))
        assert helper_info is not None
        assert helper_info.bare_name_visible is True
        assert helper_info.enclosing == (method,)


@pytest.mark.unit
class TestSignatureShape:
    """Positional / kwonly params, vararg / kwarg names and defaults mapping."""

    def test_positional_defaults_right_align(self):
        tree, index = _index(
            """
            def f(a, b, c=1, d="x"):
                return a
            """
        )
        info = index.info_for(_the_def(tree, "f"))
        assert info.positional_params == ("a", "b", "c", "d")
        # ``args.defaults`` right-aligns: only c and d carry defaults.
        assert _default_reprs(index, _the_def(tree, "f")) == {"c": "1", "d": "'x'"}

    def test_kwonly_params_and_defaults(self):
        tree, index = _index(
            """
            def f(a, *, e, g="y"):
                return a
            """
        )
        node = _the_def(tree, "f")
        info = index.info_for(node)
        assert info.positional_params == ("a",)
        assert info.kwonly_params == ("e", "g")
        # ``e`` has no default; only ``g`` appears in the defaults mapping.
        assert _default_reprs(index, node) == {"g": "'y'"}

    def test_posonly_included_in_positional_params(self):
        tree, index = _index(
            """
            def f(a, b, /, c):
                return a
            """
        )
        info = index.info_for(_the_def(tree, "f"))
        assert info.positional_params == ("a", "b", "c")

    def test_vararg_and_kwarg_names_captured(self):
        tree, index = _index(
            """
            def f(a, *args, **kwargs):
                return a
            """
        )
        info = index.info_for(_the_def(tree, "f"))
        assert info.positional_params == ("a",)
        assert info.vararg == "args"
        assert info.kwarg == "kwargs"

    def test_no_vararg_or_kwarg_is_none(self):
        tree, index = _index(
            """
            def f(a):
                return a
            """
        )
        info = index.info_for(_the_def(tree, "f"))
        assert info.vararg is None
        assert info.kwarg is None


@pytest.mark.unit
class TestReturnExprs:
    """Own-body returns only; nested defs excluded; bare return is ``None``."""

    def test_bare_return_recorded_as_none_and_nested_excluded(self):
        tree, index = _index(
            """
            def f(flag):
                if flag:
                    return "a"
                def nested():
                    return "nested_val"
                return
            """
        )
        # "a" (own), then None (bare return); the nested def's return is pruned.
        assert _return_reprs(index, _the_def(tree, "f")) == ("'a'", None)
        # The nested def carries only its own return.
        assert _return_reprs(index, _the_def(tree, "nested")) == ("'nested_val'",)

    def test_return_none_constant_kept_as_expr(self):
        tree, index = _index(
            """
            def f():
                return None
            """
        )
        # A ``return None`` keeps the Constant node (only a *bare* return is None).
        assert _return_reprs(index, _the_def(tree, "f")) == ("None",)

    def test_returns_collected_from_nested_control_flow(self):
        tree, index = _index(
            """
            def f(x):
                for i in x:
                    if i:
                        return "a"
                return "b"
            """
        )
        assert _return_reprs(index, _the_def(tree, "f")) == ("'a'", "'b'")


@pytest.mark.unit
class TestCallSites:
    """Bare-name call sites are recorded with their enclosing chain."""

    def test_sites_recorded_with_module_and_function_chains(self):
        tree, index = _index(
            """
            def helper(t):
                return spark.table(t)

            helper("cat.a.x")

            def other():
                helper("cat.a.y")
            """
        )
        sites = index.call_sites_of(_the_def(tree, "helper"))
        assert len(sites) == 2
        by_chain = {
            tuple(e.name for e in site.scope_chain): ast.unparse(site.node)
            for site in sites
        }
        # ``ast.unparse`` normalizes string literals to single quotes.
        assert by_chain == {
            (): "helper('cat.a.x')",
            ("other",): "helper('cat.a.y')",
        }

    def test_call_sites_grouped_per_function(self):
        tree, index = _index(
            """
            def a(t):
                return t

            def b(t):
                return t

            a("1")
            a("2")
            b("3")
            """
        )
        assert len(index.call_sites_of(_the_def(tree, "a"))) == 2
        assert len(index.call_sites_of(_the_def(tree, "b"))) == 1

    def test_uncalled_function_has_no_sites(self):
        tree, index = _index(
            """
            def never_called(t):
                return t
            """
        )
        assert index.call_sites_of(_the_def(tree, "never_called")) == ()


@pytest.mark.unit
class TestResolveCallee:
    """Lexical innermost-wins resolution over the recorded definitions."""

    def test_innermost_shadow_wins_inside_container(self):
        tree, index = _index(
            """
            def name():
                return "outer_level"

            def container():
                def name():
                    return "inner_level"
                name()
            """
        )
        container = _the_def(tree, "container")
        outer = _defs(tree, "name")[0]
        inner = _defs(tree, "name")[1]
        # From inside container, the nested (innermost) definition wins.
        assert index.resolve_callee("name", (container,)).node is inner
        # From module level, only the module-level definition is reachable.
        assert index.resolve_callee("name", ()).node is outer

    def test_closure_not_visible_from_module_scope(self):
        tree, index = _index(
            """
            def outer():
                def closure():
                    return "c"
                closure()
            """
        )
        outer = _the_def(tree, "outer")
        closure = _the_def(tree, "closure")
        assert index.resolve_callee("closure", ()) is None
        assert index.resolve_callee("closure", (outer,)).node is closure

    def test_same_level_duplicates_are_ambiguous(self):
        tree, index = _index(
            """
            def dup():
                return "1"

            def dup():
                return "2"
            """
        )
        assert index.resolve_callee("dup", ()) is None

    def test_unknown_name_resolves_to_none(self):
        _, index = _index(
            """
            def f():
                return 1
            """
        )
        assert index.resolve_callee("missing", ()) is None
