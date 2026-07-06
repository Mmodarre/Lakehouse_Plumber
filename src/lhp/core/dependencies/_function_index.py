"""Structural index of function definitions and bare-name call sites.

One walk over a parsed module records every ``FunctionDef`` /
``AsyncFunctionDef`` (including nested closures) together with the chain of
functions lexically enclosing it, its signature shape, and the ``return``
expressions belonging to its own body — plus every call site whose callee is
a bare name. The index is *structural only*: it never inspects bindings or
values, so one index per parsed file can be cached and reused across actions
whose YAML bindings differ.

Consumed by :mod:`lhp.core.dependencies._call_resolution`, which layers the
lazy, memoized value-resolution semantics (parameter unions across call
sites, return-value folding) on top.
"""

import ast
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

FunctionNode = Union[ast.FunctionDef, ast.AsyncFunctionDef]

_FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)


@dataclass(frozen=True)
class FunctionInfo:
    """Signature, scope chain and return shape of one function definition.

    ``enclosing`` is the chain of *function* nodes lexically wrapping the
    definition, outermost first (class bodies never appear — they do not
    contribute lexical scope). ``bare_name_visible`` is False when the
    definition's immediate parent scope is a class body: such a function
    lives in the class namespace and can never be reached by a bare-name
    call. ``defaults`` maps a parameter name to its default expression
    (positional and keyword-only alike); the expression evaluates in the
    function's *defining* scope. ``return_exprs`` holds the value of every
    ``return`` in the function's own body — nested defs excluded — with
    ``None`` standing for a bare ``return``.

    The dict field gives this frozen dataclass interior mutability; that is
    acceptable — consumers rely on identity/equality, not hashing.
    """

    node: FunctionNode
    name: str
    enclosing: Tuple[FunctionNode, ...]
    bare_name_visible: bool
    positional_params: Tuple[str, ...]
    kwonly_params: Tuple[str, ...]
    vararg: Optional[str]
    kwarg: Optional[str]
    defaults: Dict[str, ast.expr]
    return_exprs: Tuple[Optional[ast.expr], ...]


@dataclass(frozen=True)
class CallSite:
    """One bare-name ``name(...)`` call and the function chain enclosing it.

    ``scope_chain`` mirrors :attr:`FunctionInfo.enclosing`: the functions
    lexically wrapping the call, outermost first (empty for a module-level
    call). Argument expressions must be evaluated in that chain's
    environment, not the callee's.
    """

    node: ast.Call
    callee_name: str
    scope_chain: Tuple[FunctionNode, ...]


class FunctionIndex:
    """All function defs + resolved bare-name call sites of one module.

    Built once per parsed tree. ``resolve_callee`` answers "which function
    does the bare name ``name`` refer to from inside ``scope_chain``" with
    Python's lexical rules: candidates are visible definitions whose
    enclosing chain is a prefix of the call's chain; the innermost (longest
    prefix) wins; two same-name definitions at the same level are ambiguous
    and resolve to ``None`` (never speculate).
    """

    def __init__(self, tree: ast.Module) -> None:
        self._infos: Dict[FunctionNode, FunctionInfo] = {}
        self._raw_sites: List[CallSite] = []
        for stmt in tree.body:
            self._walk(stmt, (), in_class_body=False)
        self._sites_by_function: Dict[FunctionNode, List[CallSite]] = {}
        for site in self._raw_sites:
            target = self.resolve_callee(site.callee_name, site.scope_chain)
            if target is not None:
                self._sites_by_function.setdefault(target.node, []).append(site)

    def info_for(self, node: FunctionNode) -> Optional[FunctionInfo]:
        return self._infos.get(node)

    def call_sites_of(self, node: FunctionNode) -> Tuple[CallSite, ...]:
        return tuple(self._sites_by_function.get(node, ()))

    def resolve_callee(
        self, name: str, scope_chain: Tuple[FunctionNode, ...]
    ) -> Optional[FunctionInfo]:
        best: Optional[FunctionInfo] = None
        ambiguous = False
        for info in self._infos.values():
            if info.name != name or not info.bare_name_visible:
                continue
            depth = len(info.enclosing)
            if depth > len(scope_chain) or info.enclosing != scope_chain[:depth]:
                continue
            if best is not None and depth == len(best.enclosing):
                ambiguous = True
            elif best is None or depth > len(best.enclosing):
                best = info
                ambiguous = False
        return None if ambiguous else best

    def _walk(
        self,
        node: ast.AST,
        chain: Tuple[FunctionNode, ...],
        in_class_body: bool,
    ) -> None:
        if isinstance(node, _FUNCTION_NODES):
            self._record_function(node, chain, in_class_body)
            # Decorators, defaults and annotations evaluate in the DEFINING
            # scope, so they walk with the current chain, not the body's.
            for expr in self._signature_exprs(node):
                self._walk(expr, chain, in_class_body=False)
            body_chain = (*chain, node)
            for stmt in node.body:
                self._walk(stmt, body_chain, in_class_body=False)
            return

        if isinstance(node, ast.ClassDef):
            for expr in (*node.decorator_list, *node.bases):
                self._walk(expr, chain, in_class_body=False)
            for keyword in node.keywords:
                self._walk(keyword.value, chain, in_class_body=False)
            for stmt in node.body:
                self._walk(stmt, chain, in_class_body=True)
            return

        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            self._raw_sites.append(
                CallSite(node=node, callee_name=node.func.id, scope_chain=chain)
            )

        for child in ast.iter_child_nodes(node):
            self._walk(child, chain, in_class_body)

    @staticmethod
    def _signature_exprs(node: FunctionNode) -> List[ast.expr]:
        exprs: List[ast.expr] = list(node.decorator_list)
        exprs.extend(node.args.defaults)
        exprs.extend(d for d in node.args.kw_defaults if d is not None)
        return exprs

    def _record_function(
        self,
        node: FunctionNode,
        chain: Tuple[FunctionNode, ...],
        in_class_body: bool,
    ) -> None:
        args = node.args
        positional = tuple(a.arg for a in (*args.posonlyargs, *args.args))
        defaults: Dict[str, ast.expr] = {}
        # ``args.defaults`` right-aligns against the positional parameters.
        for param_name, default in zip(
            positional[len(positional) - len(args.defaults) :],
            args.defaults,
            strict=True,
        ):
            defaults[param_name] = default
        for kw_arg, kw_default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
            if kw_default is not None:
                defaults[kw_arg.arg] = kw_default

        self._infos[node] = FunctionInfo(
            node=node,
            name=node.name,
            enclosing=chain,
            bare_name_visible=not in_class_body,
            positional_params=positional,
            kwonly_params=tuple(a.arg for a in args.kwonlyargs),
            vararg=args.vararg.arg if args.vararg else None,
            kwarg=args.kwarg.arg if args.kwarg else None,
            defaults=defaults,
            return_exprs=self._own_return_exprs(node),
        )

    @staticmethod
    def _own_return_exprs(node: FunctionNode) -> Tuple[Optional[ast.expr], ...]:
        """Every ``return`` value in ``node``'s own body, source order.

        Nested function/class bodies are pruned — their returns belong to
        them. Lambdas contain no ``Return`` nodes, so they need no pruning.
        """
        out: List[Optional[ast.expr]] = []

        def collect(nodes: List[ast.AST]) -> None:
            for child in nodes:
                if isinstance(child, (*_FUNCTION_NODES, ast.ClassDef)):
                    continue
                if isinstance(child, ast.Return):
                    out.append(child.value)
                    continue
                collect(list(ast.iter_child_nodes(child)))

        collect(list(node.body))
        return tuple(out)
