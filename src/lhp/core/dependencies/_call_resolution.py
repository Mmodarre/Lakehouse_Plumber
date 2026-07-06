"""Lazy, memoized inter-procedural value resolution for one parsed module.

:class:`CallResolutionEngine` is a pure oracle the extraction visitor (and
this module's own environment replay) consults for two questions the
lexical scope stack cannot answer:

- *What value does this function's parameter carry?* — the union of the
  argument expressions across every bare-name call site of the function
  (plus the YAML seed for the entry function, plus signature defaults).
- *What value does this bare-name call return?* — the union of the callee's
  ``return`` expressions, each resolved in the callee's own environment.

It is NOT a global fixpoint: every answer is computed lazily on first ask
and memoized. Cycles (recursion), excessive call depth and value-set blowups
all degrade to ``None`` ("not statically known") — conservative in the
direction that keeps the LHP-DEP-002 advisory alive rather than silently
mis-resolving. The engine never touches tables or warnings.

Environments replay statement-level bindings through the SAME
:func:`~lhp.core.dependencies._bound_folding.binding_updates` helper the
live visitor uses, so binding semantics exist exactly once. Environments
are flow-insensitive (a whole scope folds to one binding map, rebindings
merging via :func:`~lhp.core.dependencies._bindings.merge_bound`) — a
superset of what the visitor sees mid-walk, consistent with the
union-over-binding philosophy.
"""

import ast
from typing import Dict, List, Optional, Set, Tuple

from ._bindings import Bound, DictValue, ParameterBindings, merge_bound
from ._bound_folding import binding_updates, resolve_static_bound, union_bounds
from ._function_index import CallSite, FunctionIndex, FunctionInfo, FunctionNode
from ._static_resolution import CallResolver, NameResolver

#: Re-entry depth across resolve_param / resolve_call_return past which
#: resolution degrades to "not statically known".
MAX_CALL_DEPTH = 20

#: Value-set cardinality cap: a union past this size degrades to "not
#: statically known" instead of expanding further.
MAX_VALUE_SET_SIZE = 256

_SCOPE_BARRIERS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def compute_seed_bindings(
    tree: ast.Module,
    bindings: Optional[ParameterBindings],
) -> Tuple[Optional[ast.FunctionDef], Dict[str, Bound]]:
    """Locate the YAML binding target function and its seeded parameter values.

    Mirrors codegen's lookup (``_find_function_node`` in
    ``generators/write/snapshot_cdc_source_function.py``): only direct
    children of ``tree.body``, plain ``ast.FunctionDef`` only, FIRST match
    wins. Kwonly style: each entry binds to the function's matching
    keyword-only argument name; leftover entries bind as one ``DictValue``
    to the ``**kwargs`` name when the function has one, else they stay
    silently unbound. Dict style: the positional parameter at
    ``dict_arg_index`` (within posonly + args) is bound to the whole
    parameters dict; an out-of-range index is a signature mismatch → NO
    binding at all (locked design L2 — never guess).

    Shared by the live extraction visitor (scope seeding) and the engine
    (parameter resolution for the seeded entry function) so seed semantics
    exist exactly once.
    """
    if bindings is None:
        return None, {}
    target = next(
        (
            child
            for child in tree.body
            if isinstance(child, ast.FunctionDef)
            and child.name == bindings.function_name
        ),
        None,
    )
    if target is None:
        return None, {}

    seeded: Dict[str, Bound] = {}
    if bindings.kwonly is not None:
        kwonly_names = {arg.arg for arg in target.args.kwonlyargs}
        leftovers: Dict[str, Bound] = {}
        for key, bound in bindings.kwonly.entries.items():
            if key in kwonly_names:
                seeded[key] = bound
            else:
                leftovers[key] = bound
        if leftovers and target.args.kwarg is not None:
            seeded[target.args.kwarg.arg] = DictValue(leftovers)
        return target, seeded

    if bindings.dict_arg_index is None or bindings.dict_value is None:
        return target, seeded  # Unreachable per the ParameterBindings invariant.
    positional = [*target.args.posonlyargs, *target.args.args]
    if 0 <= bindings.dict_arg_index < len(positional):
        seeded[positional[bindings.dict_arg_index].arg] = bindings.dict_value
    return target, seeded


class CallResolutionEngine:
    """Memoized parameter/return oracle over one module's function index.

    Construct once per (tree, YAML bindings) pair; the
    :class:`~lhp.core.dependencies._function_index.FunctionIndex` itself is
    bindings-independent and may be shared/cached across engines.
    """

    def __init__(
        self,
        tree: ast.Module,
        index: FunctionIndex,
        *,
        seed_target: Optional[ast.FunctionDef] = None,
        seed_bindings: Optional[Dict[str, Bound]] = None,
    ) -> None:
        self._tree = tree
        self._index = index
        self._seed_target = seed_target
        self._seed_bindings: Dict[str, Bound] = dict(seed_bindings or {})
        self._module_env: Optional[Dict[str, Bound]] = None
        self._module_env_partial: Optional[Dict[str, Bound]] = None
        self._function_envs: Dict[FunctionNode, Dict[str, Bound]] = {}
        self._envs_in_progress: Dict[FunctionNode, Dict[str, Bound]] = {}
        self._param_memo: Dict[Tuple[FunctionNode, str], Optional[Bound]] = {}
        self._params_in_progress: Set[Tuple[FunctionNode, str]] = set()
        self._return_memo: Dict[FunctionNode, Optional[Bound]] = {}
        self._returns_in_progress: Set[FunctionNode] = set()
        self._depth = 0
        # Every cycle/depth guard hit bumps this counter. A result computed
        # while ANY guard fired is conservative-but-degraded: it is returned
        # (degradation only ever moves toward "unresolved") but NOT memoized,
        # so a later clean recomputation can do better.
        self._guard_hits = 0

    # ---- Public oracle surface ----

    def resolve_param(self, func: FunctionNode, name: str) -> Optional[Bound]:
        """The value ``func``'s parameter ``name`` could carry, or ``None``.

        Union across every bare-name call site's argument for that
        parameter (evaluated in the CALLER's environment), the YAML seed
        when ``func`` is the seeded entry function, and the signature
        default for call sites that omit it. Any single unmappable or
        unresolvable contribution poisons the whole answer — a dynamic call
        site could pass anything.
        """
        key = (func, name)
        if key in self._param_memo:
            return self._param_memo[key]
        if key in self._params_in_progress or self._depth >= MAX_CALL_DEPTH:
            self._guard_hits += 1
            return None
        self._params_in_progress.add(key)
        self._depth += 1
        snapshot = self._guard_hits
        try:
            result = self._capped(self._compute_param(func, name))
        finally:
            self._depth -= 1
            self._params_in_progress.discard(key)
        if self._guard_hits == snapshot:
            self._param_memo[key] = result
        return result

    def resolve_call_return(
        self,
        call: ast.Call,
        caller_chain: Tuple[FunctionNode, ...],
    ) -> Optional[Bound]:
        """The value a bare-name ``call`` from ``caller_chain`` could return.

        The callee resolves lexically (innermost-wins) through the function
        index; its ``return`` expressions fold in the callee's own
        environment and union. Bare ``return`` / ``return None`` branches
        are skipped, but any OTHER unresolvable return poisons the whole
        answer (a dynamic branch could return any table name).
        """
        if not isinstance(call.func, ast.Name):
            return None
        info = self._index.resolve_callee(call.func.id, caller_chain)
        if info is None:
            return None
        func = info.node
        if func in self._return_memo:
            return self._return_memo[func]
        if func in self._returns_in_progress or self._depth >= MAX_CALL_DEPTH:
            self._guard_hits += 1
            return None
        self._returns_in_progress.add(func)
        self._depth += 1
        snapshot = self._guard_hits
        try:
            result = self._capped(self._fold_returns(info))
        finally:
            self._depth -= 1
            self._returns_in_progress.discard(func)
        if self._guard_hits == snapshot:
            self._return_memo[func] = result
        return result

    def call_resolver_for(self, chain: Tuple[FunctionNode, ...]) -> "CallResolver":
        """A :data:`CallResolver` evaluating calls made from inside ``chain``."""
        return lambda call: self.resolve_call_return(call, chain)

    # ---- Parameter resolution ----

    def _compute_param(self, func: FunctionNode, name: str) -> Optional[Bound]:
        info = self._index.info_for(func)
        if info is None:
            return None
        if name not in info.positional_params and name not in info.kwonly_params:
            return None

        contributions: List[Bound] = []
        if func is self._seed_target and name in self._seed_bindings:
            contributions.append(self._seed_bindings[name])

        sites = self._index.call_sites_of(func)
        for site in sites:
            bound = self._param_value_at_site(info, name, site)
            if bound is None:
                return None
            contributions.append(bound)

        if not contributions:
            # Never called and not seeded: the only static knowledge left
            # is a signature default.
            return self._default_value(info, name)

        combined: Optional[Bound] = contributions[0]
        for bound in contributions[1:]:
            combined = union_bounds(combined, bound)
            if combined is None:
                return None
        return combined

    def _param_value_at_site(
        self, info: FunctionInfo, name: str, site: CallSite
    ) -> Optional[Bound]:
        """The bound ``name`` receives from one call site, or ``None``.

        ``*args`` / ``**kwargs`` at the call site defeat static mapping for
        every parameter. A parameter the site does not pass falls back to
        its signature default (resolved in the DEFINING environment).
        """
        call = site.node
        if any(isinstance(a, ast.Starred) for a in call.args) or any(
            kw.arg is None for kw in call.keywords
        ):
            return None

        expr: Optional[ast.expr] = None
        if name in info.positional_params:
            index = info.positional_params.index(name)
            if index < len(call.args):
                expr = call.args[index]
        if expr is None:
            expr = next((kw.value for kw in call.keywords if kw.arg == name), None)
        if expr is None:
            return self._default_value(info, name)

        return resolve_static_bound(
            expr,
            self._name_resolver_for(site.scope_chain),
            call_resolver=self.call_resolver_for(site.scope_chain),
        )

    def _default_value(self, info: FunctionInfo, name: str) -> Optional[Bound]:
        default = info.defaults.get(name)
        if default is None:
            return None
        # Defaults evaluate at def time, in the defining scope.
        return resolve_static_bound(
            default,
            self._name_resolver_for(info.enclosing),
            call_resolver=self.call_resolver_for(info.enclosing),
        )

    # ---- Return folding ----

    def _fold_returns(self, info: FunctionInfo) -> Optional[Bound]:
        body_chain = (*info.enclosing, info.node)
        name_resolver = self._name_resolver_for(body_chain)
        call_resolver = self.call_resolver_for(body_chain)

        combined: Optional[Bound] = None
        saw_value = False
        for expr in info.return_exprs:
            if expr is None or (isinstance(expr, ast.Constant) and expr.value is None):
                continue
            bound = resolve_static_bound(
                expr, name_resolver, call_resolver=call_resolver
            )
            if bound is None:
                return None
            saw_value = True
            combined = bound if combined is None else union_bounds(combined, bound)
            if combined is None:
                return None
        return combined if saw_value else None

    # ---- Environments ----

    def _name_resolver_for(self, chain: Tuple[FunctionNode, ...]) -> "NameResolver":
        """A :data:`NameResolver` over ``chain``'s lexical environment.

        Walks innermost → outermost: each function's replayed local
        bindings, then its parameters (through :meth:`resolve_param`), then
        the module environment — mirroring the live visitor's scope walk
        plus its parameter fallback.
        """

        def resolve(name: str) -> Optional[Bound]:
            for func in reversed(chain):
                env = self._env_of(func)
                if name in env:
                    return env[name]
                info = self._index.info_for(func)
                if info is not None and (
                    name in info.positional_params or name in info.kwonly_params
                ):
                    param = self.resolve_param(func, name)
                    if param is not None:
                        return param
            return self._module_environment().get(name)

        return resolve

    def _env_of(self, func: FunctionNode) -> Dict[str, Bound]:
        """The replayed (memoized) local binding map of one function body.

        Re-entry during construction observes the partial map built so far
        — a lookup miss there degrades resolution toward "unknown", which
        keeps advisories alive instead of mis-resolving — and taints the
        build, so the degraded map is not memoized.
        """
        if func in self._function_envs:
            return self._function_envs[func]
        if func in self._envs_in_progress:
            self._guard_hits += 1
            return self._envs_in_progress[func]
        env: Dict[str, Bound] = (
            dict(self._seed_bindings) if func is self._seed_target else {}
        )
        self._envs_in_progress[func] = env
        snapshot = self._guard_hits
        try:
            info = self._index.info_for(func)
            enclosing = info.enclosing if info is not None else ()
            self._replay_scope(func.body, env, (*enclosing, func))
            if self._guard_hits == snapshot:
                self._function_envs[func] = env
            return env
        finally:
            del self._envs_in_progress[func]

    def _module_environment(self) -> Dict[str, Bound]:
        if self._module_env is not None:
            return self._module_env
        if self._module_env_partial is not None:
            self._guard_hits += 1
            return self._module_env_partial
        env: Dict[str, Bound] = {}
        self._module_env_partial = env
        snapshot = self._guard_hits
        try:
            self._replay_scope(self._tree.body, env, ())
            if self._guard_hits == snapshot:
                self._module_env = env
            return env
        finally:
            self._module_env_partial = None

    def _replay_scope(
        self,
        body: List[ast.stmt],
        env: Dict[str, Bound],
        chain: Tuple[FunctionNode, ...],
    ) -> None:
        """Fold one scope's statements into ``env`` (single binding replay).

        Reads during the replay see the environment built so far (source
        order) before falling back to parameters / enclosing scopes, exactly
        like the live visitor's accumulating walk.
        """

        outer_resolver = self._name_resolver_for(chain[:-1]) if chain else None

        def resolve(name: str) -> Optional[Bound]:
            if name in env:
                return env[name]
            if chain:
                func = chain[-1]
                info = self._index.info_for(func)
                if info is not None and (
                    name in info.positional_params or name in info.kwonly_params
                ):
                    param = self.resolve_param(func, name)
                    if param is not None:
                        return param
                assert outer_resolver is not None
                return outer_resolver(name)
            return None

        call_resolver = self.call_resolver_for(chain)
        for stmt in _iter_scope_statements(body):
            for name, bound in binding_updates(
                stmt, resolve, call_resolver=call_resolver
            ):
                env[name] = merge_bound(env.get(name), bound)

    # ---- Guards ----

    @staticmethod
    def _capped(bound: Optional[Bound]) -> Optional[Bound]:
        if isinstance(bound, frozenset) and len(bound) > MAX_VALUE_SET_SIZE:
            return None
        return bound


def _iter_scope_statements(body: List[ast.stmt]):
    """Yield a scope's statements recursively, source order, without
    descending into nested function/class bodies (those are other scopes)."""
    for stmt in body:
        if isinstance(stmt, _SCOPE_BARRIERS):
            continue
        yield stmt
        nested: List[ast.stmt] = []
        for child in ast.iter_child_nodes(stmt):
            if isinstance(child, ast.stmt):
                nested.append(child)
            elif isinstance(child, (ast.excepthandler, ast.match_case)):
                nested.extend(child.body)
        yield from _iter_scope_statements(nested)
