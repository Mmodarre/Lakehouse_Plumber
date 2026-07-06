"""Structured bound folding for the Python table parser.

Sibling of :mod:`lhp.core.dependencies._static_resolution` (which owns
STRING-context resolution): this module folds AST expressions to structured
:data:`~lhp.core.dependencies._bindings.Bound` values — dict / list
literals, ``or`` / ``and`` chains (union over operand value sets), an
allowlisted set of builtin collection calls (identity on the element value
set), and the single statement-level binding replay shared by the live
extraction visitor and the call-resolution engine (ONE copy of binding
semantics, per §3.1).

The two modules are mutually recursive by nature; this one holds the
module-level import (`_static_resolution` defers its few upward references).

Non-speculation and token byte fidelity rules are identical to
:mod:`._static_resolution`: any dynamic operand collapses the fold to
``None``, and ``${token}`` substrings pass through as exact bytes.
"""

import ast
from typing import Dict, FrozenSet, List, Optional, Tuple

from ._bindings import Bound, DictValue, ListValue
from ._static_resolution import (
    CallResolver,
    NameResolver,
    _resolve_bound,
    resolve_static_string_values,
)

#: Builtin collection constructors folded as identity on the element value
#: set: the call yields a collection whose ELEMENTS are exactly the
#: argument's iterated values. Order is deliberately NOT claimed — the fold
#: produces a string set, never a ``ListValue``.
_BARE_BUILTIN_FOLDS: FrozenSet[str] = frozenset({"list", "tuple", "sorted", "set"})


def resolve_static_list(
    node: ast.expr,
    name_resolver: NameResolver = None,
    *,
    call_resolver: CallResolver = None,
) -> Optional[ListValue]:
    """Resolve ``node`` to an ordered list of statically-known strings.

    Recognized forms (anything else yields ``None``):

      - ``["a", x]`` / ``("a", x)`` — list/tuple literals whose elements each
        statically resolve to exactly ONE string. A multi-valued element makes
        the WHOLE list unresolved (``None``) rather than expanding into a
        cartesian set of candidate lists.
      - an ``ast.Name`` bound (via ``name_resolver``) to a
        :class:`~lhp.core.dependencies._bindings.ListValue`.
      - subscript / ``.get`` / user-function-call lookups (``params["cols"]``,
        ``params.get("cols")``, ``_cols()``) whose resolved bound is a
        ``ListValue``.
    """
    if isinstance(node, (ast.List, ast.Tuple)):
        items: List[str] = []
        for element in node.elts:
            values = resolve_static_string_values(
                element, name_resolver, call_resolver=call_resolver
            )
            if len(values) != 1:
                return None
            items.append(next(iter(values)))
        return ListValue(tuple(items))

    bound = _resolve_bound(node, name_resolver, call_resolver=call_resolver)
    return bound if isinstance(bound, ListValue) else None


def resolve_static_dict(
    node: ast.expr,
    name_resolver: NameResolver = None,
    *,
    call_resolver: CallResolver = None,
) -> Optional[DictValue]:
    """Resolve ``node`` to a string-keyed mapping of bound values, or ``None``.

    Recognized forms (anything else yields ``None``):

      - ``{"k": <expr>, ...}`` — a dict literal. Keys must be constant strings;
        each value resolves via :func:`resolve_static_bound`. Mirroring
        :func:`~lhp.core.dependencies._bindings.bound_from_yaml`: an entry
        whose value is unbindable — or whose key is not a constant string, and
        any ``**spread`` — is DROPPED; the remaining entries still bind.
      - an ``ast.Name`` / subscript / ``.get`` / user-function-call chain
        bound to a :class:`~lhp.core.dependencies._bindings.DictValue`.
    """
    if isinstance(node, ast.Dict):
        entries: Dict[str, Bound] = {}
        for key_node, value_node in zip(node.keys, node.values, strict=False):
            if not (
                isinstance(key_node, ast.Constant) and isinstance(key_node.value, str)
            ):
                continue
            bound = resolve_static_bound(
                value_node, name_resolver, call_resolver=call_resolver
            )
            if bound is not None:
                entries[key_node.value] = bound
        return DictValue(entries)

    bound = _resolve_bound(node, name_resolver, call_resolver=call_resolver)
    return bound if isinstance(bound, DictValue) else None


def resolve_static_bound(
    node: ast.expr,
    name_resolver: NameResolver = None,
    *,
    call_resolver: CallResolver = None,
) -> Optional[Bound]:
    """Resolve an assignment RHS to a :data:`Bound` of any shape, or ``None``.

    Mirrors :func:`~lhp.core.dependencies._bindings.bound_from_yaml` over the
    AST so a Python-side ``X = {...}`` / ``X = [...]`` binds the same shape a
    YAML-seeded parameter would: a dict literal → :class:`DictValue`, a
    list/tuple literal of single-valued string elements → :class:`ListValue`
    (all-or-nothing), a ``BoolOp`` → the union over its operand bounds
    (:func:`resolve_boolop`), an allowlisted builtin collection call → its
    element value set (:func:`fold_builtin_call`), and any statically-known
    string expression → a string set. A name / subscript / ``.get`` / call
    chain resolves to whatever bound it already carries. Returns ``None``
    when nothing static is visible.
    """
    if isinstance(node, ast.Dict):
        return resolve_static_dict(node, name_resolver, call_resolver=call_resolver)
    if isinstance(node, (ast.List, ast.Tuple)):
        return resolve_static_list(node, name_resolver, call_resolver=call_resolver)
    if isinstance(node, ast.BoolOp):
        return resolve_boolop(node, name_resolver, call_resolver=call_resolver)
    if isinstance(node, ast.Call):
        folded = fold_builtin_call(node, name_resolver, call_resolver=call_resolver)
        if folded:
            return folded
    strings = resolve_static_string_values(
        node, name_resolver, call_resolver=call_resolver
    )
    if strings:
        return strings
    return _resolve_bound(node, name_resolver, call_resolver=call_resolver)


def resolve_boolop(
    node: ast.BoolOp,
    name_resolver: NameResolver = None,
    *,
    call_resolver: CallResolver = None,
) -> Optional[Bound]:
    """Fold ``a or b`` / ``a and b`` to the union over its operand bounds.

    Which operand a BoolOp yields depends on runtime truthiness, so the
    static value set is the UNION of the operands' sets — and ANY
    unresolvable operand poisons the whole expression (a dynamic branch
    could carry any value). The common ``parameters or {}`` therefore folds
    to the parameters :class:`DictValue` (entry-union with the empty dict).
    Operands of mismatched shapes (a string set vs a dict) do not union and
    poison the fold.
    """
    combined: Optional[Bound] = None
    for operand in node.values:
        bound = resolve_static_bound(
            operand, name_resolver, call_resolver=call_resolver
        )
        if bound is None:
            return None
        combined = bound if combined is None else union_bounds(combined, bound)
        if combined is None:
            return None
    return combined


def fold_builtin_call(
    node: ast.Call,
    name_resolver: NameResolver = None,
    *,
    call_resolver: CallResolver = None,
) -> Optional[FrozenSet[str]]:
    """Fold an allowlisted builtin collection call to its element value set.

    Handles ``list(x)`` / ``tuple(x)`` / ``sorted(x)`` / ``set(x)`` (exactly
    one positional argument, no keywords) and ``dict.fromkeys(x[, v])`` —
    each returns a collection whose elements are exactly ``x``'s iterated
    values (a dict iterates its keys), so the fold is identity on the value
    set. Order is never claimed. Anything outside the allowlist, or with
    ``*args`` / keywords, is not folded (``None``).
    """
    if any(isinstance(a, ast.Starred) for a in node.args):
        return None

    func = node.func
    if isinstance(func, ast.Name) and func.id in _BARE_BUILTIN_FOLDS:
        if node.keywords or len(node.args) != 1:
            return None
        return _iterated_value_set(node.args[0], name_resolver, call_resolver) or None

    if (
        isinstance(func, ast.Attribute)
        and func.attr == "fromkeys"
        and isinstance(func.value, ast.Name)
        and func.value.id == "dict"
    ):
        if node.keywords or len(node.args) not in (1, 2):
            return None
        return _iterated_value_set(node.args[0], name_resolver, call_resolver) or None

    return None


def value_set_of(bound: Optional[Bound]) -> FrozenSet[str]:
    """The element value set a bound contributes when iterated.

    A :class:`ListValue` yields its items, a :class:`DictValue` its keys
    (Python dict iteration), and a string set passes through unchanged —
    the caller decides whether treating a string-set bound as an iterable
    is appropriate for its context. ``None`` yields the empty set.
    """
    if isinstance(bound, frozenset):
        return bound
    if isinstance(bound, ListValue):
        return frozenset(bound.items)
    if isinstance(bound, DictValue):
        return frozenset(bound.entries.keys())
    return frozenset()


def binding_updates(
    stmt: ast.stmt,
    name_resolver: NameResolver = None,
    *,
    call_resolver: CallResolver = None,
) -> List[Tuple[str, Bound]]:
    """The name bindings one statement introduces, in source order.

    This is the SINGLE statement-level binding replay shared by the live
    extraction visitor and the call-resolution engine's environment builder
    — the two must agree on binding semantics, so the dispatch lives once:

      - ``a = <rhs>`` / ``a = b = <rhs>`` — every plain ``Name`` target
        binds the RHS's folded bound. Tuple/list unpacking with a parallel
        literal RHS binds element-wise; any other target shape (attributes,
        subscripts, unmatched unpacking) is intentionally dropped.
      - ``a: T = <rhs>`` — same, annotation-only statements bind nothing.
      - ``for t in <iterable>:`` — a plain ``Name`` loop target binds the
        union of the iterable's folded element values (static loop
        unrolling; a dict-bound iterable contributes its keys).

    Callers apply the returned pairs to their scope with
    :func:`~lhp.core.dependencies._bindings.merge_bound` semantics.
    """
    updates: List[Tuple[str, Bound]] = []

    if isinstance(stmt, ast.Assign):
        rhs_bound = resolve_static_bound(
            stmt.value, name_resolver, call_resolver=call_resolver
        )
        for target in stmt.targets:
            if (
                isinstance(target, (ast.Tuple, ast.List))
                and isinstance(stmt.value, (ast.Tuple, ast.List))
                and len(target.elts) == len(stmt.value.elts)
            ):
                for sub_target, sub_value in zip(
                    target.elts, stmt.value.elts, strict=False
                ):
                    sub_bound = resolve_static_bound(
                        sub_value, name_resolver, call_resolver=call_resolver
                    )
                    if sub_bound and isinstance(sub_target, ast.Name):
                        updates.append((sub_target.id, sub_bound))
            elif rhs_bound and isinstance(target, ast.Name):
                updates.append((target.id, rhs_bound))
        return updates

    if isinstance(stmt, ast.AnnAssign):
        if stmt.value is None or not isinstance(stmt.target, ast.Name):
            return updates
        bound = resolve_static_bound(
            stmt.value, name_resolver, call_resolver=call_resolver
        )
        if bound:
            updates.append((stmt.target.id, bound))
        return updates

    if isinstance(stmt, ast.For):
        if not isinstance(stmt.target, ast.Name):
            return updates
        iterable = resolve_static_bound(
            stmt.iter, name_resolver, call_resolver=call_resolver
        )
        values = value_set_of(iterable)
        if values:
            updates.append((stmt.target.id, values))
        return updates

    return updates


def union_bounds(a: Bound, b: Bound) -> Optional[Bound]:
    """Union two bounds of the same shape; mismatched shapes poison (None).

    String sets union directly. Dicts union entry-wise: keys on one side
    only pass through, shared keys union recursively (a shape conflict
    drops just that entry, leaving lookups of it unresolved). Equal
    ``ListValue``s pass through; unequal ones fold to their combined value
    set (order can no longer be claimed).
    """
    if isinstance(a, frozenset) and isinstance(b, frozenset):
        return a | b
    if isinstance(a, DictValue) and isinstance(b, DictValue):
        entries: Dict[str, Bound] = dict(a.entries)
        for key, bound in b.entries.items():
            if key not in entries:
                entries[key] = bound
                continue
            merged = union_bounds(entries[key], bound)
            if merged is None:
                del entries[key]
            else:
                entries[key] = merged
        return DictValue(entries)
    if isinstance(a, ListValue) and isinstance(b, ListValue):
        if a == b:
            return a
        return frozenset(a.items) | frozenset(b.items)
    return None


def _iterated_value_set(
    node: ast.expr,
    name_resolver: NameResolver,
    call_resolver: CallResolver,
) -> FrozenSet[str]:
    bound = resolve_static_bound(node, name_resolver, call_resolver=call_resolver)
    return value_set_of(bound)
