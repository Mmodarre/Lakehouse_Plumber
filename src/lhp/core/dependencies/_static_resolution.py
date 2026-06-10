"""Static (AST-level) string-value resolution for the Python table parser.

These helpers answer a single question: *what concrete string value(s) could
this AST node be, reasoning only from what is literally visible in the
source?* They never speculate past static visibility — any dynamic operand
collapses the whole expression to "unresolved" (an empty set / ``None``).

Resolution is *binding-aware*: an optional ``name_resolver`` callback maps a
variable name to the :data:`~lhp.core.dependencies._bindings.Bound` value it
carries in scope — a set of possible strings, an ordered string list
(:class:`~lhp.core.dependencies._bindings.ListValue`), or a string-keyed
mapping (:class:`~lhp.core.dependencies._bindings.DictValue`). On top of
those bindings the helpers understand subscripts (``params["tbl"]``),
``.get("k"[, default])`` lookups, common string methods (``.replace``,
``.upper`` / ``.lower``, the ``.strip`` family, ``sep.join(...)``) and
resolver-aware f-strings.

Token byte fidelity is a hard invariant: ``${token}`` substrings inside bound
values are NEVER resolved or altered here — they flow through every
resolution path as exact bytes.

Used by :mod:`lhp.core.dependencies.python_parser` to resolve both the
right-hand side of variable assignments and the string argument of recognized
Spark read calls (``spark.read.format("delta").table(name)``).
"""

import ast
from itertools import product
from typing import Callable, FrozenSet, List, Optional

from ._bindings import Bound, DictValue, ListValue

# F-string interpolation names that map to known LHP substitution tokens.
# When such a name does not resolve to a bound value, the literal ``{name}``
# placeholder text is preserved (legacy ``{token}`` spelling support). Any
# other unresolved interpolation leaves the whole f-string unresolved.
_KNOWN_PLACEHOLDER_NAMES: FrozenSet[str] = frozenset(
    {
        "catalog",
        "schema",
        "table",
        "bronze_schema",
        "silver_schema",
        "gold_schema",
        "migration_schema",
        "old_schema",
    }
)

#: Looks up the value bound to a variable name in the enclosing scope.
#: Returning ``None`` (or an empty set) means "not statically known".
NameResolver = Optional[Callable[[str], Optional[Bound]]]


def resolve_static_string_values(
    node: ast.expr,
    name_resolver: NameResolver = None,
) -> FrozenSet[str]:
    """Resolve ``node`` to the set of string values it could statically be.

    Recognized forms (everything else yields ``frozenset()`` — the parser never
    speculates past what is literally visible):

      - ``"literal"`` — :class:`ast.Constant` with a ``str`` value.
      - ``f"..."`` — :class:`ast.JoinedStr`, resolved via
        :func:`render_f_string` (resolver-aware; known LHP placeholder names
        preserved as ``{name}`` when unresolved).
      - ``a + b`` — :class:`ast.BinOp` with :class:`ast.Add`, string
        concatenation. Each side is resolved recursively and the cartesian
        product is concatenated. If either side is unresolvable, the whole
        expression is left unresolved.
      - ``"{}.{}".format(a, b)`` — a ``.format()`` call whose receiver is a
        constant string with positional ``{}`` fields and whose arguments all
        resolve to a single literal each.
      - an ``ast.Name`` previously bound in scope — resolved through
        ``name_resolver`` when supplied. Only a string-set bound resolves in
        string context; structured bounds (lists / dicts) do not.
      - ``name["key"]`` / ``name.get("key"[, default])`` — lookups into a
        dict-bound name whose entry is itself a string-set bound.
      - string methods on statically-resolved receivers: ``.replace(a, b)``,
        ``.upper()`` / ``.lower()``, ``.strip()`` / ``.lstrip()`` /
        ``.rstrip()`` (optional chars argument), and ``sep.join(items)``
        where ``items`` resolves via :func:`resolve_static_list`.

    Multi-valued operands combine via cartesian product throughout. Byte
    fidelity: ``${token}`` substrings in resolved values pass through as
    exact bytes.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return frozenset({node.value})

    if isinstance(node, ast.JoinedStr):
        return render_f_string(node, name_resolver)

    if isinstance(node, (ast.Name, ast.Subscript)):
        return _strings_from_bound(_resolve_bound(node, name_resolver))

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = resolve_static_string_values(node.left, name_resolver)
        right = resolve_static_string_values(node.right, name_resolver)
        if not left or not right:
            return frozenset()
        return frozenset({a + b for a in left for b in right})

    if isinstance(node, ast.Call):
        return _resolve_method_call(node, name_resolver)

    return frozenset()


def resolve_static_list(
    node: ast.expr,
    name_resolver: NameResolver = None,
) -> Optional[ListValue]:
    """Resolve ``node`` to an ordered list of statically-known strings.

    Recognized forms (anything else yields ``None``):

      - ``["a", x]`` / ``("a", x)`` — list/tuple literals whose elements each
        statically resolve to exactly ONE string. A multi-valued element makes
        the WHOLE list unresolved (``None``) rather than expanding into a
        cartesian set of candidate lists.
      - an ``ast.Name`` bound (via ``name_resolver``) to a
        :class:`~lhp.core.dependencies._bindings.ListValue`.
      - subscript / ``.get`` lookups (``params["cols"]``,
        ``params.get("cols")``) whose resolved bound is a ``ListValue``.
    """
    if isinstance(node, (ast.List, ast.Tuple)):
        items: List[str] = []
        for element in node.elts:
            values = resolve_static_string_values(element, name_resolver)
            if len(values) != 1:
                return None
            items.append(next(iter(values)))
        return ListValue(tuple(items))

    bound = _resolve_bound(node, name_resolver)
    return bound if isinstance(bound, ListValue) else None


def render_f_string(
    node: ast.JoinedStr,
    name_resolver: NameResolver = None,
) -> FrozenSet[str]:
    """Resolve an f-string to the set of strings it could statically render.

    Each interpolated expression is resolved through the full static
    machinery (resolver bindings, subscripts, ``.get``, string methods);
    multi-valued interpolations expand via cartesian product across parts. A
    plain ``ast.Name`` interpolation that does NOT resolve but matches a
    known LHP substitution-token name is preserved literally as ``{name}``
    (legacy ``{token}`` spelling support). Any other unresolved interpolation
    — or any conversion specifier / format spec (``!r``, ``:>10``) — leaves
    the WHOLE f-string unresolved (``frozenset()``).
    """
    part_choices: List[FrozenSet[str]] = []

    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            part_choices.append(frozenset({value.value}))
            continue
        if not isinstance(value, ast.FormattedValue):
            return frozenset()
        if value.conversion != -1 or value.format_spec is not None:
            return frozenset()
        resolved = resolve_static_string_values(value.value, name_resolver)
        if resolved:
            part_choices.append(resolved)
        elif (
            isinstance(value.value, ast.Name)
            and value.value.id in _KNOWN_PLACEHOLDER_NAMES
        ):
            part_choices.append(frozenset({f"{{{value.value.id}}}"}))
        else:
            return frozenset()

    return frozenset("".join(combo) for combo in product(*part_choices))


def _strings_from_bound(bound: Optional[Bound]) -> FrozenSet[str]:
    """String-context view of a bound value.

    Only the string-set form resolves in string context; a structured bound
    (:class:`~lhp.core.dependencies._bindings.ListValue` /
    :class:`~lhp.core.dependencies._bindings.DictValue`) — or no bound at all
    — is "not a string here" and yields ``frozenset()``.
    """
    if isinstance(bound, frozenset):
        return bound
    return frozenset()


def _resolve_bound(
    node: ast.expr,
    name_resolver: NameResolver = None,
) -> Optional[Bound]:
    """Resolve a name / subscript / ``.get`` chain to its bound value.

    - ``name`` — looked up through ``name_resolver``.
    - ``base["key"]`` — ``base`` must resolve to a
      :class:`~lhp.core.dependencies._bindings.DictValue` and the slice must
      be a constant string key present in its entries.
    - ``base.get(...)`` — see :func:`_resolve_get_call`.

    Anything else (missing key, non-constant slice, non-dict base) is
    unresolved (``None``).
    """
    if isinstance(node, ast.Name):
        return name_resolver(node.id) if name_resolver is not None else None

    if isinstance(node, ast.Subscript):
        base = _resolve_bound(node.value, name_resolver)
        if not isinstance(base, DictValue):
            return None
        key = node.slice
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            return base.entries.get(key.value)
        return None

    if isinstance(node, ast.Call):
        return _resolve_get_call(node, name_resolver)

    return None


def _resolve_get_call(
    node: ast.Call,
    name_resolver: NameResolver = None,
) -> Optional[Bound]:
    """Resolve ``base.get("key"[, default])`` on a dict-bound receiver.

    Key present → that entry's bound. Key absent → the default expression's
    statically-resolved bound when given, else ``None`` (a runtime ``.get``
    miss yields ``None`` — not a string). Non-constant keys, keyword
    arguments, and ``*args`` unpacking are unresolved.
    """
    func = node.func
    if not (isinstance(func, ast.Attribute) and func.attr == "get"):
        return None
    if node.keywords or any(isinstance(a, ast.Starred) for a in node.args):
        return None
    if len(node.args) not in (1, 2):
        return None

    base = _resolve_bound(func.value, name_resolver)
    if not isinstance(base, DictValue):
        return None

    key = node.args[0]
    if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
        return None
    if key.value in base.entries:
        return base.entries[key.value]
    if len(node.args) == 2:
        return _resolve_value_bound(node.args[1], name_resolver)
    return None


def _resolve_value_bound(
    node: ast.expr,
    name_resolver: NameResolver = None,
) -> Optional[Bound]:
    """Resolve an arbitrary expression to a bound value of any shape.

    Used for ``.get`` defaults: a list/tuple literal becomes a
    :class:`~lhp.core.dependencies._bindings.ListValue`, a statically-known
    string expression becomes a string set, and a name / subscript / ``.get``
    chain yields whatever bound it carries.
    """
    if isinstance(node, (ast.List, ast.Tuple)):
        return resolve_static_list(node, name_resolver)
    strings = resolve_static_string_values(node, name_resolver)
    if strings:
        return strings
    return _resolve_bound(node, name_resolver)


def _resolve_method_call(
    node: ast.Call,
    name_resolver: NameResolver = None,
) -> FrozenSet[str]:
    """Resolve a method call to its statically-known string value(s).

    Dispatches ``.format(...)``, ``.get(...)``, ``sep.join(...)`` and the
    element-wise string methods (``.replace`` / ``.upper`` / ``.lower`` /
    ``.strip`` family). Multi-valued operands combine via cartesian product.
    Keyword arguments and ``*args`` unpacking are never resolved.
    """
    if not isinstance(node.func, ast.Attribute):
        return frozenset()
    method = node.func.attr

    if method == "format":
        return _resolve_format_method_call(node, name_resolver)

    if node.keywords or any(isinstance(a, ast.Starred) for a in node.args):
        return frozenset()

    if method == "get":
        return _strings_from_bound(_resolve_get_call(node, name_resolver))

    if method == "join":
        return _resolve_join_call(node, name_resolver)

    if method == "replace":
        if len(node.args) != 2:
            return frozenset()
        receivers = resolve_static_string_values(node.func.value, name_resolver)
        olds = resolve_static_string_values(node.args[0], name_resolver)
        news = resolve_static_string_values(node.args[1], name_resolver)
        if not (receivers and olds and news):
            return frozenset()
        return frozenset(r.replace(o, n) for r in receivers for o in olds for n in news)

    if method in ("upper", "lower"):
        if node.args:
            return frozenset()
        receivers = resolve_static_string_values(node.func.value, name_resolver)
        return frozenset(getattr(r, method)() for r in receivers)

    if method in ("strip", "lstrip", "rstrip"):
        if len(node.args) > 1:
            return frozenset()
        receivers = resolve_static_string_values(node.func.value, name_resolver)
        if not receivers:
            return frozenset()
        if not node.args:
            return frozenset(getattr(r, method)() for r in receivers)
        chars = resolve_static_string_values(node.args[0], name_resolver)
        if not chars:
            return frozenset()
        return frozenset(getattr(r, method)(c) for r in receivers for c in chars)

    return frozenset()


def _resolve_join_call(
    node: ast.Call,
    name_resolver: NameResolver = None,
) -> FrozenSet[str]:
    """Resolve ``sep.join(items)`` where ``items`` is a static ordered list.

    The receiver must resolve to string value(s) (cartesian over multi-valued
    separators) and the single argument must resolve via
    :func:`resolve_static_list`.
    """
    func = node.func
    if not isinstance(func, ast.Attribute) or len(node.args) != 1:
        return frozenset()
    separators = resolve_static_string_values(func.value, name_resolver)
    if not separators:
        return frozenset()
    items = resolve_static_list(node.args[0], name_resolver)
    if items is None:
        return frozenset()
    return frozenset(sep.join(items.items) for sep in separators)


def _resolve_format_method_call(
    node: ast.Call,
    name_resolver: NameResolver = None,
) -> FrozenSet[str]:
    """Resolve a ``"...".format(...)`` call to its concrete string(s).

    Only positional substitution into a constant-string receiver is handled.
    The receiver must resolve to a single literal and every positional argument
    must resolve to exactly one value; otherwise the call is left unresolved (no
    speculation). Keyword arguments and ``*args`` / ``**kwargs`` unpacking are
    not supported.
    """
    if not (isinstance(node.func, ast.Attribute) and node.func.attr == "format"):
        return frozenset()

    if node.keywords or any(isinstance(a, ast.Starred) for a in node.args):
        return frozenset()

    receiver_values = resolve_static_string_values(node.func.value, name_resolver)
    if len(receiver_values) != 1:
        return frozenset()
    template = next(iter(receiver_values))

    arg_values: List[str] = []
    for arg in node.args:
        resolved = resolve_static_string_values(arg, name_resolver)
        if len(resolved) != 1:
            return frozenset()
        arg_values.append(next(iter(resolved)))

    try:
        return frozenset({template.format(*arg_values)})
    except (IndexError, KeyError, ValueError):
        # Mismatched / named placeholders the static args can't fill.
        return frozenset()
