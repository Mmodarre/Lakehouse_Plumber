"""Static (AST-level) string-value resolution for the Python table parser.

These helpers answer a single question: *what concrete string value(s) could
this AST node be, reasoning only from what is literally visible in the
source?* They never speculate past static visibility — any dynamic operand
collapses the whole expression to "unresolved" (an empty set).

Used by :mod:`lhp.core.dependencies.python_parser` to resolve both the
right-hand side of variable assignments and the string argument of recognized
Spark read calls (``spark.read.format("delta").table(name)``).
"""

import ast
from typing import Callable, FrozenSet, List, Optional

# F-string interpolation names that map to known LHP substitution tokens. These
# are preserved as ``{name}`` placeholders; any other interpolation collapses to
# the generic ``{var}`` marker.
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

NameResolver = Optional[Callable[[str], FrozenSet[str]]]


def resolve_static_string_values(
    node: ast.expr,
    name_resolver: NameResolver = None,
) -> FrozenSet[str]:
    """Resolve ``node`` to the set of string values it could statically be.

    Recognized forms (everything else yields ``frozenset()`` — the parser never
    speculates past what is literally visible):

      - ``"literal"`` — :class:`ast.Constant` with a ``str`` value.
      - ``f"..."`` — :class:`ast.JoinedStr`, rendered via :func:`render_f_string`
        (known schema/catalog placeholders preserved).
      - ``a + b`` — :class:`ast.BinOp` with :class:`ast.Add`, string
        concatenation. Each side is resolved recursively and the cartesian
        product is concatenated. If either side is unresolvable, the whole
        expression is left unresolved.
      - ``"{}.{}".format(a, b)`` — a ``.format()`` call whose receiver is a
        constant string with positional ``{}`` fields and whose arguments all
        resolve to a single literal each.
      - an ``ast.Name`` previously bound in scope — resolved through
        ``name_resolver`` when supplied.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return frozenset({node.value})

    if isinstance(node, ast.JoinedStr):
        rendered = render_f_string(node)
        return frozenset({rendered}) if rendered else frozenset()

    if isinstance(node, ast.Name):
        return name_resolver(node.id) if name_resolver is not None else frozenset()

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = resolve_static_string_values(node.left, name_resolver)
        right = resolve_static_string_values(node.right, name_resolver)
        if not left or not right:
            return frozenset()
        return frozenset({a + b for a in left for b in right})

    if isinstance(node, ast.Call):
        return _resolve_format_method_call(node, name_resolver)

    return frozenset()


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


def render_f_string(node: ast.JoinedStr) -> str:
    """Render an f-string as a template.

    Known schema/catalog interpolation names are substituted with ``{name}``;
    any other interpolation collapses to the generic ``{var}`` marker.
    """
    parts: List[str] = []

    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(value.value)
        elif isinstance(value, ast.FormattedValue):
            if (
                isinstance(value.value, ast.Name)
                and value.value.id in _KNOWN_PLACEHOLDER_NAMES
            ):
                parts.append(f"{{{value.value.id}}}")
            else:
                parts.append("{var}")

    return "".join(parts)
