"""Runtime table-name shim for sandbox rewrites of opaque Python reads.

A read whose table name is only known at runtime (fetched from a metadata
table, built by a helper, indexed by a dynamic key) cannot be rewritten
statically. The sandbox pass wraps every such recognized read-call site ::

    spark.read.table(x)  ->  spark.read.table(__lhp_sandbox_table(x))

and emits ONE ``__lhp_sandbox_table`` definition per module that has at least
one wrapped site. This module owns that helper's mechanics: the wrap/idempotency
text primitives, and rendering + placing the helper definition.

The helper's behaviour MIRRORS the static matcher exactly — canonicalization,
the 2<->3-part reconciliation with the catalog-uniqueness guard
(:func:`lhp.core.dependencies.match_produced_table`), and the leaf-only rename
(:func:`._renames.rename_parts`). Its body is a Jinja2 template
(``templates/sandbox/runtime_shim.py.j2``) per constitution §2.10 / §9.14 —
never a Python-code-as-string here; only the embedded data (the produced set,
the short-key -> catalogs map, and the pattern's literal prefix/suffix) is
computed and passed in. The rendered helper is pure stdlib, imports nothing,
and is Python 3.11-safe.
"""

from __future__ import annotations

import ast
import functools
import re

from jinja2 import Environment, PackageLoader

from ._renames import SandboxTableRenames, TableRenameStrategy

#: The single runtime shim function name. Emitted once per module and used as
#: the idempotency marker: an argument that is already a call to it is never
#: wrapped again, and a module that already defines it never gets a second copy.
HELPER_NAME = "__lhp_sandbox_table"

_TEMPLATE_NAME = "sandbox/runtime_shim.py.j2"

#: A leaf placeholder used only at generation time to split ``table_pattern``
#: into its literal prefix / suffix around the ``{table}`` hole. A NUL byte
#: never occurs in a pattern (literals are ``[A-Za-z0-9_]``) or a namespace
#: (identifier), so the split is unambiguous.
_LEAF_SENTINEL = "\x00"

_SHIM_CALL_RE = re.compile(rf"^\s*{re.escape(HELPER_NAME)}\s*\(")


def is_shim_call(segment: str) -> bool:
    """Whether ``segment`` is already a call to the shim helper (idempotency)."""
    return _SHIM_CALL_RE.match(segment) is not None


def wrap_arg(segment: str) -> str:
    """Wrap an argument's source text in a shim call: ``__lhp_sandbox_table(...)``."""
    return f"{HELPER_NAME}({segment})"


def module_defines_helper(source: str) -> bool:
    """Whether ``source`` already defines the shim helper (skip re-emitting)."""
    return f"def {HELPER_NAME}" in source


def helper_insertion_edit(
    source: str,
    data: bytes,
    offsets: list[int],
    renames: SandboxTableRenames,
) -> tuple[int, int, bytes]:
    """A byte-edit that inserts the shim helper at the top-of-module slot.

    The helper lands after the module docstring and any ``from __future__``
    imports (which must stay first) and before every other statement. Returns a
    zero-width ``(offset, offset, bytes)`` edit for the caller to fold into its
    one ``apply_byte_edits`` pass.
    """
    offset = _insertion_offset(source, data, offsets)
    helper = render_helper(renames).rstrip("\n")
    if offset >= len(data):
        text = "\n\n" + helper + "\n"
    else:
        text = helper + "\n\n\n"
    return offset, offset, text.encode("utf-8")


def render_helper(renames: SandboxTableRenames) -> str:
    """Render the ``__lhp_sandbox_table`` definition for ``renames``.

    Embeds the produced-FQN set and the short-key -> catalogs map as literal
    dict/set text, and the pattern's literal prefix/suffix around the leaf.
    """
    prefix, suffix = _leaf_affixes(renames.strategy)
    template = _environment().get_template(_TEMPLATE_NAME)
    return template.render(
        produced_literal=_produced_literal(renames),
        short_literal=_short_literal(renames),
        leaf_prefix=repr(prefix),
        leaf_suffix=repr(suffix),
    )


@functools.lru_cache(maxsize=1)
def _environment() -> Environment:
    """Process-local Jinja2 environment over the LHP package template tree.

    Cached so template compilation is paid once per worker. A local environment
    (not the shared generator one in ``core/codegen``) keeps this sub-package's
    downward-only import layering intact.
    """
    return Environment(  # nosec B701 — generates Python, not HTML
        loader=PackageLoader("lhp", "templates"),
        trim_blocks=True,
        lstrip_blocks=True,
        auto_reload=False,
    )


def _leaf_affixes(strategy: TableRenameStrategy) -> tuple[str, str]:
    """The literal prefix / suffix ``table_pattern`` wraps around the leaf.

    Computed by formatting the pattern with the run's namespace and a sentinel
    leaf, then splitting on the sentinel. The pattern is validated literal-safe
    and references ``{table}`` exactly once for any sane v1 pattern; a pattern
    that references it more than once cannot be reduced to a prefix/suffix pair
    and raises (surfaced as a clean flowgroup failure by the worker gate).
    """
    rendered = strategy.table_pattern.format(
        namespace=strategy.namespace, table=_LEAF_SENTINEL
    )
    parts = rendered.split(_LEAF_SENTINEL)
    if len(parts) != 2:
        raise ValueError(
            "sandbox table_pattern must reference {table} exactly once for the "
            f"runtime shim; got {strategy.table_pattern!r}"
        )
    return parts[0], parts[1]


def _produced_literal(renames: SandboxTableRenames) -> str:
    """The produced-FQN set as a Python set literal (sorted, deterministic)."""
    keys = sorted(renames.table_producers)
    if not keys:
        return "set()"
    return "{" + ", ".join(repr(key) for key in keys) + "}"


def _short_literal(renames: SandboxTableRenames) -> str:
    """The short-key -> catalogs map as a Python dict literal (sorted).

    Only the catalog keys are carried (the values are the uniqueness-guard
    inputs); the producing action ids the index also holds are irrelevant at
    runtime and dropped.
    """
    items = []
    for short in sorted(renames.table_short_to_catalogs):
        catalogs = sorted(renames.table_short_to_catalogs[short])
        rendered = "[" + ", ".join(repr(cat) for cat in catalogs) + "]"
        items.append(f"{short!r}: {rendered}")
    return "{" + ", ".join(items) + "}"


def _insertion_offset(source: str, data: bytes, offsets: list[int]) -> int:
    """Byte offset for the helper: after docstring + future imports, else top."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return 0
    body = tree.body
    anchor: ast.stmt | None = None
    idx = 0
    if body and _is_docstring(body[0]):
        anchor = body[0]
        idx = 1
    while idx < len(body) and _is_future_import(body[idx]):
        anchor = body[idx]
        idx += 1
    if anchor is not None:
        after_line = (anchor.end_lineno or anchor.lineno) + 1
        return offsets[after_line] if after_line < len(offsets) else len(data)
    return _skip_leading_trivia(data, offsets)


def _is_docstring(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    )


def _is_future_import(node: ast.stmt) -> bool:
    return isinstance(node, ast.ImportFrom) and node.module == "__future__"


def _skip_leading_trivia(data: bytes, offsets: list[int]) -> int:
    """First byte past any leading shebang / coding / comment / blank lines.

    Reached only when the module has no docstring and no ``from __future__``
    imports; keeps a coding declaration or shebang first (they must lead).
    """
    n = len(data)
    line = 1
    while line < len(offsets):
        start = offsets[line]
        if start >= n:
            break
        end = offsets[line + 1] if line + 1 < len(offsets) else n
        stripped = data[start:end].strip()
        if stripped == b"" or stripped.startswith(b"#"):
            line += 1
            continue
        return start
    return offsets[1] if len(offsets) > 1 else 0
