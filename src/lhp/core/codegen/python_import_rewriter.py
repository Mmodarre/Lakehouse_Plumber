"""Prefix-rewrite a copied user module's absolute-local imports.

A user entry/helper module copied under ``custom_python_functions/`` keeps
its original ``from helpers.foo import bar`` text, which resolved top-level
before the move but no longer does once the file lives inside the package.
:func:`rewrite_local_imports` re-points every *absolute-local* import under
the ``custom_python_functions`` prefix so it resolves post-relocation, while
leaving relative and external imports untouched.

The locality decision is delegated to
:func:`lhp.core.codegen.python_dependency_resolver.resolve_module_on_disk` —
the same on-disk predicate the closure uses — so the set of imports this
rewriter prefixes is exactly the set the closure copies (PEP 420
namespace-package members included). Rewriting is byte-precise string surgery
on the import statement spans, never an ``ast.unparse`` of the whole module:
comments, blank lines, and code outside the rewritten imports are preserved
verbatim.
"""

import ast
from pathlib import Path
from typing import NoReturn

from lhp.core.codegen.python_dependency_resolver import resolve_module_on_disk
from lhp.errors import ErrorFactory, codes

_CUSTOM_FUNCTIONS_PREFIX = "custom_python_functions"


def _raise_plain_dotted_local(dotted: str) -> NoReturn:
    """VAL-024: a plain ``import a.b`` of a *local* module cannot be rewritten.

    Once the file is relocated under ``custom_python_functions/``, the bound
    name ``a.b`` would have to become ``custom_python_functions.a.b`` at every
    call site too; rewriting only the statement would leave the call sites
    dangling. The ``from ... import ...`` forms bind a leaf name that survives
    relocation untouched, so the user is directed to those.
    """
    package, _, member = dotted.rpartition(".")
    suggestions = [
        f"Import a name from the module: 'from {dotted} import <name>'",
    ]
    if package:
        suggestions.append(
            f"Or import the module from its package: 'from {package} import {member}'"
        )
    suggestions.append(
        "Reference helpers with an explicit 'from <module> import <name>' "
        "rather than a plain dotted import of a local module"
    )
    raise ErrorFactory.validation_error(
        codes.VAL_024,
        title="Local helper imported with a plain dotted import",
        details=(
            f"The local helper '{dotted}' is imported with 'import {dotted}'. "
            f"This form cannot be relocated under 'custom_python_functions' "
            f"without rebinding every '{dotted}.*' call site, so it is "
            f"rejected. Use a 'from ... import ...' form, which binds a leaf "
            f"name that is unaffected by the relocation."
        ),
        suggestions=suggestions,
        context={"Plain-dotted local import": dotted},
    )


def _line_start_byte_offsets(data: bytes) -> list[int]:
    """Byte offset at which each 1-based source line begins (index 0 unused).

    Splits only on the line terminators CPython's tokenizer honours — ``\\n``,
    ``\\r``, ``\\r\\n`` — so the offsets line up exactly with AST ``lineno`` /
    ``col_offset`` (which are UTF-8 *byte* columns). ``str.splitlines`` is NOT
    used: it also breaks on ``\\x0b``/``\\x0c``/``\\x1c``-``\\x1e``/``\\u2028`` etc.,
    which the tokenizer treats as ordinary characters, which would skew offsets.
    """
    offsets = [0, 0]
    i = 0
    n = len(data)
    nl = ord("\n")
    cr = ord("\r")
    while i < n:
        b = data[i]
        if b == cr:
            i += 2 if i + 1 < n and data[i + 1] == nl else 1
            offsets.append(i)
        elif b == nl:
            i += 1
            offsets.append(i)
        else:
            i += 1
    return offsets


def _rebuild_import_from(node: ast.ImportFrom, new_module: str) -> str:
    """Render a single-line ``from <new_module> import <names>`` for ``node``.

    Aliases (``as``) are preserved from ``node.names``; only the module is
    swapped. The statement is emitted in canonical single-line form (any
    original line-wrapping/parenthesization is not reproduced — that text is
    fully owned by the rewritten span).
    """
    parts = [
        f"{alias.name} as {alias.asname}" if alias.asname else alias.name
        for alias in node.names
    ]
    return f"from {new_module} import {', '.join(parts)}"


def rewrite_local_imports(source: str, tree: ast.Module, root: Path) -> str:
    """Prefix-rewrite a module's absolute-local imports for relocation.

    Returns ``source`` with every absolute-local ``from`` import re-pointed
    under ``custom_python_functions`` so it still resolves once the file is
    copied beneath that package. The locality decision reuses
    :func:`resolve_module_on_disk` — the exact predicate
    :func:`resolve_local_closure` uses — so closure and rewrite agree on what
    is local (including PEP 420 namespace-package members, which the substrate
    reports ``is_local=False`` yet both functions still treat as local because
    they re-resolve on disk under ``root``).

    Rewrite/preserve/error rules (applied uniformly to the entry and every
    helper, post-substitution):

    - ``from pkg.mod import x`` (resolves under ``root``) →
      ``from custom_python_functions.pkg.mod import x``; ``from pkg import x``
      → ``from custom_python_functions.pkg import x``. Aliases are preserved.
    - ``from .sibling import x`` (relative, ``level > 0``) → unchanged.
    - ``import os`` / ``from pyspark.sql import functions`` (not under
      ``root``) → unchanged.
    - ``import pkg.mod`` / ``import pkg.mod as c`` of a *local* module →
      ``LHP-VAL-024`` (the bound dotted name cannot be relocated without
      rebinding call sites; the user is directed to a ``from`` form).

    Only the exact source span of each rewritten statement is replaced (via
    the node's ``lineno`` / ``col_offset`` / ``end_lineno`` /
    ``end_col_offset``); all other bytes — comments, blank lines, code — are
    preserved verbatim. Multi-line / parenthesized imports are handled through
    the node's end position. The whole tree is walked (``ast.walk``) so lazy /
    function-body imports are rewritten too, matching closure discovery.

    Args:
        source: The module's full source text (already token-substituted).
        tree: The parsed ``ast.Module`` for ``source``.
        root: The closure root ``R`` (the entry file's own directory).

    Returns:
        The source with absolute-local imports prefixed.

    Raises:
        LHPValidationError: ``LHP-VAL-024`` for a plain-dotted local import.
    """
    data = source.encode("utf-8")
    line_starts = _line_start_byte_offsets(data)
    edits: list[tuple[int, int, bytes]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if resolve_module_on_disk(alias.name, root) is not None:
                    _raise_plain_dotted_local(alias.name)
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level > 0 or not node.module:
            continue
        if (
            node.end_lineno is None
            or node.end_col_offset is None
            or resolve_module_on_disk(node.module, root) is None
        ):
            continue
        new_module = f"{_CUSTOM_FUNCTIONS_PREFIX}.{node.module}"
        start = line_starts[node.lineno] + node.col_offset
        end = line_starts[node.end_lineno] + node.end_col_offset
        replacement = _rebuild_import_from(node, new_module).encode("utf-8")
        edits.append((start, end, replacement))

    if not edits:
        return source

    out = data
    for start, end, replacement in sorted(edits, reverse=True):
        out = out[:start] + replacement + out[end:]
    return out.decode("utf-8")
