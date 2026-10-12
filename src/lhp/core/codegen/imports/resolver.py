"""Module-level conflict-resolution helpers for the import sub-package.

Pure functions called from :class:`lhp.core.codegen.imports.manager.ImportManager`.
Two rules live here:

1. Within a single module, a wildcard import supersedes that module's
   specific-name imports (``from x import *`` beats ``from x import a, b``).
   Sound because the wildcard binds every name the specific import bound.
2. ``from`` imports of one module that bind the same name under the same local
   name are split into one line per name, so set dedup can drop the repeat
   (issue #299). Generators register whole lines, and ``from x import a, b``
   next to ``from x import b`` would otherwise bind ``b`` twice (ruff F811).
   Only the lines involved in the overlap change; every other line keeps its
   text.

A former rule — a submodule wildcard superseding a parent-module import, e.g.
``from pyspark.sql.functions import *`` beating ``from pyspark.sql import
functions as F`` — was removed (issue #217). The two statements are not
substitutes: a submodule wildcard binds *none* of the names a parent import
binds. ``as F`` binds ``F``, the unaliased form binds ``functions``, and the
wildcard binds only the individual function names. Dropping the parent emitted
modules that raised ``NameError`` on the first ``F.col(...)`` — reachable from
any flowgroup pairing an ``F.``-emitting generator with a ``type: test``
action, since test generators contribute the wildcard. The old code also
excluded the parent's whole *module group*, taking unrelated siblings such as
``from pyspark.sql import DataFrame`` with it. Parent-module imports are now
always kept.
"""

from __future__ import annotations

import ast
import logging
from typing import Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


def resolve_conflicts(
    imports: Set[str],
    *,
    extract_module_name: Callable[[str], Optional[str]],
    is_wildcard_import: Callable[[str], bool],
) -> Set[str]:
    """Resolve same-module wildcard-vs-specific-name import conflicts.

    Args:
        imports: All collected import statements (deduplicated set).
        extract_module_name: Callable returning the base module from a
            statement (injected to avoid an intra-package import cycle and to
            keep this module a pure-function leaf).
        is_wildcard_import: Callable returning True if a statement is the
            ``from X import *`` form.

    Returns:
        The resolved set of imports, with a module's wildcard replacing that
        same module's specific-name imports. Imports from every other module
        are kept, including from a parent module of a wildcard's module.
    """
    if not imports:
        return set()

    module_groups: Dict[str, List[str]] = {}
    wildcard_modules: Set[str] = set()

    for imp in imports:
        module = extract_module_name(imp)
        if module:
            module_groups.setdefault(module, []).append(imp)
            if is_wildcard_import(imp):
                wildcard_modules.add(module)

    resolved: Set[str] = set()

    for module, module_imports in module_groups.items():
        if module in wildcard_modules:
            # Caveat: an *aliased* specific name from the same module
            # (``from x import col as c`` alongside ``from x import *``) loses
            # the ``c`` binding here. Unreachable — no LHP generator emits one.
            wildcards = [imp for imp in module_imports if is_wildcard_import(imp)]
            superseded = len(module_imports) - len(wildcards)
            if superseded:
                logger.debug(
                    f"Wildcard import for '{module}' supersedes "
                    f"{superseded} specific import(s)"
                )
            resolved.update(wildcards)
        else:
            resolved.update(module_imports)

    return resolved


def split_overlapping_from_imports(imports: Set[str]) -> Set[str]:
    """Split ``from`` imports that share a binding into one line per name.

    A binding is the (module, name, local name) a ``from`` import creates, so
    ``from x import a as b`` and ``from x import a`` do not overlap, and
    ``from x import a as a`` overlaps with ``from x import a``. Each line that
    shares a binding with another line is rewritten as one
    ``from x import <name>`` line per name it imports (keeping any real alias),
    and the returned set drops the exact duplicates that leaves.

    Lines that share no binding keep their original text, as do ``import x``
    lines, wildcard imports and anything that is not exactly one parseable
    ``from`` statement.
    """
    nodes: Dict[str, ast.ImportFrom] = {}
    owners: Dict[Tuple[int, str, str, str], Set[str]] = {}
    for statement in imports:
        node = _single_from_import(statement)
        if node is None:
            continue
        nodes[statement] = node
        for alias in node.names:
            binding = (
                node.level,
                node.module or "",
                alias.name,
                alias.asname or alias.name,
            )
            owners.setdefault(binding, set()).add(statement)

    overlapping = {
        statement
        for statements in owners.values()
        if len(statements) > 1
        for statement in statements
    }
    if not overlapping:
        return set(imports)

    logger.debug(f"Splitting {len(overlapping)} from-import(s) that bind the same name")
    resolved = set(imports) - overlapping
    for statement in overlapping:
        node = nodes[statement]
        prefix = f"from {'.' * node.level}{node.module or ''} import "
        for alias in node.names:
            suffix = (
                f" as {alias.asname}"
                if alias.asname and alias.asname != alias.name
                else ""
            )
            resolved.add(f"{prefix}{alias.name}{suffix}")
    return resolved


def _single_from_import(statement: str) -> Optional[ast.ImportFrom]:
    """Return the node when ``statement`` is exactly one non-wildcard ``from`` import."""
    if not statement.lstrip().startswith("from "):
        return None
    try:
        tree = ast.parse(statement.strip())
    except SyntaxError:
        return None
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.ImportFrom):
        return None
    node = tree.body[0]
    if any(alias.name == "*" for alias in node.names):
        return None
    return node
