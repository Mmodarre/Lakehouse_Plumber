"""Module-level conflict-resolution helpers for the import sub-package.

Pure functions called from :class:`lhp.core.codegen.imports.manager.ImportManager`.
One rule survives here:

1. Within a single module, a wildcard import supersedes that module's
   specific-name imports (``from x import *`` beats ``from x import a, b``).
   Sound because the wildcard binds every name the specific import bound.

A second rule — a submodule wildcard superseding a parent-module import, e.g.
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

import logging
from typing import Callable, Dict, List, Optional, Set

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
