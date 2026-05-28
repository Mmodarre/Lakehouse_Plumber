"""Module-level conflict-resolution helpers for the import sub-package.

Pure functions called from :class:`lhp.core.codegen.imports.manager.ImportManager`.
The two rules implemented here:

1. Wildcard imports take precedence over specific imports from the same module.
2. Submodule wildcard imports take precedence over parent-module-as-alias
   imports (e.g. ``from pyspark.sql.functions import *`` beats
   ``from pyspark.sql import functions as F``).
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Set

logger = logging.getLogger(__name__)


def _is_parent_importing_child_as_alias(import_stmt: str, child_name: str) -> bool:
    """Return True if ``import_stmt`` imports ``child_name`` (optionally aliased).

    Examples:
        ``"from pyspark.sql import functions as F"`` with child=``"functions"`` -> True
        ``"from pyspark.sql import functions"`` with child=``"functions"`` -> True
    """
    pattern = (
        rf"from\s+[^\s]+\s+import\s+.*\b{re.escape(child_name)}\b(?:\s+as\s+\w+)?"
    )
    return bool(re.search(pattern, import_stmt))


def detect_submodule_conflicts(
    module_groups: Dict[str, List[str]],
    wildcard_modules: Set[str],
    parent_child_conflicts: Dict[str, Dict[str, str]],
) -> None:
    """Populate ``parent_child_conflicts`` with parent/child wildcard collisions.

    For each wildcard module, check whether any other module is its prefix
    *and* that prefix's import statement binds the child segment as an alias.
    When both are true, record the conflict so the caller can drop the parent
    import in favour of the child wildcard.

    Mutates ``parent_child_conflicts`` in place; returns ``None``.
    """
    for wildcard_module in wildcard_modules:
        wildcard_parts = wildcard_module.split(".")

        for other_module in module_groups:
            if other_module == wildcard_module:
                continue

            other_parts = other_module.split(".")

            if (
                len(wildcard_parts) > len(other_parts)
                and wildcard_parts[: len(other_parts)] == other_parts
            ):
                parent_imports = module_groups[other_module]
                child_module_name = wildcard_parts[len(other_parts)]

                for parent_import in parent_imports:
                    if _is_parent_importing_child_as_alias(
                        parent_import, child_module_name
                    ):
                        parent_child_conflicts[wildcard_module] = {
                            "parent_module": other_module,
                            "parent_import": parent_import,
                            "child_alias": child_module_name,
                        }
                        logger.debug(
                            f"Detected submodule conflict: {wildcard_module} "
                            f"vs {parent_import}"
                        )
                        break


def resolve_conflicts(
    imports: Set[str],
    *,
    extract_module_name,
    is_wildcard_import,
) -> Set[str]:
    """Resolve import conflicts using hardcoded wildcard-precedence rules.

    Args:
        imports: All collected import statements (deduplicated set).
        extract_module_name: Callable returning the base module from a
            statement (injected to avoid an intra-package import cycle and to
            keep this module a pure-function leaf).
        is_wildcard_import: Callable returning True if a statement is the
            ``from X import *`` form.

    Returns:
        The resolved set of imports with wildcards replacing competing
        specific imports, and parent-module-as-alias lines dropped when a
        child wildcard supersedes them.
    """
    if not imports:
        return set()

    module_groups: Dict[str, List[str]] = {}
    wildcard_modules: Set[str] = set()
    parent_child_conflicts: Dict[str, Dict[str, str]] = {}

    for imp in imports:
        module = extract_module_name(imp)
        if module:
            module_groups.setdefault(module, []).append(imp)
            if is_wildcard_import(imp):
                wildcard_modules.add(module)

    detect_submodule_conflicts(
        module_groups, wildcard_modules, parent_child_conflicts
    )

    resolved: Set[str] = set()
    excluded_modules: Set[str] = set()

    # First pass: parent-module exclusions driven by child wildcard wins.
    for child_module, parent_info in parent_child_conflicts.items():
        if child_module in wildcard_modules:
            excluded_modules.add(parent_info["parent_module"])
            logger.debug(
                f"Submodule conflict: {child_module} wildcard excludes "
                f"{parent_info['parent_module']} imports"
            )

    # Second pass: per-module wildcard-vs-specific resolution.
    for module, module_imports in module_groups.items():
        if module in excluded_modules:
            continue

        if module in wildcard_modules:
            wildcards = [imp for imp in module_imports if is_wildcard_import(imp)]
            resolved.update(wildcards)
        else:
            resolved.update(module_imports)

    return resolved
