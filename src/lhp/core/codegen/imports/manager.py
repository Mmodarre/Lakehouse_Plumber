"""Unified import management orchestrator for generated pipeline modules.

:class:`ImportManager` is a facade that collects imports from two sources
(manual ``add_import`` calls and PySpark-expression detection), then
delegates conflict resolution and sorting to the module-level helpers in
:mod:`.resolver` and :mod:`.categorizer`.

Per CODING_CONSTITUTION §5.5, no service-to-service class calls: the
manager only invokes free functions from the sibling modules.
"""

from __future__ import annotations

import ast
import logging
from typing import Any, Dict, List, Set

from lhp.core.codegen.imports.categorizer import (
    extract_module_name,
    is_wildcard_import,
    sort_imports,
)
from lhp.core.codegen.imports.detector import ImportDetector
from lhp.core.codegen.imports.resolver import resolve_conflicts


class ImportManager:
    """Unified import management with zero configuration.

    Consolidates:

    * Manual imports added via :meth:`add_import` (backward-compatible API).
    * Expression-based detection through :class:`ImportDetector`.

    Conflict resolution and sorting are pure functions in the sibling
    modules; this class only holds collection state and orchestrates the
    flow.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

        # Collection buckets — kept separate so debug_info / get_stats can
        # report provenance.
        self._manual_imports: Set[str] = set()
        self._expression_imports: Set[str] = set()
        # Retained for get_stats/debug_info output shape; no longer populated
        # since file-level AST import hoisting (add_imports_from_file) was removed.
        self._file_imports: Set[str] = set()

        # Reuse the existing AST-based detector for PySpark expressions.
        self._expression_detector = ImportDetector(strategy="ast")

    def add_import(self, import_stmt: str) -> None:
        """Add a manual import statement (backward-compatible API).

        Detects silent name shadowing: if a previously-added ``from x import Y``
        line binds the same local name as the new statement but points at a
        different source module, raise ``LHPValidationError``. This prevents
        the case where two custom datasources/sinks (or python transforms)
        unintentionally export the same class/function name from different
        modules — without this check, the second import would silently shadow
        the first and SDP would register the wrong class.

        Args:
            import_stmt: Import statement like ``"from pyspark import pipelines as dp"``.
        """
        if not import_stmt or not import_stmt.strip():
            return

        stmt = import_stmt.strip()

        new_bindings = self._extract_from_import_bindings(stmt)
        if new_bindings:
            for existing in self._manual_imports | self._file_imports:
                existing_bindings = self._extract_from_import_bindings(existing)
                for name, module in new_bindings.items():
                    if name in existing_bindings and existing_bindings[name] != module:
                        from lhp.errors import (
                            ErrorCategory,
                            LHPValidationError,
                        )

                        raise LHPValidationError(
                            category=ErrorCategory.VALIDATION,
                            code_number="021",
                            title="Import name collision",
                            details=(
                                f"Two imports bind the local name '{name}' "
                                f"to different modules:\n"
                                f"  Existing: {existing}\n"
                                f"  New:      {stmt}"
                            ),
                            suggestions=[
                                f"Rename one of the '{name}' definitions in "
                                f"the source files",
                                f"Use a different alias when importing one of "
                                f"the modules (e.g. 'from {module} import "
                                f"{name} as {name}2')",
                                "Move conflicting symbols into distinct module "
                                "paths so the binding names differ",
                            ],
                            context={
                                "Conflicting Name": name,
                                "Existing Module": existing_bindings[name],
                                "New Module": module,
                            },
                        )

        self._manual_imports.add(stmt)

    def _extract_from_import_bindings(self, stmt: str) -> Dict[str, str]:
        """Extract a ``{local_name: source_module}`` map from a ``from … import …`` statement.

        Returns an empty dict for ``import x`` form, wildcard imports, or
        statements that fail to parse — the collision check then becomes a
        no-op for those cases.
        """
        try:
            tree = ast.parse(stmt)
        except SyntaxError:
            return {}

        bindings: Dict[str, str] = {}
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    local_name = alias.asname or alias.name
                    bindings[local_name] = module
        return bindings

    def add_imports_from_expression(self, expression: str) -> None:
        """Add imports detected from a PySpark expression string.

        Uses the existing :class:`ImportDetector` with established patterns.
        Detection failures are logged at DEBUG and swallowed — expression
        scanning is best-effort.

        Args:
            expression: PySpark expression like ``"F.current_timestamp()"``.
        """
        try:
            detected = self._expression_detector.detect_imports(expression)
            self._expression_imports.update(detected)
        except Exception as e:
            self.logger.debug(
                f"Expression import detection failed for '{expression}': {e}"
            )

    def get_consolidated_imports(self) -> List[str]:
        """Return the final consolidated, sorted, deduplicated import list.

        Applies wildcard-precedence resolution (via :func:`resolve_conflicts`)
        then category-sort (via :func:`sort_imports`) — both pure functions
        from the sibling modules.
        """
        all_imports = (
            self._manual_imports | self._expression_imports | self._file_imports
        )

        if not all_imports:
            return []

        self.logger.debug(
            f"Consolidating {len(all_imports)} import(s) "
            f"(manual={len(self._manual_imports)}, "
            f"expression={len(self._expression_imports)}, "
            f"file={len(self._file_imports)})"
        )

        resolved_imports = resolve_conflicts(
            all_imports,
            extract_module_name=extract_module_name,
            is_wildcard_import=is_wildcard_import,
        )

        return sort_imports(resolved_imports)

    def clear(self) -> None:
        """Drop all collected imports across every bucket."""
        self._manual_imports.clear()
        self._expression_imports.clear()
        self._file_imports.clear()

    def get_stats(self) -> Dict[str, int]:
        """Return counts per bucket plus the total unique after resolution."""
        return {
            "manual_imports": len(self._manual_imports),
            "expression_imports": len(self._expression_imports),
            "file_imports": len(self._file_imports),
            "total_unique": len(self.get_consolidated_imports()),
        }

    def debug_info(self) -> Dict[str, Any]:
        """Return a detailed snapshot for diagnostics (sorted per bucket)."""
        return {
            "manual_imports": sorted(self._manual_imports),
            "expression_imports": sorted(self._expression_imports),
            "file_imports": sorted(self._file_imports),
            "consolidated": self.get_consolidated_imports(),
            "stats": self.get_stats(),
        }
