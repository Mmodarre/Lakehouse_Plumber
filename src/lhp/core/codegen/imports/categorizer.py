"""Module-level helpers for categorizing and sorting Python import statements.

Pure functions used by :class:`lhp.core.codegen.imports.manager.ImportManager`
to group imports by category (stdlib / third-party / pyspark / dlt / custom),
detect wildcard form, extract base module names, and produce a stable
PEP 8-ish sort order.

Also exposes :func:`extract_future_imports`, the AST-based hoister used at the
assembly chokepoint to lift ``from __future__ import …`` lines to the top of
generated modules.
"""

from __future__ import annotations

import ast
import re
from typing import List, Optional, Set, Tuple

# Standard-library module names that participate in import categorization.
# Conservative subset — only the modules LHP-generated code is likely to
# emit. Anything outside this set falls through to "third_party" / "custom".
STANDARD_MODULES: frozenset[str] = frozenset(
    {
        "ast",
        "sys",
        "os",
        "re",
        "json",
        "time",
        "datetime",
        "pathlib",
        "typing",
        "dataclasses",
        "functools",
        "itertools",
        "collections",
        "logging",
        "argparse",
        "subprocess",
        "urllib",
        "http",
    }
)

# Canonical category order (PEP 8-ish): stdlib first, then third-party, then
# pyspark, then DLT-specific (pyspark.pipelines), then everything else.
_IMPORT_ORDER: Tuple[str, ...] = (
    "standard",
    "third_party",
    "pyspark",
    "dlt",
    "custom",
)


def extract_module_name(import_stmt: str) -> Optional[str]:
    """Extract the base module name from an import statement.

    Returns ``None`` if the statement is neither ``from X import …`` nor
    ``import X`` — e.g. malformed input or a leading comment.
    """
    import_stmt = import_stmt.strip()

    if import_stmt.startswith("from "):
        match = re.match(r"from\s+([^\s]+)\s+import", import_stmt)
        if match:
            return match.group(1)
    elif import_stmt.startswith("import "):
        match = re.match(r"import\s+([^\s,]+)", import_stmt)
        if match:
            return match.group(1)

    return None


def is_wildcard_import(import_stmt: str) -> bool:
    """Return True if the statement is a ``from … import *`` form."""
    return "import *" in import_stmt


def categorize_import(import_stmt: str) -> str:
    """Categorize a single import statement for sorting.

    Hardcoded conventions; DLT (``pyspark.pipelines``) is checked before the
    generic pyspark bucket so DLT lines group together.
    """
    import_stmt_lower = import_stmt.lower()

    module = extract_module_name(import_stmt)
    if module and module.split(".")[0] in STANDARD_MODULES:
        return "standard"

    if "pipelines" in import_stmt_lower:
        return "dlt"

    if "pyspark" in import_stmt_lower or "spark" in import_stmt_lower:
        return "pyspark"

    third_party_indicators = ("pandas", "numpy", "requests", "yaml", "json")
    for indicator in third_party_indicators:
        if indicator in import_stmt_lower:
            return "third_party"

    return "custom"


def sort_imports(imports: Set[str]) -> List[str]:
    """Sort imports by category then lexicographically within each category."""
    if not imports:
        return []

    categorized: dict[str, list[str]] = {cat: [] for cat in _IMPORT_ORDER}

    for imp in imports:
        category = categorize_import(imp)
        categorized[category].append(imp)

    sorted_imports: list[str] = []
    for category in _IMPORT_ORDER:
        if categorized[category]:
            categorized[category].sort()
            sorted_imports.extend(categorized[category])

    return sorted_imports


def extract_future_imports(source: str) -> Tuple[List[str], str]:
    """Extract ``from __future__ import ...`` statements from a Python source string.

    PEP 236 requires future imports to appear before any other statement (other
    than docstrings, comments, and other future imports). LHP assembles
    generated pipeline modules from multiple sources (manual imports, custom
    datasource/sink files, snapshot-CDC source functions). When any of those
    inputs contain a ``from __future__`` line, it must be hoisted to the top
    of the assembled module — this helper is the AST-based extractor used at
    the assembly chokepoint to do that.

    Uses AST so that future-looking strings inside docstrings or comments
    (e.g. ``\"\"\"from __future__ ...\"\"\"`` in a triple-quoted block) are not
    mis-extracted.

    Args:
        source: Python source code (may be a full module or a fragment).

    Returns:
        Tuple of (future_lines, source_with_those_lines_blanked). The original
        line numbering is preserved by replacing extracted lines with empty
        lines, which keeps any debug/traceback line references valid.

        On a SyntaxError (e.g. fragment), returns ([], source) so the caller
        can fall through without losing content.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return [], source

    future_lines: List[str] = []
    lineno_to_blank: Set[int] = set()
    src_lines = source.split("\n")
    for node in tree.body:
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "__future__"
            and node.level == 0
        ):
            start = node.lineno - 1
            end = (node.end_lineno or node.lineno) - 1
            future_lines.append("\n".join(src_lines[start : end + 1]).strip())
            for i in range(start, end + 1):
                lineno_to_blank.add(i)

    cleaned = "\n".join(
        "" if i in lineno_to_blank else line for i, line in enumerate(src_lines)
    )
    return future_lines, cleaned
