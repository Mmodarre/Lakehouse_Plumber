"""Shared parse substrate for user-supplied Python modules.

One parse contract (``parse_user_module``) and one import classifier
(``local_import_targets``) for every AST consumer that reads a user
``.py`` file: snapshot-CDC signature extraction, import-hoisting, and
helper-closure discovery. Centralizing the parse here keeps a single
``SyntaxError`` contract (the generic ``LHP-IO-003`` below) and a single
per-pipeline tree cache, instead of each consumer parsing independently.

``LHP-IO-003`` is overloaded repo-wide (syntax-error / multi-document /
instance-not-found); the error raised here is the *syntax-error* meaning
specifically, parameterized by the offending file path.
"""

import ast
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from lhp.errors import ErrorFactory, codes
from lhp.utils.performance_timer import incr_event

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImportTarget:
    """One classified import statement from a user module's AST.

    Carries enough to both classify the import (local vs external, the
    rewrite-hostile plain-dotted form) and locate it for a later prefix
    rewrite (``lineno`` / ``col_offset`` from the AST node).
    """

    module: Optional[str]
    level: int
    names: Tuple[str, ...]
    is_local: bool
    is_plain_dotted: bool
    lineno: int
    col_offset: int


def parse_user_module(path: Path, *, cache: Optional[dict]) -> ast.Module:
    """Read and parse a user ``.py`` file into an ``ast.Module``.

    The tree is cached by resolved absolute path so a file is parsed at
    most once per pipeline regardless of how many consumers request it.
    ``incr_event("source_parse_miss")`` fires on every actual parse (even
    when ``cache is None``); ``incr_event("source_parse_hit")`` fires only
    when a populated cache returns the tree.

    Args:
        path: Path to the user module on disk.
        cache: Per-pipeline tree cache keyed by ``str(path.resolve())``,
            or ``None`` to parse without caching.

    Returns:
        The parsed ``ast.Module``.

    Raises:
        LHPError: IO/003 when the file contains invalid Python syntax,
            parameterized by ``path``.
    """
    cache_key = str(path.resolve())

    if cache is not None:
        cached: Optional[ast.Module] = cache.get(cache_key)
        if cached is not None:
            incr_event("source_parse_hit")
            return cached

    incr_event("source_parse_miss")
    source_code = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        raise ErrorFactory.io_error(
            codes.IO_003,
            title="Python syntax error in source file",
            details=f"The Python file '{path}' contains invalid Python syntax: {e}",
            suggestions=[
                "Check the Python syntax in your file",
                "Ensure proper indentation (use spaces, not tabs)",
                "Verify all parentheses, brackets, and quotes are properly closed",
                "Test the file independently: python -m py_compile your_file.py",
            ],
        ) from e

    if cache is not None:
        cache[cache_key] = tree
    return tree


def _is_local_segment(segment: str, root: Path) -> bool:
    """True when ``segment`` resolves to a module/package directly under ``root``."""
    return (root / f"{segment}.py").exists() or (
        root / segment / "__init__.py"
    ).exists()


def local_import_targets(tree: ast.Module, root: Path) -> list[ImportTarget]:
    """Classify every import in ``tree`` against the local closure root.

    Walks the whole tree (``ast.walk``) so lazy / function-body imports
    are classified too, not just module-top imports. Classification:

    - ``ImportFrom`` with ``level > 0`` → relative; ``is_local=False``
      (relative imports are preserved untouched downstream) but ``level``
      is recorded faithfully.
    - top dotted segment resolves under ``root`` → local-absolute
      (``is_local=True``). For ``ImportFrom`` this is the prefix-rewritable
      form; for a plain ``import a.b`` / ``import a.b as c`` of a local it
      is additionally flagged ``is_plain_dotted=True`` (cannot be
      prefix-rewritten cleanly).
    - otherwise external/stdlib → ``is_local=False``.

    Args:
        tree: A parsed user module.
        root: The closure root (the entry file's own directory).

    Returns:
        One ``ImportTarget`` per import binding (one per ``ast.Import``
        alias; one per ``ast.ImportFrom`` statement).
    """
    targets: list[ImportTarget] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top_segment = alias.name.split(".")[0]
                is_local = _is_local_segment(top_segment, root)
                targets.append(
                    ImportTarget(
                        module=alias.name,
                        level=0,
                        names=(alias.asname,) if alias.asname else (alias.name,),
                        is_local=is_local,
                        is_plain_dotted=is_local,
                        lineno=node.lineno,
                        col_offset=node.col_offset,
                    )
                )
        elif isinstance(node, ast.ImportFrom):
            names = tuple(alias.name for alias in node.names)
            if node.level > 0:
                targets.append(
                    ImportTarget(
                        module=node.module,
                        level=node.level,
                        names=names,
                        is_local=False,
                        is_plain_dotted=False,
                        lineno=node.lineno,
                        col_offset=node.col_offset,
                    )
                )
                continue
            top_segment = node.module.split(".")[0] if node.module else ""
            is_local = bool(top_segment) and _is_local_segment(top_segment, root)
            targets.append(
                ImportTarget(
                    module=node.module,
                    level=0,
                    names=names,
                    is_local=is_local,
                    is_plain_dotted=False,
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                )
            )

    return targets
