"""Per-run parse caches for dependency extraction (no module-level state).

One graph build re-reads and re-parses the same helper bodies once per
referencing ACTION — a shared helper ``.py`` in a large blueprint project
was observed parsing thousands of times per ``lhp dag`` invocation. The
caches here hold exactly the bindings-INDEPENDENT artifacts:

- file contents by resolved path,
- parsed Python ``(ast.Module, FunctionIndex)`` pairs by content hash
  (the scope-aware visitor still runs per action — its YAML parameter
  bindings differ — but on a shared tree/index),
- SQL extraction results by content hash
  (:class:`~lhp.core.dependencies.sql_extraction.SqlExtractionResult` is
  treated as immutable; consumers stamp copies via ``dataclasses.replace``).

Owned by :class:`~lhp.core.dependencies.builder.DependencyGraphBuilder`
(one instance per builder, shared across its builds) and threaded into
:class:`~lhp.core.dependencies.source_parsing.SourceParser`; standalone
parser construction defaults to a fresh cache.
"""

import ast
import hashlib
from pathlib import Path
from typing import Dict, Optional, Tuple

from ._function_index import FunctionIndex
from .sql_extraction import SqlExtractionResult, extract_tables_from_sql


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class ParseCache:
    """Memoize file reads, Python parses and SQL extraction for one run."""

    def __init__(self) -> None:
        self._file_content: Dict[Path, str] = {}
        # ``None`` records an unparseable body: callers fall back to their
        # own parse-and-log failure path (rare, and per-action logging is
        # the pre-cache behavior).
        self._py_parsed: Dict[str, Optional[Tuple[ast.Module, FunctionIndex]]] = {}
        self._sql_result: Dict[str, SqlExtractionResult] = {}

    def read_text(self, path: Path) -> str:
        """Read ``path`` once per run (UTF-8); later calls reuse the bytes."""
        if path not in self._file_content:
            self._file_content[path] = path.read_text(encoding="utf-8")
        return self._file_content[path]

    def parse_python(self, code: str) -> Optional[Tuple[ast.Module, FunctionIndex]]:
        """The parsed tree + function index for ``code``, or ``None``.

        Normalization matches
        :meth:`~lhp.core.dependencies.python_parser.PythonParser.extract_tables_from_python`
        (dedent + strip), so the cached tree is exactly the one that method
        would have parsed itself.
        """
        from .python_parser import normalize_python_code

        key = _content_hash(code)
        if key not in self._py_parsed:
            try:
                tree = ast.parse(normalize_python_code(code))
            except SyntaxError:
                self._py_parsed[key] = None
            else:
                self._py_parsed[key] = (tree, FunctionIndex(tree))
        return self._py_parsed[key]

    def extract_sql(self, sql_text: str) -> SqlExtractionResult:
        """Run (or reuse) sqlglot table extraction for ``sql_text``."""
        key = _content_hash(sql_text)
        if key not in self._sql_result:
            self._sql_result[key] = extract_tables_from_sql(sql_text)
        return self._sql_result[key]
