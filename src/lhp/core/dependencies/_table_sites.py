"""Value objects describing recognized table-consuming call sites.

Produced by the extraction visitor
(:mod:`lhp.core.dependencies._extraction_visitor`) and surfaced through
:func:`lhp.core.dependencies.python_parser.collect_python_table_sites`. The
records carry exactly what a source rewriter needs: where a table-name (or
inline-SQL) string literal lives in the source bytes, or — when the argument
is statically resolvable but not a plain literal — which table names it can
take, so the caller can warn about in-scope reads it cannot rewrite.

These types live in their own module (not ``python_parser``) because both the
visitor and the parser construct/return them; placing them on the parser
would force a runtime import cycle (the parser already imports the visitor).
"""

import ast
from dataclasses import dataclass
from typing import FrozenSet, Literal, Optional, Tuple

SiteKind = Literal["table_read", "spark_sql"]


@dataclass(frozen=True)
class SourceSpan:
    """Exact location of one string-literal expression in Python source.

    Coordinates are CPython AST coordinates: ``lineno`` / ``end_lineno`` are
    1-based source lines; ``col_offset`` / ``end_col_offset`` are UTF-8
    *byte* columns within their line. The span covers the full literal
    expression — quote characters, any prefix (``r``/``b``...), and all parts
    of an implicit concatenation — so a rewriter must splice in a complete
    replacement literal, never bare text.
    """

    lineno: int
    col_offset: int
    end_lineno: int
    end_col_offset: int

    @classmethod
    def of_node(cls, node: ast.expr) -> "SourceSpan":
        """Build a span from an AST expression node's position attributes."""
        # end_lineno/end_col_offset are Optional in typeshed but always
        # populated by ast.parse() on real source (Python >= 3.8).
        assert node.end_lineno is not None and node.end_col_offset is not None
        return cls(
            lineno=node.lineno,
            col_offset=node.col_offset,
            end_lineno=node.end_lineno,
            end_col_offset=node.end_col_offset,
        )


@dataclass(frozen=True)
class PythonTableSite:
    """One recognized table-consuming call site.

    ``kind`` discriminates the call shape:

    - ``"table_read"`` — the argument IS a table name: ``spark.table``,
      ``spark.read/readStream.table``, allowlisted
      ``spark.read/readStream.format(fmt).table/load`` chains, and
      ``spark.catalog.tableExists`` / ``spark.catalog.dropTempView``.
    - ``"spark_sql"`` — ``spark.sql(...)``: the argument is SQL text that
      may reference tables.

    Classification (mutually exclusive):

    - ``rewritable=True`` — the argument is a direct ``ast.Constant`` string
      literal. ``span`` locates the literal in the source as parsed;
      ``value`` is the literal's value (the table name for ``table_read``,
      the SQL text for ``spark_sql``); ``resolved_values`` is empty.
    - ``rewritable=False`` and ``fstring=False`` — the argument is not a plain
      constant but resolved statically (variable, YAML parameter binding,
      fully-resolved f-string, concatenation, conditional union). ``span`` /
      ``value`` are ``None``; ``resolved_values`` carries the candidate TABLE
      NAMES — for ``spark_sql`` sites these are the tables extracted from each
      resolved SQL text, so matching semantics are uniform across kinds.
    - ``fstring=True`` — a ``spark_sql`` argument that is an ``ast.JoinedStr``
      whose value did NOT statically resolve (a dynamic f-string body).
      ``span`` covers the whole f-string node (its outer coordinates are
      reliable across Python versions, unlike positions of pieces INSIDE it);
      ``value`` / ``resolved_values`` are empty. The sandbox rewriter does a
      text-level analysis of the span to rewrite in-scope refs sitting in the
      literal (non-interpolated) segments, falling back to LHP-VAL-066 where
      interpolation makes matching ambiguous, or LHP-VAL-067 where the ref's
      identity is purely runtime-determined. Dependency extraction ignores
      these sites (the LHP-DEP-002 advisory already covers the dynamic read).
    - ``opaque=True`` — the argument neither is a plain literal nor statically
      resolves to anything (a bare name, call result, subscript on a dynamic
      key). For a ``table_read`` site ``span`` covers the ARGUMENT node so the
      sandbox pass can wrap it in the runtime ``__lhp_sandbox_table(...)`` shim;
      for a ``spark_sql`` site ``span`` is ``None`` (the SQL text is opaque, not
      a name argument) and the sandbox pass emits an LHP-VAL-067 advisory.
      ``value`` / ``resolved_values`` are empty. Dependency extraction ignores
      these sites (LHP-DEP-002 already covers the opaque read). A call with no
      positional argument is never recorded — there is nothing to wrap.

    ``lineno`` is the 1-based line of the call expression itself and is
    present for every classification.
    """

    kind: SiteKind
    rewritable: bool
    lineno: int
    span: Optional[SourceSpan] = None
    value: Optional[str] = None
    resolved_values: FrozenSet[str] = frozenset()
    fstring: bool = False
    opaque: bool = False


@dataclass(frozen=True)
class PythonTableSitesResult:
    """Result of :func:`collect_python_table_sites`.

    ``sites`` is in AST visit order (source order for statements; outer
    before inner for nested calls). Empty input and unparseable source both
    degrade to an empty result — mirroring ``extract_tables_from_python``,
    parse failures are logged, never raised.
    """

    sites: Tuple[PythonTableSite, ...]
