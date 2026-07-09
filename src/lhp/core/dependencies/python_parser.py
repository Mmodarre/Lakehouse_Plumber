"""Python parser utility for extracting Spark table references from Python code.

Owns the read-API allowlist (which calls count as internal-table reads) and
the extraction entry points. The scope-aware AST traversal — variable
bindings, YAML parameter-binding seeding, static loop unrolling, ``spark.sql``
resolution and the LHP-DEP-002 opaque-read advisories — lives in
:mod:`lhp.core.dependencies._extraction_visitor`.
"""

import ast
import logging
from dataclasses import dataclass
from typing import FrozenSet, List, Optional, Tuple

from ...models.dependencies import DependencyWarning
from ._bindings import ParameterBindings
from ._extraction_visitor import _TableExtractor
from ._function_index import FunctionIndex
from ._static_resolution import (
    CallResolver,
    NameResolver,
    resolve_static_string_values,
)
from ._table_sites import PythonTableSitesResult

# Spark DataFrameReader ``.format(...)`` values that denote a relational table
# read (``spark.read.format(fmt).table/load("cat.sch.t")``). Anything outside
# this allowlist — most importantly ``cloudFiles`` (Auto Loader) and the short
# name a custom Python DataSource registers — is a genuine external root and
# must NOT be surfaced as an internal table reference. The set is matched
# case-insensitively.
_INTERNAL_TABLE_FORMATS: FrozenSet[str] = frozenset(
    {"delta", "iceberg", "hive", "unity_catalog"}
)


@dataclass(frozen=True)
class PythonExtractionResult:
    """Result of table extraction from one Python body.

    ``tables`` is deduplicated and sorted.
    ``warnings`` carries LHP-DEP-002 advisories for recognized read calls
    whose table argument is only known at runtime; ``flowgroup`` / ``action``
    are blank here and stamped later by the source parser.
    """

    tables: List[str]
    warnings: List[DependencyWarning]


class PythonParser:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def extract_tables_from_python(
        self,
        python_code: str,
        *,
        bindings: Optional[ParameterBindings] = None,
        parsed: "Optional[Tuple[ast.Module, FunctionIndex]]" = None,
    ) -> PythonExtractionResult:
        """Extract internal-table references from a Python body.

        ``bindings`` optionally seeds the scope of one module-level function
        with the statically-known YAML parameter values codegen would apply
        at runtime (see :class:`~lhp.core.dependencies._bindings.ParameterBindings`).
        ``parsed`` optionally supplies the pre-parsed
        ``(tree, function_index)`` pair for this exact (normalized) code —
        the per-run parse cache's seam; both are bindings-independent, so
        one parse serves every action referencing the same helper file.
        """
        if not python_code or not isinstance(python_code, str):
            return PythonExtractionResult(tables=[], warnings=[])

        self.logger.debug(
            f"Extracting table references from Python code ({len(python_code)} chars)"
        )
        if parsed is not None:
            tree, function_index = parsed
        else:
            try:
                tree = ast.parse(self._normalize_python_code(python_code))
            except SyntaxError as e:
                self.logger.warning(f"Could not parse Python code: {e}")
                return PythonExtractionResult(tables=[], warnings=[])
            except Exception:
                self.logger.exception("Error extracting table references from Python")
                return PythonExtractionResult(tables=[], warnings=[])
            function_index = None

        extractor = _TableExtractor(
            self, bindings=bindings, function_index=function_index
        )
        extractor.visit(tree)

        if extractor.resolution_exhausted:
            self.logger.debug(
                "Inter-procedural resolution budget exhausted for this body; "
                "remaining reads degraded to unresolved (LHP-DEP-002 advisories)"
            )
        self.logger.debug(
            f"Found {len(extractor.tables)} table reference(s) in Python code"
        )
        return PythonExtractionResult(
            tables=sorted(extractor.tables), warnings=list(extractor.warnings)
        )

    def collect_table_sites(
        self,
        python_code: str,
        *,
        bindings: Optional[ParameterBindings] = None,
    ) -> PythonTableSitesResult:
        """Record every recognized table-consuming call site with rewrite metadata.

        Same recognition, scope resolution and ``bindings`` seeding as
        :meth:`extract_tables_from_python`, but the source is parsed VERBATIM
        — no dedent / strip normalization — so each recorded
        :class:`~lhp.core.dependencies._table_sites.SourceSpan` indexes
        byte-for-byte into ``python_code`` exactly as given, which is the
        contract a rewriter needs. Consequence: indented snippets that only
        parse after dedenting yield an empty result here while still
        extracting via :meth:`extract_tables_from_python`.

        Failure behavior mirrors :meth:`extract_tables_from_python`: empty /
        non-string input and unparseable source degrade to an empty result
        (the SyntaxError is logged as a warning, never raised).
        """
        if not python_code or not isinstance(python_code, str):
            return PythonTableSitesResult(sites=())

        try:
            tree = ast.parse(python_code)
        except SyntaxError as e:
            self.logger.warning(f"Could not parse Python code: {e}")
            return PythonTableSitesResult(sites=())
        except Exception:
            self.logger.exception("Error collecting table sites from Python")
            return PythonTableSitesResult(sites=())

        extractor = _TableExtractor(self, bindings=bindings)
        extractor.visit(tree)
        return PythonTableSitesResult(sites=tuple(extractor.sites))

    def extract_sql_from_python(self, python_code: str) -> List[str]:
        sql_queries = []

        try:
            normalized_code = self._normalize_python_code(python_code)
            tree = ast.parse(normalized_code)

            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    sql_query = self._extract_sql_from_call(node)
                    if sql_query:
                        sql_queries.append(sql_query)

        except SyntaxError as e:
            self.logger.warning(f"Could not parse Python code: {e}")
        except Exception:
            self.logger.exception("Error extracting SQL from Python")

        return sql_queries

    def _extract_sql_from_call(self, node: ast.Call) -> Optional[str]:
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
            and self._is_spark_object(node.func.value)
        ):
            return self._get_string_argument(node, 0)

        return None

    def _extract_table_from_call(
        self,
        node: ast.Call,
        name_resolver: NameResolver = None,
        *,
        call_resolver: CallResolver = None,
    ) -> Tuple[bool, FrozenSet[str]]:
        """Recognize a Spark read call and extract its table reference(s).

        Returns ``(matched, values)``. ``matched`` is True when the call is a
        recognized internal-table read API; ``values`` is the set of possible
        table names (multi-valued when a variable-bound argument carries
        several candidates). ``(True, frozenset())`` means "read API matched
        but the argument is opaque" — the visitor turns that into an
        LHP-DEP-002 advisory — whereas ``(False, frozenset())`` is simply
        "not a read API at all".
        """
        # ``spark.table(...)`` — bare table accessor on the spark session.
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "table"
            and self._is_spark_object(node.func.value)
        ):
            return True, self._get_string_argument_values(
                node, 0, name_resolver, call_resolver=call_resolver
            )

        # ``spark.read.table(...)`` and ``spark.readStream.table(...)`` — the
        # ``.table`` accessor on a DataFrameReader / DataStreamReader. Also
        # accepts an aliased reader receiver (``self.spark.read.table``) via
        # ``_is_spark_object`` on the innermost node.
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "table"
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr in ("read", "readStream")
            and self._is_spark_object(node.func.value.value)
        ):
            return True, self._get_string_argument_values(
                node, 0, name_resolver, call_resolver=call_resolver
            )

        # ``spark.read.format(fmt).table/load(...)`` and the ``readStream``
        # variant. Only matches when ``fmt`` is a recognized relational-table
        # format (see ``_INTERNAL_TABLE_FORMATS``); ``cloudFiles`` and custom
        # DataSource names fall through and stay external.
        matched, format_chain_values = self._extract_table_from_format_chain(
            node, name_resolver, call_resolver=call_resolver
        )
        if matched:
            return True, format_chain_values

        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "catalog"
            and self._is_spark_object(node.func.value.value)
            and node.func.attr in ["tableExists", "dropTempView"]
        ):
            return True, self._get_string_argument_values(
                node, 0, name_resolver, call_resolver=call_resolver
            )

        return False, frozenset()

    def _extract_table_from_format_chain(
        self,
        node: ast.Call,
        name_resolver: NameResolver = None,
        *,
        call_resolver: CallResolver = None,
    ) -> Tuple[bool, FrozenSet[str]]:
        """Recognize ``spark.read.format(fmt).table/load("c.s.t")`` chains.

        Handles both ``read`` and ``readStream`` receivers. The chain only
        *matches* when ``fmt`` statically resolves to a single value in
        :data:`_INTERNAL_TABLE_FORMATS` (case-insensitive) — ``cloudFiles``
        (Auto Loader), custom DataSource names, file formats, and dynamic
        format values are NOT matched, so those reads remain external (no
        table, no advisory) — preserving the prior behavior. A matched chain
        whose terminal ``.table()`` / ``.load()`` argument cannot be resolved
        (including a bare ``.load()``) returns ``(True, frozenset())`` —
        opaque.
        """
        # Terminal accessor: ``.table(arg)`` or ``.load(arg)``.
        if not (
            isinstance(node.func, ast.Attribute) and node.func.attr in ("table", "load")
        ):
            return False, frozenset()

        # Receiver must be a ``.format(fmt)`` call.
        format_call = node.func.value
        if not (
            isinstance(format_call, ast.Call)
            and isinstance(format_call.func, ast.Attribute)
            and format_call.func.attr == "format"
        ):
            return False, frozenset()

        # The ``.format`` receiver must be ``spark.read`` or ``spark.readStream``.
        reader = format_call.func.value
        if not (
            isinstance(reader, ast.Attribute)
            and reader.attr in ("read", "readStream")
            and self._is_spark_object(reader.value)
        ):
            return False, frozenset()

        # The format string must statically resolve to a single allowlisted
        # relational-table format. Anything else (cloudFiles, custom DataSource
        # names, file formats) is external — never speculate.
        format_values = self._get_string_argument_values(
            format_call, 0, name_resolver, call_resolver=call_resolver
        )
        if not (
            len(format_values) == 1
            and next(iter(format_values)).lower() in _INTERNAL_TABLE_FORMATS
        ):
            return False, frozenset()

        return True, self._get_string_argument_values(
            node, 0, name_resolver, call_resolver=call_resolver
        )

    def _is_spark_object(self, node: ast.AST) -> bool:
        """Check if an AST node represents a spark object."""
        if isinstance(node, ast.Name) and node.id == "spark":
            return True
        if isinstance(node, ast.Attribute) and node.attr == "spark":
            return True
        return False

    def _get_string_argument(
        self,
        node: ast.Call,
        arg_index: int,
        name_resolver: NameResolver = None,
        *,
        call_resolver: CallResolver = None,
    ) -> Optional[str]:
        """Extract a string argument from a function call at ``arg_index``.

        When ``name_resolver`` is supplied and the argument is an ``ast.Name``,
        the resolver is consulted to look up the variable's bindings in the
        current lexical scope; if it yields exactly one value, that value is
        returned. For multi-valued bindings, callers should use
        :meth:`_get_string_argument_values` instead — this entry point keeps
        its ``Optional[str]`` shape for the SQL-extraction path.

        Returns ``None`` when:
          - the argument is missing,
          - the argument is a non-string literal,
          - the argument is an ``ast.Name`` and no resolver is supplied (or the
            resolver yields zero or multiple values).
        """
        values = self._get_string_argument_values(
            node, arg_index, name_resolver, call_resolver=call_resolver
        )
        if len(values) == 1:
            return next(iter(values))
        return None

    def _get_string_argument_values(
        self,
        node: ast.Call,
        arg_index: int,
        name_resolver: NameResolver = None,
        *,
        call_resolver: CallResolver = None,
    ) -> FrozenSet[str]:
        """Extract all possible string values for the ``arg_index`` argument.

        Returns ``frozenset()`` when the argument cannot be resolved. When
        ``name_resolver`` is supplied and the argument is an ``ast.Name``, the
        resolver is used to look up the variable's possible bindings — this is
        how the scope-aware visitor resolves patterns like ``spark.table(tbl)``.
        """
        if len(node.args) <= arg_index:
            return frozenset()

        arg = node.args[arg_index]

        # Name reference with no resolver (the SQL-extraction path): keep the
        # historical debug message rather than silently returning empty.
        if isinstance(arg, ast.Name) and name_resolver is None:
            self.logger.debug(
                f"Found variable reference '{arg.id}' in SQL call - cannot resolve"
            )
            return frozenset()

        resolved = resolve_static_string_values(
            arg, name_resolver, call_resolver=call_resolver
        )
        if not resolved and isinstance(arg, ast.Name):
            self.logger.debug(
                f"Variable reference '{arg.id}' in spark call — "
                "unresolved in current scope"
            )
        return resolved

    def _normalize_python_code(self, python_code: str) -> str:
        return normalize_python_code(python_code)


def normalize_python_code(python_code: str) -> str:
    """Remove common indentation so indented code blocks (e.g. in test strings) parse cleanly.

    Dedent ONLY — deliberately no ``strip()``: removing leading blank lines
    would shift every AST ``lineno``, mis-anchoring LHP-DEP-002/003 warning
    lines against the real file for any body that starts with blank lines.

    Module-level so the per-run parse cache normalizes exactly the way
    :meth:`PythonParser.extract_tables_from_python` does — the cached tree
    must be byte-for-byte the one that method would have parsed itself.
    """
    if not python_code or not isinstance(python_code, str):
        return ""

    import textwrap

    return textwrap.dedent(python_code)


def extract_tables_from_python(
    python_code: str,
    *,
    bindings: Optional[ParameterBindings] = None,
    parsed: "Optional[Tuple[ast.Module, FunctionIndex]]" = None,
) -> PythonExtractionResult:
    parser = PythonParser()
    return parser.extract_tables_from_python(
        python_code, bindings=bindings, parsed=parsed
    )


def collect_python_table_sites(
    python_code: str,
    *,
    bindings: Optional[ParameterBindings] = None,
) -> PythonTableSitesResult:
    """Collect rewrite-oriented table-site records from a Python body.

    See :meth:`PythonParser.collect_table_sites` for the span contract
    (verbatim parse, AST coordinates with UTF-8 byte columns) and failure
    behavior (unparseable / empty source degrades to an empty result).
    """
    parser = PythonParser()
    return parser.collect_table_sites(python_code, bindings=bindings)


def extract_sql_from_python(python_code: str) -> List[str]:
    parser = PythonParser()
    return parser.extract_sql_from_python(python_code)
