"""Python parser utility for extracting Spark table references from Python code."""

import ast
import logging
from dataclasses import dataclass, field
from typing import Callable, FrozenSet, List, Literal, Optional, Set

from ...utils.sql_parser import extract_tables_from_sql
from ._static_resolution import resolve_static_string_values

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
class _Binding:
    """A resolved value for a variable within a scope.

    ``values`` is a frozenset because a single name may map to multiple
    literals when the user reassigns or conditionally branches::

        tbl = "a"                 # values = {"a"}
        tbl = "b"                 # values = {"a", "b"}  (union)
        if cond:
            tbl = "c"             # values = {"a", "b", "c"}
    """

    values: FrozenSet[str]


@dataclass
class _Scope:
    """A lexical scope: module body, function body, or class body."""

    kind: Literal["module", "function", "class"]
    bindings: dict = field(default_factory=dict)

    def bind(self, name: str, new_values: FrozenSet[str]) -> None:
        """Merge ``new_values`` into the binding for ``name`` (union semantics)."""
        existing = self.bindings.get(name)
        merged = (existing.values | new_values) if existing else new_values
        self.bindings[name] = _Binding(values=merged)


class _TableExtractor(ast.NodeVisitor):
    """Scope-aware AST visitor that collects Spark table references.

    Resolves variable-bound names against a lexical scope stack (module +
    nested function scopes). Class bodies push a scope but that scope is not
    visible to methods inside the class — this mirrors Python's actual
    scoping rules (methods see enclosing function/module scopes but not the
    class body scope).

    Usage::

        tree = ast.parse(code)
        extractor = _TableExtractor(parser)
        extractor.visit(tree)
        tables = extractor.tables
    """

    def __init__(self, parser: "PythonParser") -> None:
        self._parser = parser
        # Seeded with a module-level scope.
        self._scopes: List[_Scope] = [_Scope(kind="module")]
        self.tables: Set[str] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._in_scope("function", node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._in_scope("function", node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._in_scope("class", node)

    def _in_scope(self, kind: str, node: ast.AST) -> None:
        self._scopes.append(_Scope(kind=kind))
        self.generic_visit(node)
        self._scopes.pop()

    def visit_Assign(self, node: ast.Assign) -> None:
        """Collect bindings from ``a = "x"``, ``a = b = "x"``, ``a, b = "x", "y"``."""
        rhs_values = self._evaluate_rhs(node.value)

        for target in node.targets:
            # Tuple / list unpacking with parallel literal RHS.
            if (
                isinstance(target, (ast.Tuple, ast.List))
                and isinstance(node.value, (ast.Tuple, ast.List))
                and len(target.elts) == len(node.value.elts)
            ):
                for sub_target, sub_value in zip(
                    target.elts, node.value.elts, strict=False
                ):
                    sub_values = self._evaluate_rhs(sub_value)
                    if sub_values and isinstance(sub_target, ast.Name):
                        self._current_scope().bind(sub_target.id, sub_values)
            elif rhs_values:
                self._bind_target(target, rhs_values)

        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """Collect bindings from ``a: str = "x"`` (annotation-only skipped)."""
        if node.value is None:
            self.generic_visit(node)
            return

        rhs_values = self._evaluate_rhs(node.value)
        if rhs_values:
            self._bind_target(node.target, rhs_values)
        self.generic_visit(node)

    def _bind_target(self, target: ast.expr, values: FrozenSet[str]) -> None:
        """Bind ``values`` to ``target`` when it's a simple name.

        Non-``Name`` targets (attribute assignments, subscript assignments,
        unmatched tuple unpacking) are intentionally dropped — tracking those
        would require tracking object shape, which is out of scope.
        """
        if isinstance(target, ast.Name):
            self._current_scope().bind(target.id, values)

    def _evaluate_rhs(self, node: ast.expr) -> FrozenSet[str]:
        """Return the set of literal string values ``node`` could be.

        Delegates to :func:`resolve_static_string_values`, which handles the
        forms the parser can reason about statically:
          - ``"literal"`` (ast.Constant with str value)
          - ``f"..."`` (ast.JoinedStr — rendered via render_f_string)
          - ``"a" + "b"`` (ast.BinOp string concatenation, all operands static)
          - ``"{}.{}".format(a, b)`` (.format() chains, all operands static)
          - a previously bound ``ast.Name`` (resolved through the scope stack)

        Returns an empty set for anything else (function calls returning
        unknowns, subscripts). The extractor never speculates past what's
        literally visible in the source: if any operand is dynamic, the whole
        expression is left unresolved.
        """
        return resolve_static_string_values(node, name_resolver=self._resolve_name)

    def visit_Call(self, node: ast.Call) -> None:
        values = self._parser._extract_table_from_call(
            node, name_resolver=self._resolve_name
        )
        if values:
            self.tables.update(values)
        self.generic_visit(node)

    def _resolve_name(self, name: str) -> FrozenSet[str]:
        """Walk the scope stack innermost → outermost, skipping class scopes.

        Class bodies exist on the stack for correctness of scope push/pop,
        but methods cannot transparently see class-body bindings — this
        matches Python's lexical rules.
        """
        for scope in reversed(self._scopes):
            if scope.kind == "class":
                continue
            if name in scope.bindings:
                return scope.bindings[name].values
        return frozenset()

    def _current_scope(self) -> _Scope:
        return self._scopes[-1]


class PythonParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def extract_tables_from_python(self, python_code: str) -> List[str]:
        if not python_code or not isinstance(python_code, str):
            return []

        self.logger.debug(
            f"Extracting table references from Python code ({len(python_code)} chars)"
        )
        tables = set()

        sql_queries = self.extract_sql_from_python(python_code)
        for sql_query in sql_queries:
            tables.update(extract_tables_from_sql(sql_query))

        direct_tables = self._extract_direct_table_references(python_code)
        tables.update(direct_tables)

        self.logger.debug(f"Found {len(tables)} table reference(s) in Python code")
        return sorted(tables)

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

    def _extract_direct_table_references(self, python_code: str) -> Set[str]:
        """Scope-aware traversal: variable bindings are tracked through the scope stack
        so that patterns like ``tbl = "cat.sch.t"; spark.read.table(tbl)`` resolve correctly.
        """
        try:
            normalized_code = self._normalize_python_code(python_code)
            tree = ast.parse(normalized_code)
        except Exception:
            self.logger.exception("Error extracting direct table references")
            return set()

        extractor = _TableExtractor(self)
        extractor.visit(tree)
        return extractor.tables

    def _extract_sql_from_call(self, node: ast.Call) -> str:
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
            and self._is_spark_object(node.func.value)
        ):
            return self._get_string_argument(node, 0)

        if isinstance(node.func, ast.Attribute) and node.func.attr in [
            "createOrReplaceTempView",
            "createGlobalTempView",
        ]:
            pass

        return None

    def _extract_table_from_call(
        self,
        node: ast.Call,
        name_resolver: Optional[Callable[[str], FrozenSet[str]]] = None,
    ) -> FrozenSet[str]:
        """Extract table reference(s) from a function call node.

        Returns a frozenset of possible table names (empty when the call is
        not a recognized Spark table reference or the argument can't be
        resolved). A set is used because variable-bound arguments can carry
        multiple possible values when the user reassigns or conditionally
        branches.
        """
        # ``spark.table(...)`` — bare table accessor on the spark session.
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "table"
            and self._is_spark_object(node.func.value)
        ):
            return self._get_string_argument_values(node, 0, name_resolver)

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
            return self._get_string_argument_values(node, 0, name_resolver)

        # ``spark.read.format(fmt).table/load(...)`` and the ``readStream``
        # variant. Only matches when ``fmt`` is a recognized relational-table
        # format (see ``_INTERNAL_TABLE_FORMATS``); ``cloudFiles`` and custom
        # DataSource names fall through and stay external.
        format_chain_values = self._extract_table_from_format_chain(node, name_resolver)
        if format_chain_values:
            return format_chain_values

        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "catalog"
            and self._is_spark_object(node.func.value.value)
            and node.func.attr in ["tableExists", "dropTempView", "listTables"]
        ):
            if node.func.attr in ["tableExists", "dropTempView"]:
                return self._get_string_argument_values(node, 0, name_resolver)

        return frozenset()

    def _extract_table_from_format_chain(
        self,
        node: ast.Call,
        name_resolver: Optional[Callable[[str], FrozenSet[str]]] = None,
    ) -> FrozenSet[str]:
        """Recognize ``spark.read.format(fmt).table/load("c.s.t")`` chains.

        Handles both ``read`` and ``readStream`` receivers. Returns the
        resolved table reference(s) only when ``fmt`` statically resolves to a
        single value in :data:`_INTERNAL_TABLE_FORMATS` (case-insensitive) and
        the terminal ``.table()`` / ``.load()`` argument is itself statically
        resolvable. ``cloudFiles`` (Auto Loader) and custom DataSource names
        are not in the allowlist, so those reads return ``frozenset()`` and
        remain external — preserving the prior behavior.
        """
        # Terminal accessor: ``.table(arg)`` or ``.load(arg)``.
        if not (
            isinstance(node.func, ast.Attribute) and node.func.attr in ("table", "load")
        ):
            return frozenset()

        # Receiver must be a ``.format(fmt)`` call.
        format_call = node.func.value
        if not (
            isinstance(format_call, ast.Call)
            and isinstance(format_call.func, ast.Attribute)
            and format_call.func.attr == "format"
        ):
            return frozenset()

        # The ``.format`` receiver must be ``spark.read`` or ``spark.readStream``.
        reader = format_call.func.value
        if not (
            isinstance(reader, ast.Attribute)
            and reader.attr in ("read", "readStream")
            and self._is_spark_object(reader.value)
        ):
            return frozenset()

        # The format string must statically resolve to a single allowlisted
        # relational-table format. Anything else (cloudFiles, custom DataSource
        # names, file formats) is external — never speculate.
        format_values = self._get_string_argument_values(format_call, 0, name_resolver)
        if not (
            len(format_values) == 1
            and next(iter(format_values)).lower() in _INTERNAL_TABLE_FORMATS
        ):
            return frozenset()

        # ``.load()`` with no argument (custom DataSource style) yields nothing.
        return self._get_string_argument_values(node, 0, name_resolver)

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
        name_resolver: Optional[Callable[[str], FrozenSet[str]]] = None,
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
        values = self._get_string_argument_values(node, arg_index, name_resolver)
        if len(values) == 1:
            return next(iter(values))
        return None

    def _get_string_argument_values(
        self,
        node: ast.Call,
        arg_index: int,
        name_resolver: Optional[Callable[[str], FrozenSet[str]]] = None,
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

        resolved = resolve_static_string_values(arg, name_resolver)
        if not resolved and isinstance(arg, ast.Name):
            self.logger.debug(
                f"Variable reference '{arg.id}' in spark call — "
                "unresolved in current scope"
            )
        return resolved

    def _normalize_python_code(self, python_code: str) -> str:
        """Remove common indentation so indented code blocks (e.g. in test strings) parse cleanly."""
        if not python_code or not isinstance(python_code, str):
            return ""

        import textwrap

        return textwrap.dedent(python_code).strip()


def extract_tables_from_python(python_code: str) -> List[str]:
    parser = PythonParser()
    return parser.extract_tables_from_python(python_code)


def extract_sql_from_python(python_code: str) -> List[str]:
    parser = PythonParser()
    return parser.extract_sql_from_python(python_code)
