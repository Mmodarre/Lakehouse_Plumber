"""Python parser utility for extracting Spark table references from Python code."""

import ast
import logging
from dataclasses import dataclass, field
from typing import Callable, FrozenSet, List, Literal, Optional, Set

from .sql_parser import extract_tables_from_sql


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

        Handles only forms the parser can reason about statically:
          - ``"literal"`` (ast.Constant with str value)
          - ``f"..."`` (ast.JoinedStr — rendered via _process_f_string)

        Returns an empty set for anything else (BinOp concatenation, function
        calls, subscripts). The extractor never speculates past what's
        literally visible in the source.
        """
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return frozenset({node.value})
        if isinstance(node, ast.JoinedStr):
            rendered = self._parser._process_f_string(node)
            return frozenset({rendered}) if rendered else frozenset()
        return frozenset()

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
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "table"
            and self._is_spark_object(node.func.value)
        ):
            return self._get_string_argument_values(node, 0, name_resolver)

        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "table"
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "read"
            and self._is_spark_object(node.func.value.value)
        ):
            return self._get_string_argument_values(node, 0, name_resolver)

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

        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return frozenset({arg.value})

        if isinstance(arg, ast.JoinedStr):
            rendered = self._process_f_string(arg)
            return frozenset({rendered}) if rendered else frozenset()

        # Name reference — defer to the resolver, if provided.
        if isinstance(arg, ast.Name):
            if name_resolver is not None:
                resolved = name_resolver(arg.id)
                if not resolved:
                    self.logger.debug(
                        f"Variable reference '{arg.id}' in spark call — "
                        "unresolved in current scope"
                    )
                return resolved
            self.logger.debug(
                f"Found variable reference '{arg.id}' in SQL call - cannot resolve"
            )
            return frozenset()

        return frozenset()

    def _process_f_string(self, node: ast.JoinedStr) -> str:
        """Render an f-string as a template, substituting known schema/catalog variable names
        with ``{name}`` and collapsing unknown interpolations to ``{var}``."""
        parts = []

        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                if isinstance(value.value, ast.Name) and value.value.id in [
                    "catalog",
                    "schema",
                    "table",
                    "bronze_schema",
                    "silver_schema",
                    "gold_schema",
                    "migration_schema",
                    "old_schema",
                ]:
                    parts.append(f"{{{value.value.id}}}")
                else:
                    parts.append("{var}")

        return "".join(parts)

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
