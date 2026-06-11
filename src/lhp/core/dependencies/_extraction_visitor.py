"""Scope-aware AST visitor that collects Spark table references.

Split out of :mod:`lhp.core.dependencies.python_parser`: the visitor owns the
lexical scope stack, YAML parameter-binding seeding, static loop unrolling,
``spark.sql(...)`` resolution and the LHP-DEP-002 opaque-read advisories
(plus propagating the SQL extractor's LHP-DEP-003 for unparseable inline
SQL, re-stamped to the Python call's line), while read-API *recognition*
stays on ``PythonParser``.

Scopes can be pre-seeded from a
:class:`~lhp.core.dependencies._bindings.ParameterBindings` instance so the
values a flowgroup YAML declares for a Python function flow into that
function's body exactly the way codegen applies them at runtime (the target
function is looked up the same way codegen does: direct children of
``tree.body``, first match wins). Binding is never speculative — a signature
mismatch binds nothing.

Recognized read calls whose table argument cannot be statically resolved emit
an advisory :class:`~lhp.models.dependencies.DependencyWarning` (LHP-DEP-002);
``flowgroup`` / ``action`` are stamped later by the source parser.

Token byte fidelity is a hard invariant: ``${token}`` substrings inside bound
values flow through every path as exact bytes — never resolved or altered.
"""

import ast
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Literal, Optional, Set

from ...errors.codes import DEP_002
from ...models.dependencies import DependencyWarning
from ._bindings import Bound, DictValue, ParameterBindings
from ._static_resolution import resolve_static_list, resolve_static_string_values
from ._table_sites import PythonTableSite, SiteKind, SourceSpan
from .sql_extraction import extract_tables_from_sql

if TYPE_CHECKING:
    from .python_parser import PythonParser


@dataclass
class _Scope:
    """A lexical scope: module body, function body, or class body.

    ``bindings`` maps a variable name to the
    :data:`~lhp.core.dependencies._bindings.Bound` value it carries. A
    string-set rebinding merges via union — reassignment and conditional
    branches accumulate candidate values::

        tbl = "a"                 # {"a"}
        tbl = "b"                 # {"a", "b"}  (union)

    Any rebinding involving a structured value (``ListValue`` / ``DictValue``
    on either side) is last-write-wins — unioning heterogeneous shapes would
    fabricate values that no execution path produces.
    """

    kind: Literal["module", "function", "class"]
    bindings: Dict[str, Bound] = field(default_factory=dict)

    def bind(self, name: str, value: Bound) -> None:
        """Merge or replace the binding for ``name`` (see class docstring)."""
        existing = self.bindings.get(name)
        if isinstance(existing, frozenset) and isinstance(value, frozenset):
            self.bindings[name] = existing | value
        else:
            self.bindings[name] = value


class _TableExtractor(ast.NodeVisitor):
    """Scope-aware AST visitor that collects Spark table references.

    Resolves variable-bound names against a lexical scope stack (module +
    nested function scopes). Class bodies push a scope but that scope is not
    visible to methods inside the class — this mirrors Python's actual
    scoping rules (methods see enclosing function/module scopes but not the
    class body scope).

    Usage::

        tree = ast.parse(code)
        extractor = _TableExtractor(parser, bindings=bindings)
        extractor.visit(tree)
        tables, warnings = extractor.tables, extractor.warnings
        sites = extractor.sites  # rewrite metadata, see _table_sites.py
    """

    def __init__(
        self,
        parser: "PythonParser",
        bindings: Optional[ParameterBindings] = None,
    ) -> None:
        self._parser = parser
        self._bindings = bindings
        self._seed_target: Optional[ast.FunctionDef] = None
        self._scopes: List[_Scope] = [_Scope(kind="module")]
        self.tables: Set[str] = set()
        self.warnings: List[DependencyWarning] = []
        self.sites: List[PythonTableSite] = []

    # ---- Scope management & parameter-binding seeding ----

    def visit_Module(self, node: ast.Module) -> None:
        """Locate the binding target function, then walk the module body.

        Mirrors codegen's lookup (``_find_function_node`` in
        ``generators/write/snapshot_cdc_source_function.py``): only direct
        children of ``tree.body``, plain ``ast.FunctionDef`` only, FIRST
        match wins.
        """
        bindings = self._bindings
        if bindings is not None:
            self._seed_target = next(
                (
                    child
                    for child in node.body
                    if isinstance(child, ast.FunctionDef)
                    and child.name == bindings.function_name
                ),
                None,
            )
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._in_scope("function", node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._in_scope("function", node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._in_scope("class", node)

    def _in_scope(
        self, kind: Literal["module", "function", "class"], node: ast.AST
    ) -> None:
        scope = _Scope(kind=kind)
        if self._seed_target is not None and node is self._seed_target:
            self._seed_parameter_bindings(self._seed_target, scope)
        self._scopes.append(scope)
        self.generic_visit(node)
        self._scopes.pop()

    def _seed_parameter_bindings(self, node: ast.FunctionDef, scope: _Scope) -> None:
        """Seed ``scope`` from the YAML parameter bindings, mirroring codegen.

        Kwonly style: each entry binds to the function's matching
        keyword-only argument name; leftover entries bind as one
        ``DictValue`` to the ``**kwargs`` name when the function has one,
        else they stay silently unbound. Dict style: the positional
        parameter at ``dict_arg_index`` (within posonly + args) is bound to
        the whole parameters dict; an out-of-range index is a signature
        mismatch → NO binding at all (locked design L2 — never guess).
        """
        bindings = self._bindings
        if bindings is None:
            return
        if bindings.kwonly is not None:
            kwonly_names = {arg.arg for arg in node.args.kwonlyargs}
            leftovers: Dict[str, Bound] = {}
            for key, bound in bindings.kwonly.entries.items():
                if key in kwonly_names:
                    scope.bind(key, bound)
                else:
                    leftovers[key] = bound
            if leftovers and node.args.kwarg is not None:
                scope.bind(node.args.kwarg.arg, DictValue(leftovers))
            return
        if bindings.dict_arg_index is None or bindings.dict_value is None:
            return  # Unreachable per the ParameterBindings invariant.
        positional = [*node.args.posonlyargs, *node.args.args]
        if 0 <= bindings.dict_arg_index < len(positional):
            scope.bind(positional[bindings.dict_arg_index].arg, bindings.dict_value)

    # ---- Variable bindings ----

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

    def visit_For(self, node: ast.For) -> None:
        """Statically unroll ``for t in <static list>`` loops.

        When the iterable resolves to an ordered static string list and the
        loop target is a plain ``ast.Name``, the target is bound to the
        union of all iterations' values. Loops do NOT create a new scope —
        matching Python semantics — so the binding lands in the current
        scope before the body is visited.
        """
        items = resolve_static_list(node.iter, self._resolve_name)
        if items is not None and isinstance(node.target, ast.Name):
            self._current_scope().bind(node.target.id, frozenset(items.items))
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
          - subscript / ``.get`` lookups into dict-bound names

        Returns an empty set for anything else (function calls returning
        unknowns). The extractor never speculates past what's literally
        visible in the source: if any operand is dynamic, the whole
        expression is left unresolved.
        """
        return resolve_static_string_values(node, name_resolver=self._resolve_name)

    # ---- Call extraction ----

    def visit_Call(self, node: ast.Call) -> None:
        if self._is_spark_sql_call(node):
            self._extract_from_spark_sql(node)
        else:
            matched, values = self._parser._extract_table_from_call(
                node, name_resolver=self._resolve_name
            )
            if matched and values:
                self.tables.update(values)
                self._record_site("table_read", node, values)
            elif matched:
                self._warn_opaque(node)
        self.generic_visit(node)

    def _is_spark_sql_call(self, node: ast.Call) -> bool:
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
            and self._parser._is_spark_object(node.func.value)
        )

    def _extract_from_spark_sql(self, node: ast.Call) -> None:
        """Resolve the ``spark.sql(...)`` argument and extract its tables.

        The argument resolves through the full scope + parameter-binding
        machinery; each resolved SQL string flows through the sqlglot-based
        :func:`lhp.core.dependencies.sql_extraction.extract_tables_from_sql`.
        An unresolvable argument emits ONE LHP-DEP-002 advisory; an
        unparseable resolved SQL string propagates the extractor's single
        LHP-DEP-003 advisory instead, re-stamped with this call's Python line
        number (the extractor's own ``line`` points at a position *inside*
        the SQL string and is meaningless at file level).
        """
        sql_values = (
            resolve_static_string_values(node.args[0], self._resolve_name)
            if node.args
            else frozenset()
        )
        if not sql_values:
            self._warn_opaque(node)
            return
        site_tables: Set[str] = set()
        for sql_query in sql_values:
            result = extract_tables_from_sql(sql_query)
            site_tables.update(result.tables)
            self.tables.update(result.tables)
            self.warnings.extend(
                replace(warning, line=node.lineno) for warning in result.warnings
            )
        self._record_site("spark_sql", node, frozenset(site_tables))

    def _record_site(
        self, kind: SiteKind, node: ast.Call, resolved_values: FrozenSet[str]
    ) -> None:
        """Record one resolvable table-consuming call site (additive only).

        Called exactly where extraction already succeeded, so recording can
        never alter the ``tables`` / ``warnings`` outputs. A direct
        ``ast.Constant`` string argument is REWRITABLE — its exact span and
        literal value are captured; anything else that still resolved
        statically is UNREWRITABLE-RESOLVED — only the candidate table names
        and the call's line are captured. Opaque arguments never reach this
        method (they take the ``_warn_opaque`` path instead).
        """
        arg = node.args[0] if node.args else None
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            self.sites.append(
                PythonTableSite(
                    kind=kind,
                    rewritable=True,
                    lineno=node.lineno,
                    span=SourceSpan.of_node(arg),
                    value=arg.value,
                )
            )
        else:
            self.sites.append(
                PythonTableSite(
                    kind=kind,
                    rewritable=False,
                    lineno=node.lineno,
                    resolved_values=resolved_values,
                )
            )

    def _warn_opaque(self, node: ast.Call) -> None:
        """Record an LHP-DEP-002 advisory for a recognized-but-opaque read."""
        call_repr = ast.unparse(node.func)
        self.warnings.append(
            DependencyWarning(
                code=DEP_002.code,
                message=(
                    f"Cannot statically resolve the table argument of "
                    f"`{call_repr}(...)` — its value is only known at runtime."
                ),
                flowgroup="",
                action="",
                suggestion=(
                    "Declare the upstream table explicitly via `depends_on` "
                    "on the action."
                ),
                file_path=None,
                line=node.lineno,
            )
        )

    # ---- Name resolution ----

    def _resolve_name(self, name: str) -> Optional[Bound]:
        """Walk the scope stack innermost → outermost, skipping class scopes.

        Class bodies exist on the stack for correctness of scope push/pop,
        but methods cannot transparently see class-body bindings — this
        matches Python's lexical rules.
        """
        for scope in reversed(self._scopes):
            if scope.kind == "class":
                continue
            if name in scope.bindings:
                return scope.bindings[name]
        return None

    def _current_scope(self) -> _Scope:
        return self._scopes[-1]
