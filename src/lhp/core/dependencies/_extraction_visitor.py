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
function's body exactly the way codegen applies them at runtime (see
:func:`~lhp.core.dependencies._call_resolution.compute_seed_bindings`).
Binding is never speculative — a signature mismatch binds nothing.

Names the scope stack cannot answer fall back to the
:class:`~lhp.core.dependencies._call_resolution.CallResolutionEngine`: a
parameter of an enclosing function resolves to the union of the values its
call sites pass, and a bare-name user-function call folds to the union of
its return expressions — both memoized, cycle-guarded, and conservative
(unresolvable stays unresolvable, keeping the advisory alive).

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
from ._bindings import Bound, ParameterBindings, merge_bound
from ._bound_folding import binding_updates
from ._call_resolution import CallResolutionEngine, compute_seed_bindings
from ._function_index import FunctionIndex, FunctionNode
from ._static_resolution import CallResolver, resolve_static_string_values
from ._table_sites import PythonTableSite, SiteKind, SourceSpan
from .sql_extraction import extract_tables_from_sql

if TYPE_CHECKING:
    from .python_parser import PythonParser


@dataclass
class _Scope:
    """A lexical scope: module body, function body, or class body.

    ``bindings`` maps a variable name to the
    :data:`~lhp.core.dependencies._bindings.Bound` value it carries, merging
    rebindings via :func:`~lhp.core.dependencies._bindings.merge_bound`
    (string-set rebindings union; structured rebindings are
    last-write-wins)::

        tbl = "a"                 # {"a"}
        tbl = "b"                 # {"a", "b"}  (union)
    """

    kind: Literal["module", "function", "class"]
    bindings: Dict[str, Bound] = field(default_factory=dict)

    def bind(self, name: str, value: Bound) -> None:
        """Merge or replace the binding for ``name`` (see class docstring)."""
        self.bindings[name] = merge_bound(self.bindings.get(name), value)


class _TableExtractor(ast.NodeVisitor):
    """Scope-aware AST visitor that collects Spark table references.

    Resolves variable-bound names against a lexical scope stack (module +
    nested function scopes). Class bodies push a scope but that scope is not
    visible to methods inside the class — this mirrors Python's actual
    scoping rules (methods see enclosing function/module scopes but not the
    class body scope).

    ``function_index`` optionally supplies a pre-built (cached)
    :class:`~lhp.core.dependencies._function_index.FunctionIndex` for the
    SAME tree the visitor is about to walk; when absent one is built in
    ``visit_Module``.

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
        function_index: Optional[FunctionIndex] = None,
    ) -> None:
        self._parser = parser
        self._bindings = bindings
        self._prebuilt_index = function_index
        self._seed_target: Optional[ast.FunctionDef] = None
        self._seed_bindings: Dict[str, Bound] = {}
        self._engine: Optional[CallResolutionEngine] = None
        self._scopes: List[_Scope] = [_Scope(kind="module")]
        self._func_stack: List[FunctionNode] = []
        self.tables: Set[str] = set()
        self.warnings: List[DependencyWarning] = []
        self.sites: List[PythonTableSite] = []

    @property
    def resolution_exhausted(self) -> bool:
        """Whether the engine spent its guard-hit budget during the walk.

        Diagnostic only: reads that lost resolution to exhaustion already
        degraded to opaque (advisory-carrying) results.
        """
        return self._engine is not None and self._engine.exhausted

    # ---- Scope management & parameter-binding seeding ----

    def visit_Module(self, node: ast.Module) -> None:
        """Build the call-resolution engine, seed lookup, then walk the body."""
        self._seed_target, self._seed_bindings = compute_seed_bindings(
            node, self._bindings
        )
        index = self._prebuilt_index or FunctionIndex(node)
        self._engine = CallResolutionEngine(
            node,
            index,
            seed_target=self._seed_target,
            seed_bindings=self._seed_bindings,
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
            for name, bound in self._seed_bindings.items():
                scope.bind(name, bound)
        self._scopes.append(scope)
        if kind == "function":
            self._func_stack.append(node)  # type: ignore[arg-type]
        self.generic_visit(node)
        if kind == "function":
            self._func_stack.pop()
        self._scopes.pop()

    # ---- Variable bindings ----

    def visit_Assign(self, node: ast.Assign) -> None:
        """Bind ``a = "x"``, ``a = {...}`` / ``[...]``, ``a = b = ...``, ``a, b = "x", "y"``."""
        self._apply_binding_updates(node)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """Collect bindings from ``a: str = "x"`` (annotation-only skipped)."""
        self._apply_binding_updates(node)
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        """Statically unroll ``for t in <static iterable>`` loops.

        When the iterable folds to a static element value set (an ordered
        string list, a dict's keys, an allowlisted builtin collection call,
        or a folded user-function return) and the loop target is a plain
        ``ast.Name``, the target binds to the union of all iterations'
        values. Loops do NOT create a new scope — matching Python semantics
        — so the binding lands in the current scope before the body is
        visited.
        """
        self._apply_binding_updates(node)
        self.generic_visit(node)

    def _apply_binding_updates(self, stmt: ast.stmt) -> None:
        """Apply the shared statement-level binding replay to the current scope.

        Binding semantics live once in
        :func:`~lhp.core.dependencies._bound_folding.binding_updates` —
        shared with the engine's environment replay so the two can never
        drift.
        """
        for name, bound in binding_updates(
            stmt, self._resolve_name, call_resolver=self._call_resolver()
        ):
            self._current_scope().bind(name, bound)

    # ---- Call extraction ----

    def visit_Call(self, node: ast.Call) -> None:
        if self._is_spark_sql_call(node):
            self._extract_from_spark_sql(node)
        else:
            matched, values = self._parser._extract_table_from_call(
                node,
                name_resolver=self._resolve_name,
                call_resolver=self._call_resolver(),
            )
            # An empty string can never be a real table reference — it is
            # the artifact of defaults like `p.get("tbl") or ""`. Dropping
            # it keeps phantom externals out; if nothing remains the read
            # is effectively opaque and the advisory path applies.
            values = frozenset(v for v in values if v)
            if matched and values:
                self.tables.update(values)
                self._record_site("table_read", node, values)
            elif matched:
                self._warn_opaque(node)
                self._record_opaque_table_read_site(node)
        self.generic_visit(node)

    def _is_spark_sql_call(self, node: ast.Call) -> bool:
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
            and self._parser._is_spark_object(node.func.value)
        )

    def _extract_from_spark_sql(self, node: ast.Call) -> None:
        """Resolve the ``spark.sql(...)`` argument and extract its tables.

        The argument resolves through the full scope + parameter-binding +
        call-resolution machinery; each resolved SQL string flows through the
        sqlglot-based
        :func:`lhp.core.dependencies.sql_extraction.extract_tables_from_sql`.
        An unresolvable argument emits ONE LHP-DEP-002 advisory; an
        unparseable resolved SQL string propagates the extractor's single
        LHP-DEP-003 advisory instead, re-stamped with this call's Python line
        number (the extractor's own ``line`` points at a position *inside*
        the SQL string and is meaningless at file level).
        """
        sql_values = (
            resolve_static_string_values(
                node.args[0], self._resolve_name, call_resolver=self._call_resolver()
            )
            if node.args
            else frozenset()
        )
        if not sql_values:
            self._warn_opaque(node)
            arg = node.args[0] if node.args else None
            if isinstance(arg, ast.JoinedStr):
                self._record_dynamic_fstring_site(node)
            else:
                self._record_opaque_spark_sql_site(node)
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

    def _record_dynamic_fstring_site(self, node: ast.Call) -> None:
        """Record a dynamic ``spark.sql(f"...")`` body as a rewrite site.

        Fires only for an ``ast.JoinedStr`` argument that did NOT statically
        resolve — its literal segments may still name in-scope tables the
        sandbox rewriter can rename in place. Purely additive: the caller has
        already emitted the LHP-DEP-002 advisory and no table was extracted,
        so the dependency outputs (``tables`` / ``warnings``) are unchanged;
        only ``sites`` (read solely by the sandbox path) grows. Other opaque
        forms (calls, bare names) carry no literal text and are not recorded.
        """
        arg = node.args[0] if node.args else None
        if isinstance(arg, ast.JoinedStr):
            self.sites.append(
                PythonTableSite(
                    kind="spark_sql",
                    rewritable=False,
                    lineno=node.lineno,
                    span=SourceSpan.of_node(arg),
                    fstring=True,
                )
            )

    def _record_opaque_table_read_site(self, node: ast.Call) -> None:
        """Record a recognized-but-opaque table read as a shimmable site.

        Fires for a recognized read API whose table argument neither is a plain
        literal nor statically resolves (a bare name, call result, or subscript
        on a dynamic key). The sandbox pass wraps such a site in the runtime
        ``__lhp_sandbox_table(...)`` shim, so the ARGUMENT node's outer span is
        captured. Purely additive: the caller has already emitted the
        LHP-DEP-002 advisory and no table was extracted, so the dependency
        outputs (``tables`` / ``warnings``) are unchanged; only ``sites`` grows.
        A call with no positional argument (e.g. a keyword-passed name or a bare
        ``.load()``) has nothing to wrap and is not recorded.
        """
        if not node.args:
            return
        self.sites.append(
            PythonTableSite(
                kind="table_read",
                rewritable=False,
                lineno=node.lineno,
                span=SourceSpan.of_node(node.args[0]),
                opaque=True,
            )
        )

    def _record_opaque_spark_sql_site(self, node: ast.Call) -> None:
        """Record a non-f-string opaque ``spark.sql(...)`` as an advisory site.

        Fires when the argument is neither statically resolvable nor an
        ``ast.JoinedStr`` (a bare name, call result, or unresolved
        concatenation): its SQL — and any table it names — is only known at
        runtime, so the sandbox pass can neither rewrite nor verify it and emits
        an LHP-VAL-067 advisory. Purely additive: LHP-DEP-002 was already
        emitted and no table extracted, so dependency outputs are unchanged;
        only ``sites`` grows. A ``spark.sql()`` with no argument is not recorded
        (nothing to advise on). No span is captured — the opaque SQL text is not
        a rewritable name argument.
        """
        if not node.args:
            return
        self.sites.append(
            PythonTableSite(
                kind="spark_sql",
                rewritable=False,
                lineno=node.lineno,
                opaque=True,
            )
        )

    def _warn_opaque(self, node: ast.Call) -> None:
        """Record an LHP-DEP-002 advisory for a recognized-but-opaque read.

        The message names both the call head and the exact argument
        expression that failed to resolve, so the advisory points at the
        thing to fix (or to cover with ``depends_on``).
        """
        call_repr = ast.unparse(node.func)
        if node.args:
            arg_repr = ast.unparse(node.args[0])
            message = (
                f"Cannot statically resolve the table argument of "
                f"`{call_repr}(...)` — the value of `{arg_repr}` is only "
                f"known at runtime."
            )
        else:
            message = (
                f"Cannot statically resolve the table argument of "
                f"`{call_repr}(...)` — its value is only known at runtime."
            )
        self.warnings.append(
            DependencyWarning(
                code=DEP_002.code,
                message=message,
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

    # ---- Name & call resolution ----

    def _resolve_name(self, name: str) -> Optional[Bound]:
        """Walk the scope stack innermost → outermost, skipping class scopes.

        Class bodies exist on the stack for correctness of scope push/pop,
        but methods cannot transparently see class-body bindings — this
        matches Python's lexical rules. At each FUNCTION frame the lookup is
        local bindings first, then the frame's declared parameters: a
        parameter resolves through the engine to the union of the values the
        function's call sites pass (inter-procedural propagation), and it
        SHADOWS every outer binding even when that union is unknown —
        falling through to a same-named module/outer value the runtime would
        never see fabricates resolutions and silently suppresses the
        LHP-DEP-002 advisory.
        """
        func_idx = len(self._func_stack) - 1
        for scope in reversed(self._scopes):
            if scope.kind == "class":
                continue
            if name in scope.bindings:
                return scope.bindings[name]
            if scope.kind == "function":
                func = self._func_stack[func_idx]
                func_idx -= 1
                if _is_parameter_of(func, name):
                    if self._engine is not None:
                        return self._engine.resolve_param(func, name)
                    return None
        return None

    def _call_resolver(self) -> CallResolver:
        """A return-value resolver scoped to the visitor's current function chain."""
        if self._engine is None:
            return None
        return self._engine.call_resolver_for(tuple(self._func_stack))

    def _current_scope(self) -> _Scope:
        return self._scopes[-1]


def _is_parameter_of(func: FunctionNode, name: str) -> bool:
    """Whether ``name`` is any declared parameter of ``func`` (incl. *args/**kwargs)."""
    args = func.args
    if any(a.arg == name for a in (*args.posonlyargs, *args.args, *args.kwonlyargs)):
        return True
    return (args.vararg is not None and args.vararg.arg == name) or (
        args.kwarg is not None and args.kwarg.arg == name
    )
