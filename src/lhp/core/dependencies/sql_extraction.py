"""sqlglot-based extraction of table READS from SQL sources.

Parses SQL with sqlglot's Databricks dialect and returns the upstream table
references (reads only — write targets are excluded) as a deduplicated,
sorted list of dotted names.

Byte-fidelity invariant (LOCKED): substitution tokens — ``${env_token}``,
``${secret:scope/key}``, and the deprecated ``{token}`` — are NEVER resolved
at extraction time. Output table names carry the token bytes EXACTLY as
written in the source. Canonicalization (case folding, backtick stripping)
happens only in :mod:`lhp.core.dependencies._canonical` at match time, never
here.

Masking algorithm: substitution tokens are not valid SQL, so before parsing
every token occurrence is replaced with a unique placeholder identifier
``__{salt}_{i}__`` where ``i`` is the occurrence index and ``salt`` is a
deterministic alphanumeric string lengthened (seed ``lhpmask``, append ``x``)
until it appears nowhere in the input — making placeholder collisions
structurally impossible without randomness. After extraction, placeholders in
the output names are substituted back, restoring the original bytes exactly.
``%{local_var}`` is intentionally NOT masked: local variables never reach
``.sql`` files, so their presence fails parsing and surfaces an LHP-DEP-003
advisory, which is correct feedback.

Bare ``$word`` placeholders (the documented ``$source`` of SQL transforms) are
masked AFTER the forms above, with a second, distinguishable placeholder class
``__{salt}drop_{i}__``: any extracted table reference containing one is
DROPPED from the output entirely. The action's declared ``source:`` view
already carries that dependency edge, and a name assembled from a bare-$
placeholder is not statically known. Both classes share one replacements
mapping, so unmasking the full masked text is still the identity function.

Parse failure of the whole body never raises: the result carries zero tables
and exactly one LHP-DEP-003 :class:`~lhp.models.dependencies.DependencyWarning`
suggesting an explicit ``depends_on`` declaration.

Requires sqlglot >= 28, where the Databricks parser began EMITTING the
(pre-existing) :class:`sqlglot.exp.Stream` node for ``FROM STREAM`` /
``stream(...)``; on 27.x and below the same construct parses to an
:class:`sqlglot.exp.Anonymous` inside :class:`sqlglot.exp.Table`, so
``find_all(exp.Stream)`` yields nothing and the mapping below is silently
empty. ``hasattr(exp, "Stream")`` is therefore NOT a usable version probe.
Only the >= 28 shape is supported — deliberately no version-gated or
dual-shape handling, so one LHP version yields one deterministic dependency
graph.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from ...errors.codes import DEP_003
from ...models.dependencies import DependencyWarning

logger = logging.getLogger(__name__)

__all__ = ["SqlExtractionResult", "extract_tables_from_sql"]

# ${secret:scope/key} first (its body may contain non-word chars), then
# ${env_token}, then the deprecated {token} syntax. %{local_var} is excluded
# by design — see the module docstring.
_TOKEN_PATTERN = re.compile(r"\$\{secret:[^}]*\}|\$\{\w+\}|\{\w+\}")
# Bare $word placeholders (e.g. the documented `$source` of SQL transforms).
# Masked AFTER _TOKEN_PATTERN so `${catalog}` is never mis-eaten, and with the
# drop placeholder class — see the module docstring.
_BARE_DOLLAR_PATTERN = re.compile(r"\$\w+")
_SALT_SEED = "lhpmask"

# DLT table wrappers: a function call in FROM position whose name matches
# (case-insensitively) is unwrapped to its first argument.
_DLT_WRAPPERS = frozenset({"stream", "live", "snapshot"})

# Statements whose `this` is a write target, never a read.
_WRITE_STATEMENTS = (
    exp.Insert,
    exp.Create,
    exp.Merge,
    exp.Update,
    exp.Delete,
    exp.Drop,
)


@dataclass(frozen=True)
class SqlExtractionResult:
    """Result of table extraction from one SQL body.

    ``tables`` is deduplicated and sorted, with substitution-token bytes
    intact. ``warnings`` carries at most one
    LHP-DEP-003 advisory (whole-body parse failure); ``flowgroup`` / ``action``
    are blank here and stamped later by the source parser.
    """

    tables: List[str]
    warnings: List[DependencyWarning]


def extract_tables_from_sql(sql: str) -> SqlExtractionResult:
    """Extract upstream table reads from a SQL body (multi-statement aware)."""
    if not sql or not isinstance(sql, str) or not sql.strip():
        return SqlExtractionResult(tables=[], warnings=[])

    masked_sql, replacements, dropped = _mask_tokens(sql)
    try:
        statements = sqlglot.parse(masked_sql, read="databricks")
    except SqlglotError as e:
        logger.debug(f"sqlglot could not parse SQL body: {e}")
        return SqlExtractionResult(tables=[], warnings=[_parse_failure_warning(e)])

    reads: Set[str] = set()
    for statement in statements:
        if statement is None:
            continue
        reads.update(_collect_reads(statement, masked_sql))

    tables = sorted(
        {
            _unmask(name, replacements)
            for name in reads
            if not any(placeholder in name for placeholder in dropped)
        }
    )
    logger.debug(f"Found {len(tables)} table reference(s) in SQL body")
    return SqlExtractionResult(tables=tables, warnings=[])


# ---- Token masking (byte-exact round-trip) ----


def _make_salt(sql: str) -> str:
    """Return a deterministic alphanumeric salt absent from ``sql``."""
    salt = _SALT_SEED
    while salt in sql:
        salt += "x"
    return salt


def _mask_tokens(sql: str) -> Tuple[str, Dict[str, str], FrozenSet[str]]:
    """Replace tokens with placeholders; bare ``$word`` gets the drop class.

    Returns the masked text, the placeholder -> original-bytes mapping for
    BOTH classes (so unmasking the full text is always the identity), and the
    set of drop-class placeholders. ``${...}`` / ``{...}`` tokens are masked
    BEFORE ``$word`` so ``${catalog}`` is never mis-eaten by the bare-$
    pattern.
    """
    salt = _make_salt(sql)
    replacements: Dict[str, str] = {}
    dropped: Set[str] = set()

    def _preserve(match: re.Match[str]) -> str:
        placeholder = f"__{salt}_{len(replacements)}__"
        replacements[placeholder] = match.group(0)
        return placeholder

    def _drop(match: re.Match[str]) -> str:
        placeholder = f"__{salt}drop_{len(replacements)}__"
        replacements[placeholder] = match.group(0)
        dropped.add(placeholder)
        return placeholder

    masked = _TOKEN_PATTERN.sub(_preserve, sql)
    masked = _BARE_DOLLAR_PATTERN.sub(_drop, masked)
    return masked, replacements, frozenset(dropped)


def _unmask(name: str, replacements: Dict[str, str]) -> str:
    """Restore original token bytes in one extracted table name."""
    for placeholder, original in replacements.items():
        name = name.replace(placeholder, original)
    return name


# ---- Per-statement read collection ----


def _collect_reads(statement: exp.Expression, masked_sql: str) -> Set[str]:
    """Collect read table names from one statement (still masked)."""
    cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}
    write_targets = _write_target_ids(statement)
    stream_arguments = _stream_argument_names(statement, masked_sql)

    reads: Set[str] = set()
    for table in statement.find_all(exp.Table):
        if id(table) in write_targets:
            continue
        if id(table) in stream_arguments:
            # Resolved at the enclosing Stream node, where the opaqueness rule
            # is applied to the wrapped argument as a whole.
            name = stream_arguments[id(table)]
        else:
            wrapper = _dlt_wrapper_call(table)
            if wrapper is not None:
                name = _first_argument_name(wrapper)
            elif isinstance(table.this, exp.Func):
                # Non-DLT table function (e.g. read_files(...)) — not a table read.
                continue
            else:
                name = _dotted_name(table)
        if not name or name.lower() in cte_names:
            continue
        reads.add(name)
    return reads


def _write_target_ids(statement: exp.Expression) -> Set[int]:
    """Identify Table nodes that are write targets (the `this` of DML/DDL)."""
    targets: Set[int] = set()
    for node in statement.find_all(*_WRITE_STATEMENTS):
        target = node.this
        # CREATE TABLE t (cols...) wraps the target in Schema; aliased
        # targets (MERGE INTO t AS x) may wrap it in Alias.
        while isinstance(target, (exp.Schema, exp.Alias)):
            target = target.this
        if isinstance(target, exp.Table):
            targets.add(id(target))
    return targets


def _stream_argument_names(
    statement: exp.Expression, masked_sql: str
) -> Dict[int, str]:
    """Map each Table node wrapped by an ``exp.Stream`` to its resolved name.

    sqlglot >= 28 emits a dedicated :class:`sqlglot.exp.Stream` node for
    Databricks ``FROM STREAM x`` / ``FROM stream(x)``, sitting ABOVE the
    argument, which is itself re-parsed as a :class:`sqlglot.exp.Table`. Those
    inner Table nodes are reachable from ``find_all(exp.Table)`` but must NOT
    be read directly: the opaqueness rule applies to the wrapped argument as a
    whole, so it is enforced here and the caller takes the name from this
    mapping.

    ``masked_sql`` is the exact text that was parsed — needed to recover quote
    provenance, see :func:`_stream_argument_name`.

    Returns ``id(inner_table) -> name``, where an empty name marks an argument
    that is not a statically known table reference.
    """
    names: Dict[int, str] = {}
    for node in statement.find_all(exp.Stream):
        target = node.this
        # `FROM STREAM x` yields the Table directly; `FROM stream(x)` wraps it
        # in a Subquery. An alias attaches as a TableAlias INSIDE the Table or
        # Subquery, so it never adds a layer here.
        while isinstance(target, exp.Subquery):
            target = target.this
        if isinstance(target, exp.Table):
            names[id(target)] = _stream_argument_name(target, masked_sql)
    return names


def _stream_argument_name(table: exp.Table, masked_sql: str) -> str:
    """Render a Stream-wrapped Table as a dotted name, or ``""`` if opaque.

    Mirrors the contract of :func:`_first_argument_name`: only a (possibly
    dotted) identifier is a statically known table reference.

    sqlglot re-parses a Stream argument as a Table regardless of how it was
    written, and normalises the quote CHARACTER away — ``stream('x')``,
    ``stream("x")`` and ``stream(`x`)`` all yield ``Identifier(this='x',
    quoted=True)``. The original character is still recoverable from the
    part's ``meta["start"]`` byte offset into the parsed (masked) text, and
    that is what distinguishes the cases exactly — a name containing a dot is
    NOT a usable signal, since ``stream(`my.table`)`` is one legitimate
    dotted identifier while ``stream('my.table')`` is a string.

    Only the single quote marks opacity. Double-quoted reads are extracted as
    identifiers, matching this module's documented handling of a bare
    ``FROM "bronze"."customers"``, so ``stream(X)`` agrees with ``FROM X``.
    """
    parts = list(table.parts)
    if not parts:
        return ""
    for part in parts:
        if isinstance(part, exp.Func):
            return ""
        # Deliberately not extracted to a helper: `table.parts` is typed
        # `list[Expr]` on sqlglot 30 but `Expr` does not exist on 28/29, so
        # naming it in a signature would break under the declared pin floor.
        if not getattr(part, "quoted", False):
            continue
        start = part.meta.get("start")
        # Unknown provenance must not silently drop a real read.
        if isinstance(start, int) and 0 <= start < len(masked_sql):
            if masked_sql[start] == "'":
                return ""
    return ".".join(part.name for part in parts)


def _dotted_name(table: exp.Table) -> str:
    """Render a Table node as dotted text, backtick quoting stripped."""
    return ".".join(part.name for part in table.parts)


def _dlt_wrapper_call(table: exp.Table) -> Optional[exp.Func]:
    """Return the wrapped function call if ``table`` is a DLT wrapper read.

    Matches on the function NAME (case-insensitive): any
    :class:`sqlglot.exp.Func` in FROM/JOIN position named stream/live/snapshot
    qualifies. On the pinned floor (sqlglot >= 28) ``stream`` no longer reaches
    here — it parses to a dedicated :class:`sqlglot.exp.Stream` node handled by
    :func:`_stream_argument_names` — but ``live`` and ``snapshot`` still arrive
    in this shape, so this path stays load-bearing. ``"stream"`` is kept in
    :data:`_DLT_WRAPPERS` to guard the same fallback shape.
    """
    func = table.this
    if isinstance(func, exp.Func) and _function_name(func).lower() in _DLT_WRAPPERS:
        return func
    return None


def _function_name(func: exp.Func) -> str:
    if isinstance(func, exp.Anonymous):
        return func.name
    return str(func.sql_name())


def _first_argument_name(func: exp.Func) -> str:
    """Render the first argument of a wrapper call as a dotted table name.

    Only arguments that resolve to a (possibly dotted) identifier — bare or
    qualified names, including masked-token placeholder segments — produce a
    name. Anything else (a nested function call, a subquery, a string
    literal) is not a statically known table reference, so the no-name
    sentinel ``""`` is returned and the wrapper read is excluded from the
    output as opaque.
    """
    arguments = list(func.expressions)
    if not arguments and isinstance(func.this, exp.Expression):
        arguments = [func.this]
    if not arguments:
        return ""
    argument: exp.Expression = arguments[0]
    parts = getattr(argument, "parts", None)
    if parts:
        return ".".join(part.name for part in parts)
    if isinstance(argument, exp.Identifier):
        return argument.name
    return ""


# ---- Parse-failure advisory ----


def _parse_failure_warning(error: SqlglotError) -> DependencyWarning:
    """Build the single LHP-DEP-003 advisory for a whole-body parse failure."""
    line: Optional[int] = None
    description = str(error).splitlines()[0] if str(error) else "invalid SQL"

    error_details = getattr(error, "errors", None)
    if error_details:
        first = error_details[0]
        line = first.get("line")
        description = first.get("description") or description
        highlight = first.get("highlight")
        if highlight:
            description = f"{description} (near '{highlight}')"

    return DependencyWarning(
        code=DEP_003.code,
        message=f"Could not parse SQL for table extraction: {description}",
        flowgroup="",
        action="",
        suggestion=(
            "Declare the upstream table(s) explicitly via `depends_on` on the action."
        ),
        file_path=None,
        line=line,
    )
