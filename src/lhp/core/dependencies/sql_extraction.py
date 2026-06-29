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
        reads.update(_collect_reads(statement))

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


def _collect_reads(statement: exp.Expression) -> Set[str]:
    """Collect read table names from one statement (still masked)."""
    cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}
    write_targets = _write_target_ids(statement)

    reads: Set[str] = set()
    for table in statement.find_all(exp.Table):
        if id(table) in write_targets:
            continue
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


def _dotted_name(table: exp.Table) -> str:
    """Render a Table node as dotted text, backtick quoting stripped."""
    return ".".join(part.name for part in table.parts)


def _dlt_wrapper_call(table: exp.Table) -> Optional[exp.Func]:
    """Return the wrapped function call if ``table`` is a DLT wrapper read.

    Matches on the function NAME (case-insensitive), not on dialect- or
    version-specific node classes: any :class:`sqlglot.exp.Func` in FROM/JOIN
    position named stream/live/snapshot qualifies.
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
