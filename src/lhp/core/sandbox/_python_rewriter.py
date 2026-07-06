"""AST-guided sandbox rewrite of table references in Python source files.

Runs inside pool workers on external Python source (custom datasource /
transform files) AFTER the structured and inline-text passes have covered the
YAML side. :func:`lhp.core.dependencies.collect_python_table_sites` locates
every recognized table-consuming call site; this module resolves each per the
sandbox interplay contract:

- direct string-literal args (``table_read`` names and constant ``spark.sql``
  bodies) are rewritten by splicing a complete replacement literal over the
  exact byte span;
- dynamic ``spark.sql(f"...")`` bodies are delegated to :mod:`._fstring_sql`
  (rewrite in-scope refs in the literal segments, or report LHP-VAL-066 /
  LHP-VAL-067);
- statically-resolved-but-not-literal in-scope reads are reported as
  :class:`UnrewritableTableRead` records (LHP-VAL-066, warn-only);
- fully OPAQUE ``table_read`` sites (a runtime-determined name) are wrapped in
  the runtime ``__lhp_sandbox_table(...)`` shim (:mod:`._runtime_shim`) so the
  name is resolved at execution time — one helper is emitted per module that
  has at least one wrapped site;
- fully OPAQUE ``spark.sql`` sites (a runtime-built query) are reported as
  :class:`UnverifiableSqlRead` records (LHP-VAL-067, advisory-only).

One record is emitted PER SITE; the worker hook folds them into a single
per-file ``LHP-VAL-066`` and/or ``LHP-VAL-067`` warning (dedup grain
``(code, file)``).

Every match decision goes through :func:`._renames.match_renamed_table` (the
ONE canonicalization) and every replacement leaf through
:func:`._renames.rename_parts` (the choke point, via the structured rewriter's
leaf helper); the emitted shim mirrors both exactly. ``spark.sql`` bodies are
rewritten with the guarded text primitive with SQL string literals and comments
masked.

Rewrites are idempotent: renamed leaves no longer match the rename set, and an
already-wrapped site (an arg that is itself a shim call) is not re-wrapped, so a
module never gains a second helper. Unparseable source degrades to a no-op (the
worker's own AST gate reports that canonically); a rewrite that ITSELF fails to
re-parse raises ``ValueError`` — never expected, the self-check is the proof
obligation — so the all-or-nothing worker gate turns it into a clean flowgroup
failure instead of leaking a corrupt or half-rewritten file to disk.
"""

from __future__ import annotations

import ast
import io
import re
import tokenize
from dataclasses import dataclass
from typing import Optional

from lhp.core.dependencies import PythonTableSite, collect_python_table_sites
from lhp.utils.python_spans import apply_byte_edits, line_start_byte_offsets

from ._flowgroup_rewriter import _maybe_rewrite_ref
from ._fstring_sql import rewrite_fstring_sql_body
from ._renames import SandboxTableRenames, match_renamed_table
from ._runtime_shim import (
    helper_insertion_edit,
    is_shim_call,
    module_defines_helper,
    wrap_arg,
)
from ._text_rewriter import rewrite_table_refs_in_text

#: Opening delimiter of a Python string literal: optional prefix letters,
#: then a triple or single quote. ``b``-prefixed literals never reach us
#: (their value would be ``bytes``, not ``str``) and f-strings are never
#: plain constants, so in practice the prefix is empty, ``r``/``R`` or
#: ``u``/``U`` — but the pattern stays permissive and the simple-path checks
#: decide what to do with it.
_OPEN_DELIM_RE = re.compile(r"^([A-Za-z]{0,2})('''|\"\"\"|'|\")")


@dataclass(frozen=True)
class UnrewritableTableRead:
    """One in-scope table read the rewriter could not rewrite (VAL_066 grain).

    Emitted for a recognized read site whose argument is not a plain string
    literal (variable, YAML parameter binding, f-string, concatenation,
    conditional union) but statically resolved to at least one table name in
    the sandbox rename set. The site's source text is left untouched.

    ``table`` is the matched ORIGINAL in-scope spelling (the first match in
    sorted order when several resolved candidates are in scope); ``kind`` is
    the site kind (``"table_read"`` or ``"spark_sql"``). The worker hook
    folds these per-site records into one ``LHP-VAL-066`` warning per file.
    """

    lineno: int
    table: str
    kind: str


@dataclass(frozen=True)
class UnverifiableSqlRead:
    """One opaque ``spark.sql`` site whose tables are runtime-determined (VAL_067).

    Emitted for a ``spark.sql(...)`` site whose argument is not statically
    analyzable — a non-f-string opaque argument (a variable, call result, or
    unresolved concatenation), or a dynamic f-string body whose table identity
    is purely runtime-determined (an interpolation inside a dotted name) with no
    in-scope ref to rewrite or warn on. Such SQL can be neither verified nor
    rewritten to sandbox table names; the site is left untouched. The worker hook
    folds these per-site records into one advisory ``LHP-VAL-067`` per file.

    Opaque ``table_read`` sites are NOT reported here — they are wrapped in the
    runtime shim instead.
    """

    lineno: int


def rewrite_python_table_literals(
    source: str, renames: SandboxTableRenames
) -> tuple[str, tuple[UnrewritableTableRead | UnverifiableSqlRead, ...]]:
    """Rewrite in-scope table references in ``source``; report the rest.

    Returns the rewritten source (or ``source`` unchanged when there is
    nothing to do — empty rename set, unparseable input, or no matching
    sites) plus one warning record per recognized site the pass could not
    rewrite: :class:`UnrewritableTableRead` (LHP-VAL-066) for a
    statically-resolved in-scope read, and :class:`UnverifiableSqlRead`
    (LHP-VAL-067) for an opaque ``spark.sql`` body. Opaque ``table_read`` sites
    are wrapped in the ``__lhp_sandbox_table(...)`` shim and produce no record;
    one helper definition is emitted for a module with any wrapped site.

    Quote style is preserved on rewritten literals; ``spark.sql`` bodies keep
    their original formatting byte-for-byte except the renamed refs. Rewrites
    are idempotent: renamed leaves and already-wrapped sites are not touched
    again, so a second run adds neither an edit nor a duplicate helper.

    Raises ``ValueError`` if the rewritten source fails an ``ast.parse``
    self-check — internal rewrite corruption, never expected.
    """
    if not renames.table_producers:
        return source, ()
    sites = collect_python_table_sites(source).sites
    if not sites:
        return source, ()

    data = source.encode("utf-8")
    offsets = line_start_byte_offsets(data)
    edits: list[tuple[int, int, bytes]] = []
    warnings: list[UnrewritableTableRead] = []
    advisories: list[UnverifiableSqlRead] = []
    wrapped = False
    for site in sites:
        if site.opaque:
            if site.kind == "table_read":
                edit = _wrap_opaque_read(site, data, offsets)
                if edit is not None:
                    edits.append(edit)
                    wrapped = True
            else:
                advisories.append(UnverifiableSqlRead(lineno=site.lineno))
            continue
        if not site.rewritable and not site.fstring:
            matched = _first_in_scope(site.resolved_values, renames)
            if matched is not None:
                warnings.append(
                    UnrewritableTableRead(
                        lineno=site.lineno, table=matched, kind=site.kind
                    )
                )
            continue
        # Contract: span is present for rewritable and f-string sites alike.
        assert site.span is not None
        start = offsets[site.span.lineno] + site.span.col_offset
        end = offsets[site.span.end_lineno] + site.span.end_col_offset
        segment = data[start:end].decode("utf-8")
        if site.fstring:
            outcome = rewrite_fstring_sql_body(segment, renames)
            if outcome.replacement is not None:
                edits.append((start, end, outcome.replacement.encode("utf-8")))
            elif outcome.warn_table is not None:
                warnings.append(
                    UnrewritableTableRead(
                        lineno=site.lineno, table=outcome.warn_table, kind=site.kind
                    )
                )
            elif outcome.advise:
                advisories.append(UnverifiableSqlRead(lineno=site.lineno))
            continue
        assert site.value is not None
        if site.kind == "table_read":
            replacement = _table_read_replacement(site.value, segment, renames)
        else:
            replacement = _spark_sql_replacement(site.value, segment, renames)
        if replacement is not None:
            edits.append((start, end, replacement.encode("utf-8")))

    if wrapped and not module_defines_helper(source):
        edits.append(helper_insertion_edit(source, data, offsets, renames))

    all_warnings: tuple[UnrewritableTableRead | UnverifiableSqlRead, ...] = tuple(
        warnings
    ) + tuple(advisories)
    if not edits:
        return source, all_warnings
    rewritten = apply_byte_edits(data, edits).decode("utf-8")
    try:
        ast.parse(rewritten)
    except SyntaxError as exc:
        raise ValueError(
            f"sandbox rewrite produced unparseable Python "
            f"(line {exc.lineno}): {exc.msg}"
        ) from exc
    return rewritten, all_warnings


def _wrap_opaque_read(
    site: PythonTableSite, data: bytes, offsets: list[int]
) -> Optional[tuple[int, int, bytes]]:
    """A byte-edit wrapping an opaque read's argument in the shim, or ``None``.

    ``None`` when the argument is ALREADY a shim call — the in-run idempotency
    guard that stops a re-run from double-wrapping.
    """
    span = site.span  # opaque table_read always carries the argument span.
    assert span is not None
    start = offsets[span.lineno] + span.col_offset
    end = offsets[span.end_lineno] + span.end_col_offset
    segment = data[start:end].decode("utf-8")
    if is_shim_call(segment):
        return None
    return start, end, wrap_arg(segment).encode("utf-8")


def _first_in_scope(
    values: frozenset[str], renames: SandboxTableRenames
) -> Optional[str]:
    """First (sorted) resolved value that matches the rename set, if any."""
    for value in sorted(values):
        if match_renamed_table(value, renames) is not None:
            return value
    return None


def _table_read_replacement(
    value: str, segment: str, renames: SandboxTableRenames
) -> Optional[str]:
    """Replacement literal for a direct table-name literal, or ``None``.

    The new ref is computed from the literal's VALUE (leaf renamed, part
    spellings and backtick style preserved); the original SEGMENT only
    decides the quoting of the emitted literal.
    """
    new_ref = _maybe_rewrite_ref(value, renames)
    if new_ref == value:
        return None
    match = _OPEN_DELIM_RE.match(segment)
    if match and not match.group(1) and _is_single_string_token(segment):
        quote = match.group(2)
        return f"{quote}{new_ref}{quote}"
    # Implicit concatenation or prefixed (r/u) literal: collapse to a plain
    # single-quoted literal — table refs carry no escaping hazards.
    return f"'{new_ref}'"


def _spark_sql_replacement(
    value: str, segment: str, renames: SandboxTableRenames
) -> Optional[str]:
    """Replacement literal for a ``spark.sql`` constant body, or ``None``.

    Simple path (single literal, no escape sequences): rewrite the RAW text
    between the original delimiters and splice it back — original prefix,
    quotes, formatting and indentation survive byte-for-byte. Otherwise
    (implicit concatenation, or escapes making raw text diverge from the
    decoded value) rewrite the DECODED value and emit a fresh validated
    literal.
    """
    match = _OPEN_DELIM_RE.match(segment)
    if match and "\\" not in segment and _is_single_string_token(segment):
        quote = match.group(2)
        inner = segment[match.end() : len(segment) - len(quote)]
        rewritten = rewrite_table_refs_in_text(inner, renames, mask_sql_literals=True)
        if rewritten == inner:
            return None
        return f"{segment[: match.end()]}{rewritten}{quote}"

    rewritten = rewrite_table_refs_in_text(value, renames, mask_sql_literals=True)
    if rewritten == value:
        return None
    preferred = match.group(2) if match else "'"
    return _safe_literal(rewritten, preferred)


def _safe_literal(value: str, preferred: str) -> str:
    """A valid Python literal evaluating to ``value``.

    Prefers triple quotes when the value spans lines (keeping the SQL
    readable), else the original site's quote style; falls back to ``repr``
    whenever the preferred style would not round-trip (embedded delimiter,
    trailing quote, backslash).
    """
    quote = '"""' if "\n" in value and len(preferred) == 1 else preferred
    candidate = f"{quote}{value}{quote}"
    try:
        if ast.literal_eval(candidate) == value:
            return candidate
    except (ValueError, SyntaxError):
        pass
    return repr(value)


def _is_single_string_token(segment: str) -> bool:
    """Whether ``segment`` is exactly ONE string token (not a concatenation).

    The AST folds implicit concatenations of plain constants into a single
    ``Constant`` node, so node shape cannot distinguish them — the tokenizer
    can. Any tokenize failure means the segment is not a simple literal.
    """
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(segment).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return False
    return sum(1 for token in tokens if token.type == tokenize.STRING) == 1
