"""AST-guided sandbox rewrite of table literals in Python source files.

Runs inside pool workers on external Python source (custom datasource /
transform files) AFTER the structured and inline-text passes have covered the
YAML side. :func:`lhp.core.dependencies.collect_python_table_sites` locates
every recognized table-consuming call site; this module rewrites the sites it
can — direct string-literal arguments — by splicing complete replacement
literals over the exact byte spans, and reports the sites it cannot —
statically resolved variables, f-strings, concatenations, ``.format`` calls —
as :class:`UnrewritableTableRead` records. One record is emitted PER SITE;
the worker hook folds them into a single per-file ``LHP-VAL-066`` warning
(dedup grain ``(code, file)``).

Every match decision goes through :func:`._renames.match_renamed_table` (the
ONE canonicalization) and every replacement leaf through
:func:`._renames.rename_parts` (the choke point, via the structured
rewriter's leaf helper). ``spark.sql`` bodies are rewritten with the guarded
text primitive with SQL string literals and comments masked.

Unparseable source degrades to a no-op (the worker's own AST gate reports
that canonically); a rewrite that ITSELF fails to re-parse raises
``ValueError`` — never expected, the self-check is the proof obligation —
so the all-or-nothing worker gate turns it into a clean flowgroup failure
instead of leaking a corrupt or half-rewritten file to disk.
"""

from __future__ import annotations

import ast
import io
import re
import tokenize
from dataclasses import dataclass
from typing import Optional

from lhp.core.dependencies import collect_python_table_sites
from lhp.utils.python_spans import apply_byte_edits, line_start_byte_offsets

from ._flowgroup_rewriter import _maybe_rewrite_ref
from ._renames import SandboxTableRenames, match_renamed_table
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


def rewrite_python_table_literals(
    source: str, renames: SandboxTableRenames
) -> tuple[str, tuple[UnrewritableTableRead, ...]]:
    """Rewrite in-scope table literals in ``source``; report the rest.

    Returns the rewritten source (or ``source`` unchanged when there is
    nothing to do — empty rename set, unparseable input, or no matching
    sites) plus one :class:`UnrewritableTableRead` per recognized read site
    that references an in-scope table through a non-literal argument.

    Quote style is preserved on rewritten literals; ``spark.sql`` bodies keep
    their original formatting byte-for-byte except the renamed refs. Rewrites
    are idempotent: renamed leaves no longer match the rename set.

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
    for site in sites:
        if not site.rewritable:
            matched = _first_in_scope(site.resolved_values, renames)
            if matched is not None:
                warnings.append(
                    UnrewritableTableRead(
                        lineno=site.lineno, table=matched, kind=site.kind
                    )
                )
            continue
        # Contract: span/value are present iff rewritable.
        assert site.span is not None and site.value is not None
        start = offsets[site.span.lineno] + site.span.col_offset
        end = offsets[site.span.end_lineno] + site.span.end_col_offset
        segment = data[start:end].decode("utf-8")
        if site.kind == "table_read":
            replacement = _table_read_replacement(site.value, segment, renames)
        else:
            replacement = _spark_sql_replacement(site.value, segment, renames)
        if replacement is not None:
            edits.append((start, end, replacement.encode("utf-8")))

    if not edits:
        return source, tuple(warnings)
    rewritten = apply_byte_edits(data, edits).decode("utf-8")
    try:
        ast.parse(rewritten)
    except SyntaxError as exc:
        raise ValueError(
            f"sandbox rewrite produced unparseable Python "
            f"(line {exc.lineno}): {exc.msg}"
        ) from exc
    return rewritten, tuple(warnings)


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
