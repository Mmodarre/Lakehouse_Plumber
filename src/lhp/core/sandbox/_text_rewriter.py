"""Guarded text-level table-reference rewriting for sandbox mode.

The structured rewrite pass owns YAML-field writes; THIS module is the text
primitive for opaque payloads (SQL files/strings, ``spark.sql`` literals in
Python sources): given the sandbox rename set, it swaps the table-LEAF
segment of every textual occurrence of a rename-set table and leaves every
other byte of the text untouched.

This module never decides membership or applies the rename pattern itself:

- candidate spans proposed by a guarded regex are CONFIRMED through
  :func:`._renames.match_renamed_table` (the canonical 2-part<->3-part rule
  keeps its single implementation), and
- the replacement leaf comes from :func:`._renames.rename_parts` (the
  pattern-application choke point).

Idempotency falls out of the guards: a renamed leaf (``alice_tbl`` /
``tbl_alice``) never equals a rename-key leaf, the word-char lookarounds stop
re-matches that would start or end inside one, and the canonical re-check
rejects any residual candidate that no longer resolves to its own key.
"""

from __future__ import annotations

import re

from ._renames import SandboxTableRenames, match_renamed_table, rename_parts

__all__ = ["rewrite_table_refs_in_text"]

# Guards shared by every key pattern. A candidate ref must not START mid-name:
# after a word char (it would be the suffix of a longer identifier), after a
# dot (the tail of a LONGER qualified name, e.g. `sch.tbl` inside `a.sch.tbl`
# or `${catalog}.sch.tbl`), after `$` (an unresolved substitution-token
# remnant), or after a backtick (inside a quoted part). It must not be
# FOLLOWED by a word char / `$` / backtick (it would be a strict prefix of a
# longer name, e.g. `sch.tbl` inside `sch.tbl_history`). A following dot is
# deliberately ALLOWED: `sch.tbl.col` is a qualified COLUMN reference whose
# table segment must still be swapped.
_GUARD_BEFORE = r"(?<![\w$.`])"
_GUARD_AFTER = r"(?![\w$`])"

# Whitespace (including newlines) is legal around the dots of a qualified name.
_DOT = r"\s*\.\s*"

# SQL spans exempt from rewriting when ``mask_sql_literals`` is on:
# single-quoted strings (both ANSI '' doubling and backslash escapes — Spark
# SQL accepts \' by default), `--` line comments, and /* */ block comments.
# ``finditer`` scans left to right, so a `--` inside a string is consumed by
# the string alternative and never opens a phantom comment (and vice versa).
# Double-quoted spans are IDENTIFIERS in ANSI SQL, not literals: never masked.
_SQL_MASK_RE = re.compile(
    r"'(?:[^'\\]|\\.|'')*'"  # single-quoted string literal
    r"|--[^\n]*"  # line comment
    r"|/\*.*?\*/",  # block comment (non-nested)
    re.DOTALL,
)

# Two Spark SQL constructs put a table REFERENCE inside a single-quoted string
# rather than string DATA: `table_changes('cat.sch.tbl', 0)` (CDF read, first
# argument) and `IDENTIFIER('cat.sch.tbl')` (identifier substitution, sole
# argument). Their argument string is un-masked (see :func:`_masked_spans`) so
# the leaf still gets rewritten. `_GUARD_BEFORE` keeps `my_table_changes(` and
# `a.identifier(` out; `\s*` tolerates whitespace/newlines before the paren and
# the string; IGNORECASE covers `TABLE_CHANGES` / `Identifier`.
_TABLE_REF_ARG_RE = re.compile(
    rf"{_GUARD_BEFORE}(?:table_changes|identifier)\s*\(\s*",
    re.IGNORECASE,
)

# The literal qualifies only as a CLEAN argument: its closing quote is followed
# by the argument separator (`,` — table_changes' remaining args) or the call's
# `)` (IDENTIFIER's sole argument). A concatenated identifier such as
# `IDENTIFIER('cat.sch' || suffix)` fails this and stays masked — the literal
# is a name fragment there, not a whole table reference.
_ARG_CLOSE_RE = re.compile(r"\s*[,)]")

# (start, end, qualifier prefix as matched, leaf as matched) — spans in `text`.
_Candidate = tuple[int, int, str, str]


def rewrite_table_refs_in_text(
    text: str,
    renames: SandboxTableRenames,
    *,
    mask_sql_literals: bool = False,
) -> str:
    """Rewrite every textual occurrence of a rename-set table in ``text``.

    For each multi-part rename key, occurrences are matched in any authoring
    spelling (case-insensitive, per-part optional backticks, whitespace around
    dots, and — for a 3-part key — the catalog-less 2-part spelling), each
    candidate is confirmed via :func:`._renames.match_renamed_table`, and only
    the table-leaf segment is replaced through :func:`._renames.rename_parts`;
    the matched qualifier prefix is re-emitted verbatim. A trailing
    ``.column`` tail survives untouched (qualified column splice).

    With ``mask_sql_literals=True``, refs inside SQL single-quoted strings and
    ``--`` / ``/* */`` comments are left alone (span exclusion — offsets stay
    stable, the text is never mutated for masking); the sole exception is a
    literal that IS a table reference — the argument of ``table_changes`` /
    ``IDENTIFIER`` — which is un-masked so its leaf still rewrites.
    """
    # Bare (dotless) keys are never text-rewritten: a lone word like `events`
    # is far too common in SQL/prose to swap on sight; the structured pass
    # owns those writes.
    keys = [key for key in renames.table_producers if "." in key]
    if not text or not keys:
        return text

    masked = _masked_spans(text) if mask_sql_literals else []
    candidates = _collect_candidates(text, keys, renames, masked)
    if not candidates:
        return text
    return _splice(text, candidates, renames)


def _collect_candidates(
    text: str,
    keys: list[str],
    renames: SandboxTableRenames,
    masked: list[tuple[int, int]],
) -> list[_Candidate]:
    """Confirmed match spans for every key, unordered and possibly overlapping."""
    candidates: list[_Candidate] = []
    for key in keys:
        pattern = _compile_key_pattern(key)
        for match in pattern.finditer(text):
            if any(match.start() < hi and lo < match.end() for lo, hi in masked):
                continue
            # Canonical re-check: the regex only PROPOSES spans; the matching
            # rule confirms them. This rejects an ambiguous 2-part spelling
            # (multiple catalogs produce `sch.tbl`) and any candidate that
            # canonically resolves to a different key.
            if match_renamed_table(match.group(0), renames) != key:
                continue
            candidates.append(
                (match.start(), match.end(), match.group(1), match.group(2))
            )
    return candidates


def _compile_key_pattern(key: str) -> re.Pattern[str]:
    """Compile the guarded, case-insensitive pattern for one canonical key.

    Group 1 captures the qualifier prefix verbatim (every part and dot before
    the leaf, with whatever backticks/case/whitespace the author wrote);
    group 2 captures the leaf spelling. For a 3-part key the catalog part is
    OPTIONAL so the 2-part spelling of the same table also matches — the
    canonical re-check in :func:`_collect_candidates` enforces the uniqueness
    rule for that spelling.
    """
    *qualifiers, leaf = key.split(".")
    pieces = [f"{_part_pattern(part)}{_DOT}" for part in qualifiers]
    if len(qualifiers) == 2:
        pieces[0] = f"(?:{pieces[0]})?"
    return re.compile(
        f"{_GUARD_BEFORE}({''.join(pieces)})({_part_pattern(leaf)}){_GUARD_AFTER}",
        re.IGNORECASE,
    )


def _part_pattern(part: str) -> str:
    """One identifier part, spelled bare or backtick-wrapped (all-or-nothing).

    The backticked alternative comes first so `` `tbl` `` consumes its quotes;
    a bare-alternative match INSIDE quotes is impossible because the guards
    reject an adjacent backtick on either side.
    """
    escaped = re.escape(part)
    return f"(?:`{escaped}`|{escaped})"


def _splice(
    text: str, candidates: list[_Candidate], renames: SandboxTableRenames
) -> str:
    """Apply non-overlapping candidates left to right, leaf-swapped."""
    # Earliest start first; on the same start the LONGEST span wins, so a full
    # `cat.sch.tbl` match beats a shorter key matching its `cat.sch` head.
    # Later overlapping candidates are dropped (start < cursor): selected
    # spans never overlap and replacement output is never rescanned — the
    # in-pass half of the idempotency argument.
    candidates.sort(key=lambda c: (c[0], c[0] - c[1]))
    out: list[str] = []
    cursor = 0
    for start, end, prefix, leaf in candidates:
        if start < cursor:
            continue
        out.append(text[cursor:start])
        out.append(prefix)
        out.append(_renamed_leaf(leaf, renames))
        cursor = end
    out.append(text[cursor:])
    return "".join(out)


def _renamed_leaf(leaf: str, renames: SandboxTableRenames) -> str:
    """The renamed table leaf, preserving the site's spelling and backticks.

    Qualifier parts are re-emitted verbatim by the caller; only the leaf goes
    through the choke point, on its ORIGINAL spelling (backticks stripped) so
    the author's casing survives inside the new name.
    """
    quoted = leaf.startswith("`")
    bare = leaf[1:-1] if quoted else leaf
    renamed = rename_parts(renames.strategy, None, None, bare)[2]
    return f"`{renamed}`" if quoted else renamed


def _masked_spans(text: str) -> list[tuple[int, int]]:
    """Spans of SQL string literals and comments (rewrite-exempt zones).

    A single-quoted string that is the table-reference argument of
    ``table_changes`` / ``IDENTIFIER`` is deliberately NOT masked: its content
    is a table name to rewrite, not opaque string data. Every other literal
    and every comment stays masked (a ``table_changes(`` token that lives
    inside a comment or another string never opens a separate literal span at
    the un-mask offset, so it cannot leak the exemption).
    """
    arg_starts = {match.end() for match in _TABLE_REF_ARG_RE.finditer(text)}
    spans: list[tuple[int, int]] = []
    for match in _SQL_MASK_RE.finditer(text):
        if (
            match.group(0).startswith("'")
            and match.start() in arg_starts
            and _ARG_CLOSE_RE.match(text, match.end())
        ):
            continue
        spans.append(match.span())
    return spans
