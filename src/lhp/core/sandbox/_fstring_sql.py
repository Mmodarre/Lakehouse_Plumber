"""Text-level rewrite of table refs in a dynamic ``spark.sql(f"...")`` body.

A constant ``spark.sql`` body is rewritten by the byte-preserving primitive
in :mod:`._python_rewriter`; a dynamic f-string body cannot be, because the
in-scope table name may sit next to (or be split across) an interpolation.
This module analyses the RAW source of one f-string node — its outer byte
span is reliable on every supported Python version, unlike the positions of
pieces inside it — and decides, for that whole site, one of:

- REWRITE: every in-scope ref sits entirely inside literal (non-interpolated)
  text in an unambiguous SQL context; each literal segment is rewritten with
  the shared text primitive and the segment is rebuilt with interpolations
  verbatim. Same leaf-rename semantics as a constant body.
- WARN: interpolation makes matching ambiguous (a ref is split across an
  interpolation boundary, or an SQL string/comment spans an interpolation so
  the mask state depends on runtime content, or the source uses a raw prefix /
  escape sequence / embedded delimiter we will not rewrite by hand). The site
  is left untouched and a representative in-scope table is reported for the
  caller's LHP-VAL-066 fold.
- NOTHING: no in-scope table is referenced at all.

Every membership decision goes through the canonical
:func:`._renames.match_renamed_table`; every leaf replacement goes through
:func:`._text_rewriter.rewrite_table_refs_in_text` (which owns the leaf choke
point and the SQL masking) — this module never applies the rename pattern or
re-implements masking itself.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

from ._renames import SandboxTableRenames, match_renamed_table
from ._text_rewriter import rewrite_table_refs_in_text

#: Opening delimiter of a Python string literal (prefix + quote). Mirrors
#: :data:`._python_rewriter._OPEN_DELIM_RE`; kept local so this module carries
#: no dependency back onto its caller.
_OPEN_DELIM_RE = re.compile(r"^([A-Za-z]{0,2})('''|\"\"\"|'|\")")

#: One name part, unknown until runtime — stands in for an interpolation while
#: matching a (possibly split) qualified name against the rename set. A NUL
#: byte never occurs in real source, is not a word char, and is not a dot, so
#: it delimits parts and never merges with adjacent identifier text.
_WILDCARD = "\x00"

#: A dotted qualified name whose parts are identifiers (optionally backticked)
#: or the interpolation wildcard, with SQL-legal whitespace around the dots.
_PART = r"(?:[\w`]+|\x00)"
_DOTTED_RE = re.compile(rf"{_PART}(?:\s*\.\s*{_PART})+")

#: SQL lexer states used to detect string/comment spans that cross an
#: interpolation (which would make a later ref's mask state runtime-dependent).
_CLEAN, _SQUOTE, _LINE_COMMENT, _BLOCK_COMMENT = range(4)


@dataclass(frozen=True)
class FStringRewrite:
    """Outcome of analysing one dynamic ``spark.sql(f"...")`` body.

    At most one of ``replacement`` / ``warn_table`` / ``advise`` is set:

    - ``replacement`` — the rebuilt f-string source when the site is
      unambiguously rewritten.
    - ``warn_table`` — a representative in-scope table (readable, with
      ``{...}`` for an interpolated part) when the site is ambiguous but
      names an in-scope table (LHP-VAL-066 fold).
    - ``advise`` — no in-scope table was rewritten or warned, but the body
      holds a runtime-determined table reference (an interpolation sitting
      inside a dotted name) the pass can neither verify nor rewrite
      (LHP-VAL-067 advisory).

    All three unset means the body references no table whose identity the
    sandbox pass must act on (a fully literal out-of-scope ref, or no ref at
    all) — silent, preserving out-of-scope semantics.
    """

    replacement: Optional[str] = None
    warn_table: Optional[str] = None
    advise: bool = False


@dataclass(frozen=True)
class _Piece:
    """One contiguous run of the f-string body: literal text or a field.

    ``text`` is the exact source substring (fields keep their braces), so
    concatenating every piece's text reproduces the body byte-for-byte.
    """

    is_field: bool
    text: str


def rewrite_fstring_sql_body(
    segment: str, renames: SandboxTableRenames
) -> FStringRewrite:
    """Decide rewrite / warn / nothing for one dynamic f-string body.

    ``segment`` is the raw source of the ``ast.JoinedStr`` node (prefix,
    quotes and all). Any shape we cannot analyse confidently (raw prefix,
    escape sequences, implicit concatenation, a quote inside an interpolation,
    unbalanced braces) is treated as ambiguous — never rewritten.
    """
    parsed = _parse_fstring(segment)
    if parsed is None:
        return FStringRewrite(warn_table=_representative_inscope(segment, renames))

    prefix, quote, pieces = parsed
    template = _wildcard_template(pieces)
    if _is_ambiguous(pieces, renames):
        warn_table = _representative_inscope(template, renames)
        if warn_table is not None:
            return FStringRewrite(warn_table=warn_table)
        # Ambiguous with no in-scope ref (e.g. an SQL string/comment crosses an
        # interpolation): advise iff a runtime-determined table ref is present.
        return FStringRewrite(advise=_has_dynamic_table_window(template))

    rebuilt = _rewrite_clean(prefix, quote, pieces, renames)
    if rebuilt != segment:
        return FStringRewrite(replacement=rebuilt)
    # Nothing rewritten and nothing in scope: advise only when a dotted name
    # carries an interpolation (a purely runtime-determined table ref). A fully
    # literal out-of-scope ref has no wildcard window and stays silent.
    return FStringRewrite(advise=_has_dynamic_table_window(template))


def _parse_fstring(segment: str) -> Optional[Tuple[str, str, List[_Piece]]]:
    """Split a single f-string token into ``(prefix, quote, pieces)``.

    Returns ``None`` — meaning "analyse as ambiguous" — for any body the
    byte-preserving path cannot handle safely: a non-f prefix or a raw f-string
    (``rf"..."``), an escape sequence (a backslash anywhere), a delimiter
    inside the literal text (implicit concatenation such as ``f"a" "b"``), or a
    malformed / quote-bearing interpolation (see :func:`_split_body`).
    """
    match = _OPEN_DELIM_RE.match(segment)
    if match is None or match.group(1).lower() != "f":
        return None
    if "\\" in segment:
        return None
    quote = match.group(2)
    body_start = match.end()
    body_end = len(segment) - len(quote)
    if body_end < body_start or not segment.endswith(quote):
        return None
    body = segment[body_start:body_end]
    pieces = _split_body(body)
    if pieces is None:
        return None
    # A closing delimiter inside a literal run means the AST folded an implicit
    # concatenation into this node — the span is not one clean f-string.
    if any(not piece.is_field and quote in piece.text for piece in pieces):
        return None
    return match.group(1), quote, pieces


def _split_body(body: str) -> Optional[List[_Piece]]:
    """Split an f-string body into literal / field pieces by brace-tracking.

    ``{{`` / ``}}`` are literal-brace escapes; a lone ``{`` opens a field that
    runs to its matching ``}``. Returns ``None`` for a stray ``}`` or any field
    :func:`_consume_field` rejects (unterminated, or containing a quote).
    """
    pieces: List[_Piece] = []
    literal: List[str] = []
    i, n = 0, len(body)
    while i < n:
        char = body[i]
        if char == "{":
            if i + 1 < n and body[i + 1] == "{":
                literal.append("{{")
                i += 2
                continue
            if literal:
                pieces.append(_Piece(is_field=False, text="".join(literal)))
                literal = []
            field, nxt = _consume_field(body, i)
            if field is None:
                return None
            pieces.append(_Piece(is_field=True, text=field))
            i = nxt
            continue
        if char == "}":
            if i + 1 < n and body[i + 1] == "}":
                literal.append("}}")
                i += 2
                continue
            return None
        literal.append(char)
        i += 1
    if literal:
        pieces.append(_Piece(is_field=False, text="".join(literal)))
    return pieces


def _consume_field(body: str, start: int) -> Tuple[Optional[str], int]:
    """Consume the replacement field at ``body[start] == '{'``.

    Tracks nested braces (dict/set literals, ``{width}`` format specs) to find
    the matching close. A quote character makes the field opaque to our
    delimiter reasoning (and covers PEP 701 nested-quote fields uniformly
    across versions) → reject. Returns ``(field_text, next_index)`` or
    ``(None, index)`` when the field is unterminated or bears a quote.
    """
    depth = 0
    i, n = start, len(body)
    while i < n:
        char = body[i]
        if char in ("'", '"'):
            return None, i
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return body[start : i + 1], i + 1
        i += 1
    return None, i


def _is_ambiguous(pieces: List[_Piece], renames: SandboxTableRenames) -> bool:
    """Whether interpolation makes any in-scope match unsafe for this site.

    Two independent triggers:

    - An SQL string / comment span is open at an interpolation boundary, so
      the mask state of later text depends on the interpolated value.
    - A qualified name that matches the rename set is split across an
      interpolation (a wildcard part) — its full identity is runtime-only.
    """
    state = _CLEAN
    for piece in pieces:
        if piece.is_field:
            if state != _CLEAN:
                return True
        else:
            state = _sql_state_after(piece.text, state)
    template = _wildcard_template(pieces)
    return any(has_wildcard for _, has_wildcard in _inscope_windows(template, renames))


def _rewrite_clean(
    prefix: str,
    quote: str,
    pieces: List[_Piece],
    renames: SandboxTableRenames,
) -> str:
    """Rebuild the f-string, rewriting each literal run, fields verbatim.

    Each literal run has balanced SQL string/comment state (guaranteed by the
    caller's ambiguity check), so the shared masked text primitive rewrites it
    exactly as it would a constant body; interpolations are re-emitted
    byte-for-byte, so runtime behaviour is unchanged apart from the leaf name.
    """
    out = [prefix, quote]
    for piece in pieces:
        if piece.is_field:
            out.append(piece.text)
        else:
            out.append(
                rewrite_table_refs_in_text(piece.text, renames, mask_sql_literals=True)
            )
    out.append(quote)
    return "".join(out)


def _sql_state_after(text: str, state: int) -> int:
    """The SQL lexer state after scanning ``text`` from ``state``.

    Mirrors the span grammar of :data:`._text_rewriter._SQL_MASK_RE`:
    single-quoted string literals (with ``''`` doubling), ``--`` line
    comments, and ``/* */`` block comments. Double quotes are identifiers, not
    literals, and are ignored. Backslash escapes are irrelevant here — a
    backslash anywhere in the segment already routed it to the ambiguous path.
    """
    i, n = 0, len(text)
    while i < n:
        char = text[i]
        if state == _CLEAN:
            if char == "'":
                state = _SQUOTE
            elif char == "-" and i + 1 < n and text[i + 1] == "-":
                state = _LINE_COMMENT
                i += 1
            elif char == "/" and i + 1 < n and text[i + 1] == "*":
                state = _BLOCK_COMMENT
                i += 1
        elif state == _SQUOTE:
            if char == "'":
                if i + 1 < n and text[i + 1] == "'":
                    i += 1
                else:
                    state = _CLEAN
        elif state == _LINE_COMMENT:
            if char == "\n":
                state = _CLEAN
        elif char == "*" and i + 1 < n and text[i + 1] == "/":
            state = _CLEAN
            i += 1
        i += 1
    return state


def _wildcard_template(pieces: List[_Piece]) -> str:
    """Literal text with each interpolation replaced by one wildcard part."""
    return "".join(_WILDCARD if piece.is_field else piece.text for piece in pieces)


def _has_dynamic_table_window(template: str) -> bool:
    """Whether ``template`` holds a dotted name window with >=2 interpolations.

    A 2-/3-part window with two or more wildcard parts is a FULLY runtime-only
    table reference — the same shape :func:`_inscope_windows` skips (>1 wildcard)
    because no specific in-scope table can be pinned. That is the signature of
    dynamic SQL the sandbox pass can neither verify nor rewrite (LHP-VAL-067).

    A window with a SINGLE wildcard is deliberately NOT flagged: its literal
    parts either confirm an in-scope table (already an LHP-VAL-066 warn) or prove
    it out of scope (``dev.gold.{tbl}`` can never be produced) — either way the
    identity is decided statically, so it needs no advisory. A fully literal
    (wildcard-free) window is likewise never flagged, preserving out-of-scope
    silence for literal FQNs.
    """
    for run in _DOTTED_RE.finditer(template):
        parts = [part.strip() for part in run.group(0).split(".")]
        for size in (3, 2):
            for start in range(len(parts) - size + 1):
                if parts[start : start + size].count(_WILDCARD) >= 2:
                    return True
    return False


def _representative_inscope(text: str, renames: SandboxTableRenames) -> Optional[str]:
    """The smallest readable in-scope ref found in ``text``, or ``None``.

    Used only for the LHP-VAL-066 detail line. Interpolated parts show as
    ``{...}``. Determinism: the sorted-minimum candidate wins.
    """
    readables = sorted(readable for readable, _ in _inscope_windows(text, renames))
    return readables[0] if readables else None


def _inscope_windows(
    text: str, renames: SandboxTableRenames
) -> Iterator[Tuple[str, bool]]:
    """Yield ``(readable, has_wildcard)`` for each in-scope 2-/3-part window.

    Scans every dotted run in ``text`` and tests its contiguous 3- and 2-part
    windows against the rename set. A window with no wildcard is confirmed by
    the canonical :func:`match_renamed_table`; a window with exactly one
    wildcard uses :func:`_match_wildcard_window` (the interpolation stands for
    any one part). Windows with two or more wildcards are skipped — their table
    identity is fully runtime-only, so they are opaque reads (DEP-002), not
    rename-set candidates.
    """
    for run in _DOTTED_RE.finditer(text):
        parts = [part.strip() for part in run.group(0).split(".")]
        for size in (3, 2):
            for start in range(len(parts) - size + 1):
                window = parts[start : start + size]
                # Two or more interpolated parts leave the ref's identity fully
                # runtime-only — DEP-002 territory (and usually a parameter
                # binding the structured pass already renamed). Only a single
                # unknown part still pins a specific in-scope table.
                if window.count(_WILDCARD) > 1:
                    continue
                if _match_window(window, renames) is not None:
                    readable = ".".join(
                        "{...}" if part == _WILDCARD else part for part in window
                    )
                    yield readable, _WILDCARD in window


def _match_window(window: List[str], renames: SandboxTableRenames) -> Optional[str]:
    """Rename-set key matched by ``window``, or ``None``.

    Without a wildcard, defers to the canonical matcher (the LOCKED
    2-part<->3-part reconciliation rule stays in one place). With a wildcard,
    matches structurally against the producer indexes.
    """
    if _WILDCARD not in window:
        return match_renamed_table(".".join(window), renames)
    return _match_wildcard_window(window, renames)


def _match_wildcard_window(
    window: List[str], renames: SandboxTableRenames
) -> Optional[str]:
    """Best-effort match of a window with an interpolated part.

    A literal part matches case-insensitively (producer keys are canonical
    lowercase); a wildcard part matches anything. A 3-part window tries the
    3-part producer keys, then reconciles its ``schema.table`` suffix against
    2-part producers; a 2-part window tries the ``schema.table`` short index
    under the same uniqueness guard the canonical matcher uses.
    """
    if len(window) == 3:
        for key in sorted(renames.table_producers):
            key_parts = key.split(".")
            if len(key_parts) == 3 and _parts_align(window, key_parts):
                return key
        return _match_wildcard_window(window[1:], renames)
    if len(window) == 2:
        for short in sorted(renames.table_short_to_catalogs):
            short_parts = short.split(".")
            catalogs = renames.table_short_to_catalogs[short]
            if (
                len(short_parts) == 2
                and _parts_align(window, short_parts)
                and len(catalogs) == 1
            ):
                (catalog,) = catalogs
                return f"{catalog}.{short}" if catalog else short
    return None


def _parts_align(window: List[str], key_parts: List[str]) -> bool:
    """Part-wise match: wildcard matches anything, else canonical equality."""
    if len(window) != len(key_parts):
        return False
    return all(
        part == _WILDCARD or _canonical_part(part) == key_part
        for part, key_part in zip(window, key_parts, strict=False)
    )


def _canonical_part(part: str) -> str:
    """Canonicalize one name part to the producer index's spelling."""
    return part.replace("`", "").strip().lower()
