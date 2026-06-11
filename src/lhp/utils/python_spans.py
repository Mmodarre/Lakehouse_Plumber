"""Byte-offset line mapping and span editing for source-text surgery.

Pure byte/offset manipulation shared by code that rewrites Python source
via AST positions: :func:`line_start_byte_offsets` maps 1-based line numbers
to byte offsets in the encoded source (CPython reports ``col_offset`` /
``end_col_offset`` as UTF-8 *byte* columns), and :func:`apply_byte_edits`
splices ``(start, end, replacement)`` span edits into the byte string.
This module knows nothing about what the bytes mean.
"""

from collections.abc import Iterable


def line_start_byte_offsets(data: bytes) -> list[int]:
    """Byte offset at which each 1-based source line begins (index 0 unused).

    Splits only on the line terminators CPython's tokenizer honours — ``\\n``,
    ``\\r``, ``\\r\\n`` — so the offsets line up exactly with AST ``lineno`` /
    ``col_offset`` (which are UTF-8 *byte* columns). ``str.splitlines`` is NOT
    used: it also breaks on ``\\x0b``/``\\x0c``/``\\x1c``-``\\x1e``/``\\u2028`` etc.,
    which the tokenizer treats as ordinary characters, which would skew offsets.
    """
    offsets = [0, 0]
    i = 0
    n = len(data)
    nl = ord("\n")
    cr = ord("\r")
    while i < n:
        b = data[i]
        if b == cr:
            i += 2 if i + 1 < n and data[i + 1] == nl else 1
            offsets.append(i)
        elif b == nl:
            i += 1
            offsets.append(i)
        else:
            i += 1
    return offsets


def apply_byte_edits(data: bytes, edits: Iterable[tuple[int, int, bytes]]) -> bytes:
    """Apply non-overlapping ``(start, end, replacement)`` span edits to ``data``.

    Each edit replaces ``data[start:end]`` with ``replacement``. Edits are
    applied in descending offset order so earlier offsets remain valid while
    later spans are spliced — callers need not pre-sort. ``start == end``
    inserts at that offset; an empty ``edits`` returns ``data`` unchanged.
    Behaviour is undefined for overlapping spans.
    """
    out = data
    for start, end, replacement in sorted(edits, reverse=True):
        out = out[:start] + replacement + out[end:]
    return out
