"""Tests for :mod:`lhp.utils.python_spans`.

Pins the two guarantees downstream AST rewriters depend on:

- :func:`line_start_byte_offsets` operates on **bytes** and splits only on the
  terminators CPython's tokenizer honours (``\\n``, ``\\r``, ``\\r\\n``), so
  ``offsets[node.lineno] + node.col_offset`` addresses the exact UTF-8 byte
  span of an AST node — including when multi-byte characters precede the node
  on the same line (CPython reports ``col_offset`` as a byte column, not a
  character column).
- :func:`apply_byte_edits` splices ``(start, end, replacement)`` spans in
  descending offset order regardless of input order, treats ``start == end``
  as insertion, and is a no-op for an empty edit list.
"""

import ast

import pytest

from lhp.utils.python_spans import apply_byte_edits, line_start_byte_offsets

# line_start_byte_offsets


@pytest.mark.unit
def test_offsets_empty_source() -> None:
    # Index 0 is unused padding so offsets[1] is line 1.
    assert line_start_byte_offsets(b"") == [0, 0]


@pytest.mark.unit
def test_offsets_single_line_no_trailing_newline() -> None:
    assert line_start_byte_offsets(b"abc") == [0, 0]


@pytest.mark.unit
def test_offsets_trailing_newline_opens_a_new_line() -> None:
    # The tokenizer treats the empty text after a final "\n" as a line start.
    assert line_start_byte_offsets(b"abc\n") == [0, 0, 4]


@pytest.mark.unit
def test_offsets_lf_crlf_and_lone_cr_all_terminate_lines() -> None:
    # Lines: "a"(0), "b"(3, after \r\n), "c"(5, after \r), "d"(7, after \n).
    assert line_start_byte_offsets(b"a\r\nb\rc\nd") == [0, 0, 3, 5, 7]


@pytest.mark.unit
def test_offsets_ignore_terminators_the_tokenizer_ignores() -> None:
    # \x0b (vertical tab) breaks str.splitlines but NOT the CPython tokenizer;
    # it must be counted as an ordinary in-line byte.
    assert line_start_byte_offsets(b"a\x0bb\nc") == [0, 0, 4]


@pytest.mark.unit
def test_offsets_count_multibyte_chars_as_their_utf8_width() -> None:
    # "é" is 2 bytes in UTF-8, so line 2 starts at byte 3, not char 2.
    assert line_start_byte_offsets("é\nx".encode()) == [0, 0, 3]


@pytest.mark.unit
def test_offsets_align_with_ast_byte_columns_for_unicode_source() -> None:
    # CPython reports col_offset as a UTF-8 BYTE column: "é = 1; " is 7 chars
    # but 8 bytes, so the second statement's col_offset is 8. The offsets table
    # must therefore be byte-based for spans to line up.
    source = "é = 1; x = 2\n"
    tree = ast.parse(source)
    second = tree.body[1]
    assert second.col_offset == 8

    data = source.encode("utf-8")
    offsets = line_start_byte_offsets(data)
    start = offsets[second.lineno] + second.col_offset
    end = offsets[second.end_lineno] + second.end_col_offset
    assert data[start:end] == b"x = 2"


@pytest.mark.unit
def test_offsets_address_ast_spans_across_lines() -> None:
    source = "# café\nvalue = 1\n"
    tree = ast.parse(source)
    node = tree.body[0]

    data = source.encode("utf-8")
    offsets = line_start_byte_offsets(data)
    start = offsets[node.lineno] + node.col_offset
    end = offsets[node.end_lineno] + node.end_col_offset
    assert data[start:end] == b"value = 1"


# apply_byte_edits


@pytest.mark.unit
def test_edits_empty_list_returns_data_unchanged() -> None:
    assert apply_byte_edits(b"hello", []) == b"hello"


@pytest.mark.unit
def test_edits_single_replacement() -> None:
    assert apply_byte_edits(b"abcde", [(1, 3, b"ZZ")]) == b"aZZde"


@pytest.mark.unit
def test_edits_sorts_internally_so_input_order_is_irrelevant() -> None:
    data = b"0123456789"
    edits = [(1, 3, b"A"), (5, 7, b"BBB")]
    expected = b"0A34BBB789"
    assert apply_byte_edits(data, edits) == expected
    assert apply_byte_edits(data, list(reversed(edits))) == expected


@pytest.mark.unit
def test_edits_insertion_at_start_and_end() -> None:
    assert apply_byte_edits(b"abc", [(0, 0, b">>")]) == b">>abc"
    assert apply_byte_edits(b"abc", [(3, 3, b"!")]) == b"abc!"


@pytest.mark.unit
def test_edits_empty_replacement_deletes_the_span() -> None:
    assert apply_byte_edits(b"abcde", [(1, 4, b"")]) == b"ae"


@pytest.mark.unit
def test_edits_replacement_spanning_lines() -> None:
    # The span crosses two newlines; everything outside it is preserved.
    assert apply_byte_edits(b"aa\nbb\ncc\n", [(1, 7, b"X")]) == b"aXc\n"
