"""Tests for :mod:`lhp.utils.python_literals`.

Two contracts:

* Round trip: every filter's output, embedded in a generated file and passed
  through :func:`lhp.utils.file_header.normalize_content` (the funnel every
  generated file is written through), still evaluates to the original value.
* Minimal escaping: only a backslash, the active quote, a newline, a carriage
  return, NUL, the other ``str.splitlines`` boundaries and lone surrogates are
  escaped. Tabs, other control characters, DEL and non-ASCII text stay raw, so a
  value that produced valid code before generates byte-identical text.

``hypothesis`` is not a dependency, so the properties run over a hand-built
corpus plus bounded, seeded random fuzz loops.
"""

import ast
import enum
import json
import math
import random
import re

import pytest

from lhp.utils.file_header import normalize_content
from lhp.utils.python_literals import (
    PYTHON_LITERAL_FILTERS,
    py_block,
    py_comment,
    py_literal,
    py_str,
    py_str_single,
)

_C0_CONTROLS = [chr(code) for code in range(0x20)]
_LONE_SURROGATES = ["\ud800", "\udbff", "\udc00", "\udfff"]

_CORPUS = [
    "",
    "plain",
    "multi word value",
    '"',
    '""',
    '"""',
    '""""',
    '"""""""',
    'say "hi"',
    'ends with "',
    'ends with ""',
    '"starts',
    '""starts',
    "'single'",
    "'''",
    "it's",
    "\\",
    "\\\\",
    "ends with \\",
    'ends with \\"',
    "ends with \\'",
    "C:\\new\\table\\x41",
    "C:\\Users\\me",
    "\\U0001F600 \\N{BULLET} \\x41 \\0 \\u2028",
    "RLIKE '^\\\\d+$'",
    "line1\nline2",
    "line1\r\nline2",
    "trailing newline\n",
    "\n",
    "\n\n",
    "tab\tinside",
    "trailing spaces   \nnext",
    "trailing tab\t\nnext",
    "\x7f",
    "\x85",
    "\u2028",
    "\u2029",
    "a\u2028b\u2029c\x85d",
    "\U0001f600 emoji",
    "\u4e2d\u6587 CJK",
    "\xa0nbsp\xa0",
    "mixed \"quotes\" and 'single' and \\back\\slashes\\",
    *_C0_CONTROLS,
    "".join(_C0_CONTROLS),
    *_LONE_SURROGATES,
    "high \ud83d alone",
    "low \ude00 alone",
]

# Characters every filter escapes (py_block keeps a raw newline).
_ESCAPED = frozenset("\\\n\r\x00\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029")

# Whitespace py_block escapes, so normalize_content never strips it.
_BLOCK_ESCAPED_WHITESPACE = frozenset("\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029")

_NON_NEWLINE_BOUNDARIES = frozenset("\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029")

_FUZZ_ALPHABET = (
    "ab \"\\'\n\r\t{}$#%"
    + "".join(_C0_CONTROLS)
    + "\x7f\x85\xa0\u2028\u2029\u3000\U0001f600\u4e2d\ud800\udfff"
)

# No escaped character at all: tab, harmless controls, DEL, non-ASCII, both quotes.
_SAFE_ALPHABET = (
    "ab XYZ019_-{}$#%'\"\t\x01\x07\x1b\x1f\x7f\xa0\xe9\u3000\u4e2d\U0001f600"
)


def _fuzz_strings(alphabet: str, count: int, seed: int) -> list[str]:
    rng = random.Random(seed)
    return [
        "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))
        for _ in range(count)
    ]


_ALL_STRINGS = _CORPUS + _fuzz_strings(_FUZZ_ALPHABET, 3000, seed=296)

_SAFE_STRINGS = [
    "plain",
    "tab\tseparated\tvalues",
    "\t",
    "bell\x07 escape\x1b unit\x1f del\x7f",
    "caf\xe9 \u4e2d\u6587 \U0001f600",
    "it's",
    'say "hi"',
    'WHERE name = "" AND x = 1',
    "${secret:scope/key}",
    *_fuzz_strings(_SAFE_ALPHABET, 2000, seed=2962),
]


def _is_lone_surrogate(char: str) -> bool:
    return 0xD800 <= ord(char) <= 0xDFFF


# A line Python ignores: blank, or a comment after optional spaces/tabs.
_COMMENT_OR_BLANK = re.compile(r"[ \t]*(?:#|\Z)")


def _is_comment_block(source: str) -> bool:
    """``source`` parses and every line before its last is blank or a comment."""
    try:
        ast.parse(source)
    except SyntaxError:
        return False
    return all(_COMMENT_OR_BLANK.match(line) for line in source.splitlines()[:-1])


def _comment_values(values: list[str]) -> list[str]:
    """Values inside py_comment's contract.

    NUL (rejected anywhere in a source file; the parse-check reports
    LHP-CFG-031) and lone surrogates (not encodable) are outside it.
    """
    return [
        value
        for value in values
        if "\x00" not in value and not any(_is_lone_surrogate(c) for c in value)
    ]


def _eval_assignment(source: str) -> object:
    """Parse ``source`` (a single ``x = <literal>`` statement) and evaluate it."""
    module = ast.parse(source)
    (statement,) = module.body
    assert isinstance(statement, ast.Assign)
    return ast.literal_eval(statement.value)


def _strip_raw_trailing_whitespace(segment: str) -> str:
    """What ``normalize_content``'s per-line ``rstrip`` removes from one line."""
    end = len(segment)
    while (
        end
        and segment[end - 1].isspace()
        and segment[end - 1] not in _BLOCK_ESCAPED_WHITESPACE
    ):
        end -= 1
    return segment[:end]


def _block_value_after_normalize(value: str) -> str:
    """Expected decoded value of a ``\"\"\"`` literal after normalization.

    Trailing whitespace left raw at a line end inside the literal is stripped by
    the write funnel (documented trade-off); the final segment is followed by
    the closing quotes, so it keeps its trailing whitespace.
    """
    *lines, last = value.split("\n")
    return "\n".join([_strip_raw_trailing_whitespace(line) for line in lines] + [last])


def _legacy_option_literal(value: str) -> str:
    """The pre-#296 option-value site: ``"{{ v|replace('\\\\', ...)|replace('"', ...) }}"``."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


@pytest.mark.unit
class TestPyStr:
    @pytest.mark.parametrize("value", _CORPUS)
    def test_round_trips(self, value):
        assert ast.literal_eval(py_str(value)) == value

    def test_round_trips_after_normalize_content_fuzz(self):
        for value in _ALL_STRINGS:
            normalized = normalize_content(f"x = {py_str(value)}\n")
            assert _eval_assignment(normalized) == value, repr(value)

    def test_output_is_one_physical_line(self):
        for value in _ALL_STRINGS:
            assert len(py_str(value).splitlines()) == 1, repr(value)

    def test_is_double_quoted(self):
        assert py_str("abc") == '"abc"'

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("\\", '"\\\\"'),
            ('"', '"\\""'),
            ("'", '"\'"'),
            ("a\nb", '"a\\nb"'),
            ("a\rb", '"a\\rb"'),
            ("\x00", '"\\x00"'),
            ("\x0b\x0c\x1c\x1d\x1e", '"\\x0b\\x0c\\x1c\\x1d\\x1e"'),
            ("\x85\u2028\u2029", '"\\x85\\u2028\\u2029"'),
            ("\ud800\udfff", '"\\ud800\\udfff"'),
        ],
    )
    def test_escapes_exactly_the_minimal_set(self, value, expected):
        assert py_str(value) == expected

    def test_tab_controls_del_and_non_ascii_stay_raw(self):
        value = "a\tb\x01\x07\x1b\x1f\x7f caf\xe9 \U0001f600"
        assert py_str(value) == f'"{value}"'

    @pytest.mark.parametrize(
        "value",
        [
            "plain",
            'say "hi"',
            "C:\\data\\files",
            '{"topic1":[0,1]}',
            "\t",
            "a\tb\x01\x7f",
            "\u4e2d\u6587 caf\xe9 \U0001f600",
            "${secret:scope/key}",
            "__SECRET_scope_key__",
        ],
    )
    def test_matches_the_legacy_option_literal(self, value):
        assert py_str(value) == _legacy_option_literal(value)

    def test_non_strings_are_stringified(self):
        assert py_str(42) == '"42"'
        assert py_str(None) == '"None"'
        assert py_str(True) == '"True"'


@pytest.mark.unit
class TestPyStrSingle:
    @pytest.mark.parametrize("value", _CORPUS)
    def test_round_trips(self, value):
        assert ast.literal_eval(py_str_single(value)) == value

    def test_round_trips_and_survives_normalize_content_unchanged_fuzz(self):
        for value in _ALL_STRINGS:
            source = f"x = {py_str_single(value)}\n"
            assert normalize_content(source) == source, repr(value)
            assert _eval_assignment(source) == value, repr(value)

    @pytest.mark.parametrize(
        "name",
        ["_ingestion_timestamp", "_source_file", "_processing_timestamp", "col1"],
    )
    def test_ordinary_identifiers_keep_single_quotes(self, name):
        assert py_str_single(name) == f"'{name}'"

    @pytest.mark.parametrize(
        ("char", "escape"),
        [
            ("\u2028", "\\u2028"),
            ("\u2029", "\\u2029"),
            ("\x85", "\\x85"),
            ("\x0b", "\\x0b"),
            ("\x0c", "\\x0c"),
            ("\x1c", "\\x1c"),
            ("\x1d", "\\x1d"),
            ("\x1e", "\\x1e"),
            ("\x00", "\\x00"),
            ("\n", "\\n"),
            ("\r", "\\r"),
            ("\ud800", "\\ud800"),
        ],
    )
    def test_line_boundary_and_invalid_characters_are_escaped(self, char, escape):
        rendered = py_str_single(f"a{char}b")

        assert rendered == f"'a{escape}b'"
        assert char not in rendered

    def test_quotes_backslash_and_raw_characters(self):
        assert py_str_single("it's") == "'it\\'s'"
        assert py_str_single('say "hi"') == "'say \"hi\"'"
        assert py_str_single("C:\\new") == "'C:\\\\new'"
        assert py_str_single("a\tb\x01") == "'a\tb\x01'"
        assert py_str_single(42) == "'42'"


@pytest.mark.unit
class TestPyBlock:
    @pytest.mark.parametrize("value", _CORPUS)
    def test_round_trips(self, value):
        assert ast.literal_eval('"""' + py_block(value) + '"""') == value

    def test_round_trips_fuzz(self):
        for value in _ALL_STRINGS:
            assert ast.literal_eval('"""' + py_block(value) + '"""') == value, repr(
                value
            )

    def test_round_trips_after_normalize_content_modulo_trailing_whitespace(self):
        for value in _ALL_STRINGS:
            normalized = normalize_content(f'x = """{py_block(value)}"""\n')
            assert _eval_assignment(normalized) == _block_value_after_normalize(
                value
            ), repr(value)

    @pytest.mark.parametrize(
        "value",
        [
            "SELECT *\nFROM orders\nWHERE id > 0",
            "\tSELECT a, b\n\tFROM t",
            "SELECT 'single' AS s, \"one\" AS d",
            'SELECT * FROM t WHERE name = "" AND x = 1',
            "Delta source: catalog.schema.table",
            "caf\xe9 \u4e2d\u6587 \U0001f600",
            "bell\x07 unit\x1f del\x7f",
        ],
    )
    def test_normal_text_is_unchanged(self, value):
        assert py_block(value) == value

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ('a"b', 'a"b'),
            ('a""b', 'a""b'),
            ('a"""b', 'a""\\"b'),
            ('a""""b', 'a""\\""b'),
            ('a""""""b', 'a""\\"""\\"b'),
            ('"x', '"x'),
            ('""x', '""x'),
            ('x"', 'x\\"'),
            ('x""', 'x"\\"'),
            ('x"""', 'x""\\"'),
            ('x""""', 'x\\"""\\"'),
            ('"', '\\"'),
        ],
    )
    def test_quotes_escaped_only_to_break_a_triple_or_a_trailing_quote(
        self, value, expected
    ):
        assert py_block(value) == expected

    def test_backslashes_are_doubled(self):
        assert py_block("RLIKE '^\\\\d+$'") == "RLIKE '^\\\\\\\\d+$'"
        assert py_block("C:\\new") == "C:\\\\new"
        assert py_block("ends \\") == "ends \\\\"

    def test_control_characters(self):
        assert py_block("a\rb") == "a\\rb"
        assert py_block("a\nb\tc") == "a\nb\tc"
        assert py_block("\x00\x0b\x0c\x1c\x1d\x1e") == "\\x00\\x0b\\x0c\\x1c\\x1d\\x1e"
        assert py_block("\x01\x1f\x7f") == "\x01\x1f\x7f"
        assert py_block("\x85\u2028\u2029") == "\\x85\\u2028\\u2029"
        assert py_block("\ud800") == "\\ud800"

    def test_no_splitlines_boundary_other_than_newline_survives(self):
        for value in _ALL_STRINGS:
            block = py_block(value)
            assert not _NON_NEWLINE_BOUNDARIES.intersection(block), repr(value)


@pytest.mark.unit
class TestPyBlockNotAtDelimiter:
    """``py_block(at_delimiter=False)``: the value is NOT directly followed by the
    closing ``\"\"\"`` (e.g. a schema-hint line followed by ``,`` or a newline),
    so only a ``\"\"\"`` run inside the value must be broken."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ('x"', 'x"'),
            ('x""', 'x""'),
            ('note STRING COMMENT "free text"', 'note STRING COMMENT "free text"'),
            ('"', '"'),
            ('x"""', 'x""\\"'),
            ('"""', '""\\"'),
            ('a""""b', 'a""\\""b'),
            ('""""""', '""\\"""\\"'),
        ],
    )
    def test_only_triple_quote_runs_are_broken(self, value, expected):
        assert py_block(value, at_delimiter=False) == expected

    def test_default_still_protects_the_closing_delimiter(self):
        assert py_block('x"') == py_block('x"', at_delimiter=True) == 'x\\"'

    @pytest.mark.parametrize("suffix", ["\n", ",\n"])
    def test_round_trips_when_followed_by_text(self, suffix):
        for value in _ALL_STRINGS:
            block = py_block(value, at_delimiter=False)
            assert ast.literal_eval('"""' + block + suffix + '"""') == value + suffix, (
                repr(value)
            )

    def test_round_trips_after_normalize_content_modulo_trailing_whitespace(self):
        for value in _ALL_STRINGS:
            block = py_block(value, at_delimiter=False)
            normalized = normalize_content(f'x = """{block}\n"""\n')
            assert _eval_assignment(normalized) == _block_value_after_normalize(
                value + "\n"
            ), repr(value)

    def test_safe_values_are_returned_verbatim(self):
        for value in (v for v in _SAFE_STRINGS if '"""' not in v):
            assert py_block(value, at_delimiter=False) == value, repr(value)


@pytest.mark.unit
class TestMinimalEscapingInvariant:
    """A value without an escaped character is emitted verbatim."""

    def test_safe_strings_contain_no_escaped_character(self):
        for value in _SAFE_STRINGS:
            assert not _ESCAPED.intersection(value), repr(value)
            assert not any(_is_lone_surrogate(char) for char in value), repr(value)

    def test_py_str_wraps_the_value_verbatim(self):
        for value in (v for v in _SAFE_STRINGS if '"' not in v):
            assert py_str(value) == '"' + value + '"', repr(value)

    def test_py_str_single_wraps_the_value_verbatim(self):
        for value in (v for v in _SAFE_STRINGS if "'" not in v):
            assert py_str_single(value) == "'" + value + "'", repr(value)

    def test_py_block_returns_the_value_verbatim(self):
        for value in _SAFE_STRINGS:
            if '"""' in value or value.endswith('"'):
                continue
            assert py_block(value) == value, repr(value)

    def test_py_str_matches_the_legacy_option_literal(self):
        for value in _SAFE_STRINGS:
            assert py_str(value) == _legacy_option_literal(value), repr(value)


@pytest.mark.unit
class TestPyLiteral:
    def test_json_constants_become_python_constants(self):
        assert py_literal({"flag": True, "nothing": None, "off": False}) == (
            '{"flag": True, "nothing": None, "off": False}'
        )

    def test_nested_containers(self):
        value = {"a": [1, 2.5, "x"], "b": {"c": (None, True)}}
        assert py_literal(value) == '{"a": [1, 2.5, "x"], "b": {"c": [None, True]}}'

    def test_empty_containers(self):
        assert py_literal([]) == "[]"
        assert py_literal({}) == "{}"
        assert py_literal(()) == "[]"

    def test_non_finite_floats(self):
        assert py_literal(math.nan) == 'float("nan")'
        assert py_literal(math.inf) == 'float("inf")'
        assert py_literal(-math.inf) == 'float("-inf")'

    def test_dict_keys_are_converted_like_json(self):
        value = {2: "a", True: "b", None: "c", 1.5: "d", False: "e", math.inf: "f"}
        assert py_literal(value) == json.dumps(value)
        assert py_literal(value) == (
            '{"2": "a", "true": "b", "null": "c", "1.5": "d", "false": "e", '
            '"Infinity": "f"}'
        )

    def test_str_and_int_subclasses_are_spelled_like_json(self):
        """``tojson`` encodes a subclass's raw data, not its ``__str__``."""

        class Mode(str, enum.Enum):
            STREAM = "stream"

        class Level(enum.IntEnum):
            HIGH = 3

        value = {Mode.STREAM: [Mode.STREAM, Level.HIGH]}
        assert str(Mode.STREAM) != "stream"
        assert py_literal(value) == json.dumps(value) == '{"stream": ["stream", 3]}'

    @pytest.mark.parametrize(
        "value",
        ["\t", "a\tb", "caf\xe9", "\x01\x7f", "a\nb", "\u2028", "\ud800", 'q"\\'],
    )
    def test_strings_are_spelled_exactly_like_tojson(self, value):
        assert py_literal(value) == json.dumps(value)
        assert ast.literal_eval(py_literal(value)) == value

    def test_astral_characters_use_one_escape_not_a_surrogate_pair(self):
        assert py_literal("\U0001f600") == '"\\U0001f600"'
        value = {"k\U0001f600": ["a\U0001f600b", "\U0010ffff"]}
        assert py_literal(value) == (
            '{"k\\U0001f600": ["a\\U0001f600b", "\\U0010ffff"]}'
        )
        assert ast.literal_eval(py_literal(value)) == value

    def test_unsupported_type_raises_type_error(self):
        with pytest.raises(TypeError):
            py_literal({1, 2})
        with pytest.raises(TypeError):
            py_literal({(1, 2): "tuple key"})

    def test_byte_identical_to_json_dumps_without_astral_characters(self):
        """Without bool/None VALUES (spelled differently) the bytes match exactly."""
        rng = random.Random(2960)
        for _ in range(500):
            value = _random_json(rng, depth=0, with_constants=False)
            assert py_literal(value) == json.dumps(value), value

    def test_evaluates_to_the_input_structure(self):
        rng = random.Random(2961)
        for _ in range(500):
            value = _random_json(
                rng, depth=0, str_keys_only=True, alphabet=_JSON_ALPHABET + "\U0001f600"
            )
            assert ast.literal_eval(py_literal(value)) == _tuples_to_lists(value)


@pytest.mark.unit
class TestPyComment:
    def test_single_line_is_unchanged(self):
        assert py_comment("orders table") == "orders table"

    @pytest.mark.parametrize(
        "boundary",
        [
            "\n",
            "\r",
            "\r\n",
            "\x0b",
            "\x0c",
            "\x1c",
            "\x1d",
            "\x1e",
            "\x85",
            "\u2028",
            "\u2029",
        ],
    )
    def test_every_splitlines_boundary_starts_a_new_comment_line(self, boundary):
        assert py_comment(f"line1{boundary}INJECTED = 1") == "line1\n# INJECTED = 1"

    def test_injection_stays_a_comment_after_normalize(self):
        values = [*_ALL_STRINGS, "line1\nINJECTED = 1", "a\r\nb = 2\u2028c = 3"]
        for value in _comment_values(values):
            normalized = normalize_content(f"# {py_comment(value)}\nx = 1\n")
            module = ast.parse(normalized)
            assert len(module.body) == 1, repr(value)
            assert all(
                _COMMENT_OR_BLANK.match(line) for line in normalized.splitlines()[:-1]
            ), repr(value)

    def test_empty_and_trailing_newline(self):
        assert py_comment("") == ""
        assert py_comment("a\n") == "a\n"
        assert py_comment("a\n\nb") == "a\n\n# b"

    @pytest.mark.parametrize(
        "value",
        [
            "orders table",
            "a\n",
            "a\n\n",
            "\n",
            "a\n# b",
            "a\n\n# b\n",
            "a\n  # indented comment\n\t# tab",
            "a\n   \n\t\n",
            "a\n#\n#no space\n",
        ],
    )
    def test_comments_that_were_already_valid_are_unchanged(self, value):
        assert py_comment(value) == value

    @pytest.mark.parametrize(
        "suffix", ["\nx = 1\n", " table\nx = 1\n"], ids=["line-end", "mid-line"]
    )
    def test_previously_valid_comments_render_the_same_lines(self, suffix):
        """A value that rendered as a valid comment before #296 renders the same now.

        "Valid comment before" means the old text, as written, parses and every
        line ahead of the template's code is blank or a comment. ``suffix`` is the
        template text after the value: a line end, or more text on the same line.
        """
        values = [
            *_ALL_STRINGS,
            *_fuzz_strings("ab #\t\n\r\x0b\x0c\x1c\x85\u2028\xa0\x1f", 3000, seed=2963),
            "a\r",
            "a\n\r",
            "a\r\n# b\r\n",
            "a\u2028\u2028# b",
        ]
        multi_line = 0
        for value in _comment_values(values):
            before = normalize_content(f"# {value}{suffix}")
            if not _is_comment_block(before):
                continue
            multi_line += len(value.splitlines()) > 1
            assert normalize_content(f"# {py_comment(value)}{suffix}") == before, repr(
                value
            )
        assert multi_line >= 100

    def test_lines_that_would_run_as_code_are_commented(self):
        assert py_comment("line1\nINJECTED = 1") == "line1\n# INJECTED = 1"
        assert py_comment("a\n  x = 1\n# ok\n\ny") == "a\n#   x = 1\n# ok\n\n# y"
        assert py_comment("a\n\u00a0x") == "a\n# \u00a0x"
        assert py_comment("a\n\u00a0\x1f\n\t#") == "a\n\u00a0\x1f\n\t#"

    def test_other_line_breaks_become_newlines(self):
        assert py_comment("a\r\n# b\u2028c\x0c\x0cd\r\ne") == "a\n# b\n# c\n\n# d\n# e"

    def test_trailing_lone_carriage_return_is_not_kept(self):
        """The old output merged it with the template's own newline into one break."""
        assert py_comment("a\r") == "a"
        assert py_comment("a\n\r") == "a\n"
        assert py_comment("a\r\n") == "a\n"


@pytest.mark.unit
def test_filter_mapping_exposes_the_five_filters():
    assert dict(PYTHON_LITERAL_FILTERS) == {
        "py_str": py_str,
        "py_str_single": py_str_single,
        "py_block": py_block,
        "py_literal": py_literal,
        "py_comment": py_comment,
    }


# BMP only: astral characters are the one place py_literal departs from json.dumps.
_JSON_ALPHABET = 'abcXYZ019 _-"\\/{}[]:,\t\n\x01\x7f\xe9\u2028\u4e2d\ud800'


def _random_text(rng: random.Random, alphabet: str) -> str:
    return "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 8)))


def _random_json(
    rng: random.Random,
    depth: int,
    str_keys_only: bool = False,
    with_constants: bool = True,
    alphabet: str = _JSON_ALPHABET,
):
    kinds = ["str", "int", "float"] + (["bool", "none"] if with_constants else [])
    if depth < 3:
        kinds += ["list", "tuple", "dict"]
    kind = rng.choice(kinds)
    if kind == "str":
        return _random_text(rng, alphabet)
    if kind == "int":
        return rng.randint(-(10**12), 10**12)
    if kind == "float":
        return rng.uniform(-1e6, 1e6)
    if kind == "bool":
        return rng.choice([True, False])
    if kind == "none":
        return None
    children = [
        _random_json(rng, depth + 1, str_keys_only, with_constants, alphabet)
        for _ in range(rng.randint(0, 4))
    ]
    if kind == "list":
        return children
    if kind == "tuple":
        return tuple(children)
    keys = [
        _random_text(rng, alphabet)
        if str_keys_only
        else rng.choice(
            [_random_text(rng, alphabet), rng.randint(-50, 50), True, False, None, 0.5]
        )
        for _ in children
    ]
    return dict(zip(keys, children, strict=True))


def _tuples_to_lists(value):
    if isinstance(value, (list, tuple)):
        return [_tuples_to_lists(item) for item in value]
    if isinstance(value, dict):
        return {key: _tuples_to_lists(item) for key, item in value.items()}
    return value
