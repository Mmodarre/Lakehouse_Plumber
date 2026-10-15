"""Guard for #296: template values inside Python strings and comments are escaped.

Every ``src/lhp/templates/**/*.py.j2`` is scanned with Jinja's own lexer. The
template text between Jinja tags is fed through a small Python lexer that
tracks code / ``#`` comment / string state across lines, including
triple-quoted literals whose ``{{ … }}`` sits on a later line and f-string
replacement fields. Each ``{{ … }}`` expression is then checked against the
Python context it lands in:

- ``#`` comment: the last filter must be ``py_comment``;
- inside a (non-f) triple-quoted literal: the last filter must be
  ``py_block``; ``at_delimiter`` must be left at its default when the
  literal's own quote character follows the value and must be ``False``
  otherwise; the value may not directly follow the literal's own quote
  character or another value;
- inside a single-line or f-string literal: never allowed, because
  ``py_str`` / ``py_str_single`` render the whole literal, quotes included;
- code: anything except ``py_block`` / ``py_comment``.

``tojson`` may not appear in a ``.py.j2`` at all (``py_literal`` replaces it).
Intentional raw sites are listed in ``ALLOWLIST`` with a reason; an entry
that no longer matches anything fails the test.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from jinja2 import Environment, nodes

import lhp

TEMPLATES_ROOT = Path(lhp.__file__).parent / "templates"

# Same lexer settings as every LHP code-generation environment.
_ENV = Environment(trim_blocks=True, lstrip_blocks=True)  # nosec B701

_FORMAT_NAME_REASON = (
    "source text of the user's name() return literal, captured by regex; "
    "it is already literal text, so a filter would double-escape it"
)

# (template path relative to src/lhp/templates, expression) -> reason
ALLOWLIST: dict[tuple[str, str], str] = {
    ("load/custom_datasource.py.j2", "datasource_format_name"): _FORMAT_NAME_REASON,
    ("write/sinks/custom_sink.py.j2", "datasink_format_name"): _FORMAT_NAME_REASON,
    ("write/materialized_view.py.j2", "refresh_policy"): (
        "enum (auto, incremental, incremental_strict, full) validated before generation"
    ),
    ("write/sinks/kafka_sink.py.j2", "'Event Hubs' if is_event_hubs else 'Kafka'"): (
        "one of two constant labels chosen by a boolean, not user text"
    ),
    ("transform/data_quality_quarantine.py.j2", "safe_source_view"): (
        "reduced to [A-Za-z0-9_] by replace_non_identifier_chars; the same "
        "value names functions and variables in this template"
    ),
}

_ESCAPING_FILTERS = {"py_str", "py_str_single", "py_block", "py_literal", "py_comment"}
_F_STRING_PREFIXES = {"f", "rf", "fr"}


@dataclass(frozen=True)
class Violation:
    template: str
    line: int
    expression: str
    problem: str

    def __str__(self) -> str:
        return f"{self.template}:{self.line}: {{{{ {self.expression} }}}} -- {self.problem}"


class _PythonLexState:
    """Python lexical state at the cursor: code, comment or string.

    Frames are ``("code", bracket_depth, in_fstring_field)``, ``("comment",)``
    or ``("string", quote, is_f)``. Jinja expression output is opaque and does
    not change the state.
    """

    def __init__(self) -> None:
        self.frames: list[tuple] = [("code", 0, False)]
        self.word = ""
        self.last_content = ""

    def context(self) -> tuple[str, str]:
        frame = self.frames[-1]
        if frame[0] == "comment":
            return "comment", ""
        if frame[0] == "string":
            _, quote, is_f = frame
            return ("block" if len(quote) == 3 and not is_f else "string"), quote
        return "code", ""

    def hole(self) -> None:
        self.word = ""
        self.last_content = ""

    def feed(self, text: str) -> None:
        index = 0
        while index < len(text):
            frame = self.frames[-1]
            if frame[0] == "comment":
                if text[index] == "\n":
                    self.frames.pop()
                index += 1
            elif frame[0] == "string":
                index = self._string_step(text, index, frame)
            else:
                index = self._code_step(text, index, frame)

    def _string_step(self, text: str, index: int, frame: tuple) -> int:
        _, quote, is_f = frame
        char = text[index]
        if char == "\\":
            self.last_content = ""
            return index + 2
        if is_f and text.startswith(("{{", "}}"), index):
            self.last_content = char
            return index + 2
        if is_f and char == "{":
            self.frames.append(("code", 0, True))
            self.word = ""
            return index + 1
        if text.startswith(quote, index):
            self.frames.pop()
            self.word = ""
            return index + len(quote)
        if char == "\n" and len(quote) == 1:
            raise ValueError("newline inside a single-line string literal")
        self.last_content = char
        return index + 1

    def _code_step(self, text: str, index: int, frame: tuple) -> int:
        _, depth, in_field = frame
        char = text[index]
        if char in "'\"":
            quote = char * 3 if text.startswith(char * 3, index) else char
            is_f = self.word.lower() in _F_STRING_PREFIXES
            self.frames.append(("string", quote, is_f))
            self.word = ""
            self.last_content = ""
            return index + len(quote)
        if char == "#":
            self.frames.append(("comment",))
        elif char in "([{":
            self.frames[-1] = ("code", depth + 1, in_field)
        elif char == "}" and in_field and depth == 0:
            self.frames.pop()
        elif char in ")]}":
            self.frames[-1] = ("code", max(depth - 1, 0), in_field)
        self.word = self.word + char if char.isalnum() or char == "_" else ""
        return index + 1


def _expression_node(source: str) -> nodes.Expr:
    output = _ENV.parse("{{" + source + "}}").body[0]
    assert isinstance(output, nodes.Output)
    return output.nodes[0]


def _at_delimiter(node: nodes.Filter) -> bool | None:
    """The ``at_delimiter`` argument of a ``py_block`` call; None if not constant."""
    values = [kw.value for kw in node.kwargs if kw.key == "at_delimiter"] + node.args
    if not values:
        return True
    value = values[0]
    return value.value if isinstance(value, nodes.Const) else None


def _next_output(tokens: list[tuple[int, str, str]], start: int) -> tuple[str, str]:
    """Kind and text of the next output after ``start``, skipping Jinja tags."""
    for _, kind, value in tokens[start:]:
        if kind == "data" and value:
            return "data", value
        if kind == "variable_begin":
            return "variable", ""
    return "end", ""


def _problem(
    source: str, context: str, quote: str, follower: tuple[str, str], after_quote: bool
) -> str | None:
    node = _expression_node(source)
    name = node.name if isinstance(node, nodes.Filter) else None
    if context == "code":
        if name in ("py_block", "py_comment"):
            return f"|{name} used in code; it only belongs inside a literal/comment"
        return None
    if context == "comment":
        return None if name == "py_comment" else "in a # comment without |py_comment"
    if context == "string":
        return (
            "inside a single-line or f-string literal; render the whole literal "
            "with |py_str or |py_str_single"
        )
    if name != "py_block":
        return "inside a triple-quoted literal without |py_block"
    if after_quote:
        return "follows a quote inside the literal; a leading quote could close it"
    if follower[0] == "variable":
        return "directly followed by another value inside the literal"
    quote_follows = follower[0] == "data" and follower[1].startswith(quote[0])
    at_delimiter = _at_delimiter(node)
    if quote_follows and at_delimiter is not True:
        return "a quote follows the value: use |py_block (at_delimiter=True)"
    if not quote_follows and at_delimiter is not False:
        return "text follows the value: use |py_block(at_delimiter=False)"
    return None


def scan_template(source: str, template: str) -> list[Violation]:
    """Return every unescaped value and every ``tojson`` use in ``source``."""
    tokens = list(_ENV.lex(source))
    violations = [
        Violation(template, line, "tojson", "tojson: use |py_literal")
        for line, kind, value in tokens
        if (kind == "name" and value == "tojson")
        or (kind == "string" and value[1:-1] == "tojson")
    ]
    state = _PythonLexState()
    index = 0
    while index < len(tokens):
        line, kind, value = tokens[index]
        if kind == "data":
            state.feed(value)
        elif kind == "variable_begin":
            end = next(
                i for i in range(index, len(tokens)) if tokens[i][1] == "variable_end"
            )
            expression = "".join(token[2] for token in tokens[index + 1 : end])
            context, quote = state.context()
            problem = _problem(
                expression,
                context,
                quote,
                _next_output(tokens, end + 1),
                after_quote=bool(quote) and state.last_content == quote[0],
            )
            if problem:
                violations.append(
                    Violation(template, line, " ".join(expression.split()), problem)
                )
            state.hole()
            index = end
        index += 1
    return violations


def _template_violations() -> list[Violation]:
    violations: list[Violation] = []
    for path in sorted(TEMPLATES_ROOT.rglob("*.py.j2")):
        relative = path.relative_to(TEMPLATES_ROOT).as_posix()
        violations += scan_template(path.read_text(encoding="utf-8"), relative)
    return violations


@pytest.mark.unit
class TestTemplatesEscapeEmbeddedValues:
    def test_every_value_in_a_python_string_or_comment_is_escaped(self):
        unexpected = [
            str(violation)
            for violation in _template_violations()
            if (violation.template, violation.expression) not in ALLOWLIST
        ]

        assert not unexpected, "Unescaped template values:\n" + "\n".join(unexpected)

    def test_every_allowlist_entry_still_matches_a_site(self):
        used = {(v.template, v.expression) for v in _template_violations()}

        assert set(ALLOWLIST) <= used, f"Stale entries: {set(ALLOWLIST) - used}"

    def test_scan_covers_every_python_template(self):
        templates = sorted(TEMPLATES_ROOT.rglob("*.py.j2"))

        assert len(templates) >= 35
        assert all(path.read_text(encoding="utf-8") for path in templates)


def _problems(source: str) -> list[tuple[int, str]]:
    return [(v.line, v.expression) for v in scan_template(source, "t.py.j2")]


@pytest.mark.unit
class TestScanner:
    """The scanner flags the patterns the templates used before #296."""

    @pytest.mark.parametrize(
        "source",
        [
            'x = "{{ y }}"\n',
            'x = "x_{{ y }}"\n',
            "x = '{{ y }}'\n",
            'f(a="{{ y }}", b={{ z|py_str }})\n',
            'x = "{{ y|py_str }}"\n',
            'x = f"a {{ y }}"\n',
        ],
        ids=["double", "mid-string", "single", "keyword", "py_str-in-quotes", "fstr"],
    )
    def test_value_inside_single_line_literal_is_flagged(self, source):
        assert _problems(source) == [(1, source.split("{{ ")[1].split(" }}")[0])]

    def test_triple_quoted_literal_tracked_across_lines(self):
        source = (
            'spark.sql("""\n'
            "{% for line in lines %}\n"
            "        {{ line }}\n"
            "{% endfor %}\n"
            '    """)\n'
            "x = {{ ok }}\n"
        )

        assert _problems(source) == [(3, "line")]

    def test_block_mode_must_match_what_follows(self):
        assert _problems('x = """{{ a|py_block }}"""\n') == []
        assert _problems('x = """{{ a|py_block(at_delimiter=False) }} b"""\n') == []
        assert _problems('x = """{{ a|py_block }} b"""\n') == [(1, "a|py_block")]
        assert _problems('x = """{{ a|py_block(at_delimiter=False) }}"""\n') == [
            (1, "a|py_block(at_delimiter=False)")
        ]

    def test_block_value_after_a_quote_or_next_to_a_value_is_flagged(self):
        after_quote = 'x = """a "{{ v|py_block(at_delimiter=False) }} b"""\n'
        adjacent = 'x = """{{ a|py_block(at_delimiter=False) }}{{ b|py_block }}"""\n'

        assert _problems(after_quote) == [(1, "v|py_block(at_delimiter=False)")]
        assert _problems(adjacent) == [(1, "a|py_block(at_delimiter=False)")]

    def test_comment_values_need_py_comment(self):
        assert _problems("# {{ c }}\nx = 1\n") == [(1, "c")]
        assert _problems("# {{ c|py_comment }}\nx = '#'\n") == []

    def test_code_values_and_fstring_fields_are_code(self):
        source = (
            "{{ name }} = {{ value|py_literal }}\n"
            "y = f\"a '{SOURCE_{{ v }}}' {x!r} {', '.join(z)}\"\n"
            "z = {{ s|py_str }} + {{ q|py_str_single }}\n"
        )

        assert _problems(source) == []

    def test_block_filters_in_code_are_flagged(self):
        assert _problems("x = {{ a|py_block }}\n") == [(1, "a|py_block")]

    def test_strings_and_escapes_do_not_leak_state(self):
        source = (
            "a = 'it\\'s # not a comment'\n"
            'b = "{"\n'
            "c = '''one\n"
            "two'''\n"
            "d = {{ v|py_str }}\n"
        )

        assert _problems(source) == []

    @pytest.mark.parametrize(
        "source",
        [
            "x = {{ y | tojson }}\n",
            "x = struct({{ ys | map('tojson') | join(', ') }})\n",
            '{% set z = ys | map("tojson") | list %}\n',
        ],
    )
    def test_tojson_is_flagged(self, source):
        assert (1, "tojson") in _problems(source)
