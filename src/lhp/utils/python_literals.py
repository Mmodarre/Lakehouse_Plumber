"""Render values as Python source text for code-generation templates.

Escaping is minimal: only characters that would make a literal invalid, or
break it when the file is written, are escaped. Generated files go through
``normalize_content``, which splits on every ``str.splitlines`` boundary and
strips trailing whitespace per line, and are written as UTF-8. So the escaped
set is a backslash, the active quote, a newline, a carriage return, NUL, the
other line boundaries and lone surrogates. Everything else (tabs, other
control characters, DEL, non-ASCII text) stays raw, so a value with none of the
escaped characters renders exactly as it did when templates wrapped it in quotes
by hand.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any

_ESCAPES = {
    "\\": "\\\\",
    "\n": "\\n",
    "\r": "\\r",
    "\x00": "\\x00",
    **{char: f"\\x{ord(char):02x}" for char in "\x0b\x0c\x1c\x1d\x1e\x85"},
    "\u2028": "\\u2028",
    "\u2029": "\\u2029",
    **{chr(code): f"\\u{code:04x}" for code in range(0xD800, 0xE000)},
}

_TRANSLATIONS = {
    '"': str.maketrans({**_ESCAPES, '"': '\\"'}),
    "'": str.maketrans({**_ESCAPES, "'": "\\'"}),
    # Inside an open """ the newline stays raw; quotes are handled by run below.
    '"""': str.maketrans({k: v for k, v in _ESCAPES.items() if k != "\n"}),
}

_QUOTE_RUN = re.compile(r'"+')

_ASTRAL = re.compile("([\U00010000-\U0010ffff])")

# A comment line: ``#`` after the only indentation Python's tokenizer accepts.
_COMMENT_LINE = re.compile(r"[ \t]*#")


def _escape(text: str, quote: str, *, at_delimiter: bool = True) -> str:
    """Escape ``text`` for the body of a literal delimited by ``quote``.

    ``quote`` is ``'"'``, ``"'"`` or ``'\"\"\"'``. In a triple-quoted body a ``"``
    is escaped only where it would complete a ``\"\"\"`` run or, when
    ``at_delimiter`` (the closing ``\"\"\"`` directly follows the text), sit
    before the closing delimiter. A quote that was already valid there stays raw.
    """
    escaped = text.translate(_TRANSLATIONS[quote])
    if quote != '"""':
        return escaped
    return _QUOTE_RUN.sub(
        lambda run: _escape_quote_run(
            len(run.group()), at_delimiter and run.end() == len(escaped)
        ),
        escaped,
    )


def _escape_quote_run(length: int, at_end: bool) -> str:
    """Escape every third quote of a run; a trailing run is counted from its end."""
    return "".join(
        '\\"'
        if ((length - 1 - position) if at_end else (position + 1)) % 3 == 0
        else '"'
        for position in range(length)
    )


def py_str(value: Any) -> str:
    """Return a double-quoted Python string literal that evaluates to ``str(value)``."""
    return '"' + _escape(str(value), '"') + '"'


def py_str_single(value: Any) -> str:
    """Return a single-quoted Python string literal that evaluates to ``str(value)``.

    For template sites that were written with single quotes, so ordinary values
    keep today's exact bytes (``'name'``).
    """
    return "'" + _escape(str(value), "'") + "'"


def py_block(value: Any, at_delimiter: bool = True) -> str:
    """Return text that, placed inside ``\"\"\"...\"\"\"``, evaluates to ``str(value)``.

    Newlines stay raw so multi-line text (SQL, docstrings) stays readable.
    Trailing whitespace before a raw newline is still stripped by
    ``normalize_content`` when the file is written.

    Args:
        value: The text to embed.
        at_delimiter: ``True`` when the closing ``\"\"\"`` directly follows the
            value, so a trailing ``"`` must be escaped. Pass ``False`` for a value
            followed by other template text (e.g. ``,`` or a newline); then only
            ``\"\"\"`` runs inside the value are broken.
    """
    return _escape(str(value), '"""', at_delimiter=at_delimiter)


def py_literal(value: Any) -> str:
    """Return a Python literal for JSON-shaped data, spelled as ``tojson`` spelled it.

    Output matches ``json.dumps`` (separators, number text, string escapes, dict
    key conversion, the raw data of ``str``/``int`` subclasses) except: the
    constants are ``True``/``False``/``None``, non-finite floats are
    ``float(...)`` calls, and a character outside the Basic Multilingual Plane
    is one ``\\U`` escape instead of a surrogate pair (which Python would decode
    as two lone surrogates).

    Raises:
        TypeError: If ``value`` contains a type ``json.dumps`` cannot encode.
    """
    if isinstance(value, str):
        return _json_str_literal(value)
    if value is None:
        return "None"
    if value is True:
        return "True"
    if value is False:
        return "False"
    if isinstance(value, int):
        return int.__repr__(value)
    if isinstance(value, float):
        return _float_literal(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(py_literal(item) for item in value) + "]"
    if isinstance(value, dict):
        items = (
            f"{_json_str_literal(_json_key(key))}: {py_literal(item)}"
            for key, item in value.items()
        )
        return "{" + ", ".join(items) + "}"
    raise TypeError(
        f"Object of type {type(value).__name__} cannot be rendered as a Python literal"
    )


def py_comment(value: Any) -> str:
    """Return text for a ``# `` comment line in which no line can run as code.

    The template supplies the first ``# ``. The value is split on every
    ``str.splitlines`` boundary and rejoined with ``\\n``. A later line that is
    blank (whitespace only; the write step strips it) or already a comment (``#``
    after optional spaces/tabs) is kept as it is; any other later line gets
    ``# ``. A trailing line break is kept, except a lone ``\\r``, which used to
    merge with the template's own newline. So a value that was already a valid
    comment renders the same lines as before.
    """
    text = str(value)
    lines = text.splitlines()
    if not lines:
        return ""
    line_break = text.splitlines(keepends=True)[-1][len(lines[-1]) :]
    if line_break and line_break != "\r":
        lines.append("")
    return "\n".join(
        [lines[0]]
        + [
            line if not line.strip() or _COMMENT_LINE.match(line) else "# " + line
            for line in lines[1:]
        ]
    )


def _json_str_literal(text: str) -> str:
    """``json.dumps`` of ``text``'s string data, with astral characters as ``\\U``."""
    parts = _ASTRAL.split(text)
    body = "".join(
        f"\\U{ord(part):08x}" if index % 2 else json.dumps(part)[1:-1]
        for index, part in enumerate(parts)
    )
    return '"' + body + '"'


def _float_literal(value: float) -> str:
    if math.isnan(value):
        return 'float("nan")'
    if math.isinf(value):
        return 'float("inf")' if value > 0 else 'float("-inf")'
    return float.__repr__(value)


def _json_key(key: Any) -> str:
    """Convert a dict key to the string ``json.dumps`` would emit for it."""
    if isinstance(key, str):
        return key
    if isinstance(key, float):
        return json.dumps(key)
    if key is True:
        return "true"
    if key is False:
        return "false"
    if key is None:
        return "null"
    if isinstance(key, int):
        return int.__repr__(key)
    raise TypeError(
        f"keys must be str, int, float, bool or None, not {type(key).__name__}"
    )


PYTHON_LITERAL_FILTERS: Mapping[str, Callable[[Any], str]] = MappingProxyType(
    {
        "py_str": py_str,
        "py_str_single": py_str_single,
        "py_block": py_block,
        "py_literal": py_literal,
        "py_comment": py_comment,
    }
)
