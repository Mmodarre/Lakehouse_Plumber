"""A lexical scan of the STREAM reads of SQL that sqlglot cannot parse.

The shared SQL reader parses with sqlglot, which rejects some valid Databricks
SQL, such as a ``WATERMARK`` clause or ``STREAM(t) WITH (...)`` options. Only for
such a body, :func:`scan_stream_reads` finds the argument of each
``STREAM(<arg>)`` and ``STREAM <name>`` read lexically, after blanking string
literals and comments, and classifies it:

- a placeholder (``$source``, a whole ``${token}``) or a 1-part name names a
  view, not a table: it yields nothing;
- a function call (``STREAM read_files(...)``) streams from a table-valued
  function, not from a table's rows: it yields nothing;
- a 2- or 3-part name is a table read: it yields the name as written;
- anything else cannot be classified: it yields ``None``, a read whose table
  is unknown.
"""

import re
from typing import List, Optional, Tuple

from ._sql_stream_reads import _STREAM_WORD

_BLANKED = re.compile(
    r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|--[^\n]*|/\*.*?\*/", re.DOTALL
)
_PART = r"(?:`[^`]*`|\$\{[^}]*\}|[\w$])+"
_NAME = re.compile(rf"{_PART}(?:\.{_PART})*")
_PARTS = re.compile(_PART)
_FUNCTION_CALL = re.compile(rf"{_PART}\s*\(.*\)", re.DOTALL)
_BARE_PLACEHOLDER = re.compile(r"\$\w+")


def scan_stream_reads(sql: str) -> Tuple[Optional[str], ...]:
    """The tables ``sql`` reads with STREAM, found lexically.

    For SQL the shared reader cannot parse. Names are unique, sorted, and
    spelled as written; a trailing ``None`` stands for every read whose
    argument cannot be classified (see the module docstring).
    """
    text = _BLANKED.sub(" ", sql)
    names: List[str] = []
    unknown = False
    for match in _STREAM_WORD.finditer(text):
        rest = text[match.end() :]
        stripped = rest.lstrip()
        if stripped.startswith("("):
            argument = _parenthesized(stripped)
        elif rest[:1].isspace() and (name := _NAME.match(stripped)) is not None:
            if stripped[name.end() :].lstrip().startswith("("):
                continue  # STREAM <function>(...): a table-valued function
            argument = name.group(0)
        else:
            continue  # the word STREAM, not a STREAM read
        kind = _classify(argument)
        if kind is None:
            unknown = True
        elif kind:
            names.append(kind)
    reads: Tuple[Optional[str], ...] = tuple(sorted(set(names)))
    return (*reads, None) if unknown else reads


def _parenthesized(text: str) -> Optional[str]:
    """The text inside the parenthesis ``text`` opens, or ``None`` if unclosed."""
    depth = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[1:index].strip()
    return None


def _classify(argument: Optional[str]) -> Optional[str]:
    """``""`` for no table read, a table name, or ``None`` when unknown."""
    if argument is None:
        return None
    if _NAME.fullmatch(argument):
        parts = _PARTS.findall(argument)
        if len(parts) == 1 or any(_BARE_PLACEHOLDER.fullmatch(p) for p in parts):
            return ""
        return argument if len(parts) <= 3 else None
    if _FUNCTION_CALL.fullmatch(argument):
        return ""
    return None
