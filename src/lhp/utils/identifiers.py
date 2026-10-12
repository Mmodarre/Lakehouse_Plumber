"""Character replacement for names that end up inside generated Python identifiers.

``replace_non_identifier_chars`` swaps every character outside
``[a-zA-Z0-9_]`` for ``_``, one for one. It does not produce a guaranteed-valid
identifier: a leading digit or a Python keyword passes through unchanged.
Generated names already depend on exactly this mapping (quarantine flow and
schema variable names), so adding such handling would rename them.
"""

from __future__ import annotations

import re

_NON_IDENTIFIER_CHARS = re.compile(r"[^a-zA-Z0-9_]")


def replace_non_identifier_chars(value: str) -> str:
    """Return ``value`` with each character outside ``[a-zA-Z0-9_]`` replaced by ``_``."""
    return _NON_IDENTIFIER_CHARS.sub("_", value)
