"""Environment-variable flag helpers.

Two readings of a switch are needed across LHP and they are NOT the same
question. ``env_truthy`` asks "is this variable set?", which is how CI
providers and coding agents announce themselves — the value may be ``1``,
``true`` or an opaque build id, and only an empty string or an explicit
negative word means "not set". ``env_value_in`` asks "does this variable hold
one of these exact words?", which is how an explicit user switch such as
``LHP_TELEMETRY=off`` is matched.
"""

from __future__ import annotations

from typing import Iterable, Mapping

# The spellings of "no" that a marker variable may carry. Anything else with
# content — including a build id or a path — counts as set.
_FALSY_VALUES = frozenset({"0", "false", "no"})


def env_truthy(environ: Mapping[str, str], name: str) -> bool:
    """Report whether ``name`` is set to anything other than a negative word.

    Matching is case-insensitive. A missing variable and an empty value are
    both "not set".
    """
    value = environ.get(name, "")
    return bool(value) and value.lower() not in _FALSY_VALUES


def env_value_in(environ: Mapping[str, str], name: str, values: Iterable[str]) -> bool:
    """Report whether ``name``'s value is one of ``values``.

    Both sides are lowercased before comparison, so callers may pass the
    canonical spellings. A missing variable never matches.
    """
    value = environ.get(name)
    if value is None:
        return False
    return value.lower() in {candidate.lower() for candidate in values}
