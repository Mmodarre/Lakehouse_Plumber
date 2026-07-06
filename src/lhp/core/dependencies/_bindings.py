"""Statically-known YAML parameter values that seed AST dependency extraction.

Models the parameter values a flowgroup YAML declares for a Python function
(transform/load source functions, snapshot_cdc source_function) so the AST
visitor can substitute them when it walks the function body looking for table
references. Only values that are statically known as strings (or
string-shaped containers) are representable; everything else is "unbindable"
and simply absent.

Byte fidelity is a hard rule here: substitution tokens such as ``${env}`` or
``${secret:scope/key}`` inside strings are preserved EXACTLY as loaded —
never resolved, trimmed, or otherwise altered. Canonicalization of table
references happens elsewhere, at match time, not in this module.

This module is a LEAF: stdlib imports only, no ``lhp.*`` imports.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Union

__all__ = [
    "Bound",
    "DictValue",
    "ListValue",
    "ParameterBindings",
    "bound_from_yaml",
    "merge_bound",
]


@dataclass(frozen=True)
class ListValue:
    """An ordered list of statically-known strings."""

    items: Tuple[str, ...]


@dataclass(frozen=True)
class DictValue:
    """A string-keyed mapping of statically-known (bound) values.

    The dict field gives this frozen dataclass interior mutability; that is
    acceptable — consumers rely on equality, not hashing.
    """

    entries: Dict[str, "Bound"]


#: A statically-known parameter value. The ``frozenset[str]`` form is the
#: set of possible literal string values (token bytes preserved verbatim).
Bound = Union[frozenset[str], ListValue, DictValue]


@dataclass(frozen=True)
class ParameterBindings:
    """How YAML-declared parameters bind to one Python function's signature.

    Exactly one binding style is populated per instance:

    - **kwonly style** (``kwonly`` set; ``dict_arg_index``/``dict_value``
      None): codegen applies the parameters via ``functools.partial`` as
      keyword arguments (snapshot_cdc ``source_function``).
    - **positional-dict style** (``dict_arg_index`` AND ``dict_value`` set;
      ``kwonly`` None): codegen passes the whole parameters dict as a single
      positional argument at ``dict_arg_index`` (python transform/load).

    Any other combination raises ``ValueError`` at construction.
    """

    function_name: str
    kwonly: Optional[DictValue] = None
    dict_arg_index: Optional[int] = None
    dict_value: Optional[DictValue] = None

    def __post_init__(self) -> None:
        has_dict = self.dict_arg_index is not None or self.dict_value is not None
        kwonly_style = self.kwonly is not None and not has_dict
        dict_style = (
            self.kwonly is None
            and self.dict_arg_index is not None
            and self.dict_value is not None
        )
        if not (kwonly_style or dict_style):
            raise ValueError(
                "ParameterBindings requires exactly one style: kwonly XOR "
                "(dict_arg_index AND dict_value), got "
                f"kwonly={self.kwonly!r}, dict_arg_index={self.dict_arg_index!r}, "
                f"dict_value={self.dict_value!r}"
            )


def bound_from_yaml(value: object) -> Optional[Bound]:
    """Convert a YAML-loaded Python value to a :data:`Bound`, or None.

    - ``str`` → ``frozenset({value})``, bytes preserved verbatim (including
      ``${token}`` / ``${secret:scope/key}`` substrings).
    - ``list`` of ONLY ``str`` → :class:`ListValue` (order preserved). Any
      non-str element makes the whole list unbindable (None).
    - ``dict`` → :class:`DictValue`, recursing each value. Entries whose
      value is unbindable — or whose key is not a str — are dropped; the
      remaining entries still bind.
    - Anything else (``int``, ``float``, ``bool``, ``None``, tuples, …) →
      None. ``bool``/``int`` are never bindable.
    """
    if isinstance(value, str):
        return frozenset({value})
    if isinstance(value, list):
        items: list[str] = []
        for item in value:
            if not isinstance(item, str):
                return None
            items.append(item)
        return ListValue(tuple(items))
    if isinstance(value, dict):
        entries: Dict[str, Bound] = {}
        for key, raw in value.items():
            if not isinstance(key, str):
                continue
            bound = bound_from_yaml(raw)
            if bound is not None:
                entries[key] = bound
        return DictValue(entries)
    return None


def merge_bound(existing: Optional["Bound"], value: "Bound") -> "Bound":
    """Merge a rebinding onto an existing bound (shared scope semantics).

    A string-set rebinding merges via union — reassignment and conditional
    branches accumulate candidate values. Any rebinding involving a
    structured value (``ListValue`` / ``DictValue`` on either side) is
    last-write-wins: unioning heterogeneous shapes would fabricate values
    that no execution path produces. Shared by the extraction visitor's
    scope stack and the call-resolution engine's environment replay so the
    two can never drift.
    """
    if isinstance(existing, frozenset) and isinstance(value, frozenset):
        return existing | value
    return value
