"""Type-generic serializer for the public lhp.api DTO/event surface.

Constitution §9.22 forbids per-type dispatch branches that name DTO
classes; this implementation reasons about shape only
(``dataclasses.is_dataclass``, ``Mapping``, ``Sequence``, ``Path``,
scalars). The parameter is typed ``Any`` because dispatch is structural.

:stability: provisional
"""
from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lhp.api.responses import JSONValue


def to_dict(obj: Any) -> JSONValue:
    """Recursively serialise ``obj`` into a JSON-compatible value.

    Backs the round-trip contract (constitution §1.8): every public
    DTO/event in ``lhp.api`` must round-trip via ``to_dict`` +
    ``json.dumps`` + ``json.loads`` + ``__init__``.

    :raises TypeError: When ``obj`` is not of a recognised structural
        type. Names the concrete runtime type so the caller can decide
        whether to widen the input or fix the producer.

    :stability: provisional
    """
    if obj is None:
        return None

    # bool BEFORE int — bool is a subclass of int; matching int first
    # would silently coerce True → 1 / False → 0.
    if isinstance(obj, bool):
        return obj

    if isinstance(obj, (int, float, str)):
        return obj

    if isinstance(obj, Path):
        return str(obj)

    # `not isinstance(obj, type)` guard is load-bearing: is_dataclass
    # returns True for both the class object and its instances.
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {
            f.name: to_dict(getattr(obj, f.name))
            for f in dataclasses.fields(obj)
        }

    # Non-string mapping keys recurse — if recursion produces a non-str,
    # json.dumps raises downstream, signalling that the producer chose
    # an un-JSON-compatible key type. mypy can't see this runtime
    # contract, so we annotate the temp as Any.
    if isinstance(obj, Mapping):
        result: dict[str, JSONValue] = {}
        for k, v in obj.items():
            json_k: Any = k if isinstance(k, str) else to_dict(k)
            result[json_k] = to_dict(v)
        return result

    # Tuples normalise to list because JSON has no tuple type.
    if isinstance(obj, (list, tuple)):
        return [to_dict(v) for v in obj]

    raise TypeError(
        f"to_dict cannot serialise {type(obj).__name__}: {obj!r}"
    )
