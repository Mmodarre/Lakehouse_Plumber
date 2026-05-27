"""Type-generic serializer for the public lhp.api DTO/event surface.

Recursively walks any structural Python value into a JSON-compatible
representation. The constitution (§9.22) forbids per-type dispatch
branches that name DTO classes — this implementation reasons about
shape only (``dataclasses.is_dataclass``, ``Mapping``, ``Sequence``,
``Path``, scalars).

The output type is :data:`JSONValue` (defined in :mod:`lhp.api.responses`);
the parameter is typed ``Any`` because the dispatch is structural and
intentionally accepts the entire surface of DTOs and events without
naming them.

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

    Dispatch is structural — see the rule list in the module docstring.
    Used by the round-trip contract (constitution §1.8): every public
    DTO/event in ``lhp.api`` must round-trip via ``to_dict`` +
    ``json.dumps`` + ``json.loads`` + ``__init__``.

    :param obj: Any structural value composed of dataclasses, mappings,
        sequences, paths, and JSON scalars.
    :returns: A value composed only of ``None``, ``bool``, ``int``,
        ``float``, ``str``, ``list``, and ``dict[str, ...]`` — directly
        accepted by :func:`json.dumps`.
    :raises TypeError: When ``obj`` is not of a recognised structural
        type. The error names the concrete runtime type so the caller
        can decide whether to widen the input or fix the producer.

    :stability: provisional
    """
    # 1. None passes through.
    if obj is None:
        return None

    # 2. bool BEFORE int — bool is a subclass of int, and matching int
    #    first would silently coerce True → 1 / False → 0.
    if isinstance(obj, bool):
        return obj

    # 3. JSON scalars.
    if isinstance(obj, (int, float, str)):
        return obj

    # 4. Path → str. JSON has no path type; the str form round-trips
    #    back into Path(...) at the DTO constructor.
    if isinstance(obj, Path):
        return str(obj)

    # 5. Dataclass INSTANCE (not the class itself). The `not isinstance(
    #    obj, type)` guard is load-bearing: dataclasses.is_dataclass
    #    returns True for both the class object and its instances.
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {
            f.name: to_dict(getattr(obj, f.name))
            for f in dataclasses.fields(obj)
        }

    # 6. Mapping. String keys pass through; non-string keys recurse —
    #    if the recursion produces a non-str, json.dumps will raise
    #    downstream, which is the caller's signal that the producer
    #    chose an un-JSON-compatible key type. The result is typed as
    #    a dict[str, JSONValue] because that's the JSON-valid shape;
    #    non-str recursed keys are intentionally allowed through so
    #    json.dumps can surface them — mypy can't see that runtime
    #    contract, so we annotate the temp accordingly.
    if isinstance(obj, Mapping):
        result: dict[str, JSONValue] = {}
        for k, v in obj.items():
            json_k: Any = k if isinstance(k, str) else to_dict(k)
            result[json_k] = to_dict(v)
        return result

    # 7. list / tuple → list. Tuples normalise because JSON has no
    #    tuple type; this is symmetric with how DTO constructors accept
    #    sequences and convert via Tuple[...] field annotations.
    if isinstance(obj, (list, tuple)):
        return [to_dict(v) for v in obj]

    # 8. Catch-all: fail loud. Anything reaching here is a producer bug
    #    (an un-JSON-compatible value crept into a public DTO/event).
    raise TypeError(
        f"to_dict cannot serialise {type(obj).__name__}: {obj!r}"
    )
