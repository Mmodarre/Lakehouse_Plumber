"""Contract tests for the ``WarningEmitted`` event in ``lhp.api.events``.

Per constitution §1.9 + §4.8 (event field-type rules) + §13 item 4
(the locked soft-cap emission). ``WarningEmitted`` is a non-terminal,
data-only event, so it satisfies the same four contracts every public
event/DTO must:

1. ``@dataclass(frozen=True)`` (no in-place mutation).
2. Pickle round-trip with ``==`` equality (cross-process boundary).
3. JSON round-trip via ``to_dict`` → ``json`` → reconstruct.
4. Field-type discipline — no ``Any`` / ``Dict[`` / ``List[`` / live
   ``Exception``.

Plus two ``WarningEmitted``-specific guarantees:

- The **locked field order** (§13 item 4): ``message`` is the positional
  first field, so ``WarningEmitted("...", code="LHP-EVT-SOFT-CAP")``
  type-checks and constructs.
- ``file: Optional[Path]`` round-trips (``str`` on the wire → ``Path``).
"""

from __future__ import annotations

import dataclasses
import json
import pickle
import typing
from dataclasses import FrozenInstanceError, fields
from pathlib import Path
from typing import Any, get_args, get_origin, get_type_hints

import pytest

from lhp.api import WarningEmitted, to_dict


def _coerce(value: Any, hint: Any) -> Any:
    if value is None:
        return None
    origin = get_origin(hint)
    args = get_args(hint)
    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _coerce(value, non_none[0])
        return value
    if hint is Path:
        return Path(value)
    return value


def _reconstruct(cls: type, payload: dict[str, Any]) -> Any:
    hints = get_type_hints(cls)
    return cls(**{f.name: _coerce(payload[f.name], hints[f.name]) for f in fields(cls)})


@pytest.fixture
def minimal_warning() -> WarningEmitted:
    return WarningEmitted("event buffer near limit")


@pytest.fixture
def full_warning() -> WarningEmitted:
    return WarningEmitted(
        message="deprecated field used",
        code="LHP-DEP-001",
        category="DEP",
        file=Path("pipelines/bronze/customer.yaml"),
        flowgroup="customer_ingest",
    )


@pytest.mark.unit
class TestFrozenContract:
    def test_dataclass_params_frozen(self) -> None:
        params = getattr(WarningEmitted, "__dataclass_params__", None)
        assert params is not None, "WarningEmitted has no __dataclass_params__"
        assert params.frozen is True, (
            f"WarningEmitted must be @dataclass(frozen=True); got "
            f"frozen={params.frozen}"
        )


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_mutation_on_message_raises(self, full_warning: WarningEmitted) -> None:
        with pytest.raises(FrozenInstanceError):
            full_warning.message = "tampered"  # type: ignore[misc]

    def test_mutation_on_file_raises(self, full_warning: WarningEmitted) -> None:
        with pytest.raises(FrozenInstanceError):
            full_warning.file = Path("/x")  # type: ignore[misc]


@pytest.mark.unit
class TestLockedFieldOrder:
    """§13 item 4: ``message`` is the positional-first field.

    The locked soft-cap emission
    ``WarningEmitted("event buffer near limit", code="LHP-EVT-SOFT-CAP")``
    MUST construct: ``message`` positional, everything else
    keyword-defaulted.
    """

    def test_soft_cap_emission_constructs(self) -> None:
        warning = WarningEmitted("event buffer near limit", code="LHP-EVT-SOFT-CAP")
        assert warning.message == "event buffer near limit"
        assert warning.code == "LHP-EVT-SOFT-CAP"
        # The remaining structured fields default to empty / None.
        assert warning.category == ""
        assert warning.file is None
        assert warning.flowgroup is None

    def test_message_is_first_positional_field(self) -> None:
        field_names = [f.name for f in dataclasses.fields(WarningEmitted)]
        assert field_names[0] == "message", (
            f"WarningEmitted field order is locked (§13 item 4): 'message' "
            f"must be first; got {field_names}"
        )

    def test_message_only_construction(self, minimal_warning: WarningEmitted) -> None:
        assert minimal_warning.message == "event buffer near limit"
        assert minimal_warning.code == ""
        assert minimal_warning.category == ""
        assert minimal_warning.file is None
        assert minimal_warning.flowgroup is None


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_minimal_warning_pickle(self, minimal_warning: WarningEmitted) -> None:
        restored = pickle.loads(pickle.dumps(minimal_warning))
        assert restored == minimal_warning

    def test_full_warning_pickle(self, full_warning: WarningEmitted) -> None:
        restored = pickle.loads(pickle.dumps(full_warning))
        assert restored == full_warning
        # Path field survives as Path, not stringified.
        assert isinstance(restored.file, Path)
        assert restored.file == Path("pipelines/bronze/customer.yaml")


@pytest.mark.unit
class TestJSONRoundTripViaToDict:
    def test_minimal_warning_round_trip(self, minimal_warning: WarningEmitted) -> None:
        payload = to_dict(minimal_warning)
        rehydrated = json.loads(json.dumps(payload))
        assert rehydrated == payload
        reconstructed = _reconstruct(WarningEmitted, rehydrated)
        assert reconstructed == minimal_warning

    def test_full_warning_round_trip_with_path(
        self, full_warning: WarningEmitted
    ) -> None:
        payload = to_dict(full_warning)
        # Path serialises to str on the wire.
        assert payload["file"] == "pipelines/bronze/customer.yaml"
        rehydrated = json.loads(json.dumps(payload))
        assert rehydrated == payload
        reconstructed = _reconstruct(WarningEmitted, rehydrated)
        assert reconstructed == full_warning
        assert isinstance(reconstructed.file, Path)


_BANNED_FIELD_PATTERNS = {
    "Dict[": "Use Mapping[str, JSONValue] per §4.8, not Dict.",
    "List[": "Use Tuple[T, ...] per §4.8, not List.",
    "Exception": "Events must not carry live Exception instances (§4.8).",
    "LHPError": "Events must not carry LHPError instances (§4.8 / §9.21 carve-out is ErrorEmitted only).",
}


def _annotation_strings() -> dict[str, str]:
    return {
        f.name: f.type if isinstance(f.type, str) else str(f.type)
        for f in dataclasses.fields(WarningEmitted)
    }


@pytest.mark.unit
class TestFieldTypeContract:
    def test_no_banned_field_annotations(self) -> None:
        for name, annotation in _annotation_strings().items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"WarningEmitted.{name}: annotation {annotation!r} contains "
                    f"banned token {needle!r}. {reason}"
                )

    def test_no_any_in_annotations(self) -> None:
        for name, annotation in _annotation_strings().items():
            assert "Any" not in annotation, (
                f"WarningEmitted.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_resolved_hints_carry_no_exception(self) -> None:
        hints = get_type_hints(WarningEmitted)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text and "LHPError" not in text, (
                f"WarningEmitted.{name}: resolved type {text!r} must not carry a "
                f"live error type (§4.8)."
            )
