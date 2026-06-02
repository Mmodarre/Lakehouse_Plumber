"""Contract tests for the progress events in ``lhp.api.events``.

Covers ``PhaseStarted`` / ``PhaseCompleted`` and ``PipelineStarted`` /
``PipelineCompleted`` / ``PipelineFailed`` — the non-terminal progress
events frozen by FREEZE-1. Each satisfies the four event contracts
(§1.9 + §4.8):

1. ``@dataclass(frozen=True)``.
2. Pickle round-trip with ``==``.
3. JSON round-trip via ``to_dict`` → ``json`` → reconstruct.
4. Field-type discipline (no ``Any`` / ``Dict[`` / ``List[`` / Exception).

Plus a hierarchy guard: none of these are ``OperationCompleted`` (they
are non-terminal), and the ``WarningEmitted`` locked field-order call
type-checks and constructs.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError, fields
from typing import Any, get_type_hints

import pytest

from lhp.api import (
    LHPEvent,
    OperationCompleted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    WarningEmitted,
    to_dict,
)

# One representative, fully-populated instance per event class. These
# events carry only primitive fields (str / float / int / bool), so
# reconstruction is a plain ``cls(**payload)`` — no structural coercion.
_INSTANCES = [
    pytest.param(PhaseStarted(phase="discovery"), id="PhaseStarted"),
    pytest.param(
        PhaseCompleted(phase="generation", duration_s=1.5, success=True),
        id="PhaseCompleted",
    ),
    pytest.param(PipelineStarted(pipeline="bronze"), id="PipelineStarted"),
    pytest.param(
        PipelineCompleted(pipeline="bronze", duration_s=2.25, files_written=3),
        id="PipelineCompleted",
    ),
    pytest.param(
        PipelineFailed(pipeline="bronze", code="LHP-VAL-021", message="boom"),
        id="PipelineFailed",
    ),
]

_ALL_PROGRESS_CLASSES = [
    PhaseStarted,
    PhaseCompleted,
    PipelineStarted,
    PipelineCompleted,
    PipelineFailed,
]


@pytest.mark.unit
class TestFrozenContract:
    @pytest.mark.parametrize("cls", _ALL_PROGRESS_CLASSES)
    def test_dataclass_params_frozen(self, cls: type) -> None:
        params = getattr(cls, "__dataclass_params__", None)
        assert params is not None, f"{cls.__name__} has no __dataclass_params__"
        assert params.frozen is True, (
            f"{cls.__name__} must be @dataclass(frozen=True); "
            f"got frozen={params.frozen}"
        )


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_phase_started_mutation_raises(self) -> None:
        with pytest.raises(FrozenInstanceError):
            PhaseStarted(phase="x").phase = "y"  # type: ignore[misc]

    def test_phase_completed_mutation_raises(self) -> None:
        with pytest.raises(FrozenInstanceError):
            PhaseCompleted(phase="x", duration_s=1.0, success=True).success = False  # type: ignore[misc]

    def test_pipeline_completed_mutation_raises(self) -> None:
        with pytest.raises(FrozenInstanceError):
            PipelineCompleted(
                pipeline="p", duration_s=1.0, files_written=1
            ).files_written = 2  # type: ignore[misc]

    def test_pipeline_failed_mutation_raises(self) -> None:
        with pytest.raises(FrozenInstanceError):
            PipelineFailed(pipeline="p", code="C", message="m").code = "D"  # type: ignore[misc]


@pytest.mark.unit
class TestPickleRoundTrip:
    @pytest.mark.parametrize("instance", _INSTANCES)
    def test_pickle_round_trip(self, instance: Any) -> None:
        restored = pickle.loads(pickle.dumps(instance))
        assert restored == instance


@pytest.mark.unit
class TestJSONRoundTripViaToDict:
    @pytest.mark.parametrize("instance", _INSTANCES)
    def test_round_trip(self, instance: Any) -> None:
        payload = to_dict(instance)
        rehydrated = json.loads(json.dumps(payload))
        assert rehydrated == payload
        cls = type(instance)
        reconstructed = cls(**{f.name: rehydrated[f.name] for f in fields(cls)})
        assert reconstructed == instance


@pytest.mark.unit
class TestEventHierarchy:
    """Progress events are ``LHPEvent`` but NOT ``OperationCompleted``."""

    @pytest.mark.parametrize("instance", _INSTANCES)
    def test_is_lhpevent_not_terminal(self, instance: Any) -> None:
        assert isinstance(instance, LHPEvent)
        assert not isinstance(instance, OperationCompleted)


@pytest.mark.unit
class TestWarningEmittedFieldOrderTypeCheck:
    """The locked ``WarningEmitted`` field-order call (§13 item 4).

    A focused guard kept alongside the progress events so the spec call
    ``WarningEmitted("...", code="LHP-EVT-SOFT-CAP")`` is exercised in
    this module too; the full ``WarningEmitted`` quartet lives in
    ``test_warning_emitted_contract.py``.
    """

    def test_soft_cap_emission_constructs(self) -> None:
        warning = WarningEmitted("event buffer near limit", code="LHP-EVT-SOFT-CAP")
        assert warning.message == "event buffer near limit"
        assert warning.code == "LHP-EVT-SOFT-CAP"
        assert isinstance(warning, LHPEvent)
        assert not isinstance(warning, OperationCompleted)


_BANNED_FIELD_PATTERNS = ("Any", "Dict[", "List[", "Exception", "LHPError")


@pytest.mark.unit
class TestFieldTypeContract:
    @pytest.mark.parametrize("cls", _ALL_PROGRESS_CLASSES)
    def test_no_banned_field_annotations(self, cls: type) -> None:
        annotations = {
            f.name: (f.type if isinstance(f.type, str) else str(f.type))
            for f in dataclasses.fields(cls)
        }
        for name, annotation in annotations.items():
            for needle in _BANNED_FIELD_PATTERNS:
                assert needle not in annotation, (
                    f"{cls.__name__}.{name}: annotation {annotation!r} contains "
                    f"banned token {needle!r} (§4.8)."
                )

    @pytest.mark.parametrize("cls", _ALL_PROGRESS_CLASSES)
    def test_resolved_hints_carry_no_exception(self, cls: type) -> None:
        hints = get_type_hints(cls)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text and "LHPError" not in text, (
                f"{cls.__name__}.{name}: resolved type {text!r} must not carry a "
                f"live error type (§4.8)."
            )
