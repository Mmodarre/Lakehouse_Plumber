"""Contract tests for the generation-plan DTOs + terminal event.

Covers ``PlannedFileView`` and ``GenerationPlan`` in
``lhp.api.responses`` and the terminal ``GenerationPlanCompleted`` event
in ``lhp.api.events`` (frozen by FREEZE-1). Each satisfies the four
DTO/event contracts (§1.9 + §4.8):

1. ``@dataclass(frozen=True)``.
2. Pickle round-trip with ``==``.
3. JSON round-trip via ``to_dict`` → ``json`` → reconstruct.
4. Field-type discipline (no ``Any`` / ``Dict[`` / ``List[`` / Exception;
   ``PlannedFileView.kind`` is a ``Literal`` discriminator, not bare str).

Plus two plan-specific guarantees the spec pins:

(a) ``GenerationPlanCompleted.to_dict → json → reconstruct`` round-trips
    the embedded ``GenerationPlan`` (a terminal event carrying a nested
    DTO that itself carries a tuple of nested DTOs).
(b) A **multi-file** ``GenerationPlan`` (≥2 ``PlannedFileView``s, mixed
    ``kind``s, real ``Path``s, multi-line ``content``) JSON round-trips
    byte-faithfully.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
import typing
from dataclasses import FrozenInstanceError, fields, is_dataclass
from pathlib import Path
from typing import Any, get_args, get_origin, get_type_hints

import pytest

from lhp.api import (
    GenerationPlan,
    GenerationPlanCompleted,
    LHPEvent,
    OperationCompleted,
    PlannedFileView,
    to_dict,
)

# ``GenerationPlanCompleted.response`` is annotated as the string forward
# ref ``"GenerationPlan"`` (events.py cannot import responses at runtime
# without a cycle). Supply the missing names to ``get_type_hints``.
_TYPE_NS: dict[str, Any] = {
    "GenerationPlan": GenerationPlan,
    "PlannedFileView": PlannedFileView,
}


def _coerce(value: Any, hint: Any) -> Any:
    """Coerce a JSON-loaded value back to its declared type (structural).

    Walks the type hint with no class-name dispatch (§9.22). ``Literal``
    values pass through unchanged (their ``get_origin`` is not a real
    class), which is correct for the ``kind`` discriminator string.
    """
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
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_coerce(v, args[0]) for v in value)
        return tuple(_coerce(v, a) for v, a in zip(value, args, strict=True))
    if isinstance(hint, type) and is_dataclass(hint):
        return _reconstruct(hint, value)
    return value


def _reconstruct(cls: type, payload: dict[str, Any]) -> Any:
    hints = get_type_hints(cls, localns=_TYPE_NS)
    return cls(**{f.name: _coerce(payload[f.name], hints[f.name]) for f in fields(cls)})


@pytest.fixture
def planned_file() -> PlannedFileView:
    return PlannedFileView(
        path=Path("generated/dev/bronze/customer.py"),
        content="import dlt\n\n\n@dlt.table\ndef customer():\n    return spark.range(1)\n",
        pipeline="bronze",
        kind="flowgroup",
    )


@pytest.fixture
def multi_file_plan() -> GenerationPlan:
    """A realistic multi-file plan: 4 files, every ``kind``, multi-line."""
    files = (
        PlannedFileView(
            path=Path("generated/dev/bronze/customer.py"),
            content="import dlt\n\n\n@dlt.table\ndef customer():\n    return df\n",
            pipeline="bronze",
            kind="flowgroup",
        ),
        PlannedFileView(
            path=Path("generated/dev/bronze/__init__.py"),
            content="",
            pipeline="bronze",
            kind="aux",
        ),
        PlannedFileView(
            path=Path("generated/dev/_helpers/udfs.py"),
            content="def to_upper(s: str) -> str:\n    return s.upper()\n",
            pipeline="bronze",
            kind="helper",
        ),
        PlannedFileView(
            path=Path("generated/dev/silver/_monitoring.py"),
            content="# synthetic monitoring pipeline\nEVENT_LOG = 'main.evt'\n",
            pipeline="silver",
            kind="monitoring",
        ),
    )
    return GenerationPlan(
        files=files,
        output_location=Path("generated/dev"),
        pipeline_count=2,
        file_count=len(files),
    )


@pytest.fixture
def single_file_plan(planned_file: PlannedFileView) -> GenerationPlan:
    return GenerationPlan(
        files=(planned_file,),
        output_location=Path("generated/dev"),
        pipeline_count=1,
        file_count=1,
    )


@pytest.fixture
def plan_completed(multi_file_plan: GenerationPlan) -> GenerationPlanCompleted:
    return GenerationPlanCompleted(response=multi_file_plan)


_FROZEN_CLASSES = [PlannedFileView, GenerationPlan, GenerationPlanCompleted]


@pytest.mark.unit
class TestFrozenContract:
    @pytest.mark.parametrize("cls", _FROZEN_CLASSES)
    def test_dataclass_params_frozen(self, cls: type) -> None:
        assert dataclasses.is_dataclass(cls), f"{cls.__name__} is not a dataclass"
        params = getattr(cls, "__dataclass_params__", None)
        assert params is not None, f"{cls.__name__} has no __dataclass_params__"
        assert params.frozen is True, (
            f"{cls.__name__} must be @dataclass(frozen=True); "
            f"got frozen={params.frozen}"
        )


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_planned_file_mutation_raises(self, planned_file: PlannedFileView) -> None:
        with pytest.raises(FrozenInstanceError):
            planned_file.pipeline = "silver"  # type: ignore[misc]

    def test_generation_plan_mutation_raises(
        self, single_file_plan: GenerationPlan
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            single_file_plan.file_count = 99  # type: ignore[misc]

    def test_plan_completed_mutation_raises(
        self, plan_completed: GenerationPlanCompleted
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            plan_completed.response = None  # type: ignore[misc]


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_planned_file_pickle(self, planned_file: PlannedFileView) -> None:
        restored = pickle.loads(pickle.dumps(planned_file))
        assert restored == planned_file
        assert isinstance(restored.path, Path)

    def test_multi_file_plan_pickle(self, multi_file_plan: GenerationPlan) -> None:
        restored = pickle.loads(pickle.dumps(multi_file_plan))
        assert restored == multi_file_plan
        assert all(isinstance(f.path, Path) for f in restored.files)

    def test_plan_completed_pickle(
        self, plan_completed: GenerationPlanCompleted
    ) -> None:
        restored = pickle.loads(pickle.dumps(plan_completed))
        assert restored == plan_completed
        assert restored.response == plan_completed.response


@pytest.mark.unit
class TestJSONRoundTripViaToDict:
    def test_planned_file_round_trip(self, planned_file: PlannedFileView) -> None:
        payload = to_dict(planned_file)
        assert payload["path"] == "generated/dev/bronze/customer.py"
        assert payload["kind"] == "flowgroup"  # Literal serialises as its str value.
        rehydrated = json.loads(json.dumps(payload))
        assert rehydrated == payload
        reconstructed = _reconstruct(PlannedFileView, rehydrated)
        assert reconstructed == planned_file
        assert isinstance(reconstructed.path, Path)

    def test_single_file_plan_round_trip(
        self, single_file_plan: GenerationPlan
    ) -> None:
        payload = to_dict(single_file_plan)
        rehydrated = json.loads(json.dumps(payload))
        assert rehydrated == payload
        reconstructed = _reconstruct(GenerationPlan, rehydrated)
        assert reconstructed == single_file_plan

    def test_multi_file_plan_round_trips_byte_faithfully(
        self, multi_file_plan: GenerationPlan
    ) -> None:
        """(b) ≥2 files, mixed kinds, real Paths, multi-line content."""
        payload = to_dict(multi_file_plan)
        rehydrated = json.loads(json.dumps(payload))
        assert rehydrated == payload
        reconstructed = _reconstruct(GenerationPlan, rehydrated)
        # Full structural equality holds.
        assert reconstructed == multi_file_plan
        # And content is byte-faithful (multi-line, empty, with newlines).
        for original, restored in zip(
            multi_file_plan.files, reconstructed.files, strict=True
        ):
            assert restored.content == original.content
            assert restored.path == original.path
            assert restored.kind == original.kind
            assert isinstance(restored.path, Path)
        # Every distinct ``kind`` survived the trip.
        assert {f.kind for f in reconstructed.files} == {
            "flowgroup",
            "aux",
            "helper",
            "monitoring",
        }

    def test_generation_plan_completed_round_trips_embedded_plan(
        self, plan_completed: GenerationPlanCompleted
    ) -> None:
        """(a) Terminal event → to_dict → json → reconstruct → embedded plan."""
        payload = to_dict(plan_completed)
        rehydrated = json.loads(json.dumps(payload))
        assert rehydrated == payload
        reconstructed = _reconstruct(GenerationPlanCompleted, rehydrated)
        assert reconstructed == plan_completed
        # The embedded GenerationPlan (and its tuple of PlannedFileViews)
        # is reconstructed faithfully.
        assert reconstructed.response == plan_completed.response
        assert isinstance(reconstructed.response, GenerationPlan)
        assert len(reconstructed.response.files) == 4


@pytest.mark.unit
class TestEventHierarchy:
    """``GenerationPlanCompleted`` is a terminal ``OperationCompleted``."""

    def test_is_operation_completed(
        self, plan_completed: GenerationPlanCompleted
    ) -> None:
        assert isinstance(plan_completed, OperationCompleted)
        assert isinstance(plan_completed, LHPEvent)


_BANNED_FIELD_PATTERNS = ("Any", "Dict[", "List[", "Exception", "LHPError")


@pytest.mark.unit
class TestFieldTypeContract:
    @pytest.mark.parametrize("cls", _FROZEN_CLASSES)
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

    def test_kind_is_literal_discriminator(self) -> None:
        """``PlannedFileView.kind`` must be a ``Literal``, not bare ``str`` (§4.8)."""
        hints = get_type_hints(PlannedFileView, localns=_TYPE_NS)
        kind_hint = hints["kind"]
        assert get_origin(kind_hint) is typing.Literal, (
            f"PlannedFileView.kind must be a Literal discriminator (§4.8); "
            f"got {kind_hint!r}"
        )
        assert set(get_args(kind_hint)) == {
            "flowgroup",
            "aux",
            "helper",
            "test_hook",
            "uc_tagging_hook",
            "monitoring",
        }

    @pytest.mark.parametrize("cls", _FROZEN_CLASSES)
    def test_resolved_hints_carry_no_exception(self, cls: type) -> None:
        hints = get_type_hints(cls, localns=_TYPE_NS)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text and "LHPError" not in text, (
                f"{cls.__name__}.{name}: resolved type {text!r} must not carry a "
                f"live error type (§4.8)."
            )
