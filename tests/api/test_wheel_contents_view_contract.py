"""DTO contract tests for ``lhp.api.views.WheelContentsView`` and ``WheelModuleView``.

Per constitution §8.3 + §9.15. Both views are the public, frozen
projections of the result of inspecting a built wheel (``.whl``). Every
field must be JSON-serialisable, the types are frozen, and the
projection must survive both pickle and JSON round-trips so external
consumers (CLI, future WebUI, future VSCode extension) can capture the
wheel contents across process and wire boundaries.

Four contracts are covered per view:

1. ``@dataclass(frozen=True)``.
2. Attribute assignment raises ``FrozenInstanceError``.
3. Pickle round-trip with ``==`` equality.
4. JSON-style round-trip via flat field values, then field-by-field
   equality after reconstruction.
5. Field-type contract — no bare ``Any``, no ``Dict``/``List``, no
   ``Exception``/``LHPError``, no Pydantic ``BaseModel``.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Mapping, get_type_hints

import pytest

from lhp.api.views import WheelContentsView, WheelModuleView


def _module_to_json_safe_dict(view: WheelModuleView) -> dict[str, object]:
    return {"arcname": view.arcname, "size_bytes": view.size_bytes}


def _module_from_json_safe_dict(payload: Mapping[str, object]) -> WheelModuleView:
    return WheelModuleView(
        arcname=payload["arcname"],  # type: ignore[arg-type]
        size_bytes=payload["size_bytes"],  # type: ignore[arg-type]
    )


def _wheel_contents_to_json_safe_dict(view: WheelContentsView) -> dict[str, object]:
    return {
        # Path → str for JSON-shape.
        "wheel_path": str(view.wheel_path),
        "pipeline": view.pipeline,
        "env": view.env,
        # Tuple[WheelModuleView, ...] → list of dicts.
        "modules": [_module_to_json_safe_dict(m) for m in view.modules],
        "module_count": view.module_count,
    }


def _wheel_contents_from_json_safe_dict(
    payload: Mapping[str, object],
) -> WheelContentsView:
    modules_payload = payload["modules"]
    assert isinstance(modules_payload, list)
    return WheelContentsView(
        wheel_path=Path(payload["wheel_path"]),  # type: ignore[arg-type]
        pipeline=payload["pipeline"],  # type: ignore[arg-type]
        env=payload["env"],  # type: ignore[arg-type]
        modules=tuple(_module_from_json_safe_dict(m) for m in modules_payload),
        module_count=payload["module_count"],  # type: ignore[arg-type]
    )


@pytest.fixture
def populated_wheel_module() -> WheelModuleView:
    return WheelModuleView(arcname="my_pipeline/transformations.py", size_bytes=2048)


@pytest.fixture
def empty_wheel_contents_view() -> WheelContentsView:
    """An empty wheel-contents view — no modules."""
    return WheelContentsView(
        wheel_path=Path("/tmp/empty-0.1.0-py3-none-any.whl"),
        pipeline=None,
        env=None,
        modules=(),
        module_count=0,
    )


@pytest.fixture
def populated_wheel_contents_view() -> WheelContentsView:
    return WheelContentsView(
        wheel_path=Path("/dist/my_pipeline-1.2.3-py3-none-any.whl"),
        pipeline="my_pipeline",
        env="prod",
        modules=(
            WheelModuleView(arcname="my_pipeline/__init__.py", size_bytes=128),
            WheelModuleView(arcname="my_pipeline/transformations.py", size_bytes=2048),
        ),
        module_count=2,
    )


@pytest.mark.unit
class TestFrozenContract:
    def test_wheel_module_view_is_frozen(self) -> None:
        params = getattr(WheelModuleView, "__dataclass_params__", None)
        assert params is not None, "WheelModuleView has no __dataclass_params__"
        assert params.frozen is True

    def test_wheel_contents_view_is_frozen(self) -> None:
        params = getattr(WheelContentsView, "__dataclass_params__", None)
        assert params is not None, "WheelContentsView has no __dataclass_params__"
        assert params.frozen is True


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_wheel_module_view_mutation_raises(
        self, populated_wheel_module: WheelModuleView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_wheel_module.arcname = "tampered"  # type: ignore[misc]

    def test_wheel_contents_view_wheel_path_mutation_raises(
        self, populated_wheel_contents_view: WheelContentsView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_wheel_contents_view.wheel_path = Path("/tampered")  # type: ignore[misc]

    def test_wheel_contents_view_pipeline_mutation_raises(
        self, populated_wheel_contents_view: WheelContentsView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_wheel_contents_view.pipeline = "tampered"  # type: ignore[misc]

    def test_wheel_contents_view_modules_mutation_raises(
        self, populated_wheel_contents_view: WheelContentsView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_wheel_contents_view.modules = ()  # type: ignore[misc]


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_wheel_module_view_pickles(
        self, populated_wheel_module: WheelModuleView
    ) -> None:
        restored = pickle.loads(pickle.dumps(populated_wheel_module))
        assert restored == populated_wheel_module

    def test_empty_wheel_contents_view_pickles(
        self, empty_wheel_contents_view: WheelContentsView
    ) -> None:
        restored = pickle.loads(pickle.dumps(empty_wheel_contents_view))
        assert restored == empty_wheel_contents_view

    def test_populated_wheel_contents_view_pickles(
        self, populated_wheel_contents_view: WheelContentsView
    ) -> None:
        restored = pickle.loads(pickle.dumps(populated_wheel_contents_view))
        assert restored == populated_wheel_contents_view


@pytest.mark.unit
class TestJSONRoundTrip:
    def test_wheel_module_view_json_round_trip(
        self, populated_wheel_module: WheelModuleView
    ) -> None:
        projection = _module_to_json_safe_dict(populated_wheel_module)
        wire = json.loads(json.dumps(projection))
        restored = _module_from_json_safe_dict(wire)
        assert restored == populated_wheel_module

    def test_populated_wheel_contents_view_json_round_trip(
        self, populated_wheel_contents_view: WheelContentsView
    ) -> None:
        projection = _wheel_contents_to_json_safe_dict(populated_wheel_contents_view)
        wire = json.loads(json.dumps(projection))
        restored = _wheel_contents_from_json_safe_dict(wire)
        assert restored == populated_wheel_contents_view
        # Field-by-field for clearer drift signal.
        assert restored.wheel_path == populated_wheel_contents_view.wheel_path
        assert restored.pipeline == populated_wheel_contents_view.pipeline
        assert restored.env == populated_wheel_contents_view.env
        assert restored.modules == populated_wheel_contents_view.modules
        assert restored.module_count == populated_wheel_contents_view.module_count

    def test_empty_wheel_contents_view_json_round_trip(
        self, empty_wheel_contents_view: WheelContentsView
    ) -> None:
        projection = _wheel_contents_to_json_safe_dict(empty_wheel_contents_view)
        wire = json.loads(json.dumps(projection))
        restored = _wheel_contents_from_json_safe_dict(wire)
        assert restored == empty_wheel_contents_view


_BANNED_FIELD_PATTERNS = {
    "Dict[": "Use Mapping[str, JSONValue] per §4.8, not Dict.",
    "List[": "Use Tuple[T, ...] per §4.8, not List.",
    "Exception": "DTOs must not carry live Exception instances (§4.8).",
    "LHPError": "DTOs must not carry LHPError instances (§4.8).",
    "BaseModel": "Public DTOs must not embed Pydantic models (§9.12).",
}


def _annotation_strings(cls: type) -> dict[str, str]:
    return {
        f.name: f.type if isinstance(f.type, str) else str(f.type)
        for f in dataclasses.fields(cls)
    }


@pytest.mark.unit
class TestFieldTypeContract:
    def test_wheel_module_view_no_banned_annotations(self) -> None:
        for name, annotation in _annotation_strings(WheelModuleView).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"WheelModuleView.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_wheel_module_view_no_any(self) -> None:
        for name, annotation in _annotation_strings(WheelModuleView).items():
            assert "Any" not in annotation, (
                f"WheelModuleView.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_wheel_contents_view_no_banned_annotations(self) -> None:
        for name, annotation in _annotation_strings(WheelContentsView).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"WheelContentsView.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_wheel_contents_view_no_any(self) -> None:
        for name, annotation in _annotation_strings(WheelContentsView).items():
            assert "Any" not in annotation, (
                f"WheelContentsView.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_resolved_hints_carry_no_exception_or_lhperror(self) -> None:
        for cls in (WheelModuleView, WheelContentsView):
            hints = get_type_hints(cls)
            for name, hint in hints.items():
                text = repr(hint)
                assert "Exception" not in text, (
                    f"{cls.__name__}.{name}: resolved type {text!r} carries an "
                    f"Exception."
                )
                assert "LHPError" not in text, (
                    f"{cls.__name__}.{name}: resolved type {text!r} carries an "
                    f"LHPError."
                )

    def test_wheel_contents_view_required_fields_present(self) -> None:
        field_names = {f.name for f in dataclasses.fields(WheelContentsView)}
        required = {
            "wheel_path",
            "pipeline",
            "env",
            "modules",
            "module_count",
        }
        missing = required - field_names
        assert not missing, f"WheelContentsView is missing required fields: {missing}."

    def test_wheel_module_view_required_fields_present(self) -> None:
        field_names = {f.name for f in dataclasses.fields(WheelModuleView)}
        required = {"arcname", "size_bytes"}
        missing = required - field_names
        assert not missing, f"WheelModuleView is missing required fields: {missing}."


@pytest.mark.unit
class TestPublicExport:
    def test_wheel_contents_view_in_lhp_api(self) -> None:
        import lhp.api as api

        assert "WheelContentsView" in api.__all__
        assert api.WheelContentsView is WheelContentsView

    def test_wheel_module_view_in_lhp_api(self) -> None:
        import lhp.api as api

        assert "WheelModuleView" in api.__all__
        assert api.WheelModuleView is WheelModuleView
