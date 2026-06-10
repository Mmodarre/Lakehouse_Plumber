"""DTO contract tests for ``lhp.api.responses.WheelExtractionResult``.

Per constitution §8.3 + §9.15. The result is the public, frozen return
shape (a one-shot result DTO, §1.3) for extracting the modules from a
built wheel (``.whl``) to disk. Unlike :class:`DependencyOutputsResult`
— which carries ``success`` / ``error_*`` and reports failure via
``success=False`` — wheel extraction instead *raises* on failure and
only ever returns this result on success, so there are deliberately no
``success`` / ``error_message`` / ``error_code`` fields (they would be
permanently dead state). Every field must
be JSON-serialisable, the type is frozen, and the projection must
survive both pickle and JSON round-trips so external consumers (CLI,
future WebUI, future VSCode extension) can capture the extraction
outcome across process and wire boundaries.

Five contracts are covered:

1. ``@dataclass(frozen=True)``.
2. Attribute assignment raises ``FrozenInstanceError``.
3. Pickle round-trip with ``==`` equality.
4. JSON-style round-trip via flat field values, then field-by-field
   equality after reconstruction.
5. Field-type contract — no bare ``Any``, no ``Dict``/``List``, no
   ``Exception``/``LHPError``, no Pydantic ``BaseModel``; and the
   deliberate absence of ``success`` / ``error_*`` state.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Mapping, get_type_hints

import pytest

from lhp.api.responses import WheelExtractionResult


def _extraction_to_json_safe_dict(view: WheelExtractionResult) -> dict[str, object]:
    return {
        # Path → str for JSON-shape.
        "wheel_path": str(view.wheel_path),
        "output_dir": str(view.output_dir),
        # Tuple[Path, ...] → list of str.
        "written_paths": [str(p) for p in view.written_paths],
        "written_count": view.written_count,
    }


def _extraction_from_json_safe_dict(
    payload: Mapping[str, object],
) -> WheelExtractionResult:
    paths_payload = payload["written_paths"]
    assert isinstance(paths_payload, list)
    return WheelExtractionResult(
        wheel_path=Path(payload["wheel_path"]),  # type: ignore[arg-type]
        output_dir=Path(payload["output_dir"]),  # type: ignore[arg-type]
        written_paths=tuple(Path(p) for p in paths_payload),
        written_count=payload["written_count"],  # type: ignore[arg-type]
    )


@pytest.fixture
def empty_extraction_result() -> WheelExtractionResult:
    """An extraction that wrote no files."""
    return WheelExtractionResult(
        wheel_path=Path("/dist/empty-0.1.0-py3-none-any.whl"),
        output_dir=Path("/tmp/extracted"),
        written_paths=(),
        written_count=0,
    )


@pytest.fixture
def populated_extraction_result() -> WheelExtractionResult:
    return WheelExtractionResult(
        wheel_path=Path("/dist/my_pipeline-1.2.3-py3-none-any.whl"),
        output_dir=Path("/tmp/extracted/my_pipeline"),
        written_paths=(
            Path("/tmp/extracted/my_pipeline/__init__.py"),
            Path("/tmp/extracted/my_pipeline/transformations.py"),
        ),
        written_count=2,
    )


@pytest.mark.unit
class TestFrozenContract:
    def test_wheel_extraction_result_is_frozen(self) -> None:
        params = getattr(WheelExtractionResult, "__dataclass_params__", None)
        assert params is not None, "WheelExtractionResult has no __dataclass_params__"
        assert params.frozen is True


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_wheel_path_mutation_raises(
        self, populated_extraction_result: WheelExtractionResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_extraction_result.wheel_path = Path("/tampered")  # type: ignore[misc]

    def test_output_dir_mutation_raises(
        self, populated_extraction_result: WheelExtractionResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_extraction_result.output_dir = Path("/tampered")  # type: ignore[misc]

    def test_written_paths_mutation_raises(
        self, populated_extraction_result: WheelExtractionResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_extraction_result.written_paths = ()  # type: ignore[misc]

    def test_written_count_mutation_raises(
        self, populated_extraction_result: WheelExtractionResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_extraction_result.written_count = 99  # type: ignore[misc]


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_empty_extraction_result_pickles(
        self, empty_extraction_result: WheelExtractionResult
    ) -> None:
        restored = pickle.loads(pickle.dumps(empty_extraction_result))
        assert restored == empty_extraction_result

    def test_populated_extraction_result_pickles(
        self, populated_extraction_result: WheelExtractionResult
    ) -> None:
        restored = pickle.loads(pickle.dumps(populated_extraction_result))
        assert restored == populated_extraction_result


@pytest.mark.unit
class TestJSONRoundTrip:
    def test_populated_extraction_result_json_round_trip(
        self, populated_extraction_result: WheelExtractionResult
    ) -> None:
        projection = _extraction_to_json_safe_dict(populated_extraction_result)
        wire = json.loads(json.dumps(projection))
        restored = _extraction_from_json_safe_dict(wire)
        assert restored == populated_extraction_result
        # Field-by-field for clearer drift signal.
        assert restored.wheel_path == populated_extraction_result.wheel_path
        assert restored.output_dir == populated_extraction_result.output_dir
        assert restored.written_paths == populated_extraction_result.written_paths
        assert restored.written_count == populated_extraction_result.written_count

    def test_empty_extraction_result_json_round_trip(
        self, empty_extraction_result: WheelExtractionResult
    ) -> None:
        projection = _extraction_to_json_safe_dict(empty_extraction_result)
        wire = json.loads(json.dumps(projection))
        restored = _extraction_from_json_safe_dict(wire)
        assert restored == empty_extraction_result


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
    def test_wheel_extraction_result_no_banned_annotations(self) -> None:
        for name, annotation in _annotation_strings(WheelExtractionResult).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"WheelExtractionResult.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_wheel_extraction_result_no_any(self) -> None:
        for name, annotation in _annotation_strings(WheelExtractionResult).items():
            assert "Any" not in annotation, (
                f"WheelExtractionResult.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_resolved_hints_carry_no_exception_or_lhperror(self) -> None:
        hints = get_type_hints(WheelExtractionResult)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text, (
                f"WheelExtractionResult.{name}: resolved type {text!r} carries an "
                f"Exception."
            )
            assert "LHPError" not in text, (
                f"WheelExtractionResult.{name}: resolved type {text!r} carries an "
                f"LHPError."
            )

    def test_wheel_extraction_result_required_fields_present(self) -> None:
        field_names = {f.name for f in dataclasses.fields(WheelExtractionResult)}
        required = {
            "wheel_path",
            "output_dir",
            "written_paths",
            "written_count",
        }
        missing = required - field_names
        assert not missing, (
            f"WheelExtractionResult is missing required fields: {missing}."
        )

    def test_wheel_extraction_result_has_no_success_or_error_state(self) -> None:
        # One-shot result DTO (§1.3): extraction raises on failure (unlike
        # DependencyOutputsResult's success=False path), so success/error
        # fields would be permanently True/None dead state.
        field_names = {f.name for f in dataclasses.fields(WheelExtractionResult)}
        banned = {"success", "error_message", "error_code"}
        present = banned & field_names
        assert not present, (
            f"WheelExtractionResult must not carry one-shot dead state "
            f"{present} — extraction raises on failure (§1.3)."
        )


@pytest.mark.unit
class TestPublicExport:
    def test_wheel_extraction_result_in_lhp_api(self) -> None:
        import lhp.api as api

        assert "WheelExtractionResult" in api.__all__
        assert api.WheelExtractionResult is WheelExtractionResult
