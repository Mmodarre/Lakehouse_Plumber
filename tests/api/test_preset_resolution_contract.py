"""DTO contract tests for ``lhp.api.views.PresetResolutionResult``.

Per constitution §8.3 + §9.15. The result is the public, frozen
projection of a preset's resolved ``extends`` chain and deep-merged
``defaults`` payload (see :meth:`InspectionFacade.resolve_preset`).
Every field must be JSON-serialisable, the type is frozen, and the
projection must survive both pickle and JSON round-trips so external
consumers (CLI, web IDE) can capture the resolution across process and
wire boundaries.

Contracts covered (house style — see
``tests/api/test_substitution_view_contract.py``):

1. ``@dataclass(frozen=True)``.
2. Attribute assignment raises ``FrozenInstanceError``.
3. Pickle round-trip with ``==`` equality.
4. JSON-style round-trip via flat field values, then field-by-field
   equality after reconstruction.
5. Field-type contract — no bare ``Any``, no ``Dict``/``List``, no
   ``Exception``/``LHPError``, no Pydantic ``BaseModel``.
6. Public export from ``lhp.api``.

Behavioral coverage of the facade method itself lives in
``tests/api/test_resolve_preset.py``.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from typing import Mapping, get_type_hints

import pytest

from lhp.api.views import PresetResolutionResult


def _to_json_safe_dict(result: PresetResolutionResult) -> dict[str, object]:
    return {
        "name": result.name,
        # Tuple → list for JSON-shape.
        "chain": list(result.chain),
        # Mapping[str, JSONValue] → plain dict; values already JSON-safe
        # (parsed-YAML scalars / lists / mappings), so serialise as-is.
        "merged_config": dict(result.merged_config),
    }


def _from_json_safe_dict(payload: Mapping[str, object]) -> PresetResolutionResult:
    chain_payload = payload["chain"]
    assert isinstance(chain_payload, list)
    return PresetResolutionResult(
        name=payload["name"],  # type: ignore[arg-type]
        chain=tuple(chain_payload),
        merged_config=dict(payload["merged_config"]),  # type: ignore[arg-type]
    )


@pytest.fixture
def minimal_result() -> PresetResolutionResult:
    """A base preset with no ``extends`` and no defaults."""
    return PresetResolutionResult(name="base_layer", chain=("base_layer",))


@pytest.fixture
def populated_result() -> PresetResolutionResult:
    return PresetResolutionResult(
        name="silver_layer",
        chain=("base_layer", "bronze_layer", "silver_layer"),
        merged_config={
            "load_actions": {"cloudfiles": {"format": "parquet"}},
            "operational_metadata": ["_ingest_ts", "_source_file"],
            "write_actions": {
                "streaming_table": {
                    "table_properties": {"delta.enableChangeDataFeed": "true"}
                }
            },
        },
    )


@pytest.mark.unit
class TestFrozenContract:
    def test_preset_resolution_result_is_frozen(self) -> None:
        params = getattr(PresetResolutionResult, "__dataclass_params__", None)
        assert params is not None, "PresetResolutionResult has no __dataclass_params__"
        assert params.frozen is True


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_name_mutation_raises(
        self, populated_result: PresetResolutionResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_result.name = "tampered"  # type: ignore[misc]

    def test_chain_mutation_raises(
        self, populated_result: PresetResolutionResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_result.chain = ()  # type: ignore[misc]

    def test_merged_config_mutation_raises(
        self, populated_result: PresetResolutionResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_result.merged_config = {}  # type: ignore[misc]


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_minimal_result_pickles(
        self, minimal_result: PresetResolutionResult
    ) -> None:
        restored = pickle.loads(pickle.dumps(minimal_result))
        assert restored == minimal_result

    def test_populated_result_pickles(
        self, populated_result: PresetResolutionResult
    ) -> None:
        restored = pickle.loads(pickle.dumps(populated_result))
        assert restored == populated_result


@pytest.mark.unit
class TestJSONRoundTrip:
    def test_populated_result_json_round_trip(
        self, populated_result: PresetResolutionResult
    ) -> None:
        projection = _to_json_safe_dict(populated_result)
        wire = json.loads(json.dumps(projection))
        restored = _from_json_safe_dict(wire)
        assert restored == populated_result
        # Field-by-field for clearer drift signal.
        assert restored.name == populated_result.name
        assert restored.chain == populated_result.chain
        assert dict(restored.merged_config) == dict(populated_result.merged_config)

    def test_minimal_result_json_round_trip(
        self, minimal_result: PresetResolutionResult
    ) -> None:
        projection = _to_json_safe_dict(minimal_result)
        wire = json.loads(json.dumps(projection))
        restored = _from_json_safe_dict(wire)
        assert restored == minimal_result


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
    def test_no_banned_annotations(self) -> None:
        for name, annotation in _annotation_strings(PresetResolutionResult).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"PresetResolutionResult.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_no_any(self) -> None:
        for name, annotation in _annotation_strings(PresetResolutionResult).items():
            assert "Any" not in annotation, (
                f"PresetResolutionResult.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_resolved_hints_carry_no_exception_or_lhperror(self) -> None:
        hints = get_type_hints(PresetResolutionResult)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text, (
                f"PresetResolutionResult.{name}: resolved type {text!r} carries "
                f"an Exception."
            )
            assert "LHPError" not in text, (
                f"PresetResolutionResult.{name}: resolved type {text!r} carries "
                f"an LHPError."
            )

    def test_required_fields_present(self) -> None:
        field_names = {f.name for f in dataclasses.fields(PresetResolutionResult)}
        required = {"name", "chain", "merged_config"}
        missing = required - field_names
        assert not missing, (
            f"PresetResolutionResult is missing required fields: {missing}."
        )


@pytest.mark.unit
class TestPublicExport:
    def test_preset_resolution_result_in_lhp_api(self) -> None:
        import lhp.api as api

        assert "PresetResolutionResult" in api.__all__
        assert api.PresetResolutionResult is PresetResolutionResult
