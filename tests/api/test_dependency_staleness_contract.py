"""DTO contract tests for ``lhp.api.responses.DependencyStalenessResult``.

Per constitution §8.3 + §9.15. The result is the public, frozen return shape
(a one-shot result DTO, §1.3) for
:meth:`lhp.api.DependencyFacade.describe_graph_staleness` — the freshness
metadata backing the ``lhp web`` serve-stale + manual-refresh model. Every
field must be JSON-serialisable, the type is frozen, and the projection must
survive both pickle and JSON round-trips so external consumers (the web IDE,
future clients) can capture the freshness state across process and wire
boundaries.

Five contracts are covered:

1. ``@dataclass(frozen=True)``.
2. Attribute assignment raises ``FrozenInstanceError``.
3. Pickle round-trip with ``==`` equality.
4. JSON round-trip via the canonical :func:`lhp.api.to_dict`, then
   field-by-field equality.
5. Field-type contract — no bare ``Any``, no ``Dict``/``List``, no
   ``Exception``/``LHPError``, no Pydantic ``BaseModel``; declared fields
   present.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from typing import Mapping, get_type_hints

import pytest

from lhp.api import to_dict
from lhp.api.responses import DependencyStalenessResult


def _from_json_safe_dict(payload: Mapping[str, object]) -> DependencyStalenessResult:
    return DependencyStalenessResult(
        stale=payload["stale"],  # type: ignore[arg-type]
        fingerprint=payload["fingerprint"],  # type: ignore[arg-type]
        built_at=payload["built_at"],  # type: ignore[arg-type]
    )


@pytest.fixture
def fresh_result() -> DependencyStalenessResult:
    """A persisted, non-stale build."""
    return DependencyStalenessResult(
        stale=False,
        fingerprint="0.9.1|1|b5ef523f5e8f50cb",
        built_at="2026-07-12T07:36:30.729061+00:00",
    )


@pytest.fixture
def never_built_result() -> DependencyStalenessResult:
    """No persisted build yet (or cache disabled): stale, no timestamp."""
    return DependencyStalenessResult(stale=True, fingerprint="", built_at=None)


@pytest.mark.unit
class TestFrozenContract:
    def test_is_frozen(self) -> None:
        params = getattr(DependencyStalenessResult, "__dataclass_params__", None)
        assert params is not None
        assert params.frozen is True


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_stale_mutation_raises(
        self, fresh_result: DependencyStalenessResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            fresh_result.stale = True  # type: ignore[misc]

    def test_fingerprint_mutation_raises(
        self, fresh_result: DependencyStalenessResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            fresh_result.fingerprint = "tampered"  # type: ignore[misc]

    def test_built_at_mutation_raises(
        self, fresh_result: DependencyStalenessResult
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            fresh_result.built_at = None  # type: ignore[misc]


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_fresh_result_pickles(
        self, fresh_result: DependencyStalenessResult
    ) -> None:
        assert pickle.loads(pickle.dumps(fresh_result)) == fresh_result

    def test_never_built_result_pickles(
        self, never_built_result: DependencyStalenessResult
    ) -> None:
        assert pickle.loads(pickle.dumps(never_built_result)) == never_built_result


@pytest.mark.unit
class TestJSONRoundTrip:
    def test_fresh_result_json_round_trip(
        self, fresh_result: DependencyStalenessResult
    ) -> None:
        wire = json.loads(json.dumps(to_dict(fresh_result)))
        restored = _from_json_safe_dict(wire)
        assert restored == fresh_result
        assert restored.stale == fresh_result.stale
        assert restored.fingerprint == fresh_result.fingerprint
        assert restored.built_at == fresh_result.built_at

    def test_never_built_result_json_round_trip(
        self, never_built_result: DependencyStalenessResult
    ) -> None:
        wire = json.loads(json.dumps(to_dict(never_built_result)))
        assert _from_json_safe_dict(wire) == never_built_result


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
        for name, annotation in _annotation_strings(DependencyStalenessResult).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"DependencyStalenessResult.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_no_any(self) -> None:
        for name, annotation in _annotation_strings(DependencyStalenessResult).items():
            assert "Any" not in annotation, (
                f"DependencyStalenessResult.{name}: annotation {annotation!r} "
                f"contains 'Any'. Use precise types per §4.8."
            )

    def test_resolved_hints_carry_no_exception_or_lhperror(self) -> None:
        hints = get_type_hints(DependencyStalenessResult)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text, (
                f"{name}: resolved type {text!r} carries Exception."
            )
            assert "LHPError" not in text, (
                f"{name}: resolved type {text!r} carries LHPError."
            )

    def test_required_fields_present(self) -> None:
        field_names = {f.name for f in dataclasses.fields(DependencyStalenessResult)}
        required = {"stale", "fingerprint", "built_at"}
        assert not (required - field_names)


@pytest.mark.unit
class TestPublicExport:
    def test_in_lhp_api(self) -> None:
        import lhp.api as api

        assert "DependencyStalenessResult" in api.__all__
        assert api.DependencyStalenessResult is DependencyStalenessResult
