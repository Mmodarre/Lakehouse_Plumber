"""DTO contract tests for ``lhp.api.views.SubstitutionView`` and ``SecretReferenceView``.

Per constitution §8.3 + §9.15. Both views are the public, frozen
projections of internal :class:`EnhancedSubstitutionManager` state.
Every field must be JSON-serialisable, the types are frozen, and the
projection must survive both pickle and JSON round-trips so external
consumers (CLI, future WebUI, future VSCode extension) can capture
the resolved substitution context across process and wire boundaries.

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
from typing import Mapping, get_type_hints

import pytest

from lhp.api.views import SecretReferenceView, SubstitutionView

# ---------------------------------------------------------------------------
# Helpers — JSON-shape projections.
# ---------------------------------------------------------------------------


def _secret_to_json_safe_dict(view: SecretReferenceView) -> dict[str, object]:
    return {"scope": view.scope, "key": view.key}


def _secret_from_json_safe_dict(payload: Mapping[str, object]) -> SecretReferenceView:
    return SecretReferenceView(
        scope=payload["scope"],  # type: ignore[arg-type]
        key=payload["key"],  # type: ignore[arg-type]
    )


def _substitution_to_json_safe_dict(view: SubstitutionView) -> dict[str, object]:
    return {
        "env": view.env,
        # Mapping → plain dict for JSON-shape.
        "tokens": dict(view.tokens),
        # Mapping[str, JSONValue] → plain dict; values already JSON-safe
        # (nested dicts/lists of scalars), so serialise as-is.
        "raw_mappings": dict(view.raw_mappings),
        # Tuple[SecretReferenceView, ...] → list of dicts.
        "secret_references": [
            _secret_to_json_safe_dict(r) for r in view.secret_references
        ],
        "default_secret_scope": view.default_secret_scope,
    }


def _substitution_from_json_safe_dict(
    payload: Mapping[str, object],
) -> SubstitutionView:
    refs_payload = payload["secret_references"]
    assert isinstance(refs_payload, list)
    return SubstitutionView(
        env=payload["env"],  # type: ignore[arg-type]
        tokens=dict(payload["tokens"]),  # type: ignore[arg-type]
        raw_mappings=dict(payload["raw_mappings"]),  # type: ignore[arg-type]
        secret_references=tuple(_secret_from_json_safe_dict(r) for r in refs_payload),
        default_secret_scope=payload["default_secret_scope"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_secret_ref() -> SecretReferenceView:
    return SecretReferenceView(scope="my-scope", key="db_password")


@pytest.fixture
def empty_substitution_view() -> SubstitutionView:
    """An empty substitution context — defaults preserved."""
    return SubstitutionView(env="dev", tokens={})


@pytest.fixture
def populated_substitution_view() -> SubstitutionView:
    """A fully-populated view: tokens, secrets, default scope."""
    return SubstitutionView(
        env="prod",
        tokens={
            "catalog": "main",
            "schema": "analytics",
            "workspace_env": "prod",
        },
        raw_mappings={
            "catalog_map": {"prod": "main", "dev": "sandbox"},
            "schema": "bronze",
        },
        secret_references=(
            SecretReferenceView(scope="my-scope", key="db_password"),
            SecretReferenceView(scope="other-scope", key="api_token"),
        ),
        default_secret_scope="my-scope",
    )


# ---------------------------------------------------------------------------
# Frozen contract.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFrozenContract:
    def test_secret_reference_view_is_frozen(self) -> None:
        params = getattr(SecretReferenceView, "__dataclass_params__", None)
        assert params is not None, "SecretReferenceView has no __dataclass_params__"
        assert params.frozen is True

    def test_substitution_view_is_frozen(self) -> None:
        params = getattr(SubstitutionView, "__dataclass_params__", None)
        assert params is not None, "SubstitutionView has no __dataclass_params__"
        assert params.frozen is True


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_secret_reference_view_mutation_raises(
        self, populated_secret_ref: SecretReferenceView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_secret_ref.scope = "tampered"  # type: ignore[misc]

    def test_substitution_view_env_mutation_raises(
        self, populated_substitution_view: SubstitutionView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_substitution_view.env = "tampered"  # type: ignore[misc]

    def test_substitution_view_tokens_mutation_raises(
        self, populated_substitution_view: SubstitutionView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_substitution_view.tokens = {}  # type: ignore[misc]

    def test_substitution_view_secret_refs_mutation_raises(
        self, populated_substitution_view: SubstitutionView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_substitution_view.secret_references = ()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Pickle round-trip.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_secret_reference_view_pickles(
        self, populated_secret_ref: SecretReferenceView
    ) -> None:
        restored = pickle.loads(pickle.dumps(populated_secret_ref))
        assert restored == populated_secret_ref

    def test_empty_substitution_view_pickles(
        self, empty_substitution_view: SubstitutionView
    ) -> None:
        restored = pickle.loads(pickle.dumps(empty_substitution_view))
        assert restored == empty_substitution_view

    def test_populated_substitution_view_pickles(
        self, populated_substitution_view: SubstitutionView
    ) -> None:
        restored = pickle.loads(pickle.dumps(populated_substitution_view))
        assert restored == populated_substitution_view


# ---------------------------------------------------------------------------
# JSON round-trip (field-flat projection).
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestJSONRoundTrip:
    def test_secret_reference_view_json_round_trip(
        self, populated_secret_ref: SecretReferenceView
    ) -> None:
        projection = _secret_to_json_safe_dict(populated_secret_ref)
        wire = json.loads(json.dumps(projection))
        restored = _secret_from_json_safe_dict(wire)
        assert restored == populated_secret_ref

    def test_populated_substitution_view_json_round_trip(
        self, populated_substitution_view: SubstitutionView
    ) -> None:
        projection = _substitution_to_json_safe_dict(populated_substitution_view)
        wire = json.loads(json.dumps(projection))
        restored = _substitution_from_json_safe_dict(wire)
        assert restored == populated_substitution_view
        # Field-by-field for clearer drift signal.
        assert restored.env == populated_substitution_view.env
        assert dict(restored.tokens) == dict(populated_substitution_view.tokens)
        assert dict(restored.raw_mappings) == dict(
            populated_substitution_view.raw_mappings
        )
        assert (
            restored.secret_references == populated_substitution_view.secret_references
        )
        assert (
            restored.default_secret_scope
            == populated_substitution_view.default_secret_scope
        )

    def test_empty_substitution_view_json_round_trip(
        self, empty_substitution_view: SubstitutionView
    ) -> None:
        projection = _substitution_to_json_safe_dict(empty_substitution_view)
        wire = json.loads(json.dumps(projection))
        restored = _substitution_from_json_safe_dict(wire)
        assert restored == empty_substitution_view


# ---------------------------------------------------------------------------
# Field-type contract.
# ---------------------------------------------------------------------------


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
    def test_secret_reference_view_no_banned_annotations(self) -> None:
        for name, annotation in _annotation_strings(SecretReferenceView).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"SecretReferenceView.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_secret_reference_view_no_any(self) -> None:
        for name, annotation in _annotation_strings(SecretReferenceView).items():
            assert "Any" not in annotation, (
                f"SecretReferenceView.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_substitution_view_no_banned_annotations(self) -> None:
        for name, annotation in _annotation_strings(SubstitutionView).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"SubstitutionView.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_substitution_view_no_any(self) -> None:
        for name, annotation in _annotation_strings(SubstitutionView).items():
            assert "Any" not in annotation, (
                f"SubstitutionView.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_resolved_hints_carry_no_exception_or_lhperror(self) -> None:
        for cls in (SecretReferenceView, SubstitutionView):
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

    def test_substitution_view_required_fields_present(self) -> None:
        field_names = {f.name for f in dataclasses.fields(SubstitutionView)}
        required = {
            "env",
            "tokens",
            "raw_mappings",
            "secret_references",
            "default_secret_scope",
        }
        missing = required - field_names
        assert not missing, f"SubstitutionView is missing required fields: {missing}."

    def test_secret_reference_view_required_fields_present(self) -> None:
        field_names = {f.name for f in dataclasses.fields(SecretReferenceView)}
        required = {"scope", "key"}
        missing = required - field_names
        assert not missing, (
            f"SecretReferenceView is missing required fields: {missing}."
        )


# ---------------------------------------------------------------------------
# Public exposure.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPublicExport:
    def test_substitution_view_in_lhp_api(self) -> None:
        import lhp.api as api

        assert "SubstitutionView" in api.__all__
        assert api.SubstitutionView is SubstitutionView

    def test_secret_reference_view_in_lhp_api(self) -> None:
        import lhp.api as api

        assert "SecretReferenceView" in api.__all__
        assert api.SecretReferenceView is SecretReferenceView
