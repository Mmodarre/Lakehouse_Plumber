"""DTO contract tests for ``lhp.api.views.ValidationIssueView``.

Per constitution §8.3 + §9.7 + §9.15. The view is the flat projection
of a structured ``LHPError`` (or unstructured warning string) — every
field is JSON-serialisable, the type is frozen, and a CLI rendering
pass should be able to reconstitute an instance from the same fields
that get logged to telemetry.

Six contracts are covered:

1. ``@dataclass(frozen=True)``.
2. Attribute assignment raises ``FrozenInstanceError``.
3. Pickle round-trip (cross-process boundary).
4. JSON-style round-trip via flat field values.
5. Field-type contract (no Any / Dict / List / Exception / LHPError).
6. Full flat-field round-trip — construct, serialize to JSON-safe dict,
   reconstruct via ``ValidationIssueView(**...)``, and assert
   field-by-field equality.
"""
from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from typing import Mapping, get_type_hints

import pytest

from lhp.api.responses import JSONValue
from lhp.api.views import ValidationIssueView


# ---------------------------------------------------------------------------
# Helpers — JSON-safe projection of a ValidationIssueView.
#
# The view's ``context`` field is typed ``Mapping[str, JSONValue]`` and
# defaults to a plain ``dict`` (see ``src/lhp/api/views.py:50``). The
# helpers below project to / from a JSON-shape dict so a round-trip
# test can verify field-by-field equality after wire serialization.
# ---------------------------------------------------------------------------


def _to_json_safe_dict(view: ValidationIssueView) -> dict[str, JSONValue]:
    """Project a ValidationIssueView into a JSON-serialisable dict.

    Production CLI rendering does the same projection — ``Tuple``
    becomes ``list`` through JSON. The function below is the canonical
    projection used by the round-trip test.
    """
    return {
        "code": view.code,
        "category": view.category,
        "severity": view.severity,
        "title": view.title,
        "details": view.details,
        "pipeline_name": view.pipeline_name,
        "flowgroup_name": view.flowgroup_name,
        # Tuple → list for JSON-shape.
        "suggestions": list(view.suggestions),
        # Mapping → plain dict for JSON-shape.
        "context": dict(view.context),
        "doc_link": view.doc_link,
    }


def _from_json_safe_dict(payload: Mapping[str, object]) -> ValidationIssueView:
    """Reconstruct a ValidationIssueView from the projection above.

    Reverses the Tuple conversion so equality holds against the
    original instance.
    """
    return ValidationIssueView(
        code=payload["code"],  # type: ignore[arg-type]
        category=payload["category"],  # type: ignore[arg-type]
        severity=payload["severity"],  # type: ignore[arg-type]
        title=payload["title"],  # type: ignore[arg-type]
        details=payload["details"],  # type: ignore[arg-type]
        pipeline_name=payload["pipeline_name"],  # type: ignore[arg-type]
        flowgroup_name=payload["flowgroup_name"],  # type: ignore[arg-type]
        suggestions=tuple(payload["suggestions"]),  # type: ignore[arg-type]
        context=dict(payload["context"]),  # type: ignore[arg-type]
        doc_link=payload["doc_link"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_view() -> ValidationIssueView:
    """A fully-populated view representing a structured LHPError-derived issue.

    Every field is set; ``context`` is a plain ``dict`` (the production
    default factory in ``views.py``); ``suggestions`` is a non-empty
    tuple. The field annotation is ``Mapping[str, JSONValue]`` so any
    Mapping shape works, but the production default is ``dict``.
    """
    return ValidationIssueView(
        code="LHP-VAL-021",
        category="VAL",
        severity="error",
        title="Foo",
        details="bar",
        pipeline_name="my_pipeline",
        flowgroup_name="my_fg",
        suggestions=("fix x", "fix y"),
        context={"a": 1, "b": "two"},
        doc_link="https://docs.example.com/lhp/errors/LHP-VAL-021",
    )


@pytest.fixture
def picklable_view() -> ValidationIssueView:
    """A populated view with a plain dict context.

    Identical in shape to ``populated_view`` — the production default
    factory for ``context`` is ``dict`` (picklable), and the field
    annotation ``Mapping[str, JSONValue]`` accepts any Mapping.
    """
    return ValidationIssueView(
        code="LHP-VAL-021",
        category="VAL",
        severity="error",
        title="Foo",
        details="bar",
        pipeline_name="my_pipeline",
        flowgroup_name="my_fg",
        suggestions=("fix x", "fix y"),
        context={"a": 1, "b": "two"},
        doc_link="https://docs.example.com/lhp/errors/LHP-VAL-021",
    )


@pytest.fixture
def warning_view() -> ValidationIssueView:
    """An unstructured warning — sparse fields, defaults preserved."""
    return ValidationIssueView(
        code="",
        category="VAL",
        severity="warning",
        title="Deprecated field used",
    )


# ---------------------------------------------------------------------------
# 1. Frozen-check contract.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFrozenContract:
    def test_dataclass_params_frozen(self) -> None:
        params = getattr(ValidationIssueView, "__dataclass_params__", None)
        assert params is not None, "ValidationIssueView has no __dataclass_params__"
        assert params.frozen is True, (
            "ValidationIssueView must be @dataclass(frozen=True); "
            f"got frozen={params.frozen}"
        )


# ---------------------------------------------------------------------------
# 2. Frozen-mutation contract.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFrozenMutationRaises:
    def test_mutation_on_required_field_raises(
        self, populated_view: ValidationIssueView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_view.code = "tampered"  # type: ignore[misc]

    def test_mutation_on_optional_field_raises(
        self, populated_view: ValidationIssueView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_view.details = "tampered"  # type: ignore[misc]

    def test_mutation_on_tuple_field_raises(
        self, populated_view: ValidationIssueView
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            populated_view.suggestions = ()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 3. Pickle round-trip.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_picklable_view_round_trip(
        self, picklable_view: ValidationIssueView
    ) -> None:
        restored = pickle.loads(pickle.dumps(picklable_view))
        assert restored == picklable_view

    def test_warning_view_round_trip(
        self, warning_view: ValidationIssueView
    ) -> None:
        """An unstructured warning round-trips cleanly.

        The default ``context`` factory in ``views.py`` is ``dict``, so
        the view is picklable out of the box — no construction
        workaround required.
        """
        restored = pickle.loads(pickle.dumps(warning_view))
        assert restored == warning_view


# ---------------------------------------------------------------------------
# 4. JSON-style round-trip via flat fields.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestJSONRoundTripViaFields:
    def test_populated_view_context_round_trips_via_json(
        self, populated_view: ValidationIssueView
    ) -> None:
        """Mapping[str, JSONValue] survives dict() → json.dumps → json.loads."""
        ctx = dict(populated_view.context)
        round_tripped = json.loads(json.dumps(ctx))
        assert round_tripped == {"a": 1, "b": "two"}

    def test_populated_view_suggestions_round_trip_via_json(
        self, populated_view: ValidationIssueView
    ) -> None:
        """Tuple[str, ...] survives list() → json.dumps → json.loads."""
        round_tripped = json.loads(json.dumps(list(populated_view.suggestions)))
        assert round_tripped == ["fix x", "fix y"]

    def test_warning_view_defaults_are_json_safe(
        self, warning_view: ValidationIssueView
    ) -> None:
        """Default suggestions=() and context={} serialise cleanly."""
        assert json.loads(json.dumps(list(warning_view.suggestions))) == []
        assert json.loads(json.dumps(dict(warning_view.context))) == {}


# ---------------------------------------------------------------------------
# 5. Field-type contract.
# ---------------------------------------------------------------------------


_BANNED_FIELD_PATTERNS = {
    "Dict[": "Use Mapping[str, JSONValue] per §4.8, not Dict.",
    "List[": "Use Tuple[T, ...] per §4.8, not List.",
    "Exception": "DTOs must not carry live Exception instances (§4.8).",
    "LHPError": "DTOs must not carry LHPError instances; use error_code (§4.8).",
}


def _annotation_strings() -> dict[str, str]:
    return {
        f.name: f.type if isinstance(f.type, str) else str(f.type)
        for f in dataclasses.fields(ValidationIssueView)
    }


@pytest.mark.unit
class TestFieldTypeContract:
    def test_no_banned_field_annotations(self) -> None:
        for name, annotation in _annotation_strings().items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"ValidationIssueView.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    def test_no_any_in_annotations(self) -> None:
        for name, annotation in _annotation_strings().items():
            assert "Any" not in annotation, (
                f"ValidationIssueView.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8."
            )

    def test_resolved_hints_carry_no_exception_or_lhperror(self) -> None:
        """Defence-in-depth check against ``Union[X, LHPError]``-style drift."""
        hints = get_type_hints(ValidationIssueView)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text, (
                f"ValidationIssueView.{name}: resolved type {text!r} carries an "
                f"Exception. Use flat fields (code, category, doc_link) instead."
            )
            assert "LHPError" not in text, (
                f"ValidationIssueView.{name}: resolved type {text!r} carries an "
                f"LHPError. Use flat fields (code, category, doc_link) instead."
            )

    def test_required_fields_present(self) -> None:
        """The five fields explicitly required by the §8.3 view-DTO contract."""
        field_names = {f.name for f in dataclasses.fields(ValidationIssueView)}
        required = {"code", "category", "severity", "title", "context"}
        missing = required - field_names
        assert not missing, (
            f"ValidationIssueView is missing required fields: {missing}. "
            f"Per §8.3, every validation view must carry code/category/severity/"
            f"title and a structured context map."
        )


# ---------------------------------------------------------------------------
# 6. Flat-field round-trip — the LHPError-projection contract.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFlatFieldRoundTrip:
    """Build → asdict-like projection → JSON → reconstruct → equality.

    This is the rendering-pass contract: CLI panels and log emitters
    consume the flat field values; an external consumer that captures
    those fields must be able to reconstruct an equal view from them.
    """

    def test_full_flat_field_round_trip(
        self, populated_view: ValidationIssueView
    ) -> None:
        # 1. Project to JSON-safe dict via the helper above.
        projection = _to_json_safe_dict(populated_view)

        # 2. Round-trip through JSON to verify shape compatibility.
        wire = json.loads(json.dumps(projection))

        # 3. Reconstruct.
        restored = _from_json_safe_dict(wire)

        # 4. Equality holds — both instances compare on flat fields.
        assert restored == populated_view

        # 5. And field-by-field for clear failure messages on drift.
        assert restored.code == populated_view.code
        assert restored.category == populated_view.category
        assert restored.severity == populated_view.severity
        assert restored.title == populated_view.title
        assert restored.details == populated_view.details
        assert restored.pipeline_name == populated_view.pipeline_name
        assert restored.flowgroup_name == populated_view.flowgroup_name
        assert restored.suggestions == populated_view.suggestions
        # Mapping equality is element-wise; compare via dict() for a
        # stable element-wise check regardless of concrete Mapping type.
        assert dict(restored.context) == dict(populated_view.context)
        assert restored.doc_link == populated_view.doc_link

    def test_asdict_works_on_picklable_view(
        self, picklable_view: ValidationIssueView
    ) -> None:
        """``dataclasses.asdict`` works directly on a ValidationIssueView.

        The production ``context`` default factory is ``dict`` (see
        ``views.py``), so ``asdict`` succeeds without unwrapping.
        ``asdict`` preserves tuples as tuples — JSON-shape conversion
        (tuple → list) is a separate step done by ``json.dumps``.
        """
        d = dataclasses.asdict(picklable_view)
        assert d["code"] == "LHP-VAL-021"
        assert d["suggestions"] == ("fix x", "fix y")
        assert d["context"] == {"a": 1, "b": "two"}
        # And the asdict output is JSON-safe (tuple → list at this step).
        round_tripped = json.loads(json.dumps(d))
        assert round_tripped["suggestions"] == ["fix x", "fix y"]
