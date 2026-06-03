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
import os
import pickle
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Mapping, get_type_hints

import pytest
import yaml as _yaml

from lhp.api import (
    BatchValidationResponse,
    LakehousePlumberApplicationFacade,
    collect_response,
)
from lhp.api.responses import JSONValue
from lhp.api.views import ValidationIssueView

# The view's ``context`` field is typed ``Mapping[str, JSONValue]`` and
# defaults to a plain ``dict`` (see ``src/lhp/api/views.py:50``). Helpers
# below project to / from a JSON-shape dict so a round-trip test can
# verify field-by-field equality after wire serialization.


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
        # Path → str for JSON-shape (None stays None).
        "file_path": str(view.file_path) if view.file_path is not None else None,
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
    raw_file_path = payload["file_path"]
    return ValidationIssueView(
        code=payload["code"],  # type: ignore[arg-type]
        category=payload["category"],  # type: ignore[arg-type]
        severity=payload["severity"],  # type: ignore[arg-type]
        title=payload["title"],  # type: ignore[arg-type]
        details=payload["details"],  # type: ignore[arg-type]
        pipeline_name=payload["pipeline_name"],  # type: ignore[arg-type]
        flowgroup_name=payload["flowgroup_name"],  # type: ignore[arg-type]
        # str → Path on reconstruct (None stays None).
        file_path=Path(raw_file_path) if raw_file_path is not None else None,  # type: ignore[arg-type]
        suggestions=tuple(payload["suggestions"]),  # type: ignore[arg-type]
        context=dict(payload["context"]),  # type: ignore[arg-type]
        doc_link=payload["doc_link"],  # type: ignore[arg-type]
    )


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
    return ValidationIssueView(
        code="",
        category="VAL",
        severity="warning",
        title="Deprecated field used",
    )


@pytest.mark.unit
class TestFrozenContract:
    def test_dataclass_params_frozen(self) -> None:
        params = getattr(ValidationIssueView, "__dataclass_params__", None)
        assert params is not None, "ValidationIssueView has no __dataclass_params__"
        assert params.frozen is True, (
            "ValidationIssueView must be @dataclass(frozen=True); "
            f"got frozen={params.frozen}"
        )


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


@pytest.mark.unit
class TestPickleRoundTrip:
    def test_picklable_view_round_trip(
        self, picklable_view: ValidationIssueView
    ) -> None:
        restored = pickle.loads(pickle.dumps(picklable_view))
        assert restored == picklable_view

    def test_warning_view_round_trip(self, warning_view: ValidationIssueView) -> None:
        """An unstructured warning round-trips cleanly.

        The default ``context`` factory in ``views.py`` is ``dict``, so
        the view is picklable out of the box — no construction
        workaround required.
        """
        restored = pickle.loads(pickle.dumps(warning_view))
        assert restored == warning_view


@pytest.mark.unit
class TestJSONRoundTripViaFields:
    def test_populated_view_context_round_trips_via_json(
        self, populated_view: ValidationIssueView
    ) -> None:
        ctx = dict(populated_view.context)
        round_tripped = json.loads(json.dumps(ctx))
        assert round_tripped == {"a": 1, "b": "two"}

    def test_populated_view_suggestions_round_trip_via_json(
        self, populated_view: ValidationIssueView
    ) -> None:
        round_tripped = json.loads(json.dumps(list(populated_view.suggestions)))
        assert round_tripped == ["fix x", "fix y"]

    def test_warning_view_defaults_are_json_safe(
        self, warning_view: ValidationIssueView
    ) -> None:
        assert json.loads(json.dumps(list(warning_view.suggestions))) == []
        assert json.loads(json.dumps(dict(warning_view.context))) == {}


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
        projection = _to_json_safe_dict(populated_view)
        wire = json.loads(json.dumps(projection))
        restored = _from_json_safe_dict(wire)
        assert restored == populated_view
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
        round_tripped = json.loads(json.dumps(d))
        assert round_tripped["suggestions"] == ["fix x", "fix y"]


@pytest.mark.unit
class TestLocationFields:
    """``file_path`` and ``flowgroup_name`` exist, default to ``None``, and
    round-trip — frozen by FREEZE-1 (``file_path`` is added here;
    ``flowgroup_name`` already existed). This only guarantees the surface.
    """

    def test_file_path_field_exists_and_defaults_none(self) -> None:
        field_names = {f.name for f in dataclasses.fields(ValidationIssueView)}
        assert "file_path" in field_names, (
            "ValidationIssueView must carry a file_path field (FREEZE-1)."
        )
        view = ValidationIssueView(
            code="", category="VAL", severity="warning", title="t"
        )
        assert view.file_path is None

    def test_flowgroup_name_field_exists_and_defaults_none(self) -> None:
        field_names = {f.name for f in dataclasses.fields(ValidationIssueView)}
        assert "flowgroup_name" in field_names, (
            "ValidationIssueView must carry a flowgroup_name field."
        )
        view = ValidationIssueView(
            code="", category="VAL", severity="warning", title="t"
        )
        assert view.flowgroup_name is None

    def test_file_path_is_optional_path(self) -> None:
        """The resolved annotation is ``Optional[Path]``.

        ``repr`` may render either as ``typing.Optional[pathlib.Path]`` or
        the expanded ``typing.Union[pathlib.Path, NoneType]`` depending on
        the Python version; both encode optionality, so accept either.
        """
        hints = get_type_hints(ValidationIssueView)
        text = repr(hints["file_path"])
        assert "Path" in text and ("Optional" in text or "NoneType" in text), (
            f"file_path must be Optional[Path]; got {text!r}"
        )

    def test_non_none_file_path_round_trips_via_to_dict(self) -> None:
        from lhp.api import to_dict

        view = ValidationIssueView(
            code="LHP-VAL-021",
            category="VAL",
            severity="error",
            title="Missing source",
            pipeline_name="bronze",
            flowgroup_name="customer_ingest",
            file_path=Path("pipelines/bronze/customer.yaml"),
        )
        payload = to_dict(view)
        assert payload["file_path"] == "pipelines/bronze/customer.yaml"
        assert payload["flowgroup_name"] == "customer_ingest"
        wire = json.loads(json.dumps(payload))
        restored = ValidationIssueView(
            code=wire["code"],
            category=wire["category"],
            severity=wire["severity"],
            title=wire["title"],
            details=wire["details"],
            pipeline_name=wire["pipeline_name"],
            flowgroup_name=wire["flowgroup_name"],
            file_path=Path(wire["file_path"]),
            suggestions=tuple(wire["suggestions"]),
            context=dict(wire["context"]),
            doc_link=wire["doc_link"],
        )
        assert restored == view
        assert isinstance(restored.file_path, Path)

    def test_full_flat_field_round_trip_includes_file_path(self) -> None:
        view = ValidationIssueView(
            code="LHP-VAL-021",
            category="VAL",
            severity="error",
            title="t",
            file_path=Path("pipelines/bronze/customer.yaml"),
            flowgroup_name="customer_ingest",
        )
        wire = json.loads(json.dumps(_to_json_safe_dict(view)))
        restored = _from_json_safe_dict(wire)
        assert restored == view
        assert restored.file_path == Path("pipelines/bronze/customer.yaml")
        assert isinstance(restored.file_path, Path)


def _write_flowgroup_yaml(
    project_root: Path, pipeline: str, flowgroup: str, actions: list
) -> Path:
    """Write one flowgroup YAML and return its path."""
    pdir = project_root / "pipelines" / pipeline
    pdir.mkdir(parents=True, exist_ok=True)
    path = pdir / f"{flowgroup}.yaml"
    with open(path, "w") as f:
        _yaml.dump(
            {"pipeline": pipeline, "flowgroup": flowgroup, "actions": actions}, f
        )
    return path


def _streaming_write(name: str, source: str, table: str) -> dict:
    return {
        "name": name,
        "type": "write",
        "source": source,
        "write_target": {
            "type": "streaming_table",
            "catalog": "${catalog}",
            "schema": "${bronze_schema}",
            "table": table,
            "create_table": True,
        },
    }


def _cloudfiles_load(name: str, target: str) -> dict:
    return {
        "name": name,
        "type": "load",
        "target": target,
        "source": {"type": "cloudfiles", "path": "${landing_path}/x", "format": "json"},
    }


@pytest.fixture
def attribution_project(tmp_path: Path):
    """Write a project with three pipelines and yield (facade, fg→path map).

    * ``p_attr`` / ``attr_fg`` — a write action with NO ``source``: a
      per-flowgroup ``LHP-VAL-007`` validate ATTRIBUTES to that flowgroup +
      file (the structured-error path through ``FlowgroupOutcome.lhp_error``).
    * ``p_cross`` / ``cross_fg`` — one flowgroup with two ``create_table: true``
      writes to the SAME table: a cross-flowgroup ``LHP-CFG-004`` from the
      §9.24 barrier, which has NO single owning flowgroup (attribution None).
    * ``p_clean`` / ``clean_fg`` — validates clean (no issues).
    """
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text("name: attribution\nversion: '1.0'\n")
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        _yaml.dump(
            {
                "dev": {
                    "catalog": "dev_catalog",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/dev/landing",
                }
            },
            f,
        )

    paths: dict[tuple[str, str], Path] = {}
    # Per-flowgroup attributed error: a write with no source.
    paths[("p_attr", "attr_fg")] = _write_flowgroup_yaml(
        project_root,
        "p_attr",
        "attr_fg",
        [_streaming_write("w_no_source", "v_missing_source", "attr_table")],
    )
    # Cross-flowgroup error: two create_table writes to the same table.
    paths[("p_cross", "cross_fg")] = _write_flowgroup_yaml(
        project_root,
        "p_cross",
        "cross_fg",
        [
            _cloudfiles_load("load_cross", "v_cross_raw"),
            _streaming_write("w_a", "v_cross_raw", "shared_table"),
            _streaming_write("w_b", "v_cross_raw", "shared_table"),
        ],
    )
    # Clean pipeline.
    paths[("p_clean", "clean_fg")] = _write_flowgroup_yaml(
        project_root,
        "p_clean",
        "clean_fg",
        [
            _cloudfiles_load("load_clean", "v_clean_raw"),
            _streaming_write("w_clean", "v_clean_raw", "clean_table"),
        ],
    )

    prev = os.getcwd()
    os.chdir(project_root)
    facade = LakehousePlumberApplicationFacade.for_project(
        project_root, enforce_version=False
    )
    try:
        yield facade, paths
    finally:
        os.chdir(prev)


@pytest.mark.integration
class TestPerIssueAttributionPopulated:
    @staticmethod
    def _validate(facade) -> BatchValidationResponse:
        return collect_response(
            facade.validation.validate_pipelines(
                pipeline_fields=["p_attr", "p_cross", "p_clean"], env="dev"
            )
        )

    def test_per_flowgroup_issue_carries_flowgroup_name_and_file_path(
        self, attribution_project
    ) -> None:
        facade, paths = attribution_project
        response = self._validate(facade)

        attr = response.pipeline_responses["p_attr"]
        assert attr.success is False
        # Every finding for this pipeline is attributed to its flowgroup + file.
        assert attr.issues, "expected at least one finding for p_attr"
        for issue in attr.issues:
            assert isinstance(issue, ValidationIssueView)
            assert issue.flowgroup_name == "attr_fg"
            assert issue.file_path == paths[("p_attr", "attr_fg")]
            assert isinstance(issue.file_path, Path)

    def test_cross_flowgroup_issue_carries_none_attribution(
        self, attribution_project
    ) -> None:
        facade, _paths = attribution_project
        response = self._validate(facade)

        cross = response.pipeline_responses["p_cross"]
        assert cross.success is False
        # A cross-flowgroup finding has no single owning flowgroup: both the
        # flowgroup name and the source file are None.
        cross_cfg = [i for i in cross.issues if i.code == "LHP-CFG-004"]
        assert cross_cfg, f"expected a cross-fg LHP-CFG-004; got {cross.issues}"
        for issue in cross_cfg:
            assert issue.flowgroup_name is None
            assert issue.file_path is None

    def test_clean_pipeline_has_no_issues(self, attribution_project) -> None:
        facade, _paths = attribution_project
        response = self._validate(facade)

        clean = response.pipeline_responses["p_clean"]
        assert clean.success is True
        assert clean.issues == ()

    def test_consumer_ignoring_new_fields_still_works(
        self, attribution_project
    ) -> None:
        """A consumer that only reads code/severity/title (ignoring the new
        location fields entirely) sees the same findings — adding
        flowgroup_name / file_path is purely additive."""
        facade, _paths = attribution_project
        response = self._validate(facade)

        # Project every finding down to the pre-attribution fields only.
        legacy_view = [
            (i.code, i.severity, bool(i.title))
            for pr in response.pipeline_responses.values()
            for i in pr.issues
        ]
        # The attributed VAL-007 and the cross-fg CFG-004 both still surface.
        assert ("LHP-VAL-007", "error", True) in legacy_view
        assert ("LHP-CFG-004", "error", True) in legacy_view
        assert response.success is False
