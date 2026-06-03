"""DTO contract tests for response shapes in ``lhp.api.responses``.

Per constitution §8.3 + §9.7 + §9.15, every public DTO must satisfy four
non-optional contracts:

1. ``@dataclass(frozen=True)`` (no in-place mutation).
2. Pickle round-trip with ``==`` equality (cross-process boundary).
3. JSON round-trip via flat field values (telemetry / event-bus).
4. Field-type discipline — no ``Any`` (outside ``JSONValue``), no
   ``Dict[``/``List[`` (must use ``Mapping``/``Tuple`` per §4.8), and no
   live ``Exception`` / ``LHPError`` typed fields (replaced by flat
   ``error_code`` strings).

The sweep covers ``GenerationResponse``, ``BatchGenerationResponse``,
``ValidationResponse``, ``BatchValidationResponse``, and
``InitProjectResult`` (the latter is added by B4 — the test skips
gracefully if absent at collection time).
"""

from __future__ import annotations

import dataclasses
import json
import os
import pickle
import shutil
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import MappingProxyType
from typing import get_type_hints

import pytest

from lhp.api import (
    LakehousePlumberApplicationFacade,
    collect_response,
)
from lhp.api.responses import (
    BatchGenerationResponse,
    BatchValidationResponse,
    GenerationResponse,
    ValidationResponse,
)
from lhp.api.views import ValidationIssueView
from lhp.models import FlowGroup

try:
    from lhp.api.responses import InitProjectResult
except ImportError:  # pragma: no cover - B4 lands this in parallel.
    InitProjectResult = None  # type: ignore[assignment,misc]


_TYPE_HINT_NAMESPACE = {
    "ValidationIssueView": ValidationIssueView,
    "GenerationResponse": GenerationResponse,
    "ValidationResponse": ValidationResponse,
}


def _resolved_hints(cls: type) -> dict[str, object]:
    """Return ``get_type_hints`` output with forward refs resolved.

    ``responses.py`` uses string forward refs for ``ValidationIssueView``
    (it cannot import ``views`` without a cycle), so the default
    ``get_type_hints(cls)`` call cannot resolve them. We pass an
    explicit ``localns`` containing the missing names.
    """
    return get_type_hints(cls, localns=_TYPE_HINT_NAMESPACE)


def _field_type_str(field: dataclasses.Field) -> str:
    """Return the raw string annotation for a field.

    Because every DTO module uses ``from __future__ import annotations``,
    ``f.type`` is the raw string source of the annotation. This is the
    surface the field-type contract polices.
    """
    t = field.type
    return t if isinstance(t, str) else str(t)


def _json_round_trip(payload: object) -> object:
    return json.loads(json.dumps(payload))


# Production default factory for ``Mapping[str, JSONValue]`` fields is plain
# ``dict`` (picklable). The field annotation is ``Mapping`` so any Mapping
# shape is accepted; one targeted test below exercises ``MappingProxyType``.


@pytest.fixture
def issue_view() -> ValidationIssueView:
    return ValidationIssueView(
        code="LHP-VAL-021",
        category="VAL",
        severity="error",
        title="Missing source",
        details="The source 'customer' is not defined",
        pipeline_name="bronze",
        flowgroup_name="customer_ingest",
        suggestions=("Add a source action for 'customer'",),
        context={"action": "customer_ingest", "source": "customer"},
        doc_link="https://lhp.docs/errors/LHP-VAL-021",
    )


@pytest.fixture
def generation_response(issue_view: ValidationIssueView) -> GenerationResponse:
    return GenerationResponse(
        success=True,
        generated_filenames=("bronze.py", "silver.py"),
        files_written=2,
        total_flowgroups=2,
        output_location=Path("/tmp/lhp_out"),
        performance_info={"duration_s": 1.42, "files_per_s": 1.41},
        duration_s=1.42,
        error_message=None,
        error_code=None,
        error=None,
    )


@pytest.fixture
def failed_generation_response(issue_view: ValidationIssueView) -> GenerationResponse:
    return GenerationResponse(
        success=False,
        generated_filenames=(),
        files_written=0,
        total_flowgroups=1,
        output_location=None,
        performance_info={},
        duration_s=0.01,
        error_message="Validation failed",
        error_code="LHP-VAL-021",
        error=issue_view,
    )


@pytest.fixture
def batch_generation_response(
    generation_response: GenerationResponse,
) -> BatchGenerationResponse:
    return BatchGenerationResponse(
        success=True,
        pipeline_responses={"bronze": generation_response},
        total_files_written=2,
        aggregate_generated_filenames=("bronze.py", "silver.py"),
        output_location=Path("/tmp/lhp_out"),
        error_message=None,
        error_code=None,
    )


@pytest.fixture
def validation_response(issue_view: ValidationIssueView) -> ValidationResponse:
    return ValidationResponse(
        success=False,
        issues=(issue_view,),
        validated_pipelines=("bronze",),
        error_message="One error found",
    )


@pytest.fixture
def batch_validation_response(
    validation_response: ValidationResponse,
) -> BatchValidationResponse:
    return BatchValidationResponse(
        success=False,
        pipeline_responses={"bronze": validation_response},
        total_errors=1,
        total_warnings=0,
        validated_pipelines=("bronze",),
        error_message="One error across 1 pipeline",
        error_code="LHP-VAL-021",
    )


@pytest.mark.unit
class TestFrozenContract:
    """§4.4 / §9.7: every public DTO must be a frozen dataclass."""

    @pytest.mark.parametrize(
        "cls",
        [
            GenerationResponse,
            BatchGenerationResponse,
            ValidationResponse,
            BatchValidationResponse,
            pytest.param(
                InitProjectResult,
                marks=pytest.mark.skipif(
                    InitProjectResult is None,
                    reason="InitProjectResult not yet present (added by B4).",
                ),
            ),
        ],
    )
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
    def test_generation_response_mutation_raises(
        self, generation_response: GenerationResponse
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            generation_response.success = False  # type: ignore[misc]

    def test_batch_generation_response_mutation_raises(
        self, batch_generation_response: BatchGenerationResponse
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            batch_generation_response.success = False  # type: ignore[misc]

    def test_validation_response_mutation_raises(
        self, validation_response: ValidationResponse
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            validation_response.success = True  # type: ignore[misc]

    def test_batch_validation_response_mutation_raises(
        self, batch_validation_response: BatchValidationResponse
    ) -> None:
        with pytest.raises(FrozenInstanceError):
            batch_validation_response.success = True  # type: ignore[misc]

    @pytest.mark.skipif(
        InitProjectResult is None,
        reason="InitProjectResult not yet present (added by B4).",
    )
    def test_init_project_result_mutation_raises(self) -> None:
        assert InitProjectResult is not None  # for type-checkers
        result = InitProjectResult(
            success=True,
            target_dir=Path("/tmp/proj"),
            created_files=(Path("/tmp/proj/lhp.yaml"),),
            created_dirs=(Path("/tmp/proj"),),
            bundle_enabled=False,
        )
        with pytest.raises(FrozenInstanceError):
            result.success = False  # type: ignore[misc]


@pytest.mark.unit
class TestPickleRoundTrip:
    """§9.7: every DTO must survive pickle.dumps → pickle.loads → equality."""

    def test_generation_response_pickle_success(
        self, generation_response: GenerationResponse
    ) -> None:
        restored = pickle.loads(pickle.dumps(generation_response))
        assert restored == generation_response

    def test_generation_response_pickle_failure_path(
        self, failed_generation_response: GenerationResponse
    ) -> None:
        restored = pickle.loads(pickle.dumps(failed_generation_response))
        assert restored == failed_generation_response
        assert restored.error is not None
        assert restored.error.code == "LHP-VAL-021"

    def test_batch_generation_response_pickle(
        self, batch_generation_response: BatchGenerationResponse
    ) -> None:
        restored = pickle.loads(pickle.dumps(batch_generation_response))
        assert restored == batch_generation_response

    def test_validation_response_pickle(
        self, validation_response: ValidationResponse
    ) -> None:
        restored = pickle.loads(pickle.dumps(validation_response))
        assert restored == validation_response

    def test_batch_validation_response_pickle(
        self, batch_validation_response: BatchValidationResponse
    ) -> None:
        restored = pickle.loads(pickle.dumps(batch_validation_response))
        assert restored == batch_validation_response

    @pytest.mark.skipif(
        InitProjectResult is None,
        reason="InitProjectResult not yet present (added by B4).",
    )
    def test_init_project_result_pickle(self) -> None:
        assert InitProjectResult is not None
        result = InitProjectResult(
            success=True,
            target_dir=Path("/tmp/proj"),
            created_files=(Path("/tmp/proj/lhp.yaml"), Path("/tmp/proj/README.md")),
            created_dirs=(Path("/tmp/proj"), Path("/tmp/proj/pipelines")),
            bundle_enabled=True,
            error_message=None,
            error_code=None,
        )
        restored = pickle.loads(pickle.dumps(result))
        assert restored == result
        # Path fields round-trip as Path, not stringified.
        assert isinstance(restored.target_dir, Path)
        assert all(isinstance(p, Path) for p in restored.created_files)


@pytest.mark.unit
class TestJSONRoundTripViaFields:
    """Mapping[str, JSONValue] fields round-trip cleanly through json.

    This is a sanity check that the JSONValue-typed fields ARE in fact
    JSON-serialisable — exercising both the production default
    (plain ``dict``) and a ``MappingProxyType`` wrapper to confirm the
    field accepts any Mapping shape.
    """

    def test_generation_response_performance_info_json_round_trip_plain_dict(
        self,
    ) -> None:
        info = {"duration_s": 1.5, "files": 3, "nested": {"k": "v"}}
        gen = GenerationResponse(
            success=True,
            generated_filenames=("a.py",),
            files_written=1,
            total_flowgroups=1,
            output_location=None,
            performance_info=info,
        )
        round_tripped = _json_round_trip(dict(gen.performance_info))
        assert round_tripped == info

    def test_generation_response_performance_info_json_round_trip_proxy(self) -> None:
        info_proxy = MappingProxyType(
            {"duration_s": 1.5, "files": 3, "nested": ("a", "b")}
        )
        gen = GenerationResponse(
            success=True,
            generated_filenames=("a.py",),
            files_written=1,
            total_flowgroups=1,
            output_location=None,
            performance_info=info_proxy,
        )
        round_tripped = _json_round_trip(dict(gen.performance_info))
        # Tuple becomes list through json — that's a JSON-shape invariant.
        assert round_tripped == {
            "duration_s": 1.5,
            "files": 3,
            "nested": ["a", "b"],
        }

    def test_batch_generation_response_pipeline_responses_keys_are_strings(
        self, batch_generation_response: BatchGenerationResponse
    ) -> None:
        """Mapping keys are strings — the JSONValue contract requires this."""
        for key in batch_generation_response.pipeline_responses.keys():
            assert isinstance(key, str)

    def test_validation_response_validated_pipelines_json_round_trip(
        self, validation_response: ValidationResponse
    ) -> None:
        round_tripped = _json_round_trip(list(validation_response.validated_pipelines))
        assert round_tripped == ["bronze"]

    @pytest.mark.skipif(
        InitProjectResult is None,
        reason="InitProjectResult not yet present (added by B4).",
    )
    def test_init_project_result_path_tuples_round_trip(self) -> None:
        """Path-tuple fields can be stringified and JSON-round-tripped."""
        assert InitProjectResult is not None
        files = (Path("/tmp/proj/lhp.yaml"), Path("/tmp/proj/README.md"))
        result = InitProjectResult(
            success=True,
            target_dir=Path("/tmp/proj"),
            created_files=files,
            created_dirs=(),
            bundle_enabled=False,
        )
        stringified = [str(p) for p in result.created_files]
        round_tripped = _json_round_trip(stringified)
        assert round_tripped == [str(p) for p in files]


_BANNED_FIELD_PATTERNS = {
    "Dict[": "Use Mapping[str, JSONValue] per §4.8, not Dict.",
    "List[": "Use Tuple[T, ...] per §4.8, not List.",
    "Exception": "DTOs must not carry live Exception instances (§4.8).",
    "LHPError": "DTOs must not carry LHPError instances; use error_code (§4.8).",
}


_ALLOWED_ANY_CONTEXTS = (
    # JSONValue itself widens to Union[...] — it never contains the
    # string "Any", but if it ever did this would be the only allowed
    # context. We allow no other appearance of "Any" in any field.
    "JSONValue",
)


def _annotation_strings_for(cls: type) -> dict[str, str]:
    """Return {field_name: annotation_string} for all dataclass fields."""
    return {f.name: _field_type_str(f) for f in dataclasses.fields(cls)}


@pytest.mark.unit
class TestFieldTypeContract:
    """§4.8: DTO field annotations stay within the allowed type vocabulary."""

    @pytest.mark.parametrize(
        "cls",
        [
            GenerationResponse,
            BatchGenerationResponse,
            ValidationResponse,
            BatchValidationResponse,
            pytest.param(
                InitProjectResult,
                marks=pytest.mark.skipif(
                    InitProjectResult is None,
                    reason="InitProjectResult not yet present (added by B4).",
                ),
            ),
        ],
    )
    def test_no_banned_field_annotations(self, cls: type) -> None:
        """No field annotation may contain Dict[/List[/Exception/LHPError."""
        for name, annotation in _annotation_strings_for(cls).items():
            for needle, reason in _BANNED_FIELD_PATTERNS.items():
                assert needle not in annotation, (
                    f"{cls.__name__}.{name}: annotation {annotation!r} "
                    f"contains banned token {needle!r}. {reason}"
                )

    @pytest.mark.parametrize(
        "cls",
        [
            GenerationResponse,
            BatchGenerationResponse,
            ValidationResponse,
            BatchValidationResponse,
            pytest.param(
                InitProjectResult,
                marks=pytest.mark.skipif(
                    InitProjectResult is None,
                    reason="InitProjectResult not yet present (added by B4).",
                ),
            ),
        ],
    )
    def test_no_any_outside_jsonvalue(self, cls: type) -> None:
        """No field annotation may contain the bare word ``Any``.

        ``JSONValue`` is the only sanctioned widening; it does NOT
        expand to ``Any`` in its definition (it's an explicit Union),
        so the bare-word rule applies uniformly.
        """
        for name, annotation in _annotation_strings_for(cls).items():
            if annotation in _ALLOWED_ANY_CONTEXTS:
                continue
            assert "Any" not in annotation, (
                f"{cls.__name__}.{name}: annotation {annotation!r} contains "
                f"'Any'. Use precise types per §4.8 (JSONValue is the only "
                f"sanctioned widening, and it expands to a Union — not Any)."
            )

    @pytest.mark.parametrize(
        "cls",
        [
            GenerationResponse,
            BatchGenerationResponse,
            ValidationResponse,
            BatchValidationResponse,
            pytest.param(
                InitProjectResult,
                marks=pytest.mark.skipif(
                    InitProjectResult is None,
                    reason="InitProjectResult not yet present (added by B4).",
                ),
            ),
        ],
    )
    def test_resolved_hints_contain_no_exception_or_lhperror_types(
        self, cls: type
    ) -> None:
        """After forward-ref resolution, no field is typed as a live error.

        The string-form check above is a substring scan; this check
        inspects the *resolved* hint objects to defend against
        annotations of the form ``Optional["LHPError"]`` that the
        string scan would still flag, but also against constructs like
        ``Union[X, Exception]`` that survive a stringly-typed scan.
        """
        hints = _resolved_hints(cls)
        for name, hint in hints.items():
            text = repr(hint)
            assert "Exception" not in text, (
                f"{cls.__name__}.{name}: resolved type {text!r} carries an "
                f"Exception. DTOs must store error_code: str instead."
            )
            assert "LHPError" not in text, (
                f"{cls.__name__}.{name}: resolved type {text!r} carries an "
                f"LHPError. DTOs must store error_code: str instead."
            )


@pytest.mark.unit
class TestResponseHelpers:
    """Light coverage of the read-only convenience methods on responses."""

    def test_generation_response_is_successful(
        self, generation_response: GenerationResponse
    ) -> None:
        assert generation_response.is_successful() is True

    def test_validation_response_error_counts(
        self, validation_response: ValidationResponse
    ) -> None:
        assert validation_response.error_count == 1
        assert validation_response.warning_count == 0
        assert validation_response.has_errors() is True
        assert validation_response.has_warnings() is False


_FIXTURE_PATH = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"


@pytest.fixture
def project_facade(tmp_path: Path):
    """Real facade over an isolated copy of the e2e fixture project.

    Mirrors the boundary-only setup in ``test_event_protocol.py``: deep-copy
    the fixture per test, swap CWD to the project root (LHP resolves several
    relative paths off ``Path.cwd()``), and build a genuine facade. No
    internal logic is mocked (§8.8) — the validate path runs end to end.
    """
    project_root = tmp_path / "test_project"
    shutil.copytree(_FIXTURE_PATH, project_root)
    original_cwd = os.getcwd()
    os.chdir(project_root)
    try:
        yield LakehousePlumberApplicationFacade.for_project(project_root)
    finally:
        os.chdir(original_cwd)


@pytest.mark.unit
class TestBatchValidationDuplicateContract:
    """Validate surfaces duplicate (pipeline, flowgroup) pairs as a
    failed ``BatchValidationResponse`` carrying ``LHP-VAL-009`` — the same
    detection the generate path runs (§9.24), differing only in surfacing
    (non-zero-exit DTO here vs. a raise on the generate path)."""

    def test_duplicate_pipeline_flowgroup_yields_val_009_batch(
        self, project_facade: LakehousePlumberApplicationFacade
    ) -> None:
        duplicate_flowgroups = [
            FlowGroup(pipeline="dup_pipeline", flowgroup="dup_fg", actions=[]),
            FlowGroup(pipeline="dup_pipeline", flowgroup="dup_fg", actions=[]),
        ]
        response = collect_response(
            project_facade.validation.validate_pipelines(
                pipeline_fields=("dup_pipeline",),
                env="dev",
                pre_discovered_all_flowgroups=duplicate_flowgroups,
            )
        )
        assert isinstance(response, BatchValidationResponse)
        assert response.error_code == "LHP-VAL-009"
        assert response.success is False
