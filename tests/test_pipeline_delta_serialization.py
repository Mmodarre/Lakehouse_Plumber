"""Pickle round-trip invariants for :class:`PipelineDelta`.

Workers return a :class:`PipelineDelta` across the ``spawn``-process boundary;
:mod:`pickle` is the serialisation contract. The delta MUST round-trip without
loss for both the success and failure shapes, including the error-string fields
in the failure shape (no exception objects survive pickling cleanly without
hand-rolled support — the delta carries strings only).

These tests resolve Plan 2's B1 verification: pickle round-trip on
``PipelineDelta`` instances (both success and failure shapes).
"""

from __future__ import annotations

import pickle
from pathlib import Path

import pytest

from lhp.errors import (
    ErrorCategory,
    LHPConfigError,
    LHPFileError,
    LHPValidationError,
    MultiDocumentError,
)
from lhp.models.processing import PipelineDelta


class TestPipelineDeltaSerialization:
    def test_success_delta_round_trips(self):
        original = PipelineDelta.success_(
            "bronze_ingest",
            files_written=5,
            artifacts_count=1,
            generated_filenames=("a.py",),
        )
        restored = pickle.loads(pickle.dumps(original))
        assert restored.pipeline_name == "bronze_ingest"
        assert restored.success is True
        assert restored.files_written == 5
        assert restored.artifacts_count == 1
        assert restored.generated_filenames == ("a.py",)
        assert restored.error_type is None
        assert restored.error_message is None
        assert restored.error_traceback is None

    def test_failure_delta_round_trips_with_string_error_fields(self):
        try:
            raise ValueError("simulated parse error")
        except ValueError as exc:
            original = PipelineDelta.failure("silver_join", exc)

        restored = pickle.loads(pickle.dumps(original))
        assert restored.pipeline_name == "silver_join"
        assert restored.success is False
        assert restored.error_type == "ValueError"
        assert "simulated parse error" in restored.error_message
        assert restored.error_traceback is not None
        assert "ValueError" in restored.error_traceback
        assert restored.files_written == 0

    @pytest.mark.parametrize(
        "exc",
        [
            ValueError("bad value"),
            FileNotFoundError("missing"),
            RuntimeError("boom"),
        ],
    )
    def test_failure_delta_carries_picklable_strings_only(self, exc):
        """The exception type and traceback travel as strings — no live object."""
        delta = PipelineDelta.failure("p", exc)
        # All error fields are strings (or None for success).
        for field in (delta.error_type, delta.error_message, delta.error_traceback):
            assert field is None or isinstance(field, str)
        # Pickle must succeed without exception-object pickling.
        restored = pickle.loads(pickle.dumps(delta))
        assert restored.error_type == type(exc).__name__

    def test_repeated_round_trips_are_stable(self):
        """Double round-trip keeps all fields equal — no aliasing on dataclass slots."""
        delta = PipelineDelta.success_("p", files_written=3, artifacts_count=0)
        once = pickle.loads(pickle.dumps(delta))
        twice = pickle.loads(pickle.dumps(once))
        assert delta.pipeline_name == twice.pipeline_name
        assert delta.files_written == twice.files_written
        assert delta.artifacts_count == twice.artifacts_count
        assert delta.generated_filenames == twice.generated_filenames


@pytest.mark.parametrize(
    "error_cls,code_number,category",
    [
        (LHPValidationError, "007", ErrorCategory.VALIDATION),
        (LHPConfigError, "011", ErrorCategory.CONFIG),
        (LHPFileError, "001", ErrorCategory.IO),
    ],
)
def test_pipeline_delta_pickle_with_lhp_subclass(error_cls, code_number, category):
    """Whole-PipelineDelta pickle preserves LHPError subclass identity."""
    err = error_cls(
        category=category,
        code_number=code_number,
        title="Test failure",
        details="The thing broke.",
        context={"Pipeline": "x"},
        suggestions=["Fix the thing"],
    )
    delta = PipelineDelta.failure("test_pipeline", err)
    restored = pickle.loads(pickle.dumps(delta))
    assert restored.lhp_error is not None
    assert restored.lhp_error.__class__ is error_cls
    assert restored.lhp_error.code == f"LHP-{category.value}-{code_number}"
    assert restored.lhp_error.title == "Test failure"
    assert restored.lhp_error.context == {"Pipeline": "x"}
    assert restored.lhp_error.suggestions == ["Fix the thing"]
    assert restored.error_message == str(err)  # legacy field still populated


def test_pipeline_delta_pickle_with_non_lhp_exception():
    """Non-LHP exception: lhp_error is None, string fields preserved."""
    exc = ValueError("simulated parse error")
    delta = PipelineDelta.failure("test_pipeline", exc)
    restored = pickle.loads(pickle.dumps(delta))
    assert restored.lhp_error is None
    assert restored.error_type == "ValueError"
    assert "simulated parse error" in restored.error_message
    assert restored.error_traceback != ""


def test_pipeline_delta_pickle_with_multi_document_error():
    """MultiDocumentError has its own __reduce__ signature — verify round-trip."""
    err = MultiDocumentError(
        file_path=Path("/tmp/test.yaml"),
        num_documents=3,
        error_context="some context",
    )
    delta = PipelineDelta.failure("test_pipeline", err)
    restored = pickle.loads(pickle.dumps(delta))
    assert restored.lhp_error is not None
    assert restored.lhp_error.__class__ is MultiDocumentError
    assert restored.lhp_error.code == "LHP-IO-003"


def test_pipeline_delta_pickle_with_python_function_conflict_error():
    """PythonFunctionConflictError has a custom 3-arg ``__init__`` and must
    define its own ``__reduce__`` so the spawn-pool pickle round-trip
    preserves subclass identity (otherwise the parent ``LHPError.__reduce__``
    would attempt the 8-arg base reconstruction and unpickling would raise
    ``TypeError`` — silently collapsing the original LHP-VAL-019 error into
    a non-LHP wrap on the parent side)."""
    from lhp.errors import PythonFunctionConflictError

    err = PythonFunctionConflictError(
        destination="dest.py",
        existing_source="a/src.py",
        new_source="b/src.py",
    )
    delta = PipelineDelta.failure("test_pipeline", err)
    restored = pickle.loads(pickle.dumps(delta))
    assert restored.lhp_error is not None
    assert restored.lhp_error.__class__ is PythonFunctionConflictError
    assert restored.lhp_error.code == "LHP-VAL-019"
    assert restored.lhp_error.destination == "dest.py"
    assert restored.lhp_error.existing_source == "a/src.py"
    assert restored.lhp_error.new_source == "b/src.py"
