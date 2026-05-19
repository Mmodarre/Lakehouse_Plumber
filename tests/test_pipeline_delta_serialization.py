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

import pytest

from lhp.core.state_models import PipelineDelta


class TestPipelineDeltaSerialization:
    def test_success_delta_round_trips(self):
        original = PipelineDelta.success_(
            "bronze_ingest",
            files_written=5,
            files_skipped=2,
            artifacts_count=1,
            generated_filenames=("a.py",),
        )
        restored = pickle.loads(pickle.dumps(original))
        assert restored.pipeline_name == "bronze_ingest"
        assert restored.success is True
        assert restored.files_written == 5
        assert restored.files_skipped == 2
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
        assert restored.files_skipped == 0

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
        delta = PipelineDelta.success_(
            "p", files_written=3, files_skipped=1, artifacts_count=0
        )
        once = pickle.loads(pickle.dumps(delta))
        twice = pickle.loads(pickle.dumps(once))
        assert delta.pipeline_name == twice.pipeline_name
        assert delta.files_written == twice.files_written
        assert delta.files_skipped == twice.files_skipped
        assert delta.artifacts_count == twice.artifacts_count
        assert delta.generated_filenames == twice.generated_filenames
