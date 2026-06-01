"""Pickle round-trip suite for :class:`FlowgroupOutcome`.

Requirement N6: ``FlowgroupOutcome`` instances cross a
``ProcessPoolExecutor(mp_context="spawn")`` boundary (worker → main
thread), so they MUST pickle losslessly — including their carried
errors. ``spawn`` re-imports modules in the child and ships every
argument/return value via ``pickle``; anything that does not survive a
``pickle.loads(pickle.dumps(x))`` round-trip is silently corrupted or
turns a real LHP error into an unpickling ``TypeError`` on the parent.

Equality semantics this suite relies on (all verified against the live
classes, not assumed):

  - ``FlowgroupOutcome`` is ``@dataclass(frozen=True, slots=True)`` so it
    gets a field-wise ``__eq__``.
  - ``FlowGroup`` (Pydantic ``BaseModel``) and ``CopiedModuleRecord``
    (frozen dataclass) both compare by value, so an outcome carrying only
    those round-trips under whole-object ``==``.
  - ``LHPError`` and its subclasses define **no** value ``__eq__`` — they
    compare by identity. An unpickled error is therefore a *different*
    object, so an outcome carrying ``lhp_error`` will NOT satisfy
    whole-object ``==``. For those cases we assert field-equivalence on
    the reconstructed error (subclass type + ``code`` + the load-bearing
    attributes) instead, which is the property N6 actually needs.
"""

import pickle
from pathlib import Path

import pytest

from lhp.errors.categories import ErrorCategory
from lhp.errors.types import LHPError, MultiDocumentError, PythonFunctionConflictError
from lhp.models import Action, ActionType, FlowGroup
from lhp.models.processing import CopiedModuleRecord, FlowgroupOutcome


def _round_trip(obj):
    """Pickle then unpickle ``obj`` and return the reconstructed copy."""
    return pickle.loads(pickle.dumps(obj))


def _make_flowgroup() -> FlowGroup:
    """A minimal but non-trivial valid flowgroup (exercises nested models)."""
    return FlowGroup(
        pipeline="pipe_a",
        flowgroup="fg_a",
        actions=[
            Action(name="load_raw", type=ActionType.LOAD, target="raw_data"),
            Action(
                name="clean",
                type=ActionType.TRANSFORM,
                source="raw_data",
                target="clean_data",
            ),
        ],
    )


def _make_copy_record() -> CopiedModuleRecord:
    return CopiedModuleRecord(
        source_path="custom/transforms.py",
        dest_path=Path("generated/custom/transforms.py"),
        content="def transform(df):\n    return df\n",
        module_path="custom.transforms",
        custom_functions_dir=Path("generated/custom"),
    )


def _assert_scaffold_preserved(restored, original) -> None:
    """The non-error scaffolding of any outcome must survive verbatim."""
    assert isinstance(restored, FlowgroupOutcome)
    assert restored.pipeline == original.pipeline
    assert restored.flowgroup_name == original.flowgroup_name
    assert restored.success is original.success
    assert restored.errors == original.errors


class TestFlowgroupOutcomePickle:
    """``pickle.loads(pickle.dumps(x))`` losslessness for every channel."""

    def test_ok_outcome_with_payload_round_trips_by_value(self):
        """Case 1: an ``ok`` outcome carrying a resolved flowgroup,
        auxiliary files, and a copy record round-trips under whole-object
        ``==`` (all carried types compare by value)."""
        flowgroup = _make_flowgroup()
        record = _make_copy_record()
        outcome = FlowgroupOutcome.ok(
            "pipe_a",
            "fg_a",
            resolved_flowgroup=flowgroup,
            formatted_code="import dlt\n",
            auxiliary_files=(("monitoring.py", "print('hi')\n"),),
            copy_records=(record,),
        )

        restored = _round_trip(outcome)

        # Whole-object equality holds: FlowGroup + CopiedModuleRecord are
        # value types, and a success outcome carries no error object.
        assert restored == outcome
        # Spot-check the payload survived structurally, not just by ==.
        _assert_scaffold_preserved(restored, outcome)
        assert restored.success is True
        assert restored.formatted_code == "import dlt\n"
        assert restored.auxiliary_files == (("monitoring.py", "print('hi')\n"),)
        assert restored.resolved_flowgroup == flowgroup
        assert restored.resolved_flowgroup.actions[0].name == "load_raw"
        assert restored.copy_records == (record,)
        assert restored.copy_records[0].dest_path == Path(
            "generated/custom/transforms.py"
        )
        assert restored.lhp_error is None

    def test_failure_with_base_lhp_error_preserves_error_fields(self):
        """Case 2: a ``failure`` carrying a base :class:`LHPError`.

        The error compares by identity, so whole-object ``==`` is False;
        assert the reconstructed error keeps its exact class, code, and
        the context/suggestions transported by ``LHPError.__reduce__``.
        """
        error = LHPError(
            category=ErrorCategory.GENERAL,
            code_number="001",
            title="Boom",
            details="Something broke",
            suggestions=["Try again"],
            context={"flowgroup": "fg_a"},
        )
        outcome = FlowgroupOutcome.failure("pipe_a", "fg_a", lhp_error=error)

        restored = _round_trip(outcome)

        _assert_scaffold_preserved(restored, outcome)
        assert restored.success is False
        assert restored.resolved_flowgroup is None
        # Identity-based __eq__ means the carried error is a fresh object.
        assert restored.lhp_error is not error
        # ...but every load-bearing attribute survives the spawn boundary.
        assert type(restored.lhp_error) is LHPError
        assert restored.lhp_error.code == "LHP-GEN-001"
        assert restored.lhp_error.code_number == "001"
        assert restored.lhp_error.category is ErrorCategory.GENERAL
        assert restored.lhp_error.title == "Boom"
        assert restored.lhp_error.details == "Something broke"
        assert restored.lhp_error.suggestions == ["Try again"]
        assert restored.lhp_error.context == {"flowgroup": "fg_a"}

    def test_failure_with_python_function_conflict_error_preserves_subclass(self):
        """Case 3: a ``failure`` carrying
        :class:`PythonFunctionConflictError` (LHP-VAL-019).

        This subclass has a 3-arg ``__init__`` and a custom ``__reduce__``;
        the round-trip must keep its subclass identity, ``code``, and the
        three conflict fields rather than collapsing to the base class.
        """
        error = PythonFunctionConflictError(
            destination="generated/custom/dup.py",
            existing_source="a/funcs.py",
            new_source="b/funcs.py",
        )
        outcome = FlowgroupOutcome.failure("pipe_a", "fg_a", lhp_error=error)

        restored = _round_trip(outcome)

        _assert_scaffold_preserved(restored, outcome)
        restored_error = restored.lhp_error
        assert type(restored_error) is PythonFunctionConflictError
        assert isinstance(restored_error, LHPError)
        assert restored_error.code == "LHP-VAL-019"
        assert restored_error.code_number == "019"
        assert restored_error.destination == "generated/custom/dup.py"
        assert restored_error.existing_source == "a/funcs.py"
        assert restored_error.new_source == "b/funcs.py"

    def test_failure_with_multi_document_error_preserves_subclass(self):
        """Case 4: a ``failure`` carrying :class:`MultiDocumentError`
        (LHP-IO-003).

        Custom ``__reduce__`` ships ``(file_path, num_documents,
        error_context)``; ``__init__`` normalizes ``file_path`` to a
        ``Path``, which must survive as a ``Path``.
        """
        error = MultiDocumentError(
            file_path="config/flowgroups/multi.yaml",
            num_documents=3,
            error_context="flowgroup file config/flowgroups/multi.yaml",
        )
        outcome = FlowgroupOutcome.failure("pipe_a", "fg_a", lhp_error=error)

        restored = _round_trip(outcome)

        _assert_scaffold_preserved(restored, outcome)
        restored_error = restored.lhp_error
        assert type(restored_error) is MultiDocumentError
        assert isinstance(restored_error, LHPError)
        assert restored_error.code == "LHP-IO-003"
        assert restored_error.code_number == "003"
        assert restored_error.num_documents == 3
        assert restored_error.file_path == Path("config/flowgroups/multi.yaml")
        assert isinstance(restored_error.file_path, Path)
        assert (
            restored_error.error_context
            == "flowgroup file config/flowgroups/multi.yaml"
        )

    def test_failure_with_non_lhp_error_uses_string_channel(self):
        """Case 5 (canonical path): a non-LHP ``Exception`` is captured by
        the worker contract via the string ``errors`` channel, NOT
        ``lhp_error``. That outcome carries only value types, so it
        round-trips under whole-object ``==``.
        """
        message = "ValueError: bad worker input"
        outcome = FlowgroupOutcome.failure("pipe_a", "fg_a", errors=(message,))

        restored = _round_trip(outcome)

        assert restored == outcome
        _assert_scaffold_preserved(restored, outcome)
        assert restored.success is False
        assert restored.lhp_error is None
        assert restored.errors == (message,)

    def test_standalone_non_lhp_exception_object_still_pickles(self):
        """Case 5 (defensive): even though the canonical transport for a
        non-LHP failure is the string channel, a raw non-LHP exception
        object must itself survive a pickle round-trip — a stdlib
        ``Exception`` carries no un-picklable state.
        """
        original = ValueError("bad worker input")

        restored = _round_trip(original)

        assert type(restored) is ValueError
        assert restored.args == original.args

    def test_failure_with_no_channel_degrades_and_round_trips(self):
        """Case 6: ``failure()`` with neither channel set degrades to
        ``("unknown error",)`` (it must never raise) and round-trips by
        value."""
        outcome = FlowgroupOutcome.failure("pipe_a", "fg_a")

        # Sanity: the degrade rule fired before we even pickle.
        assert outcome.lhp_error is None
        assert outcome.errors == ("unknown error",)

        restored = _round_trip(outcome)

        assert restored == outcome
        _assert_scaffold_preserved(restored, outcome)
        assert restored.success is False
        assert restored.errors == ("unknown error",)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
