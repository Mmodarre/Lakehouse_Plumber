"""``validate_data_quality_transform`` owns the data_quality ``readMode`` rules.

Issue #310: ``readMode: batch`` passed ``lhp validate`` and then failed in
``lhp generate``, where the only check lived. A ``dqe`` action reads a stream
or a batch; a ``quarantine`` action reads only a stream, because its
dead-letter recycling is a streaming flow. Any other value is LHP-VAL-068.
"""

from typing import Optional

import pytest

from lhp.core.validators.action._dq_transform import validate_data_quality_transform
from lhp.errors import LHPError
from lhp.models import Action

pytestmark = pytest.mark.unit

_PREFIX = "Action[1] 'dq'"


def _dq(read_mode: Optional[str], mode: Optional[str] = None) -> Action:
    quarantine = (
        {"dlq_table": "c.s.dlq", "source_table": "c.s.src"}
        if mode == "quarantine"
        else None
    )
    return Action(
        name="dq",
        type="transform",
        transform_type="data_quality",
        source="v_candidates",
        target="v_candidates_dq",
        expectations_file="expectations/rules.yaml",
        readMode=read_mode,
        mode=mode,
        quarantine=quarantine,
    )


def _read_mode_errors(action: Action) -> list:
    # No project root: the expectations file is not read.
    return [
        error
        for error in validate_data_quality_transform(action, _PREFIX, None)
        if isinstance(error, LHPError) and error.code == "LHP-VAL-068"
    ]


@pytest.mark.parametrize("mode", [None, "dqe"], ids=["default", "dqe"])
@pytest.mark.parametrize("read_mode", [None, "stream", "batch"])
def test_dqe_reads_stream_or_batch(mode: Optional[str], read_mode: Optional[str]):
    action = _dq(read_mode, mode)

    assert validate_data_quality_transform(action, _PREFIX, None) == []


@pytest.mark.parametrize("read_mode", [None, "stream"])
def test_quarantine_reads_a_stream(read_mode: Optional[str]):
    action = _dq(read_mode, "quarantine")

    assert validate_data_quality_transform(action, _PREFIX, None) == []


def test_quarantine_batch_is_val_068():
    [error] = validate_data_quality_transform(_dq("batch", "quarantine"), _PREFIX, None)

    assert isinstance(error, LHPError)
    assert error.code == "LHP-VAL-068"
    assert error.title == "Invalid readMode 'batch' in action 'dq'"
    assert error.context["Action Type"] == "data_quality"
    assert "Valid modes: 'stream'" in error.details
    assert error.suggestions[0] == "Set readMode to one of: 'stream'"
    assert not any("batch" in suggestion for suggestion in error.suggestions)


@pytest.mark.parametrize(
    ("mode", "valid"),
    [
        (None, "'stream', 'batch'"),
        ("dqe", "'stream', 'batch'"),
        ("quarantine", "'stream'"),
    ],
    ids=["default", "dqe", "quarantine"],
)
@pytest.mark.parametrize("read_mode", ["Stream", "micro_batch"])
def test_unknown_read_mode_is_val_068(mode: Optional[str], valid: str, read_mode: str):
    [error] = _read_mode_errors(_dq(read_mode, mode))

    assert error.context["Provided"] == read_mode
    assert f"Valid modes: {valid}" in error.details
    assert error.suggestions[0] == f"Set readMode to one of: {valid}"


def test_read_mode_error_sits_beside_the_other_errors():
    # An invalid mode still reports its own error; the readMode check is
    # one of the action's errors, not a short circuit.
    errors = validate_data_quality_transform(_dq("bogus", "fast"), _PREFIX, None)

    assert [e for e in errors if isinstance(e, str)] == [
        f"{_PREFIX}: Data quality transform 'mode' must be 'dqe' or 'quarantine', "
        "got 'fast'"
    ]
    assert [e.code for e in errors if isinstance(e, LHPError)] == ["LHP-VAL-068"]
