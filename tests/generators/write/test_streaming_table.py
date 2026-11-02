"""The streaming-table generator's mode invariant (#345).

``WriteActionValidator`` rejects an unknown ``write_target.mode`` (LHP-VAL-006)
before generation, so the generator only asserts it, against the same
``STREAMING_TABLE_MODES`` set.
"""

import pytest

from lhp.core.validators import STREAMING_TABLE_MODES
from lhp.core.validators.action import write as write_validator
from lhp.generators.write import streaming_table
from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models import Action

pytestmark = pytest.mark.unit


def _action(**write_target) -> Action:
    return Action(
        name="write_customer",
        type="write",
        source="v_customer",
        write_target={
            "type": "streaming_table",
            "catalog": "main",
            "schema": "silver",
            "table": "customer",
            **write_target,
        },
    )


def test_generator_and_validator_share_one_mode_set():
    assert streaming_table.STREAMING_TABLE_MODES is STREAMING_TABLE_MODES
    assert write_validator.STREAMING_TABLE_MODES is STREAMING_TABLE_MODES


def test_unvalidated_mode_is_an_internal_error():
    with pytest.raises(AssertionError, match="'cdcc'"):
        StreamingTableWriteGenerator().generate(_action(mode="cdcc"), {})


@pytest.mark.parametrize("write_target", [{}, {"mode": "standard"}])
def test_standard_mode_generates_an_append_flow(write_target):
    code = StreamingTableWriteGenerator().generate(_action(**write_target), {})

    assert "@dp.append_flow(" in code
