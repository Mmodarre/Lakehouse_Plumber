"""An invalid ``readMode`` has its own error code (issue #299).

``LHP-VAL-007`` used to mean both "FlowGroup validation failed" (the wrapper
around the list of configuration errors) and "invalid readMode". The readMode
error now reports ``LHP-VAL-068``, and its advice names only the modes the
action accepts: the cloudfiles, kafka and data_quality generators accept only
``stream``, yet the old suggestions told users to "Use 'batch' for batch reads".
"""

import pytest

from lhp.errors import LHPError, codes
from lhp.errors.factory import ErrorFactory
from lhp.generators.load.cloudfiles import CloudFilesLoadGenerator
from lhp.generators.load.kafka import KafkaLoadGenerator
from lhp.generators.transform.data_quality import DataQualityTransformGenerator
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit


def test_val_068_is_registered():
    assert codes.VAL_068.code == "LHP-VAL-068"
    assert codes.VAL_068 in codes.ALL_CODES
    assert "VAL_068" in codes.__all__


def test_factory_reports_val_068():
    error = ErrorFactory.invalid_read_mode("my_action", "kafka", "batch", ["stream"])

    assert error.code == "LHP-VAL-068"
    assert error.title == "Invalid readMode 'batch' in action 'my_action'"


def test_stream_only_advice_never_suggests_batch():
    error = ErrorFactory.invalid_read_mode("my_action", "kafka", "batch", ["stream"])

    assert error.suggestions
    assert not any("batch" in suggestion for suggestion in error.suggestions)
    assert "Set readMode to one of: 'stream'" in error.suggestions


def test_advice_covers_batch_only_when_batch_is_valid():
    error = ErrorFactory.invalid_read_mode(
        "my_action", "load", "bogus", ["stream", "batch"]
    )

    assert "Set readMode to one of: 'stream', 'batch'" in error.suggestions
    assert any("spark.read)" in suggestion for suggestion in error.suggestions)


def _cloudfiles_batch():
    return CloudFilesLoadGenerator(), Action(
        name="cf_batch",
        type=ActionType.LOAD,
        target="v_data",
        readMode="batch",
        source={"type": "cloudfiles", "path": "/data/in", "format": "json"},
    )


def _kafka_batch():
    return KafkaLoadGenerator(), Action(
        name="kafka_batch",
        type=ActionType.LOAD,
        target="v_data",
        readMode="batch",
        source={
            "type": "kafka",
            "bootstrap_servers": "localhost:9092",
            "subscribe": "events",
        },
    )


def _data_quality_batch():
    return DataQualityTransformGenerator(), Action(
        name="dq_batch",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.DATA_QUALITY,
        source="v_source",
        target="v_validated",
        readMode="batch",
        expectations_file="expectations/q.yaml",
    )


@pytest.mark.parametrize(
    "build",
    [_cloudfiles_batch, _kafka_batch, _data_quality_batch],
    ids=["cloudfiles", "kafka", "data_quality"],
)
def test_stream_only_generators_raise_val_068(build):
    generator, action = build()

    with pytest.raises(LHPError) as excinfo:
        generator.generate(action, {})

    assert excinfo.value.code == "LHP-VAL-068"
    assert not any("batch" in suggestion for suggestion in excinfo.value.suggestions)
