"""The data_quality generator reads with the effective read mode it is given.

Code generation passes ``effective_read_mode`` in the context (a dqe action
with no ``readMode`` follows its source view's kind, #344); the generator
renders ``spark.read`` for ``batch`` and ``spark.readStream`` for ``stream``.
Quarantine mode always reads a stream. A ``readMode`` validation rejects is
an internal invariant here, not a user-facing error (#310).
"""

from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from lhp.generators.transform import DataQualityTransformGenerator
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit

_STREAM = 'df = spark.readStream.table("v_source")'
_BATCH = 'df = spark.read.table("v_source")'


@pytest.fixture
def project(tmp_path: Path) -> Path:
    (tmp_path / "rules.yaml").write_text('{"id IS NOT NULL": {"action": "drop"}}')
    return tmp_path


def _dq(read_mode: Optional[str] = None, **fields: Any) -> Action:
    return Action(
        name="dq",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.DATA_QUALITY,
        source="v_source",
        target="v_validated",
        readMode=read_mode,
        expectations_file="rules.yaml",
        **fields,
    )


def _context(project: Path, effective_read_mode: Optional[str]) -> Dict[str, Any]:
    return {"spec_dir": project, "effective_read_mode": effective_read_mode}


class TestDqeRead:
    @pytest.mark.parametrize(
        ("effective", "expected", "absent"),
        [("batch", _BATCH, "readStream"), ("stream", _STREAM, "spark.read.")],
    )
    def test_context_read_mode_selects_the_read(
        self, project: Path, effective: str, expected: str, absent: str
    ):
        code = DataQualityTransformGenerator().generate(
            _dq(), _context(project, effective)
        )

        assert expected in code
        assert absent not in code

    @pytest.mark.parametrize(
        ("read_mode", "expected"),
        [(None, _STREAM), ("stream", _STREAM), ("batch", _BATCH)],
        ids=["default", "stream", "batch"],
    )
    def test_without_context_value_reads_what_the_action_asks(
        self, project: Path, read_mode: Optional[str], expected: str
    ):
        # A generator called on its own has no flowgroup to inherit from.
        for context in ({"spec_dir": project}, _context(project, None)):
            code = DataQualityTransformGenerator().generate(_dq(read_mode), context)

            assert expected in code

    def test_explicit_batch_with_batch_context(self, project: Path):
        code = DataQualityTransformGenerator().generate(
            _dq("batch", mode="dqe"), _context(project, "batch")
        )

        assert _BATCH in code
        assert '@dp.expect_all_or_drop({"id IS NOT NULL": "id IS NOT NULL"})' in code


class TestQuarantineRead:
    def _quarantine(self, read_mode: Optional[str] = None) -> Action:
        return _dq(
            read_mode,
            mode="quarantine",
            quarantine={"dlq_table": "c.s.dlq", "source_table": "c.s.src"},
        )

    def test_always_reads_a_stream(self, project: Path):
        code = DataQualityTransformGenerator().generate(
            self._quarantine(), _context(project, "stream")
        )

        assert "spark.readStream.table" in code
        assert "spark.read.table" not in code

    def test_batch_is_an_invariant_violation(self, project: Path):
        with pytest.raises(
            AssertionError,
            match=r"Unvalidated data_quality readMode 'batch' in action 'dq'",
        ):
            DataQualityTransformGenerator().generate(
                self._quarantine("batch"), _context(project, "stream")
            )

    def test_consumer_read_mode_is_stream(self):
        generator = DataQualityTransformGenerator
        assert generator.consumer_read_mode(self._quarantine()) == "stream"
        assert generator.consumer_read_mode(self._quarantine("batch")) == "stream"


@pytest.mark.parametrize("read_mode", ["Stream", "micro_batch"])
def test_unknown_read_mode_is_an_invariant_violation(project: Path, read_mode: str):
    with pytest.raises(AssertionError, match=f"readMode '{read_mode}'"):
        DataQualityTransformGenerator().generate(
            _dq(read_mode), _context(project, None)
        )


@pytest.mark.parametrize(
    ("read_mode", "expected"),
    [(None, "stream"), ("stream", "stream"), ("batch", "batch"), ("Stream", None)],
)
def test_dqe_consumer_read_mode(read_mode: Optional[str], expected: Optional[str]):
    # The generator's own declaration; inheritance from the source view is
    # lhp.core.dependencies.effective_read_mode's, over the flowgroup.
    assert DataQualityTransformGenerator.consumer_read_mode(_dq(read_mode)) == expected
