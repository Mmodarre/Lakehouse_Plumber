"""``LoadActionValidator`` read-mode rules, checked at validate time.

Auto Loader and Kafka loads always read with ``spark.readStream``, so any other
``readMode`` (action-level or the nested ``source.readMode``) is
``LHP-VAL-068``. A Delta stream read has no end, so ``endingVersion`` /
``endingTimestamp`` with ``readMode: stream`` is ``LHP-VAL-013``.
"""

from __future__ import annotations

from typing import Any, List

import pytest

from lhp.core.registry import ActionRegistry
from lhp.core.validators import ConfigFieldValidator, LoadActionValidator
from lhp.errors import LHPError
from lhp.models import Action, ActionType

pytestmark = pytest.mark.unit


def _errors(source: dict, read_mode: str | None = None) -> List[Any]:
    action = Action(
        name="load_it",
        type=ActionType.LOAD,
        source=source,
        target="v_out",
        readMode=read_mode,
    )
    return LoadActionValidator(ActionRegistry(), ConfigFieldValidator()).validate(
        action, "Action[0] 'load_it'"
    )


def _lhp_errors(errors: List[Any]) -> List[LHPError]:
    return [e for e in errors if isinstance(e, LHPError)]


_CLOUDFILES = {"type": "cloudfiles", "path": "/data/in", "format": "json"}
_KAFKA = {"type": "kafka", "bootstrap_servers": "b:9092", "subscribe": "topic"}
_DELTA = {"type": "delta", "catalog": "c", "schema": "s", "table": "t"}


class TestStreamOnlyLoads:
    @pytest.mark.parametrize(
        ("source", "action_type"), [(_CLOUDFILES, "cloudfiles"), (_KAFKA, "kafka")]
    )
    @pytest.mark.parametrize("where", ["action", "source"])
    def test_batch_is_val_068(self, source: dict, action_type: str, where: str) -> None:
        if where == "action":
            errors = _errors(source, read_mode="batch")
        else:
            errors = _errors({**source, "readMode": "batch"})

        [error] = _lhp_errors(errors)
        assert error.code == "LHP-VAL-068"
        assert error.title == "Invalid readMode 'batch' in action 'load_it'"
        assert error.context["Action Type"] == action_type
        assert "Set readMode to one of: 'stream'" in error.suggestions
        assert not any("batch" in suggestion for suggestion in error.suggestions)

    @pytest.mark.parametrize("source", [_CLOUDFILES, _KAFKA])
    @pytest.mark.parametrize("read_mode", [None, "stream"])
    def test_stream_or_unset_is_valid(self, source: dict, read_mode: str) -> None:
        assert _errors(source, read_mode=read_mode) == []

    def test_action_read_mode_wins_over_source(self) -> None:
        errors = _errors({**_CLOUDFILES, "readMode": "batch"}, read_mode="stream")
        assert errors == []


class TestDeltaStreamBounds:
    @pytest.mark.parametrize("bound", ["endingVersion", "endingTimestamp"])
    @pytest.mark.parametrize("where", ["action", "source"])
    def test_ending_bound_on_stream_read_is_val_013(
        self, bound: str, where: str
    ) -> None:
        source = {**_DELTA, "options": {"readChangeFeed": "true", bound: "20"}}
        if where == "action":
            errors = _errors(source, read_mode="stream")
        else:
            errors = _errors({**source, "readMode": "stream"})

        [error] = _lhp_errors(errors)
        assert error.code == "LHP-VAL-013"
        assert error.context["Option A"] == bound
        assert error.context["Option B"] == "readMode: stream"
        assert f"{bound} is only supported in batch mode." in error.details
        assert (
            f"Use readMode: batch with {bound}, or remove {bound} for streaming"
            in error.suggestions
        )

    def test_both_bounds_report_both(self) -> None:
        source = {
            **_DELTA,
            "options": {"endingVersion": "20", "endingTimestamp": "2024-01-01"},
        }
        errors = _lhp_errors(_errors(source, read_mode="stream"))
        assert [e.context["Option A"] for e in errors] == [
            "endingVersion",
            "endingTimestamp",
        ]

    @pytest.mark.parametrize("read_mode", [None, "batch"])
    def test_ending_bound_on_batch_read_is_valid(self, read_mode: str) -> None:
        source = {
            **_DELTA,
            "options": {
                "readChangeFeed": "true",
                "startingVersion": "1",
                "endingVersion": "20",
            },
        }
        assert _errors(source, read_mode=read_mode) == []

    def test_stream_read_without_bound_is_valid(self) -> None:
        source = {**_DELTA, "options": {"readChangeFeed": "true"}}
        assert _errors(source, read_mode="stream") == []
