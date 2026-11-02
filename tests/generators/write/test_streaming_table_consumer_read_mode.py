"""``StreamingTableWriteGenerator.consumer_read_mode``: the flow's read, by mode.

A streaming table's flow reads its source view with ``spark.readStream`` or
``spark.read`` depending on ``write_target.mode``; ``None`` marks a read that
is not certain (a ``once`` CDC flow) or no view read at all (``snapshot_cdc``).
"""

from __future__ import annotations

import pytest

from lhp.generators.write import StreamingTableWriteGenerator
from lhp.models import Action, ActionType

pytestmark = pytest.mark.unit


def _write(
    mode: str | None, read_mode: str | None = None, once: bool | None = None
) -> Action:
    target = {"type": "streaming_table", "catalog": "c", "schema": "s", "table": "t"}
    if mode is not None:
        target["mode"] = mode
    return Action(
        name="w",
        type=ActionType.WRITE,
        source="v",
        write_target=target,
        readMode=read_mode,
        once=once,
    )


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (_write(None), "stream"),
        (_write("standard"), "stream"),
        (_write("standard", "batch"), "batch"),
        (_write("standard", "batch", once=True), "batch"),
        (_write("standard", "stream"), "stream"),
        (_write("replace"), "stream"),
        (_write("replace", "batch"), "stream"),
        (_write("cdc"), "stream"),
        (_write("cdc", "batch"), "stream"),
        (_write("cdc", once=True), None),
        (_write("cdc", once=False), "stream"),
        (_write("snapshot_cdc"), None),
        (_write("not_a_mode"), None),
    ],
    ids=[
        "no-mode-is-standard",
        "standard-default-stream",
        "standard-readMode-batch",
        "standard-once-batch",
        "standard-readMode-stream",
        "replace-always-stream",
        "replace-ignores-readMode",
        "cdc-stream",
        "cdc-ignores-readMode",
        "cdc-once-not-certain",
        "cdc-once-false",
        "snapshot_cdc-no-view-read",
        "unvalidated-mode",
    ],
)
def test_consumer_read_mode(action: Action, expected: str | None) -> None:
    assert StreamingTableWriteGenerator.consumer_read_mode(action) == expected


def test_read_mode_is_the_rendered_fallback() -> None:
    # The template's flow falls back to this value when a flow sets no
    # readMode of its own.
    assert StreamingTableWriteGenerator.read_mode(_write("standard")) == "stream"
    assert StreamingTableWriteGenerator.read_mode(_write("cdc", "batch")) == "batch"
