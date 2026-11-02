"""The combined-write rules code generation and ``effective_read_mode`` share.

Code generation renders the writes of a flowgroup that target one table as one
write built from the group's lead: the write that creates the table, else the
first in dependency order. A flow with no ``readMode`` reads with the lead's.
"""

from typing import Any, Optional

import pytest

from lhp.core.codegen.grouping import WriteActionGrouper
from lhp.core.dependencies import combined_write_lead, write_table_key
from lhp.core.dependencies._combined_writes import combined_flow
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit


def _write(
    name: str,
    *,
    source: str = "v",
    read_mode: Optional[str] = None,
    write_target: Optional[dict] = None,
    **fields: Any,
) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        readMode=read_mode,
        write_target=write_target
        if write_target is not None
        else {
            "type": "streaming_table",
            "catalog": "c",
            "schema": "s",
            "table": "t",
            "create_table": False,
        },
        **fields,
    )


class TestWriteTableKey:
    @pytest.mark.parametrize(
        ("write_target", "source", "expected"),
        [
            ({"catalog": "c", "schema": "s", "table": "t"}, "v", "c.s.t"),
            ({"catalog": "c", "schema": "s", "name": "n"}, "v", "c.s.n"),
            ({"schema": "s", "table": "t"}, "v", "t"),
            ({"type": "sink", "sink_type": "kafka"}, "v", "w"),
            ({}, {"catalog": "c", "schema": "s", "table": "src"}, "c.s.src"),
        ],
        ids=["three-part", "name", "table-only", "no-table", "source-mapping"],
    )
    def test_key(self, write_target: dict, source: Any, expected: str):
        action = Action(
            name="w", type=ActionType.WRITE, source=source, write_target=write_target
        )

        assert write_table_key(action) == expected

    def test_grouper_groups_by_the_key(self):
        writes = [
            _write("w_a"),
            _write("w_b", write_target={"type": "streaming_table", "table": "u"}),
            _write("w_c"),
        ]

        grouped = WriteActionGrouper().group_write_actions_by_target(writes)

        assert {key: [a.name for a in group] for key, group in grouped.items()} == {
            "c.s.t": ["w_a", "w_c"],
            "u": ["w_b"],
        }


class TestCombinedWriteLead:
    def test_the_creator_leads_wherever_it_is(self):
        creator = _write(
            "w_creator",
            write_target={
                "type": "streaming_table",
                "table": "t",
                "create_table": True,
            },
        )
        group = [_write("w_a"), creator, _write("w_b")]

        assert combined_write_lead(group) is creator

    def test_without_a_creator_the_first_leads(self):
        group = [_write("w_a"), _write("w_b")]

        assert combined_write_lead(group) is group[0]

    def test_grouper_builds_the_combined_write_from_the_lead(self):
        creator = _write(
            "w_creator",
            read_mode="batch",
            write_target={
                "type": "streaming_table",
                "table": "t",
                "create_table": True,
            },
        )
        group = [_write("w_a"), creator]

        combined = WriteActionGrouper().create_combined_write_action(group, "t")

        assert combined._table_creator is creator
        assert combined.name == "w_creator"
        assert combined.readMode == "batch"


class TestCombinedFlow:
    def test_flow_takes_the_leads_read_mode(self):
        lead = _write("w_lead", read_mode="batch", once=True)
        flow = _write("w_flow")

        assert combined_flow(flow, [lead, flow]).readMode == "batch"

    @pytest.mark.parametrize(
        "flow",
        [
            _write("w_flow", read_mode="stream"),
            _write("w_flow", write_target={"type": "streaming_table", "table": "u"}),
        ],
        ids=["own-readMode", "own-table"],
    )
    def test_flow_keeps_its_own_read(self, flow: Action):
        lead = _write("w_lead", read_mode="batch", once=True)

        assert combined_flow(flow, [lead, flow]) is flow

    def test_lead_without_read_mode_changes_nothing(self):
        lead = _write("w_lead")
        flow = _write("w_flow")

        assert combined_flow(flow, [lead, flow]) is flow

    def test_lead_is_first_in_dependency_order(self):
        # w_backfill is listed first, but its source is one level deeper, so
        # w_flow resolves first and leads.
        transform = Action(
            name="t_hist2",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.PYTHON,
            source="v_hist",
            target="v_hist2",
        )
        backfill = _write("w_backfill", source="v_hist2", read_mode="batch", once=True)
        flow = _write("w_flow", source="v_s1")

        assert combined_flow(flow, [transform, backfill, flow]) is flow

    def test_cycle_is_unknown(self):
        loop = [
            Action(
                name=f"t_{a}",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.PYTHON,
                source=f"v_{b}",
                target=f"v_{a}",
            )
            for a, b in (("a", "b"), ("b", "a"))
        ]
        backfill = _write("w_backfill", source="v_a", read_mode="batch", once=True)
        flow = _write("w_flow", source="v_b")

        assert combined_flow(flow, [*loop, backfill, flow]) is None

    def test_non_write_is_unchanged(self):
        action = Action(
            name="t",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.PYTHON,
            source="v",
            target="v2",
        )

        assert combined_flow(action, [action]) is action
