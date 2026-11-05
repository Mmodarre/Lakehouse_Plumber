"""Producer facts: what a write produces, and indexing from ``(table, action_id)``.

``index_produced_tables`` is the facts-based variant of
``build_producer_indexes`` (which now delegates to it), so a project-wide
check over facts collected per flowgroup sees exactly the indexes, and the
2-part uniqueness rule, that dependency analysis and the sandbox see.
"""

from __future__ import annotations

import pytest

from lhp.core.dependencies import (
    build_producer_indexes,
    index_produced_tables,
    is_replace_where_write,
    match_produced_table,
    produced_tables,
)
from lhp.models import Action, ActionType, FlowGroup

pytestmark = pytest.mark.unit


def _write(name: str, write_target: dict) -> Action:
    return Action(
        name=name, type=ActionType.WRITE, source="v", write_target=write_target
    )


_ST = {"type": "streaming_table", "catalog": "C", "schema": "s", "table": "t"}
_SINK = {
    "type": "sink",
    "sink_type": "delta",
    "sink_name": "x",
    "options": {"tableName": "s.sunk"},
}
_KAFKA = {"type": "sink", "sink_type": "kafka", "sink_name": "k", "options": {}}


def test_produced_tables_per_write_shape():
    assert produced_tables(_write("st", _ST)) == ("C.s.t",)
    assert produced_tables(_write("sink", _SINK)) == ("s.sunk",)
    assert produced_tables(_write("kafka", _KAFKA)) == ()
    load = Action(name="l", type=ActionType.LOAD, source={"type": "delta"}, target="v")
    assert produced_tables(load) == ()


def test_index_from_facts_matches_the_flowgroup_builder():
    fg_a = FlowGroup(pipeline="p", flowgroup="a", actions=[_write("st", _ST)])
    fg_b = FlowGroup(
        pipeline="q",
        flowgroup="b",
        actions=[_write("sink", _SINK), _write("st2", {**_ST, "catalog": "d"})],
    )
    entries = [
        (table, f"{fg.pipeline}.{fg.flowgroup}.{action.name}")
        for fg in (fg_a, fg_b)
        for action in fg.actions
        for table in produced_tables(action)
    ]

    assert index_produced_tables(entries) == build_producer_indexes([fg_a, fg_b])


def test_two_part_uniqueness_rule_holds_over_fact_indexes():
    one_catalog = index_produced_tables([("c.s.t", "p.f.a")])
    two_catalogs = index_produced_tables([("c.s.t", "p.f.a"), ("d.s.t", "p.f.b")])

    assert match_produced_table("s.t", *one_catalog) == "c.s.t"
    assert match_produced_table("s.t", *two_catalogs) is None


@pytest.mark.parametrize(
    ("write_target", "expected"),
    [
        ({**_ST, "mode": "replace_where"}, True),
        ({**_ST, "mode": "replace"}, False),
        (_ST, False),
        ({**_ST, "type": "materialized_view", "mode": "replace_where"}, False),
    ],
    ids=["replace_where", "replace", "standard", "materialized-view"],
)
def test_is_replace_where_write(write_target, expected):
    assert is_replace_where_write(_write("w", write_target)) is expected


def test_only_writes_are_replace_where_writes():
    load = Action(name="l", type=ActionType.LOAD, source={"type": "delta"}, target="v")
    assert is_replace_where_write(load) is False
