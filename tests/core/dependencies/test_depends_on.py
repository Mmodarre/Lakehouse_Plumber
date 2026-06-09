"""Acceptance tests: the ``depends_on`` escape-hatch wires edges.

These exercise the additive ``action.depends_on`` union in
:meth:`SourceParser.extract_action_sources` by building a real action graph
over in-memory flowgroups. An action that declares ``depends_on`` but has no
parseable SQL/Python source must still form an INTERNAL edge to a producer of
the declared table (matched through the canonical edge-matcher), while a
``depends_on`` entry with no producer stays external.

Covered:

- ``depends_on`` with NO parseable source -> internal edge A->B,
- ``depends_on`` is canonicalized (case-variant / backtick entry still matches),
- ``depends_on`` is ADDITIVE (unioned on top of a parseable source),
- ``depends_on`` with no producer stays external (no phantom edge),
- ``depends_on`` is unset/empty -> no extra edges (regression guard).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.core.dependencies.builder import DependencyGraphBuilder
from lhp.models import Action, ActionType, FlowGroup


def _producer_fg(pipeline: str, catalog: str, schema: str, table: str) -> FlowGroup:
    """A flowgroup that produces ``catalog.schema.table`` via a WRITE action."""
    return FlowGroup(
        pipeline=pipeline,
        flowgroup="producer_fg",
        actions=[
            Action(
                name="prep",
                type=ActionType.TRANSFORM,
                source="raw.src",
                target="v_prep",
            ),
            Action(
                name="write_a",
                type=ActionType.WRITE,
                source="v_prep",
                write_target={
                    "type": "streaming_table",
                    "catalog": catalog,
                    "schema": schema,
                    "table": table,
                },
            ),
        ],
    )


def _build(flowgroups: list[FlowGroup]):
    builder = DependencyGraphBuilder(project_root=Path("/tmp/nonexistent_lhp_root"))
    return builder.build_from_flowgroups(flowgroups, file_paths={}).action_graph


@pytest.mark.unit
class TestDependsOnEscapeHatch:
    def test_depends_on_with_no_parseable_source_forms_internal_edge(self):
        """B has NO source, only ``depends_on: [cat.sch.a]`` -> internal edge A->B."""
        producer = _producer_fg("p1", "cat", "sch", "a")
        consumer = FlowGroup(
            pipeline="p2",
            flowgroup="consumer_fg",
            actions=[
                Action(
                    name="b",
                    type=ActionType.TRANSFORM,
                    target="v_b",
                    depends_on=["cat.sch.a"],
                )
            ],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge("producer_fg.write_a", "consumer_fg.b")
        assert (
            graph.get_edge_data("producer_fg.write_a", "consumer_fg.b")[
                "dependency_type"
            ]
            == "internal"
        )
        assert "external_sources" not in graph.nodes["consumer_fg.b"]

    def test_depends_on_entry_is_canonicalized(self):
        """A case-variant / backtick-quoted ``depends_on`` entry still matches."""
        producer = _producer_fg("p1", "cat", "sch", "a")
        consumer = FlowGroup(
            pipeline="p2",
            flowgroup="consumer_fg",
            actions=[
                Action(
                    name="b",
                    type=ActionType.TRANSFORM,
                    target="v_b",
                    depends_on=["`Cat`.`Sch`.`A`"],
                )
            ],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge("producer_fg.write_a", "consumer_fg.b")

    def test_depends_on_is_additive_to_parseable_source(self):
        """``depends_on`` adds an edge ON TOP of the action's parseable source."""
        # producer_fg writes cat.sch.a; a second producer writes a view consumed
        # by B's SQL source. B must end up with BOTH edges.
        producer = _producer_fg("p1", "cat", "sch", "a")
        view_producer = FlowGroup(
            pipeline="p2",
            flowgroup="view_fg",
            actions=[
                Action(
                    name="make_view",
                    type=ActionType.TRANSFORM,
                    source="raw.y",
                    target="v_upstream",
                )
            ],
        )
        consumer = FlowGroup(
            pipeline="p2",
            flowgroup="consumer_fg",
            actions=[
                Action(
                    name="b",
                    type=ActionType.TRANSFORM,
                    sql="SELECT * FROM v_upstream",
                    target="v_b",
                    depends_on=["cat.sch.a"],
                )
            ],
        )

        graph = _build([producer, view_producer, consumer])

        # parseable source edge (view, same pipeline)
        assert graph.has_edge("view_fg.make_view", "consumer_fg.b")
        # depends_on edge (global table)
        assert graph.has_edge("producer_fg.write_a", "consumer_fg.b")

    def test_depends_on_with_no_producer_stays_external(self):
        """A ``depends_on`` table with no producer is external, not a phantom edge."""
        consumer = FlowGroup(
            pipeline="p1",
            flowgroup="consumer_fg",
            actions=[
                Action(
                    name="b",
                    type=ActionType.TRANSFORM,
                    target="v_b",
                    depends_on=["nocat.nosch.notable"],
                )
            ],
        )

        graph = _build([consumer])

        assert graph.nodes["consumer_fg.b"].get("external_sources") == [
            "nocat.nosch.notable"
        ]

    def test_unset_depends_on_adds_no_edges(self):
        """``depends_on`` unset (None) leaves the parsed sources unchanged."""
        producer = _producer_fg("p1", "cat", "sch", "a")
        consumer = FlowGroup(
            pipeline="p2",
            flowgroup="consumer_fg",
            actions=[Action(name="b", type=ActionType.TRANSFORM, target="v_b")],
        )

        graph = _build([producer, consumer])

        assert not graph.has_edge("producer_fg.write_a", "consumer_fg.b")
        assert "external_sources" not in graph.nodes["consumer_fg.b"]
