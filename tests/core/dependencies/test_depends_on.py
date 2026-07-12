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
- ``depends_on`` is unset/empty -> no extra edges (regression guard),
- a non-empty ``depends_on`` SUPPRESSES the action's extraction
  advisories (the user has taken manual control) while still forming
  its edges; an identical action without it keeps the advisory.
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

        assert graph.has_edge("p1.producer_fg.write_a", "p2.consumer_fg.b")
        assert (
            graph.get_edge_data("p1.producer_fg.write_a", "p2.consumer_fg.b")[
                "dependency_type"
            ]
            == "internal"
        )
        assert "external_sources" not in graph.nodes["p2.consumer_fg.b"]

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

        assert graph.has_edge("p1.producer_fg.write_a", "p2.consumer_fg.b")

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
        assert graph.has_edge("p2.view_fg.make_view", "p2.consumer_fg.b")
        # depends_on edge (global table)
        assert graph.has_edge("p1.producer_fg.write_a", "p2.consumer_fg.b")

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

        assert graph.nodes["p1.consumer_fg.b"].get("external_sources") == [
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

        assert not graph.has_edge("p1.producer_fg.write_a", "p2.consumer_fg.b")
        assert "external_sources" not in graph.nodes["p2.consumer_fg.b"]


@pytest.mark.unit
class TestDependsOnSuppression:
    """Non-empty ``depends_on`` suppresses LHP-DEP-002/003 for that action."""

    _OPAQUE_MODULE = (
        "def do_it(df, spark, parameters):\n    return spark.read.table(helper())\n"
    )

    def _consumer(self, depends_on: list[str] | None) -> FlowGroup:
        return FlowGroup(
            pipeline="p2",
            flowgroup="consumer_fg",
            actions=[
                Action(
                    name="opaque_act",
                    type=ActionType.TRANSFORM,
                    transform_type="python",
                    module_path="transforms/opaque.py",
                    function_name="do_it",
                    source="v_in",
                    target="v_out",
                    depends_on=depends_on,
                )
            ],
        )

    def _build_graphs(self, tmp_path, depends_on: list[str] | None):
        module = tmp_path / "transforms/opaque.py"
        module.parent.mkdir(parents=True, exist_ok=True)
        module.write_text(self._OPAQUE_MODULE)
        builder = DependencyGraphBuilder(project_root=tmp_path)
        return builder.build_from_flowgroups(
            [_producer_fg("p1", "cat", "sch", "orders"), self._consumer(depends_on)],
            file_paths={},
        )

    def test_depends_on_suppresses_advisory_and_still_forms_edge(self, tmp_path):
        graphs = self._build_graphs(tmp_path, depends_on=["cat.sch.orders"])

        # The declared upstream forms the INTERNAL edge...
        assert graphs.action_graph.has_edge(
            "p1.producer_fg.write_a", "p2.consumer_fg.opaque_act"
        )
        # ...and the action's opaque-read advisory is suppressed.
        assert graphs.extraction_warnings == []

    def test_without_depends_on_the_advisory_remains(self, tmp_path):
        graphs = self._build_graphs(tmp_path, depends_on=None)

        [warning] = graphs.extraction_warnings
        assert warning.code == "LHP-DEP-002"
        assert (warning.flowgroup, warning.action) == ("consumer_fg", "opaque_act")


@pytest.mark.unit
class TestTrustDependsOn:
    """Opt-in trust mode: a non-empty depends_on skips body extraction."""

    def _consumer_with_missing_sql(self) -> FlowGroup:
        return FlowGroup(
            pipeline="p2",
            flowgroup="consumer_fg",
            actions=[
                Action(
                    name="b",
                    type=ActionType.TRANSFORM,
                    sql_path="does/not/exist.sql",
                    target="v_b",
                    depends_on=["cat.sch.a"],
                )
            ],
        )

    def test_trust_skips_body_extraction_entirely(self):
        # The sql_path does not exist: default mode raises LHP-IO-002 while
        # resolving it; trust mode never touches the file and still forms
        # the declared edge.
        producer = _producer_fg("p1", "cat", "sch", "a")
        consumer = self._consumer_with_missing_sql()
        builder = DependencyGraphBuilder(project_root=Path("/tmp/nonexistent_lhp_root"))
        graph = builder.build_from_flowgroups(
            [producer, consumer], file_paths={}, trust_depends_on=True
        ).action_graph
        assert graph.has_edge("p1.producer_fg.write_a", "p2.consumer_fg.b")

    def test_default_mode_still_extracts_bodies(self):
        from lhp.errors import LHPError

        producer = _producer_fg("p1", "cat", "sch", "a")
        consumer = self._consumer_with_missing_sql()
        builder = DependencyGraphBuilder(project_root=Path("/tmp/nonexistent_lhp_root"))
        with pytest.raises(LHPError):
            builder.build_from_flowgroups([producer, consumer], file_paths={})

    def test_trust_without_depends_on_extracts_normally(self):
        # An action with NO depends_on is unaffected by trust mode: its
        # explicit source still resolves to an internal edge.
        producer = _producer_fg("p1", "cat", "sch", "a")
        consumer = FlowGroup(
            pipeline="p2",
            flowgroup="consumer_fg",
            actions=[
                Action(
                    name="b",
                    type=ActionType.TRANSFORM,
                    source="cat.sch.a",
                    target="v_b",
                )
            ],
        )
        builder = DependencyGraphBuilder(project_root=Path("/tmp/nonexistent_lhp_root"))
        graph = builder.build_from_flowgroups(
            [producer, consumer], file_paths={}, trust_depends_on=True
        ).action_graph
        assert graph.has_edge("p1.producer_fg.write_a", "p2.consumer_fg.b")

    def test_trust_unions_explicit_source_with_depends_on(self):
        producer_a = _producer_fg("p1", "cat", "sch", "a")
        consumer = FlowGroup(
            pipeline="p2",
            flowgroup="consumer_fg",
            actions=[
                Action(
                    name="b",
                    type=ActionType.TRANSFORM,
                    source="cat.sch.a",
                    target="v_b",
                    depends_on=["cat.sch.other"],
                )
            ],
        )
        builder = DependencyGraphBuilder(project_root=Path("/tmp/nonexistent_lhp_root"))
        graph = builder.build_from_flowgroups(
            [producer_a, consumer], file_paths={}, trust_depends_on=True
        ).action_graph
        assert graph.has_edge("p1.producer_fg.write_a", "p2.consumer_fg.b")
        externals = graph.nodes["p2.consumer_fg.b"].get("external_sources", [])
        assert "cat.sch.other" in externals


@pytest.mark.unit
class TestViewMatchCanonicalization:
    """Pipeline-scoped view matching is case/backtick-insensitive."""

    def test_depends_on_matches_case_variant_view_target(self):
        fg = FlowGroup(
            pipeline="p1",
            flowgroup="fg",
            actions=[
                Action(
                    name="prep",
                    type=ActionType.TRANSFORM,
                    source="raw.src",
                    target="Raw_Events",
                ),
                Action(
                    name="consume",
                    type=ActionType.TRANSFORM,
                    target="v_out",
                    depends_on=["Raw_Events"],
                ),
            ],
        )
        graph = _build([fg])
        assert graph.has_edge("p1.fg.prep", "p1.fg.consume")

    def test_parsed_case_variant_source_matches_view(self):
        fg = FlowGroup(
            pipeline="p1",
            flowgroup="fg",
            actions=[
                Action(
                    name="prep",
                    type=ActionType.TRANSFORM,
                    source="raw.src",
                    target="raw_events",
                ),
                Action(
                    name="consume",
                    type=ActionType.TRANSFORM,
                    source="RAW_EVENTS",
                    target="v_out",
                ),
            ],
        )
        graph = _build([fg])
        assert graph.has_edge("p1.fg.prep", "p1.fg.consume")

    def test_same_named_view_in_sibling_pipeline_stays_scoped(self):
        producer_p1 = FlowGroup(
            pipeline="p1",
            flowgroup="fg1",
            actions=[
                Action(
                    name="prep",
                    type=ActionType.TRANSFORM,
                    source="raw.src",
                    target="shared_view",
                ),
            ],
        )
        consumer_p2 = FlowGroup(
            pipeline="p2",
            flowgroup="fg2",
            actions=[
                Action(
                    name="consume",
                    type=ActionType.TRANSFORM,
                    source="shared_view",
                    target="v_out",
                ),
            ],
        )
        graph = _build([producer_p1, consumer_p2])
        assert not graph.has_edge("p1.fg1.prep", "p2.fg2.consume")
