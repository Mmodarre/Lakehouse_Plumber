"""Acceptance tests: canonicalized table producer/consumer matching.

These exercise the wired ``_canonical`` seam at BOTH choke points of
:meth:`DependencyGraphBuilder._build_action_graph` (producer registration +
edge-match lookup) by building a real action graph over in-memory flowgroups.

Covered:

- case-variant 3-part match (``MyCat.MySchema.MyTable`` vs ``mycat...``),
- backtick-quoted 3-part match,
- 2-part source -> unique-catalog 3-part producer match,
- AMBIGUOUS 2-part source against two distinct catalogs -> NO edge,
- view scoping is NOT canonicalized / globalized (regression guard).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.core.dependencies.builder import DependencyGraphBuilder
from lhp.models import Action, ActionType, FlowGroup


def _write_action(name: str, source: str, catalog: str, schema: str, table: str):
    """A WRITE action that reads `source` and produces catalog.schema.table."""
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "streaming_table",
            "catalog": catalog,
            "schema": schema,
            "table": table,
        },
    )


def _transform_reader(name: str, source: str, target: str):
    """A TRANSFORM action reading `source` and producing a (view) `target`."""
    return Action(name=name, type=ActionType.TRANSFORM, source=source, target=target)


def _flowgroup(name: str, pipeline: str, actions: list) -> FlowGroup:
    return FlowGroup(pipeline=pipeline, flowgroup=name, actions=actions)


def _build(flowgroups: list[FlowGroup]):
    builder = DependencyGraphBuilder(project_root=Path("/tmp/nonexistent_lhp_root"))
    return builder.build_from_flowgroups(flowgroups, file_paths={}).action_graph


@pytest.mark.unit
class TestCanonicalTableMatching:
    def test_case_variant_three_part_match(self):
        """Producer `MyCat.MySchema.MyTable`, reader `mycat.myschema.mytable`."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _write_action("write_t", "v_prep", "MyCat", "MySchema", "MyTable"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_t", "mycat.myschema.mytable", "v_out")],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge("p1.producer_fg.write_t", "p2.consumer_fg.read_t")
        assert "external_sources" not in graph.nodes["p2.consumer_fg.read_t"]

    def test_backtick_quoted_match(self):
        """Reader `` `mycat`.`myschema`.`mytable` `` matches a plain producer."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _write_action("write_t", "v_prep", "mycat", "myschema", "mytable"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_t", "`mycat`.`myschema`.`mytable`", "v_out")],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge("p1.producer_fg.write_t", "p2.consumer_fg.read_t")

    def test_two_part_to_three_part_unique_catalog_match(self):
        """Producer `cat.sch.tbl`, reader `sch.tbl` (unique catalog) -> edge."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _write_action("write_t", "v_prep", "cat", "sch", "tbl"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_t", "sch.tbl", "v_out")],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge("p1.producer_fg.write_t", "p2.consumer_fg.read_t")

    def test_ambiguous_two_part_against_two_catalogs_no_edge(self):
        """Producers `catA.sch.tbl` AND `catB.sch.tbl`, reader `sch.tbl` -> NO edge.

        A bare schema.table is ambiguous across catalogs for GLOBAL tables, so
        it must stay external (no phantom edge).
        """
        producer_a = _flowgroup(
            "producer_a_fg",
            "p1",
            [
                _transform_reader("prep_a", "raw.a", "v_prep_a"),
                _write_action("write_a", "v_prep_a", "catA", "sch", "tbl"),
            ],
        )
        producer_b = _flowgroup(
            "producer_b_fg",
            "p2",
            [
                _transform_reader("prep_b", "raw.b", "v_prep_b"),
                _write_action("write_b", "v_prep_b", "catB", "sch", "tbl"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p3",
            [_transform_reader("read_t", "sch.tbl", "v_out")],
        )

        graph = _build([producer_a, producer_b, consumer])

        assert not graph.has_edge("p1.producer_a_fg.write_a", "p3.consumer_fg.read_t")
        assert not graph.has_edge("p2.producer_b_fg.write_b", "p3.consumer_fg.read_t")
        # Stays external, not internal.
        assert graph.nodes["p3.consumer_fg.read_t"].get("external_sources") == [
            "sch.tbl"
        ]

    def test_views_remain_pipeline_scoped_not_canonicalized(self):
        """A same-named view in a sibling pipeline must NOT match (no globalizing).

        View `target` keys stay pipeline-scoped and uncanonicalized; the table
        reconciliation rule must not leak into the view path.
        """
        # p1 produces a view `v_shared`; a consumer in a DIFFERENT pipeline p2
        # reads `v_shared` and must NOT pick up p1's view (pipeline-scoped).
        pipeline1 = _flowgroup(
            "fg1",
            "p1",
            [_transform_reader("make_view", "raw.x", "v_shared")],
        )
        pipeline2 = _flowgroup(
            "fg2",
            "p2",
            [_transform_reader("use_view", "v_shared", "v_out")],
        )

        graph = _build([pipeline1, pipeline2])

        assert not graph.has_edge("p1.fg1.make_view", "p2.fg2.use_view")
        assert graph.nodes["p2.fg2.use_view"].get("external_sources") == ["v_shared"]


@pytest.mark.unit
class TestPipelineQualifiedNodeIds:
    def test_same_flowgroup_name_across_pipelines_yields_distinct_nodes(self):
        """Two flowgroups BOTH named `orders`, in pipelines `bronze` and `silver`,
        must produce distinct pipeline-qualified nodes at the flowgroup AND action
        levels — the pre-qualification id format merged them into one node."""
        bronze_orders = _flowgroup(
            "orders",
            "bronze",
            [_transform_reader("load_orders", "raw.orders", "v_orders")],
        )
        silver_orders = _flowgroup(
            "orders",
            "silver",
            [_transform_reader("load_orders", "ext.orders", "v_orders")],
        )

        builder = DependencyGraphBuilder(project_root=Path("/tmp/nonexistent_lhp_root"))
        graphs = builder.build_from_flowgroups(
            [bronze_orders, silver_orders], file_paths={}
        )

        # Flowgroup level: two distinct qualified nodes, not one merged `orders`.
        assert set(graphs.flowgroup_graph.nodes) == {"bronze.orders", "silver.orders"}
        # Action level: same-named actions stay distinct under their pipelines.
        assert set(graphs.action_graph.nodes) == {
            "bronze.orders.load_orders",
            "silver.orders.load_orders",
        }
