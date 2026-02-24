"""Tests for the graph serializer service."""

import pytest
import networkx as nx

from lhp.api.services.graph_serializer import (
    _build_metadata,
    _classify_edge_type,
    _extract_edges,
    _extract_nodes,
    serialize_graph,
)
from lhp.api.schemas.dependency import GraphEdge, GraphNode


pytestmark = pytest.mark.api


class TestClassifyEdgeType:
    """Tests for edge type classification from graph node attributes."""

    def test_same_pipeline_and_flowgroup_is_internal(self):
        g = nx.DiGraph()
        g.add_node("a", pipeline="p1", flowgroup="fg1")
        g.add_node("b", pipeline="p1", flowgroup="fg1")
        assert _classify_edge_type(g, "a", "b") == "internal"

    def test_same_pipeline_different_flowgroup_is_cross_flowgroup(self):
        g = nx.DiGraph()
        g.add_node("a", pipeline="p1", flowgroup="fg1")
        g.add_node("b", pipeline="p1", flowgroup="fg2")
        assert _classify_edge_type(g, "a", "b") == "cross_flowgroup"

    def test_different_pipeline_is_cross_pipeline(self):
        g = nx.DiGraph()
        g.add_node("a", pipeline="p1", flowgroup="fg1")
        g.add_node("b", pipeline="p2", flowgroup="fg1")
        assert _classify_edge_type(g, "a", "b") == "cross_pipeline"

    def test_missing_pipeline_attr_is_external(self):
        g = nx.DiGraph()
        g.add_node("a")
        g.add_node("b")
        assert _classify_edge_type(g, "a", "b") == "external"

    def test_flowgroup_level_same_pipeline_is_cross_flowgroup(self):
        """Flowgroup-level nodes have pipeline attr but no flowgroup attr."""
        g = nx.DiGraph()
        g.add_node("fg1", pipeline="p1")
        g.add_node("fg2", pipeline="p1")
        assert _classify_edge_type(g, "fg1", "fg2") == "cross_flowgroup"

    def test_flowgroup_level_different_pipeline_is_cross_pipeline(self):
        """Flowgroup-level nodes in different pipelines."""
        g = nx.DiGraph()
        g.add_node("fg1", pipeline="p1")
        g.add_node("fg2", pipeline="p2")
        assert _classify_edge_type(g, "fg1", "fg2") == "cross_pipeline"


class TestExtractNodes:
    """Tests for node extraction from a DiGraph."""

    def test_extracts_nodes_with_data(self):
        g = nx.DiGraph()
        g.add_node(
            "p1.fg1.load",
            label="load",
            type="load_action",
            pipeline="p1",
            flowgroup="fg1",
            stage=0,
        )
        nodes = _extract_nodes(g)
        assert len(nodes) == 1
        n = nodes[0]
        assert n.id == "p1.fg1.load"
        assert n.label == "load"
        assert n.type == "load_action"
        assert n.pipeline == "p1"
        assert n.flowgroup == "fg1"

    def test_infers_fields_from_node_id_when_data_missing(self):
        g = nx.DiGraph()
        g.add_node("pipeline.flowgroup.action")
        nodes = _extract_nodes(g)
        assert len(nodes) == 1
        n = nodes[0]
        assert n.label == "action"  # last part
        assert n.pipeline == "pipeline"
        assert n.flowgroup == "flowgroup"

    def test_extra_attributes_go_to_metadata(self):
        g = nx.DiGraph()
        g.add_node("p1.fg1.act", label="act", type="t", custom_field="val")
        nodes = _extract_nodes(g)
        assert "custom_field" in nodes[0].metadata
        assert nodes[0].metadata["custom_field"] == "val"


class TestExtractEdges:
    """Tests for edge extraction from a DiGraph."""

    def test_uses_explicit_edge_type(self):
        g = nx.DiGraph()
        g.add_edge("a.b.c", "a.b.d", type="explicit_type")
        edges = _extract_edges(g)
        assert len(edges) == 1
        assert edges[0].type == "explicit_type"

    def test_falls_back_to_classification(self):
        """When edge has no 'type' attr, _classify_edge_type uses node attrs."""
        g = nx.DiGraph()
        g.add_node("a", pipeline="p1", flowgroup="fg1")
        g.add_node("b", pipeline="p1", flowgroup="fg2")
        g.add_edge("a", "b")
        edges = _extract_edges(g)
        assert edges[0].type == "cross_flowgroup"


class TestBuildMetadata:
    """Tests for metadata construction including cycle detection."""

    def test_detects_cycles(self):
        g = nx.DiGraph()
        g.add_edge("a", "b")
        g.add_edge("b", "a")
        g.graph["external_sources"] = []
        g.graph["stages"] = 1
        nodes = _extract_nodes(g)
        edges = _extract_edges(g)
        meta = _build_metadata(g, "action", nodes, edges)
        assert meta.has_circular is True
        assert len(meta.circular_dependencies) >= 1

    def test_no_cycles_in_dag(self):
        g = nx.DiGraph()
        g.add_edge("a", "b")
        g.add_edge("b", "c")
        g.graph["external_sources"] = []
        g.graph["stages"] = 2
        nodes = _extract_nodes(g)
        edges = _extract_edges(g)
        meta = _build_metadata(g, "flowgroup", nodes, edges)
        assert meta.has_circular is False
        assert meta.circular_dependencies == []

    def test_caps_cycles_at_20(self):
        # Build a graph with many small cycles
        g = nx.DiGraph()
        for i in range(25):
            g.add_edge(f"a{i}", f"b{i}")
            g.add_edge(f"b{i}", f"a{i}")
        g.graph["external_sources"] = []
        g.graph["stages"] = 0
        nodes = _extract_nodes(g)
        edges = _extract_edges(g)
        meta = _build_metadata(g, "action", nodes, edges)
        assert len(meta.circular_dependencies) <= 20

    def test_reads_external_sources_from_graph(self):
        g = nx.DiGraph()
        g.add_node("a")
        g.graph["external_sources"] = ["catalog.schema.table"]
        g.graph["stages"] = 0
        nodes = _extract_nodes(g)
        edges = _extract_edges(g)
        meta = _build_metadata(g, "pipeline", nodes, edges)
        assert "catalog.schema.table" in meta.external_sources

    def test_metadata_level_matches_input(self):
        g = nx.DiGraph()
        g.add_node("a")
        g.graph["external_sources"] = []
        g.graph["stages"] = 0
        nodes = _extract_nodes(g)
        edges = _extract_edges(g)
        meta = _build_metadata(g, "flowgroup", nodes, edges)
        assert meta.level == "flowgroup"
