"""Unit tests for the webapp pipeline-graph serializer.

Exercises :func:`serialize_pipeline_graph`, which projects the public
:class:`lhp.api.DependencyAnalysisResult` onto the React-Flow-facing
``GraphResponse`` schema. Each test constructs a ``DependencyAnalysisResult``
directly (no project / facade), keeping the suite self-sufficient.

Edge semantics under test: for a pipeline ``P`` with ``depends_on=[D]`` the
serializer emits the edge ``D -> P`` (source = dependency, target = dependent).
"""

import pytest

from lhp.api import (
    DependencyAnalysisResult,
    DependencyGraphEdgeView,
    DependencyGraphNodeView,
    DependencyGraphView,
)
from lhp.webapp.schemas.dependency import GraphEdge, GraphNode, GraphResponse
from lhp.webapp.services.graph_serializer import (
    serialize_action_graph,
    serialize_flowgroup_graph,
    serialize_pipeline_graph,
)

pytestmark = pytest.mark.webapp


def _result(
    *,
    pipeline_dependencies: dict[str, tuple[str, ...]] | None = None,
    execution_stages: tuple[tuple[str, ...], ...] = (),
    circular_dependencies: tuple[tuple[str, ...], ...] = (),
    external_sources: tuple[str, ...] = (),
) -> DependencyAnalysisResult:
    """Build a public DependencyAnalysisResult with sensible derived counts."""
    deps = pipeline_dependencies or {}
    return DependencyAnalysisResult(
        pipeline_dependencies=deps,
        execution_stages=execution_stages,
        circular_dependencies=circular_dependencies,
        external_sources=external_sources,
        total_pipelines=len(deps),
        total_external_sources=len(external_sources),
    )


def _edge_pairs(response: GraphResponse) -> set[tuple[str, str]]:
    """Collect (source, target) pairs from a response's edges."""
    return {(e.source, e.target) for e in response.edges}


def _node_by_id(response: GraphResponse, node_id: str) -> GraphNode:
    """Fetch the single node with the given id."""
    matches = [n for n in response.nodes if n.id == node_id]
    assert len(matches) == 1, f"expected exactly one node {node_id!r}, got {matches}"
    return matches[0]


class TestLinearChain:
    """A -> B -> C: each pipeline depends on the previous one."""

    def _build(self) -> GraphResponse:
        result = _result(
            pipeline_dependencies={
                "a": (),
                "b": ("a",),
                "c": ("b",),
            },
            execution_stages=(("a",), ("b",), ("c",)),
        )
        return serialize_pipeline_graph(result)

    def test_returns_graph_response(self):
        response = self._build()
        assert isinstance(response, GraphResponse)

    def test_one_node_per_pipeline(self):
        response = self._build()
        assert {n.id for n in response.nodes} == {"a", "b", "c"}
        assert all(n.type == "pipeline" for n in response.nodes)

    def test_edges_point_from_dependency_to_dependent(self):
        response = self._build()
        # depends_on=[a] on b => edge a -> b; depends_on=[b] on c => edge b -> c.
        assert _edge_pairs(response) == {("a", "b"), ("b", "c")}
        assert all(e.type == "pipeline" for e in response.edges)

    def test_stage_assignment_follows_execution_stages(self):
        response = self._build()
        assert _node_by_id(response, "a").stage == 0
        assert _node_by_id(response, "b").stage == 1
        assert _node_by_id(response, "c").stage == 2

    def test_metadata_counts_and_no_cycle(self):
        response = self._build()
        meta = response.metadata
        assert meta.level == "pipeline"
        assert meta.total_nodes == 3
        assert meta.total_edges == 2
        assert meta.stages == 3
        assert meta.has_circular is False
        assert meta.circular_dependencies == []
        assert meta.external_sources == []

    def test_depends_on_count_in_node_metadata(self):
        response = self._build()
        assert _node_by_id(response, "a").metadata["depends_on_count"] == 0
        assert _node_by_id(response, "b").metadata["depends_on_count"] == 1
        assert _node_by_id(response, "c").metadata["depends_on_count"] == 1


class TestExternalSource:
    """External sources become standalone nodes with no incident edges."""

    def _build(self) -> GraphResponse:
        result = _result(
            pipeline_dependencies={"bronze": (), "silver": ("bronze",)},
            execution_stages=(("bronze",), ("silver",)),
            external_sources=("catalog.raw.events", "s3://landing/files"),
        )
        return serialize_pipeline_graph(result)

    def test_external_sources_emitted_as_nodes(self):
        response = self._build()
        external = [n for n in response.nodes if n.type == "external"]
        assert {n.id for n in external} == {
            "catalog.raw.events",
            "s3://landing/files",
        }

    def test_external_node_shape(self):
        response = self._build()
        node = _node_by_id(response, "catalog.raw.events")
        assert node.type == "external"
        assert node.label == "catalog.raw.events"
        assert node.pipeline == ""
        assert node.metadata.get("external") is True

    def test_external_nodes_have_no_edges(self):
        response = self._build()
        external_ids = {"catalog.raw.events", "s3://landing/files"}
        for edge in response.edges:
            assert edge.source not in external_ids
            assert edge.target not in external_ids
        # Only the pipeline-to-pipeline edge survives.
        assert _edge_pairs(response) == {("bronze", "silver")}

    def test_metadata_external_sources_and_counts(self):
        response = self._build()
        meta = response.metadata
        # 2 pipelines + 2 external sources.
        assert meta.total_nodes == 4
        assert meta.external_sources == [
            "catalog.raw.events",
            "s3://landing/files",
        ]
        assert meta.has_circular is False


class TestCycle:
    """A cyclic depends_on graph is reflected in metadata and back-edges."""

    def _build(self) -> GraphResponse:
        # a depends on b and b depends on a => a <-> b cycle. The analyzer would
        # surface this as a formatted description string in circular_dependencies.
        result = _result(
            pipeline_dependencies={
                "a": ("b",),
                "b": ("a",),
            },
            circular_dependencies=(("pipeline level: a -> b -> a",),),
        )
        return serialize_pipeline_graph(result)

    def test_has_circular_true(self):
        response = self._build()
        assert response.metadata.has_circular is True

    def test_circular_dependencies_passed_through(self):
        response = self._build()
        assert response.metadata.circular_dependencies == [
            ["pipeline level: a -> b -> a"]
        ]

    def test_cycle_reflected_in_edges(self):
        response = self._build()
        # depends_on=[b] on a => b -> a; depends_on=[a] on b => a -> b.
        assert _edge_pairs(response) == {("b", "a"), ("a", "b")}
        assert response.metadata.total_edges == 2

    def test_both_pipelines_are_nodes(self):
        response = self._build()
        assert {n.id for n in response.nodes} == {"a", "b"}


def test_empty_result_yields_empty_graph():
    """An empty analysis result serializes to an empty, well-formed graph."""
    response = serialize_pipeline_graph(_result())

    assert response.nodes == []
    assert response.edges == []
    assert response.metadata.level == "pipeline"
    assert response.metadata.total_nodes == 0
    assert response.metadata.total_edges == 0
    assert response.metadata.has_circular is False


def test_dependency_target_only_pipeline_still_becomes_node():
    """A pipeline named only as a dependency (not a key) is still a node."""
    # "raw" is referenced by "bronze" but has no entry of its own.
    result = _result(pipeline_dependencies={"bronze": ("raw",)})
    response = serialize_pipeline_graph(result)

    assert {n.id for n in response.nodes} == {"bronze", "raw"}
    assert _node_by_id(response, "raw").type == "pipeline"
    assert _edge_pairs(response) == {("raw", "bronze")}


def test_emitted_schema_field_names_are_frontend_facing():
    """GraphNode/GraphEdge carry exactly the id/type/label/source/target fields."""
    result = _result(
        pipeline_dependencies={"p": ("q",)},
        execution_stages=(("q",), ("p",)),
    )
    response = serialize_pipeline_graph(result)

    node = _node_by_id(response, "p")
    assert isinstance(node, GraphNode)
    assert node.id == "p"
    assert node.label == "p"
    assert node.type == "pipeline"

    edge = response.edges[0]
    assert isinstance(edge, GraphEdge)
    assert edge.source == "q"
    assert edge.target == "p"
    assert edge.type == "pipeline"


def _fg_node(
    flowgroup: str, pipeline: str, external_sources: list[str] | None = None
) -> DependencyGraphNodeView:
    metadata: dict = {"action_count": 1}
    if external_sources is not None:
        metadata["external_sources"] = external_sources
    return DependencyGraphNodeView(
        id=flowgroup,
        label=flowgroup,
        type="flowgroup",
        pipeline=pipeline,
        flowgroup=flowgroup,
        metadata=metadata,
    )


def _action_node(
    flowgroup: str, action: str, action_type: str, pipeline: str
) -> DependencyGraphNodeView:
    return DependencyGraphNodeView(
        id=f"{flowgroup}.{action}",
        label=action,
        type=action_type,
        pipeline=pipeline,
        flowgroup=flowgroup,
    )


class TestSerializeFlowgroupGraph:
    """Level serializer for the pipeline drill modal."""

    def _build(self) -> GraphResponse:
        graph = DependencyGraphView(
            level="flowgroup",
            nodes=(
                _fg_node("fg_a", "bronze", external_sources=["raw.landing"]),
                _fg_node("fg_b", "bronze"),
                _fg_node("fg_c", "silver"),
            ),
            edges=(
                DependencyGraphEdgeView(source="fg_a", target="fg_b", type="flowgroup"),
                DependencyGraphEdgeView(source="fg_b", target="fg_c", type="flowgroup"),
            ),
        )
        result = DependencyAnalysisResult(
            pipeline_dependencies={"bronze": (), "silver": ("bronze",)},
            execution_stages=(("bronze",), ("silver",)),
            external_sources=("raw.landing",),
            total_pipelines=2,
            total_external_sources=1,
            flowgroup_graph=graph,
        )
        return serialize_flowgroup_graph(result)

    def test_level_is_flowgroup(self):
        response = self._build()
        assert response.metadata.level == "flowgroup"

    def test_flowgroup_nodes_projected(self):
        response = self._build()
        node = _node_by_id(response, "fg_a")
        assert node.type == "flowgroup"
        assert node.pipeline == "bronze"
        assert node.flowgroup == "fg_a"
        assert node.metadata["action_count"] == 1

    def test_same_pipeline_edge_is_internal(self):
        response = self._build()
        edge = next(
            e for e in response.edges if e.source == "fg_a" and e.target == "fg_b"
        )
        assert edge.type == "internal"

    def test_cross_pipeline_edge_is_classified(self):
        response = self._build()
        edge = next(
            e for e in response.edges if e.source == "fg_b" and e.target == "fg_c"
        )
        assert edge.type == "cross_pipeline"

    def test_external_source_becomes_node_with_edge(self):
        response = self._build()
        node = _node_by_id(response, "raw.landing")
        assert node.type == "external"
        assert node.metadata.get("external") is True
        assert ("raw.landing", "fg_a") in _edge_pairs(response)
        external_edge = next(e for e in response.edges if e.source == "raw.landing")
        assert external_edge.type == "external"

    def test_metadata_counts_include_external(self):
        response = self._build()
        assert response.metadata.total_nodes == 4  # 3 flowgroups + 1 external
        assert response.metadata.total_edges == 3  # 2 flowgroup + 1 external
        assert response.metadata.stages == 2

    def test_missing_snapshot_raises_value_error(self):
        with pytest.raises(ValueError, match="include_graphs"):
            serialize_flowgroup_graph(_result())


class TestSerializeActionGraph:
    """Level serializer for the flowgroup drill modal."""

    def _build(self) -> GraphResponse:
        graph = DependencyGraphView(
            level="action",
            nodes=(
                _action_node("fg_a", "load_x", "load", "bronze"),
                _action_node("fg_a", "write_x", "write", "bronze"),
                _action_node("fg_b", "load_y", "load", "bronze"),
                _action_node("fg_c", "load_z", "load", "silver"),
            ),
            edges=(
                DependencyGraphEdgeView(
                    source="fg_a.load_x", target="fg_a.write_x", type="internal"
                ),
                DependencyGraphEdgeView(
                    source="fg_a.write_x", target="fg_b.load_y", type="internal"
                ),
                DependencyGraphEdgeView(
                    source="fg_a.write_x", target="fg_c.load_z", type="internal"
                ),
            ),
        )
        result = DependencyAnalysisResult(
            pipeline_dependencies={"bronze": (), "silver": ("bronze",)},
            execution_stages=(("bronze",), ("silver",)),
            total_pipelines=2,
            action_graph=graph,
        )
        return serialize_action_graph(result)

    def test_level_is_action(self):
        response = self._build()
        assert response.metadata.level == "action"

    def test_action_nodes_projected_with_flowgroup(self):
        response = self._build()
        node = _node_by_id(response, "fg_a.load_x")
        assert node.type == "load"
        assert node.label == "load_x"
        assert node.flowgroup == "fg_a"
        assert node.pipeline == "bronze"

    def test_same_flowgroup_edge_is_internal(self):
        response = self._build()
        edge = next(e for e in response.edges if e.target == "fg_a.write_x")
        assert edge.type == "internal"

    def test_cross_flowgroup_same_pipeline_edge(self):
        response = self._build()
        edge = next(e for e in response.edges if e.target == "fg_b.load_y")
        assert edge.type == "cross_flowgroup"

    def test_cross_pipeline_edge(self):
        response = self._build()
        edge = next(e for e in response.edges if e.target == "fg_c.load_z")
        assert edge.type == "cross_pipeline"

    def test_missing_snapshot_raises_value_error(self):
        with pytest.raises(ValueError, match="include_graphs"):
            serialize_action_graph(_result())
