"""DTO contract tests for the dependency-graph snapshot views.

Per constitution §8.3 + §9.15, every public DTO must satisfy four
non-optional contracts: frozen (``FrozenInstanceError`` on mutation),
pickle round-trip with ``==`` equality, JSON round-trip via
:func:`lhp.api.to_dict`, and field-type discipline (§4.8 — no ``Any``
outside ``JSONValue``, no bare ``List[`` / ``Dict[``).

Covers :class:`DependencyGraphNodeView`, :class:`DependencyGraphEdgeView`,
:class:`DependencyGraphView`, and the opt-in graph fields on
:class:`DependencyAnalysisResult` (populated by
``analyze_dependencies(include_graphs=True)``).
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError

import pytest

from lhp.api import (
    DependencyAnalysisResult,
    DependencyGraphEdgeView,
    DependencyGraphNodeView,
    DependencyGraphView,
    to_dict,
)


@pytest.fixture
def node_view() -> DependencyGraphNodeView:
    # ``external_sources`` is a list (not a tuple) on purpose: the converter
    # stores the nx attribute value as-is, and JSON round-trip equality
    # requires the sample to match that production shape.
    return DependencyGraphNodeView(
        id="customer_ingest.load_customer",
        label="load_customer",
        type="load",
        pipeline="bronze",
        flowgroup="customer_ingest",
        metadata={"target": "v_customer_raw", "external_sources": ["raw.customer"]},
    )


@pytest.fixture
def edge_view() -> DependencyGraphEdgeView:
    return DependencyGraphEdgeView(
        source="customer_ingest.load_customer",
        target="customer_ingest.write_customer",
        type="internal",
    )


@pytest.fixture
def graph_view(
    node_view: DependencyGraphNodeView, edge_view: DependencyGraphEdgeView
) -> DependencyGraphView:
    write_node = DependencyGraphNodeView(
        id="customer_ingest.write_customer",
        label="write_customer",
        type="write",
        pipeline="bronze",
        flowgroup="customer_ingest",
        metadata={"target": "catalog.bronze.customer"},
    )
    return DependencyGraphView(
        level="action", nodes=(node_view, write_node), edges=(edge_view,)
    )


@pytest.fixture
def result_with_graphs(graph_view: DependencyGraphView) -> DependencyAnalysisResult:
    flowgroup_graph = DependencyGraphView(
        level="flowgroup",
        nodes=(
            DependencyGraphNodeView(
                id="customer_ingest",
                label="customer_ingest",
                type="flowgroup",
                pipeline="bronze",
                flowgroup="customer_ingest",
                metadata={"action_count": 2, "external_sources": ["raw.customer"]},
            ),
        ),
    )
    pipeline_graph = DependencyGraphView(
        level="pipeline",
        nodes=(
            DependencyGraphNodeView(
                id="bronze",
                label="bronze",
                type="pipeline",
                pipeline="bronze",
                metadata={"flowgroup_count": 1, "action_count": 2},
            ),
        ),
    )
    return DependencyAnalysisResult(
        pipeline_dependencies={"bronze": ()},
        execution_stages=(("bronze",),),
        external_sources=("raw.customer",),
        total_pipelines=1,
        total_external_sources=1,
        action_graph=graph_view,
        flowgroup_graph=flowgroup_graph,
        pipeline_graph=pipeline_graph,
    )


def _reconstruct_node(payload: dict) -> DependencyGraphNodeView:
    return DependencyGraphNodeView(**payload)


def _reconstruct_edge(payload: dict) -> DependencyGraphEdgeView:
    return DependencyGraphEdgeView(**payload)


def _reconstruct_graph(payload: dict) -> DependencyGraphView:
    return DependencyGraphView(
        level=payload["level"],
        nodes=tuple(_reconstruct_node(n) for n in payload["nodes"]),
        edges=tuple(_reconstruct_edge(e) for e in payload["edges"]),
    )


class TestFrozen:
    """Contract 1 — every graph DTO is immutable."""

    def test_node_view_is_frozen(self, node_view: DependencyGraphNodeView) -> None:
        with pytest.raises(FrozenInstanceError):
            node_view.label = "other"  # type: ignore[misc]

    def test_edge_view_is_frozen(self, edge_view: DependencyGraphEdgeView) -> None:
        with pytest.raises(FrozenInstanceError):
            edge_view.type = "external"  # type: ignore[misc]

    def test_graph_view_is_frozen(self, graph_view: DependencyGraphView) -> None:
        with pytest.raises(FrozenInstanceError):
            graph_view.level = "pipeline"  # type: ignore[misc]


class TestPickleRoundTrip:
    """Contract 2 — pickle round-trip preserves equality."""

    def test_graph_view(self, graph_view: DependencyGraphView) -> None:
        assert pickle.loads(pickle.dumps(graph_view)) == graph_view

    def test_result_with_graphs(
        self, result_with_graphs: DependencyAnalysisResult
    ) -> None:
        restored = pickle.loads(pickle.dumps(result_with_graphs))
        assert restored == result_with_graphs
        assert restored.action_graph == result_with_graphs.action_graph


class TestJsonRoundTrip:
    """Contract 3 — ``to_dict`` output survives json.dumps/loads losslessly."""

    def test_graph_view_round_trip(self, graph_view: DependencyGraphView) -> None:
        payload = json.loads(json.dumps(to_dict(graph_view)))
        assert _reconstruct_graph(payload) == graph_view

    def test_node_metadata_survives(self, node_view: DependencyGraphNodeView) -> None:
        payload = json.loads(json.dumps(to_dict(node_view)))
        assert payload["metadata"] == {
            "target": "v_customer_raw",
            "external_sources": ["raw.customer"],
        }
        assert _reconstruct_node(payload) == node_view

    def test_result_graph_fields_serialize(
        self, result_with_graphs: DependencyAnalysisResult
    ) -> None:
        payload = json.loads(json.dumps(to_dict(result_with_graphs)))
        assert payload["action_graph"]["level"] == "action"
        assert payload["flowgroup_graph"]["level"] == "flowgroup"
        assert payload["pipeline_graph"]["level"] == "pipeline"
        assert (
            _reconstruct_graph(payload["action_graph"])
            == result_with_graphs.action_graph
        )


class TestDefaults:
    """The graph fields are opt-in: absent unless include_graphs=True."""

    def test_graphs_default_to_none(self) -> None:
        result = DependencyAnalysisResult()
        assert result.action_graph is None
        assert result.flowgroup_graph is None
        assert result.pipeline_graph is None

    def test_none_graphs_serialize_as_null(self) -> None:
        payload = json.loads(json.dumps(to_dict(DependencyAnalysisResult())))
        assert payload["action_graph"] is None
        assert payload["flowgroup_graph"] is None
        assert payload["pipeline_graph"] is None


class TestFieldTypeDiscipline:
    """Contract 4 — §4.8 field-type rules on the raw string annotations."""

    @pytest.mark.parametrize(
        "cls",
        [DependencyGraphNodeView, DependencyGraphEdgeView, DependencyGraphView],
        ids=lambda c: c.__name__,
    )
    def test_no_forbidden_annotations(self, cls: type) -> None:
        for field in dataclasses.fields(cls):
            annotation = field.type if isinstance(field.type, str) else str(field.type)
            assert "Any" not in annotation, f"{cls.__name__}.{field.name}"
            assert "List[" not in annotation, f"{cls.__name__}.{field.name}"
            assert "Dict[" not in annotation, f"{cls.__name__}.{field.name}"
            assert "list[" not in annotation, f"{cls.__name__}.{field.name}"
            assert "dict[" not in annotation, f"{cls.__name__}.{field.name}"

    def test_metadata_is_json_value_mapping(self) -> None:
        (metadata_field,) = [
            f
            for f in dataclasses.fields(DependencyGraphNodeView)
            if f.name == "metadata"
        ]
        annotation = (
            metadata_field.type
            if isinstance(metadata_field.type, str)
            else str(metadata_field.type)
        )
        assert annotation == "Mapping[str, JSONValue]"
