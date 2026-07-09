"""Tests for the package-private graph primitives in ``_graph_ops``."""

import networkx as nx
import pytest

from lhp.core.dependencies._graph_ops import topological_generations


@pytest.mark.unit
class TestTopologicalGenerations:
    def test_cyclic_graph_returns_empty_list(self):
        graph = nx.DiGraph()
        graph.add_edge("a", "b")
        graph.add_edge("b", "a")

        assert topological_generations(graph) == []

    def test_empty_graph_returns_empty_list(self):
        assert topological_generations(nx.DiGraph()) == []

    def test_acyclic_graph_orders_producers_first(self):
        graph = nx.DiGraph()
        graph.add_edge("a", "b")
        graph.add_edge("a", "c")
        graph.add_edge("b", "d")
        graph.add_edge("c", "d")

        generations = topological_generations(graph)

        assert [set(generation) for generation in generations] == [
            {"a"},
            {"b", "c"},
            {"d"},
        ]
