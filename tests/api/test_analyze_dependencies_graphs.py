"""Behavioural tests for ``analyze_dependencies(include_graphs=True)``.

Drives the opt-in graph snapshots through the public entrypoint
``LakehousePlumberApplicationFacade.for_project(...).inspection`` against a
small real on-disk project (the ``_write_minimal_project`` pattern shared
with ``test_inspection_orphans_behaviour.py``), asserting the projected
node/edge shapes rather than just exercising the DTOs in isolation.

``enforce_version=False`` relaxes the ``required_lhp_version`` gate so the
fixture is independent of the installed package version.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from lhp.api import DependencyGraphView, LakehousePlumberApplicationFacade

pytestmark = pytest.mark.unit

ENV = "dev"
PIPELINE = "bronze"
FLOWGROUP = "fg_customers"


def _write_minimal_project(root: Path) -> None:
    """One pipeline (``bronze``) with one flowgroup: load -> transform -> write."""
    (root / "lhp.yaml").write_text(
        textwrap.dedent("""\
            name: dependency_graphs_project
            version: "1.0"
            description: "Dependency graph behaviour fixture"
            author: "Test Author"
            """)
    )
    for sub in ("presets", "templates"):
        (root / sub).mkdir(parents=True, exist_ok=True)

    subs = root / "substitutions"
    subs.mkdir(parents=True, exist_ok=True)
    (subs / f"{ENV}.yaml").write_text(
        textwrap.dedent(f"""\
            {ENV}:
              catalog: dev_catalog
              schema: bronze
            """)
    )

    fg_dir = root / "pipelines" / PIPELINE
    fg_dir.mkdir(parents=True, exist_ok=True)
    (fg_dir / "customers.yaml").write_text(
        textwrap.dedent(f"""\
            pipeline: {PIPELINE}
            flowgroup: {FLOWGROUP}
            actions:
              - name: load_customers
                type: load
                source:
                  type: sql
                  sql: "SELECT 1 AS id"
                target: v_customers_raw
              - name: clean_customers
                type: transform
                transform_type: sql
                source: v_customers_raw
                sql: "SELECT * FROM v_customers_raw"
                target: v_customers_clean
              - name: write_customers
                type: write
                source: v_customers_clean
                write_target:
                  type: streaming_table
                  database: "${{catalog}}.${{schema}}"
                  table: customers
            """)
    )


@pytest.fixture
def facade(tmp_path: Path) -> LakehousePlumberApplicationFacade:
    _write_minimal_project(tmp_path)
    return LakehousePlumberApplicationFacade.for_project(
        tmp_path, enforce_version=False
    )


class TestIncludeGraphsOptIn:
    def test_default_leaves_graphs_none(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        result = facade.inspection.analyze_dependencies()
        assert result.action_graph is None
        assert result.flowgroup_graph is None
        assert result.pipeline_graph is None

    def test_opt_in_populates_all_three_levels(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        result = facade.inspection.analyze_dependencies(include_graphs=True)
        assert isinstance(result.action_graph, DependencyGraphView)
        assert isinstance(result.flowgroup_graph, DependencyGraphView)
        assert isinstance(result.pipeline_graph, DependencyGraphView)
        assert result.action_graph.level == "action"
        assert result.flowgroup_graph.level == "flowgroup"
        assert result.pipeline_graph.level == "pipeline"


class TestActionGraphProjection:
    def test_nodes_are_keyed_flowgroup_dot_action(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        graph = facade.inspection.analyze_dependencies(include_graphs=True).action_graph
        assert graph is not None
        assert {n.id for n in graph.nodes} == {
            f"{FLOWGROUP}.load_customers",
            f"{FLOWGROUP}.clean_customers",
            f"{FLOWGROUP}.write_customers",
        }

    def test_node_projection_fields(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        graph = facade.inspection.analyze_dependencies(include_graphs=True).action_graph
        assert graph is not None
        load = next(n for n in graph.nodes if n.label == "load_customers")
        assert load.type == "load"
        assert load.pipeline == PIPELINE
        assert load.flowgroup == FLOWGROUP
        assert load.metadata["target"] == "v_customers_raw"

    def test_edges_follow_the_view_chain(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        graph = facade.inspection.analyze_dependencies(include_graphs=True).action_graph
        assert graph is not None
        pairs = {(e.source, e.target) for e in graph.edges}
        assert pairs == {
            (f"{FLOWGROUP}.load_customers", f"{FLOWGROUP}.clean_customers"),
            (f"{FLOWGROUP}.clean_customers", f"{FLOWGROUP}.write_customers"),
        }
        assert all(e.type == "internal" for e in graph.edges)


class TestDerivedLevels:
    def test_flowgroup_graph_projection(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        graph = facade.inspection.analyze_dependencies(
            include_graphs=True
        ).flowgroup_graph
        assert graph is not None
        (node,) = graph.nodes
        assert node.id == FLOWGROUP
        assert node.flowgroup == FLOWGROUP
        assert node.pipeline == PIPELINE
        assert node.type == "flowgroup"
        assert node.metadata["action_count"] == 3
        assert graph.edges == ()

    def test_pipeline_graph_projection(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        graph = facade.inspection.analyze_dependencies(
            include_graphs=True
        ).pipeline_graph
        assert graph is not None
        (node,) = graph.nodes
        assert node.id == PIPELINE
        assert node.pipeline == PIPELINE
        assert node.type == "pipeline"
        assert node.metadata["flowgroup_count"] == 1

    def test_unknown_pipeline_filter_yields_empty_graphs(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        result = facade.inspection.analyze_dependencies(
            pipeline_filter="does_not_exist", include_graphs=True
        )
        assert result.action_graph is not None
        assert result.flowgroup_graph is not None
        assert result.action_graph.nodes == ()
        assert result.flowgroup_graph.nodes == ()
