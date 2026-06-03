"""Tests for dependency models."""

from unittest.mock import Mock

import networkx as nx
import pytest

from lhp.models.dependencies import (
    ActionDependencyInfo,
    DependencyAnalysisResult,
    DependencyGraphs,
    FlowgroupDependencyInfo,
    PipelineDependency,
)


class TestDependencyGraphs:
    def setup_method(self):
        self.action_graph = nx.DiGraph()
        self.flowgroup_graph = nx.DiGraph()
        self.pipeline_graph = nx.DiGraph()
        self.metadata = {"test": "data"}

        self.graphs = DependencyGraphs(
            action_graph=self.action_graph,
            flowgroup_graph=self.flowgroup_graph,
            pipeline_graph=self.pipeline_graph,
            metadata=self.metadata,
        )

    def test_initialization(self):
        assert self.graphs.action_graph is self.action_graph
        assert self.graphs.flowgroup_graph is self.flowgroup_graph
        assert self.graphs.pipeline_graph is self.pipeline_graph
        assert self.graphs.metadata is self.metadata

    def test_get_graph_by_level_action(self):
        result = self.graphs.get_graph_by_level("action")
        assert result is self.action_graph

    def test_get_graph_by_level_flowgroup(self):
        result = self.graphs.get_graph_by_level("flowgroup")
        assert result is self.flowgroup_graph

    def test_get_graph_by_level_pipeline(self):
        result = self.graphs.get_graph_by_level("pipeline")
        assert result is self.pipeline_graph

    def test_get_graph_by_level_invalid(self):
        with pytest.raises(ValueError) as exc_info:
            self.graphs.get_graph_by_level("invalid_level")

        error_msg = str(exc_info.value)
        assert "invalid_level" in error_msg
        assert "Unknown dependency graph level" in error_msg

    def test_get_graph_by_level_case_sensitive(self):
        with pytest.raises(ValueError):
            self.graphs.get_graph_by_level("ACTION")

        with pytest.raises(ValueError):
            self.graphs.get_graph_by_level("Pipeline")

    def test_graphs_are_networkx_digraphs(self):
        assert isinstance(self.graphs.action_graph, nx.DiGraph)
        assert isinstance(self.graphs.flowgroup_graph, nx.DiGraph)
        assert isinstance(self.graphs.pipeline_graph, nx.DiGraph)

    def test_metadata_access(self):
        assert self.graphs.metadata["test"] == "data"

        # Test metadata modification
        self.graphs.metadata["new_key"] = "new_value"
        assert self.graphs.metadata["new_key"] == "new_value"


class TestPipelineDependency:
    def test_initialization_minimal(self):
        dep = PipelineDependency(
            pipeline="test_pipeline",
            depends_on=["dep1", "dep2"],
            flowgroup_count=3,
            action_count=10,
            external_sources=["external.table1"],
        )

        assert dep.pipeline == "test_pipeline"
        assert dep.depends_on == ["dep1", "dep2"]
        assert dep.flowgroup_count == 3
        assert dep.action_count == 10
        assert dep.external_sources == ["external.table1"]
        assert dep.can_run_parallel is False  # Default value
        assert dep.stage is None  # Default value

    def test_initialization_complete(self):
        dep = PipelineDependency(
            pipeline="test_pipeline",
            depends_on=["dep1"],
            flowgroup_count=2,
            action_count=5,
            external_sources=[],
            can_run_parallel=True,
            stage=2,
        )

        assert dep.pipeline == "test_pipeline"
        assert dep.depends_on == ["dep1"]
        assert dep.flowgroup_count == 2
        assert dep.action_count == 5
        assert dep.external_sources == []
        assert dep.can_run_parallel is True
        assert dep.stage == 2

    def test_empty_dependencies(self):
        dep = PipelineDependency(
            pipeline="root_pipeline",
            depends_on=[],
            flowgroup_count=1,
            action_count=1,
            external_sources=[],
        )

        assert dep.depends_on == []
        assert dep.external_sources == []

    def test_multiple_dependencies(self):
        dependencies = ["pipeline1", "pipeline2", "pipeline3"]
        external_sources = ["ext1.table", "ext2.table", "ext3.table"]

        dep = PipelineDependency(
            pipeline="dependent_pipeline",
            depends_on=dependencies,
            flowgroup_count=5,
            action_count=15,
            external_sources=external_sources,
        )

        assert dep.depends_on == dependencies
        assert dep.external_sources == external_sources

    def test_stage_assignment(self):
        dep = PipelineDependency(
            pipeline="test_pipeline",
            depends_on=[],
            flowgroup_count=1,
            action_count=1,
            external_sources=[],
        )

        # Initially no stage
        assert dep.stage is None

        # Assign stage
        dep.stage = 3
        assert dep.stage == 3

    def test_parallel_execution_flag(self):
        dep = PipelineDependency(
            pipeline="test_pipeline",
            depends_on=[],
            flowgroup_count=1,
            action_count=1,
            external_sources=[],
        )

        # Default is False
        assert dep.can_run_parallel is False

        # Can be set to True
        dep.can_run_parallel = True
        assert dep.can_run_parallel is True


class TestDependencyAnalysisResult:
    def setup_method(self):
        self.graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

        self.pipeline_dependencies = {
            "pipeline1": PipelineDependency(
                pipeline="pipeline1",
                depends_on=[],
                flowgroup_count=2,
                action_count=5,
                external_sources=["ext1.table"],
            ),
            "pipeline2": PipelineDependency(
                pipeline="pipeline2",
                depends_on=["pipeline1"],
                flowgroup_count=1,
                action_count=3,
                external_sources=[],
            ),
            "pipeline3": PipelineDependency(
                pipeline="pipeline3",
                depends_on=["pipeline1"],
                flowgroup_count=1,
                action_count=2,
                external_sources=["ext2.table"],
            ),
        }

        self.execution_stages = [["pipeline1"], ["pipeline2", "pipeline3"]]
        self.circular_dependencies = []
        self.external_sources = ["ext1.table", "ext2.table"]

        self.result = DependencyAnalysisResult(
            graphs=self.graphs,
            pipeline_dependencies=self.pipeline_dependencies,
            execution_stages=self.execution_stages,
            circular_dependencies=self.circular_dependencies,
            external_sources=self.external_sources,
        )

    def test_initialization(self):
        assert self.result.graphs is self.graphs
        assert self.result.pipeline_dependencies is self.pipeline_dependencies
        assert self.result.execution_stages is self.execution_stages
        assert self.result.circular_dependencies is self.circular_dependencies
        assert self.result.external_sources is self.external_sources

    def test_total_pipelines_property(self):
        assert self.result.total_pipelines == 3

    def test_total_external_sources_property(self):
        assert self.result.total_external_sources == 2

    def test_get_pipeline_execution_order(self):
        execution_order = self.result.get_pipeline_execution_order()
        expected_order = ["pipeline1", "pipeline2", "pipeline3"]
        assert execution_order == expected_order

    def test_empty_execution_stages(self):
        empty_result = DependencyAnalysisResult(
            graphs=self.graphs,
            pipeline_dependencies=self.pipeline_dependencies,
            execution_stages=[],
            circular_dependencies=[],
            external_sources=self.external_sources,
        )

        assert empty_result.get_pipeline_execution_order() == []

    def test_single_stage_execution(self):
        single_stage_result = DependencyAnalysisResult(
            graphs=self.graphs,
            pipeline_dependencies={
                "pipeline1": self.pipeline_dependencies["pipeline1"]
            },
            execution_stages=[["pipeline1"]],
            circular_dependencies=[],
            external_sources=[],
        )

        execution_order = single_stage_result.get_pipeline_execution_order()
        assert execution_order == ["pipeline1"]
        assert single_stage_result.total_pipelines == 1

    def test_with_circular_dependencies(self):
        circular_result = DependencyAnalysisResult(
            graphs=self.graphs,
            pipeline_dependencies=self.pipeline_dependencies,
            execution_stages=[],  # No execution order due to cycles
            circular_dependencies=[["pipeline cycle: A -> B -> A"]],
            external_sources=self.external_sources,
        )

        assert len(circular_result.circular_dependencies) == 1
        assert circular_result.get_pipeline_execution_order() == []

    def test_no_external_sources(self):
        no_external_result = DependencyAnalysisResult(
            graphs=self.graphs,
            pipeline_dependencies=self.pipeline_dependencies,
            execution_stages=self.execution_stages,
            circular_dependencies=[],
            external_sources=[],
        )

        assert no_external_result.total_external_sources == 0


class TestActionDependencyInfo:
    def test_initialization(self):
        action_info = ActionDependencyInfo(
            name="test_action",
            type="transform",
            flowgroup="test_flowgroup",
            pipeline="test_pipeline",
            sources=["source1", "source2"],
            target="target_table",
            external_sources=["external.table"],
            internal_sources=["internal.table"],
        )

        assert action_info.name == "test_action"
        assert action_info.type == "transform"
        assert action_info.flowgroup == "test_flowgroup"
        assert action_info.pipeline == "test_pipeline"
        assert action_info.sources == ["source1", "source2"]
        assert action_info.target == "target_table"
        assert action_info.external_sources == ["external.table"]
        assert action_info.internal_sources == ["internal.table"]

    def test_has_external_dependencies_true(self):
        action_info = ActionDependencyInfo(
            name="test_action",
            type="load",
            flowgroup="test_fg",
            pipeline="test_pipeline",
            sources=[],
            target=None,
            external_sources=["external.table1", "external.table2"],
            internal_sources=[],
        )

        assert action_info.has_external_dependencies() is True

    def test_has_external_dependencies_false(self):
        action_info = ActionDependencyInfo(
            name="test_action",
            type="load",
            flowgroup="test_fg",
            pipeline="test_pipeline",
            sources=[],
            target=None,
            external_sources=[],
            internal_sources=["internal.table"],
        )

        assert action_info.has_external_dependencies() is False

    def test_has_internal_dependencies_true(self):
        action_info = ActionDependencyInfo(
            name="test_action",
            type="transform",
            flowgroup="test_fg",
            pipeline="test_pipeline",
            sources=[],
            target=None,
            external_sources=[],
            internal_sources=["internal.table1"],
        )

        assert action_info.has_internal_dependencies() is True

    def test_has_internal_dependencies_false(self):
        action_info = ActionDependencyInfo(
            name="test_action",
            type="load",
            flowgroup="test_fg",
            pipeline="test_pipeline",
            sources=[],
            target=None,
            external_sources=["external.table"],
            internal_sources=[],
        )

        assert action_info.has_internal_dependencies() is False

    def test_no_dependencies(self):
        action_info = ActionDependencyInfo(
            name="standalone_action",
            type="load",
            flowgroup="test_fg",
            pipeline="test_pipeline",
            sources=[],
            target="output.table",
            external_sources=[],
            internal_sources=[],
        )

        assert action_info.has_external_dependencies() is False
        assert action_info.has_internal_dependencies() is False

    def test_both_dependency_types(self):
        action_info = ActionDependencyInfo(
            name="complex_action",
            type="transform",
            flowgroup="test_fg",
            pipeline="test_pipeline",
            sources=["mixed_sources"],
            target="output.table",
            external_sources=["external.table"],
            internal_sources=["internal.table"],
        )

        assert action_info.has_external_dependencies() is True
        assert action_info.has_internal_dependencies() is True


class TestFlowgroupDependencyInfo:
    def setup_method(self):
        self.actions = [
            ActionDependencyInfo(
                name="load_action",
                type="load",
                flowgroup="test_fg",
                pipeline="test_pipeline",
                sources=[],
                target="bronze.table",
                external_sources=["external.raw"],
                internal_sources=[],
            ),
            ActionDependencyInfo(
                name="transform_action",
                type="transform",
                flowgroup="test_fg",
                pipeline="test_pipeline",
                sources=["bronze.table"],
                target="silver.table",
                external_sources=[],
                internal_sources=["bronze.table"],
            ),
            ActionDependencyInfo(
                name="write_action",
                type="write",
                flowgroup="test_fg",
                pipeline="test_pipeline",
                sources=["silver.table"],
                target=None,
                external_sources=[],
                internal_sources=["silver.table"],
            ),
        ]

        self.flowgroup_info = FlowgroupDependencyInfo(
            name="test_flowgroup",
            pipeline="test_pipeline",
            actions=self.actions,
            depends_on_flowgroups=["upstream_fg"],
            external_sources=["external.raw"],
        )

    def test_initialization(self):
        assert self.flowgroup_info.name == "test_flowgroup"
        assert self.flowgroup_info.pipeline == "test_pipeline"
        assert self.flowgroup_info.actions == self.actions
        assert self.flowgroup_info.depends_on_flowgroups == ["upstream_fg"]
        assert self.flowgroup_info.external_sources == ["external.raw"]

    def test_action_count_property(self):
        assert self.flowgroup_info.action_count == 3

    def test_get_load_actions(self):
        load_actions = self.flowgroup_info.get_load_actions()
        assert len(load_actions) == 1
        assert load_actions[0].name == "load_action"
        assert load_actions[0].type == "load"

    def test_get_write_actions(self):
        write_actions = self.flowgroup_info.get_write_actions()
        assert len(write_actions) == 1
        assert write_actions[0].name == "write_action"
        assert write_actions[0].type == "write"

    def test_get_transform_actions(self):
        transform_actions = self.flowgroup_info.get_transform_actions()
        assert len(transform_actions) == 1
        assert transform_actions[0].name == "transform_action"
        assert transform_actions[0].type == "transform"

    def test_empty_flowgroup(self):
        empty_flowgroup = FlowgroupDependencyInfo(
            name="empty_fg",
            pipeline="test_pipeline",
            actions=[],
            depends_on_flowgroups=[],
            external_sources=[],
        )

        assert empty_flowgroup.action_count == 0
        assert empty_flowgroup.get_load_actions() == []
        assert empty_flowgroup.get_write_actions() == []
        assert empty_flowgroup.get_transform_actions() == []

    def test_single_action_type_flowgroup(self):
        load_only_actions = [
            ActionDependencyInfo(
                name="load1",
                type="load",
                flowgroup="load_fg",
                pipeline="test_pipeline",
                sources=[],
                target="table1",
                external_sources=[],
                internal_sources=[],
            ),
            ActionDependencyInfo(
                name="load2",
                type="load",
                flowgroup="load_fg",
                pipeline="test_pipeline",
                sources=[],
                target="table2",
                external_sources=[],
                internal_sources=[],
            ),
        ]

        load_only_flowgroup = FlowgroupDependencyInfo(
            name="load_only_fg",
            pipeline="test_pipeline",
            actions=load_only_actions,
            depends_on_flowgroups=[],
            external_sources=[],
        )

        assert load_only_flowgroup.action_count == 2
        assert len(load_only_flowgroup.get_load_actions()) == 2
        assert len(load_only_flowgroup.get_write_actions()) == 0
        assert len(load_only_flowgroup.get_transform_actions()) == 0

    def test_no_dependencies(self):
        no_deps_flowgroup = FlowgroupDependencyInfo(
            name="independent_fg",
            pipeline="test_pipeline",
            actions=self.actions,
            depends_on_flowgroups=[],
            external_sources=[],
        )

        assert no_deps_flowgroup.depends_on_flowgroups == []
        assert no_deps_flowgroup.external_sources == []

    def test_multiple_dependencies(self):
        multi_deps_flowgroup = FlowgroupDependencyInfo(
            name="dependent_fg",
            pipeline="test_pipeline",
            actions=[],
            depends_on_flowgroups=["fg1", "fg2", "fg3"],
            external_sources=["ext1.table", "ext2.table"],
        )

        assert len(multi_deps_flowgroup.depends_on_flowgroups) == 3
        assert len(multi_deps_flowgroup.external_sources) == 2


@pytest.mark.parametrize(
    "pipeline_count,expected", [(0, 0), (1, 1), (5, 5), (100, 100)]
)
def test_dependency_analysis_result_total_pipelines(pipeline_count, expected):
    pipeline_deps = {
        f"pipeline_{i}": PipelineDependency(
            pipeline=f"pipeline_{i}",
            depends_on=[],
            flowgroup_count=1,
            action_count=1,
            external_sources=[],
        )
        for i in range(pipeline_count)
    }

    result = DependencyAnalysisResult(
        graphs=DependencyGraphs(nx.DiGraph(), nx.DiGraph(), nx.DiGraph(), {}),
        pipeline_dependencies=pipeline_deps,
        execution_stages=[],
        circular_dependencies=[],
        external_sources=[],
    )

    assert result.total_pipelines == expected


@pytest.mark.parametrize(
    "external_count,expected", [(0, 0), (1, 1), (10, 10), (50, 50)]
)
def test_dependency_analysis_result_total_external_sources(external_count, expected):
    external_sources = [f"external.table_{i}" for i in range(external_count)]

    result = DependencyAnalysisResult(
        graphs=DependencyGraphs(nx.DiGraph(), nx.DiGraph(), nx.DiGraph(), {}),
        pipeline_dependencies={},
        execution_stages=[],
        circular_dependencies=[],
        external_sources=external_sources,
    )

    assert result.total_external_sources == expected
