"""Tests for dependency models."""

import dataclasses

import networkx as nx
import pytest

from lhp.models.dependencies import (
    DependencyAnalysisResult,
    DependencyGraphs,
    DependencyWarning,
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

    def test_single_stage_total_pipelines(self):
        single_stage_result = DependencyAnalysisResult(
            graphs=self.graphs,
            pipeline_dependencies={
                "pipeline1": self.pipeline_dependencies["pipeline1"]
            },
            execution_stages=[["pipeline1"]],
            circular_dependencies=[],
            external_sources=[],
        )

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

    def test_no_external_sources(self):
        no_external_result = DependencyAnalysisResult(
            graphs=self.graphs,
            pipeline_dependencies=self.pipeline_dependencies,
            execution_stages=self.execution_stages,
            circular_dependencies=[],
            external_sources=[],
        )

        assert no_external_result.total_external_sources == 0


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


class TestDependencyWarning:
    def test_initialization_complete(self):
        warning = DependencyWarning(
            code="LHP-DEP-002",
            message="Opaque table read in Python source",
            flowgroup="fg1",
            action="load_data",
            suggestion="Add an explicit depends_on entry",
            file_path="pipelines/fg1.py",
            line=42,
        )

        assert warning.code == "LHP-DEP-002"
        assert warning.message == "Opaque table read in Python source"
        assert warning.flowgroup == "fg1"
        assert warning.action == "load_data"
        assert warning.suggestion == "Add an explicit depends_on entry"
        assert warning.file_path == "pipelines/fg1.py"
        assert warning.line == 42

    def test_optional_fields_default_to_none(self):
        warning = DependencyWarning(
            code="LHP-DEP-003",
            message="SQL source could not be parsed",
            flowgroup="fg2",
            action="transform_data",
            suggestion="Check the SQL syntax",
        )

        assert warning.file_path is None
        assert warning.line is None

    def test_frozen(self):
        warning = DependencyWarning(
            code="LHP-DEP-002",
            message="msg",
            flowgroup="fg",
            action="act",
            suggestion="sug",
        )

        with pytest.raises(dataclasses.FrozenInstanceError):
            warning.code = "LHP-DEP-003"

        with pytest.raises(dataclasses.FrozenInstanceError):
            warning.line = 7


class TestWarningFieldDefaults:
    def test_dependency_graphs_extraction_warnings_defaults_empty(self):
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

        assert graphs.extraction_warnings == []

    def test_dependency_graphs_extraction_warnings_not_shared(self):
        graphs_a = DependencyGraphs(nx.DiGraph(), nx.DiGraph(), nx.DiGraph(), {})
        graphs_b = DependencyGraphs(nx.DiGraph(), nx.DiGraph(), nx.DiGraph(), {})

        graphs_a.extraction_warnings.append(
            DependencyWarning(
                code="LHP-DEP-002",
                message="msg",
                flowgroup="fg",
                action="act",
                suggestion="sug",
            )
        )

        assert graphs_b.extraction_warnings == []

    def test_dependency_analysis_result_warnings_defaults_empty(self):
        result = DependencyAnalysisResult(
            graphs=DependencyGraphs(nx.DiGraph(), nx.DiGraph(), nx.DiGraph(), {}),
            pipeline_dependencies={},
            execution_stages=[],
            circular_dependencies=[],
            external_sources=[],
        )

        assert result.warnings == []
