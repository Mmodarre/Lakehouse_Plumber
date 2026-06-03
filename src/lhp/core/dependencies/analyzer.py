"""Dependency analysis core: pure topological / cycle / external-source analysis.

This module hosts only the pure analysis core (``DependencyAnalyzer``).
The dependency-analysis surface is split across:

- composition root → ``service.py`` (``DependencyAnalysisService``)
- discovery + graph construction → ``builder.py`` (``DependencyGraphBuilder``)
- DOT/JSON/text serialization → ``output.py`` (module-level functions)
"""

import logging
from typing import Dict, List

import networkx as nx

from lhp.models import FlowGroup

from ...models.dependencies import (
    DependencyAnalysisResult,
    DependencyGraphs,
    PipelineDependency,
)


class DependencyAnalyzer:
    """Pure dependency-graph analysis.

    Stateless apart from the logger. All inputs come in via method
    arguments; outputs are returned. Composition root (``service.py``)
    is responsible for wiring this together with a builder, a metrics
    service, and the output module.

    :stability: provisional
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def analyze(self, graphs: DependencyGraphs) -> DependencyAnalysisResult:
        """Run topological / cycle / external-source analysis on the given graphs.

        Args:
            graphs: Pre-built action/flowgroup/pipeline dependency graphs.

        Returns:
            Complete dependency analysis result with execution order and statistics.
        """
        self.logger.info("Starting dependency analysis...")

        pipeline_dependencies = self._analyze_pipeline_dependencies(graphs)

        # Detect circular dependencies before topological sort — nx raises on cycles.
        circular_dependencies = self._detect_circular_dependencies(graphs)

        if circular_dependencies:
            self.logger.warning(
                "Skipping execution order generation due to circular dependencies"
            )
            execution_stages = []
        else:
            execution_stages = self._get_execution_order(graphs.pipeline_graph)

        external_sources = self._collect_external_sources(graphs)
        self._update_pipeline_stages(pipeline_dependencies, execution_stages)

        result = DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies=pipeline_dependencies,
            execution_stages=execution_stages,
            circular_dependencies=circular_dependencies,
            external_sources=external_sources,
        )

        self.logger.info(
            f"Analysis complete: {result.total_pipelines} pipelines, "
            f"{len(execution_stages)} execution stages, "
            f"{len(circular_dependencies)} circular dependencies detected"
        )

        return result

    def partition_result_by_job(
        self,
        global_result: DependencyAnalysisResult,
        flowgroups: List[FlowGroup],
    ) -> Dict[str, DependencyAnalysisResult]:
        """Partition a global dependency analysis result by job_name.

        Filters the existing global result's graphs by the flowgroups
        belonging to each job, avoiding a re-analysis pass per job. This
        is significantly faster for multi-job pipelines.

        Args:
            global_result: The complete dependency analysis result
            flowgroups: All flowgroups (used to determine job grouping)

        Returns:
            Dictionary mapping job_name to per-job DependencyAnalysisResult
        """
        # Group flowgroups by job_name
        job_groups: Dict[str, List[FlowGroup]] = {}
        for fg in flowgroups:
            job_name = fg.job_name or "_default"
            if job_name not in job_groups:
                job_groups[job_name] = []
            job_groups[job_name].append(fg)

        global_external_sources = set(global_result.external_sources)
        job_results: Dict[str, DependencyAnalysisResult] = {}

        for job_name in sorted(job_groups.keys()):
            job_flowgroups = job_groups[job_name]
            job_fg_names = {fg.flowgroup for fg in job_flowgroups}
            job_pipeline_names = {fg.pipeline for fg in job_flowgroups}

            job_action_graph = nx.DiGraph()
            for node, data in global_result.graphs.action_graph.nodes(data=True):
                if data.get("flowgroup") in job_fg_names:
                    job_action_graph.add_node(node, **data)

            for u, v, data in global_result.graphs.action_graph.edges(data=True):
                if u in job_action_graph and v in job_action_graph:
                    job_action_graph.add_edge(u, v, **data)

            job_fg_graph = nx.DiGraph()
            for node, data in global_result.graphs.flowgroup_graph.nodes(data=True):
                if node in job_fg_names:
                    job_fg_graph.add_node(node, **data)

            for u, v, data in global_result.graphs.flowgroup_graph.edges(data=True):
                if u in job_fg_graph and v in job_fg_graph:
                    job_fg_graph.add_edge(u, v, **data)

            job_pipeline_graph = nx.DiGraph()
            for node, data in global_result.graphs.pipeline_graph.nodes(data=True):
                if node in job_pipeline_names:
                    job_pipeline_graph.add_node(node, **data)

            for u, v, data in global_result.graphs.pipeline_graph.edges(data=True):
                if u in job_pipeline_graph and v in job_pipeline_graph:
                    job_pipeline_graph.add_edge(u, v, **data)

            job_graphs = DependencyGraphs(
                action_graph=job_action_graph,
                flowgroup_graph=job_fg_graph,
                pipeline_graph=job_pipeline_graph,
                metadata={
                    "total_pipelines": len(job_pipeline_names),
                    "total_flowgroups": len(job_fg_names),
                    "job_name": job_name,
                },
            )

            # Re-analyze pipeline dependencies from the partitioned graphs,
            # producing correct intra-job depends_on lists by construction
            # (cross-job dependencies are handled by the master job).
            job_pipeline_deps = self._analyze_pipeline_dependencies(job_graphs)
            job_circular_deps = self._detect_circular_dependencies(job_graphs)

            if job_circular_deps:
                job_execution_stages = []
            else:
                job_execution_stages = self._get_execution_order(job_pipeline_graph)

            job_external = self._collect_external_sources(job_graphs)
            cross_job_sources = set(job_external) - global_external_sources
            if cross_job_sources:
                self.logger.info(
                    f"Job '{job_name}' depends on {len(cross_job_sources)} source(s) "
                    f"from other jobs: {', '.join(sorted(list(cross_job_sources)[:5]))}"
                )

            job_results[job_name] = DependencyAnalysisResult(
                graphs=job_graphs,
                pipeline_dependencies=job_pipeline_deps,
                execution_stages=job_execution_stages,
                circular_dependencies=job_circular_deps,
                external_sources=job_external,
            )

            self.logger.debug(
                f"Partitioned job '{job_name}': {len(job_fg_names)} flowgroups, "
                f"{job_action_graph.number_of_nodes()} actions"
            )

        return job_results

    def get_execution_order(self, graphs: DependencyGraphs) -> List[List[str]]:
        """
        Get pipeline execution order using topological sorting.

        Args:
            graphs: Dependency graphs

        Returns:
            List of execution stages, where each stage contains pipelines that can run in parallel
        """
        return self._get_execution_order(graphs.pipeline_graph)

    def detect_circular_dependencies(self, graphs: DependencyGraphs) -> List[List[str]]:
        """
        Detect circular dependencies at all levels.

        Args:
            graphs: Dependency graphs

        Returns:
            List of circular dependency chains
        """
        return self._detect_circular_dependencies(graphs)

    def _is_external_source(self, source: str, all_targets: set) -> bool:
        """
        Check if a source is external using explicit tracking.

        A source is external if it's not produced by any action in the project.
        """
        return source not in all_targets

    def _analyze_pipeline_dependencies(
        self, graphs: DependencyGraphs
    ) -> Dict[str, PipelineDependency]:
        """Analyze dependencies for each pipeline."""
        pipeline_deps = {}

        for pipeline in graphs.pipeline_graph.nodes():
            node_data = graphs.pipeline_graph.nodes[pipeline]
            depends_on = list(graphs.pipeline_graph.predecessors(pipeline))

            pipeline_deps[pipeline] = PipelineDependency(
                pipeline=pipeline,
                depends_on=depends_on,
                flowgroup_count=node_data.get("flowgroup_count", 0),
                action_count=node_data.get("action_count", 0),
                external_sources=node_data.get("external_sources", []),
            )

        return pipeline_deps

    def _get_execution_order(self, pipeline_graph: nx.DiGraph) -> List[List[str]]:
        """Get pipeline execution order using topological sorting."""
        if not pipeline_graph.nodes():
            return []

        try:
            return list(nx.topological_generations(pipeline_graph))
        except nx.NetworkXError:
            self.logger.exception("Error in topological sorting")
            return []

    def _detect_circular_dependencies(
        self, graphs: DependencyGraphs
    ) -> List[List[str]]:
        """Detect all circular dependencies at all graph levels.

        Uses nx.simple_cycles() to find ALL cycles (not just the first one),
        capped at 20 cycles to avoid overwhelming output.
        """
        MAX_CYCLES = 20
        circular_dependencies: List[List[str]] = []

        for level_name, graph in [
            ("action", graphs.action_graph),
            ("flowgroup", graphs.flowgroup_graph),
            ("pipeline", graphs.pipeline_graph),
        ]:
            self.logger.debug(
                f"Checking {level_name} graph for cycles ({graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges)"
            )

            cycles_found = 0
            for cycle_nodes in nx.simple_cycles(graph):
                if len(circular_dependencies) >= MAX_CYCLES:
                    self.logger.warning(
                        f"Reached maximum cycle reporting limit ({MAX_CYCLES}). "
                        "Additional cycles may exist."
                    )
                    return circular_dependencies

                # Format cycle: [A, B, C] -> "A -> B -> C -> A"
                cycle_path = [*list(cycle_nodes), cycle_nodes[0]]
                cycle_description = f"{level_name} level: {' -> '.join(cycle_path)}"
                circular_dependencies.append([cycle_description])
                cycles_found += 1
                self.logger.warning(
                    f"Circular dependency detected: {cycle_description}"
                )

            if cycles_found:
                self.logger.info(f"Found {cycles_found} cycle(s) at {level_name} level")

        return circular_dependencies

    def _collect_external_sources(self, graphs: DependencyGraphs) -> List[str]:
        """Collect all external sources identified across the project."""
        external_sources = set()

        for node in graphs.action_graph.nodes():
            node_external_sources = graphs.action_graph.nodes[node].get(
                "external_sources", []
            )
            external_sources.update(node_external_sources)

        return sorted(external_sources)

    def _update_pipeline_stages(
        self,
        pipeline_dependencies: Dict[str, PipelineDependency],
        execution_stages: List[List[str]],
    ) -> None:
        """Update pipeline dependencies with stage information."""
        for stage_idx, stage_pipelines in enumerate(execution_stages):
            for pipeline in stage_pipelines:
                if pipeline in pipeline_dependencies:
                    pipeline_dependencies[pipeline].stage = stage_idx
                    pipeline_dependencies[pipeline].can_run_parallel = (
                        len(stage_pipelines) > 1
                    )
