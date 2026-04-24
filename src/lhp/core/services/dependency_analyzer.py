"""Dependency analysis service for LakehousePlumber using NetworkX."""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

import networkx as nx

from ...models.config import Action, ActionType, FlowGroup
from ...models.dependencies import (
    ActionDependencyInfo,
    DependencyAnalysisResult,
    DependencyGraphs,
    FlowgroupDependencyInfo,
    PipelineDependency,
)
from ...parsers.yaml_parser import YAMLParser
from ...utils.error_formatter import ErrorCategory, LHPError
from ...utils.source_extractor import extract_action_sources
from ..project_config_loader import ProjectConfigLoader
from .flowgroup_discoverer import FlowgroupDiscoverer


class DependencyAnalyzer:
    """
    Service for analyzing dependencies across actions, flowgroups, and pipelines.

    Uses NetworkX to build and analyze dependency graphs at multiple levels,
    enabling orchestration planning and execution order determination.
    """

    def __init__(
        self, project_root: Path, config_loader: Optional[ProjectConfigLoader] = None
    ):
        """
        Initialize dependency analyzer.

        Args:
            project_root: Root directory of the LakehousePlumber project
            config_loader: Optional config loader for project configuration
        """
        self.project_root = project_root
        self.config_loader = config_loader or ProjectConfigLoader(project_root)
        self.flowgroup_discoverer = FlowgroupDiscoverer(
            project_root, self.config_loader
        )
        self.yaml_parser = YAMLParser()
        self.logger = logging.getLogger(__name__)

        # Initialize components for template expansion
        from ...presets.preset_manager import PresetManager
        from ...utils.substitution import EnhancedSubstitutionManager
        from ..secret_validator import SecretValidator
        from ..template_engine import TemplateEngine
        from ..validator import ConfigValidator
        from .flowgroup_processor import FlowgroupProcessor

        self.template_engine = TemplateEngine(project_root / "templates")
        self.preset_manager = PresetManager(project_root / "presets")

        # Load project config for metadata validation
        project_config = self.config_loader.load_project_config()
        self.config_validator = ConfigValidator(project_root, project_config)
        self.secret_validator = SecretValidator()
        self.flowgroup_processor = FlowgroupProcessor(
            self.template_engine,
            self.preset_manager,
            self.config_validator,
            self.secret_validator,
        )

        # Cache for discovered flowgroups and their file paths
        self._flowgroups: Optional[List[FlowGroup]] = None
        self._flowgroup_file_paths: Dict[str, Path] = {}

    def get_project_name(self) -> str:
        """
        Get the project name from lhp.yaml configuration.

        Returns:
            Project name from lhp.yaml's 'name' field, or falls back to directory name
            if lhp.yaml doesn't exist or has no name field.
        """
        try:
            project_config = self.config_loader.load_project_config()
            if project_config and project_config.name:
                return project_config.name
        except Exception as e:
            self.logger.debug(f"Could not load project config for name: {e}")

        # Fallback to directory name for backward compatibility
        return self.project_root.name if self.project_root else "lhp_project"

    def build_dependency_graphs(
        self, pipeline_filter: Optional[str] = None
    ) -> DependencyGraphs:
        """
        Build dependency graphs at action, flowgroup, and pipeline levels.

        Args:
            pipeline_filter: Optional pipeline name to analyze only specific pipeline

        Returns:
            DependencyGraphs containing all three levels of dependency information
        """
        self.logger.info("Building dependency graphs...")

        # Discover all flowgroups
        flowgroups = self.get_flowgroups(pipeline_filter)

        if not flowgroups:
            self.logger.warning("No flowgroups found for analysis")
            return self._create_empty_graphs()

        # Build action-level dependency graph first
        action_graph = self._build_action_graph(flowgroups)

        # Derive flowgroup-level dependencies from action dependencies
        flowgroup_graph = self._build_flowgroup_graph(flowgroups, action_graph)

        # Derive pipeline-level dependencies from flowgroup dependencies
        pipeline_graph = self._build_pipeline_graph(flowgroups, flowgroup_graph)

        # Collect metadata
        metadata = self._build_metadata(
            flowgroups, action_graph, flowgroup_graph, pipeline_graph
        )

        self.logger.info(
            f"Built dependency graphs: {len(action_graph.nodes)} actions, "
            f"{len(flowgroup_graph.nodes)} flowgroups, {len(pipeline_graph.nodes)} pipelines"
        )

        return DependencyGraphs(
            action_graph=action_graph,
            flowgroup_graph=flowgroup_graph,
            pipeline_graph=pipeline_graph,
            metadata=metadata,
        )

    def analyze_dependencies(
        self, pipeline_filter: Optional[str] = None
    ) -> DependencyAnalysisResult:
        """
        Perform complete dependency analysis.

        Args:
            pipeline_filter: Optional pipeline name to analyze only specific pipeline

        Returns:
            Complete dependency analysis result with execution order and statistics
        """
        self.logger.info("Starting dependency analysis...")

        # Build dependency graphs
        graphs = self.build_dependency_graphs(pipeline_filter)

        # Analyze pipeline dependencies
        pipeline_dependencies = self._analyze_pipeline_dependencies(graphs)

        # Detect circular dependencies BEFORE trying to get execution order
        circular_dependencies = self._detect_circular_dependencies(graphs)

        # Get execution order using topological sorting (only if no circular dependencies)
        if circular_dependencies:
            self.logger.warning(
                "Skipping execution order generation due to circular dependencies"
            )
            execution_stages = []
        else:
            execution_stages = self._get_execution_order(graphs.pipeline_graph)

        # Collect external sources
        external_sources = self._collect_external_sources(graphs)

        # Update pipeline dependencies with stage information
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

    def analyze_dependencies_by_job(
        self,
    ) -> Tuple[Dict[str, DependencyAnalysisResult], DependencyAnalysisResult]:
        """
        Perform dependency analysis grouped by job_name.

        First analyzes all flowgroups together (global view), then analyzes
        each job_name group separately.

        Returns:
            Tuple of:
            - Dictionary mapping job_name to DependencyAnalysisResult (per-job analysis)
            - DependencyAnalysisResult for global analysis (all flowgroups together)

        Note: In single-job mode, both tuple elements reference the same result.

        Raises:
            LHPError: If job_name validation fails
        """
        from .job_name_validator import validate_job_names

        self.logger.info("Starting multi-job dependency analysis...")

        # Get all flowgroups
        flowgroups = self.get_flowgroups()

        if not flowgroups:
            self.logger.warning("No flowgroups found for analysis")
            # Return empty results - create minimal result for consistency
            from ...models.dependencies import DependencyGraphs

            empty_graphs = self._create_empty_graphs()
            empty_result = DependencyAnalysisResult(
                graphs=empty_graphs,
                pipeline_dependencies={},
                execution_stages=[],
                circular_dependencies=[],
                external_sources=[],
            )
            return {}, empty_result

        # Validate job_name usage (all-or-nothing rule)
        validate_job_names(flowgroups)

        # Check if any flowgroup has job_name
        has_job_name = any(fg.job_name for fg in flowgroups)

        if not has_job_name:
            # No job_name defined - return single result with default key
            self.logger.info("No job_name defined - performing single-job analysis")
            result = self.analyze_dependencies()
            project_name = self.get_project_name()
            job_results = {f"{project_name}_orchestration": result}
            return job_results, result  # Return tuple: (job_results, global_result)

        # Group flowgroups by job_name
        job_groups: Dict[str, List[FlowGroup]] = {}
        for fg in flowgroups:
            if fg.job_name not in job_groups:
                job_groups[fg.job_name] = []
            job_groups[fg.job_name].append(fg)

        self.logger.info(
            f"Found {len(job_groups)} job group(s): {', '.join(sorted(job_groups.keys()))}"
        )

        # First: Analyze all flowgroups together for global view
        self.logger.info("Step 1: Analyzing all flowgroups together (global view)")
        global_result = self.analyze_dependencies()

        # Store global external sources for reference
        global_external_sources = set(global_result.external_sources)

        # Second: Analyze each job group separately
        self.logger.info(
            f"Step 2: Analyzing {len(job_groups)} job group(s) individually"
        )
        job_results: Dict[str, DependencyAnalysisResult] = {}

        for job_name in sorted(job_groups.keys()):
            self.logger.info(f"  Analyzing job: {job_name}")
            job_flowgroups = job_groups[job_name]

            # Temporarily override cached flowgroups
            original_flowgroups = self._flowgroups
            self._flowgroups = job_flowgroups

            try:
                # Analyze this job's dependencies
                job_result = self.analyze_dependencies()

                # Track external sources specific to this job
                # (sources that are external to this job but might be internal to the project)
                job_external = set(job_result.external_sources)

                # Additional metadata: sources produced by other jobs in the project
                cross_job_sources = job_external - global_external_sources
                if cross_job_sources:
                    self.logger.info(
                        f"    Job '{job_name}' depends on {len(cross_job_sources)} source(s) "
                        f"from other jobs: {', '.join(sorted(list(cross_job_sources)[:5]))}"
                    )

                job_results[job_name] = job_result

            finally:
                # Restore original flowgroups
                self._flowgroups = original_flowgroups

        self.logger.info(
            f"Multi-job analysis complete: {len(job_results)} job(s), "
            f"{len(flowgroups)} total flowgroups"
        )

        return job_results, global_result  # Return tuple: (job_results, global_result)

    def partition_result_by_job(
        self,
        global_result: DependencyAnalysisResult,
        flowgroups: List[FlowGroup],
    ) -> Dict[str, DependencyAnalysisResult]:
        """Partition a global dependency analysis result by job_name.

        Instead of re-running analyze_dependencies() for each job, this method
        filters the existing graphs by the flowgroups belonging to each job.
        This is significantly faster for multi-job pipelines.

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

            # Filter action graph: keep nodes belonging to this job's flowgroups
            job_action_graph = nx.DiGraph()
            for node, data in global_result.graphs.action_graph.nodes(data=True):
                if data.get("flowgroup") in job_fg_names:
                    job_action_graph.add_node(node, **data)

            # Keep edges where both nodes are in the filtered graph
            for u, v, data in global_result.graphs.action_graph.edges(data=True):
                if u in job_action_graph and v in job_action_graph:
                    job_action_graph.add_edge(u, v, **data)

            # Filter flowgroup graph
            job_fg_graph = nx.DiGraph()
            for node, data in global_result.graphs.flowgroup_graph.nodes(data=True):
                if node in job_fg_names:
                    job_fg_graph.add_node(node, **data)

            for u, v, data in global_result.graphs.flowgroup_graph.edges(data=True):
                if u in job_fg_graph and v in job_fg_graph:
                    job_fg_graph.add_edge(u, v, **data)

            # Filter pipeline graph
            job_pipeline_graph = nx.DiGraph()
            for node, data in global_result.graphs.pipeline_graph.nodes(data=True):
                if node in job_pipeline_names:
                    job_pipeline_graph.add_node(node, **data)

            for u, v, data in global_result.graphs.pipeline_graph.edges(data=True):
                if u in job_pipeline_graph and v in job_pipeline_graph:
                    job_pipeline_graph.add_edge(u, v, **data)

            # Build partitioned graphs
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

            # Filter pipeline dependencies, keeping only intra-job depends_on
            # (cross-job dependencies are handled by the master job)
            job_pipeline_deps = {}
            for name, dep in global_result.pipeline_dependencies.items():
                if name in job_pipeline_names:
                    job_pipeline_deps[name] = PipelineDependency(
                        pipeline=dep.pipeline,
                        depends_on=[
                            d for d in dep.depends_on if d in job_pipeline_names
                        ],
                        flowgroup_count=dep.flowgroup_count,
                        action_count=dep.action_count,
                        external_sources=dep.external_sources,
                    )

            # Detect circular deps for this partition
            job_circular_deps = self._detect_circular_dependencies(job_graphs)

            # Compute execution order for this partition
            if job_circular_deps:
                job_execution_stages = []
            else:
                job_execution_stages = self._get_execution_order(job_pipeline_graph)

            # Collect external sources for this job
            job_external = self._collect_external_sources(job_graphs)

            # Log cross-job dependencies
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

    def export_to_dot(self, graphs: DependencyGraphs, level: str = "pipeline") -> str:
        """
        Export dependency graph to DOT format.

        Args:
            graphs: Dependency graphs
            level: Graph level to export ("action", "flowgroup", or "pipeline")

        Returns:
            DOT format string representation of the graph
        """
        graph = graphs.get_graph_by_level(level)

        # Use networkx built-in DOT export with custom attributes
        dot_lines = [f"digraph {level}_dependencies {{"]
        dot_lines.append("  rankdir=LR;")
        dot_lines.append("  node [shape=box];")

        # Add nodes with attributes
        for node in graph.nodes():
            node_attrs = graph.nodes[node]
            label = node

            # Add additional info to label based on level
            if level == "pipeline" and "flowgroup_count" in node_attrs:
                label = f"{node}\\n({node_attrs['flowgroup_count']} flowgroups)"
            elif level == "flowgroup" and "action_count" in node_attrs:
                label = f"{node}\\n({node_attrs['action_count']} actions)"

            dot_lines.append(f'  "{node}" [label="{label}"];')

        # Add edges
        for source, target in graph.edges():
            dot_lines.append(f'  "{source}" -> "{target}";')

        dot_lines.append("}")

        return "\n".join(dot_lines)

    def export_to_json(self, result: DependencyAnalysisResult) -> Dict[str, Any]:
        """
        Export dependency analysis to structured JSON format.

        Args:
            result: Complete dependency analysis result

        Returns:
            Structured dictionary suitable for JSON serialization
        """
        return {
            "metadata": {
                "total_pipelines": result.total_pipelines,
                "total_external_sources": result.total_external_sources,
                "total_stages": len(result.execution_stages),
                "has_circular_dependencies": len(result.circular_dependencies) > 0,
            },
            "pipelines": {
                name: {
                    "depends_on": dep.depends_on,
                    "flowgroup_count": dep.flowgroup_count,
                    "action_count": dep.action_count,
                    "external_sources": dep.external_sources,
                    "can_run_parallel": dep.can_run_parallel,
                    "stage": dep.stage,
                }
                for name, dep in result.pipeline_dependencies.items()
            },
            "execution_stages": result.execution_stages,
            "external_sources": result.external_sources,
            "circular_dependencies": result.circular_dependencies,
        }

    # Placeholder methods for future advanced features
    def get_critical_path(self, graphs: DependencyGraphs) -> List[str]:
        """TODO: Find longest dependency chain through the pipeline graph."""
        # Implementation: nx.dag_longest_path(graphs.pipeline_graph)
        raise NotImplementedError("Critical path analysis - Phase 2")

    def get_parallelization_opportunities(
        self, graphs: DependencyGraphs
    ) -> Dict[str, Any]:
        """TODO: Identify pipelines that can run in parallel."""
        # Implementation: Use topological_generations for parallel groups
        raise NotImplementedError("Parallelization analysis - Phase 2")

    def get_centrality_metrics(self, graphs: DependencyGraphs) -> Dict[str, float]:
        """TODO: Identify hub pipelines with high dependency count."""
        # Implementation: nx.betweenness_centrality(graphs.pipeline_graph)
        raise NotImplementedError("Centrality analysis - Phase 3")

    # Private helper methods

    def get_flowgroups(self, pipeline_filter: Optional[str] = None) -> List[FlowGroup]:
        """Get flowgroups, optionally filtered by pipeline."""
        if self._flowgroups is None:
            # Discover raw flowgroups with their file paths
            raw_flowgroups_with_paths = (
                self.flowgroup_discoverer.discover_all_flowgroups_with_paths()
            )

            # Process flowgroups (expand templates, apply presets) and build file path mapping
            processed_flowgroups = []
            for fg, yaml_file_path in raw_flowgroups_with_paths:
                try:
                    # Create a basic substitution manager (without env-specific values)
                    from ...utils.substitution import EnhancedSubstitutionManager

                    substitution_mgr = EnhancedSubstitutionManager(
                        substitution_file=None,  # No substitution file needed for dependency analysis
                        env="dev",  # Use a default env
                        skip_validation=True,  # Skip unresolved token validation for dependency analysis
                    )

                    # Process the flowgroup (expand templates, apply presets)
                    processed_fg = self.flowgroup_processor.process_flowgroup(
                        fg, substitution_mgr
                    )
                    processed_flowgroups.append(processed_fg)

                    # Store the file path mapping for this flowgroup
                    self._flowgroup_file_paths[processed_fg.flowgroup] = yaml_file_path
                except Exception as e:
                    # If processing fails (e.g., template/preset errors), use raw flowgroup
                    self.logger.warning(
                        f"Could not process flowgroup {fg.flowgroup}: {e}"
                    )
                    processed_flowgroups.append(fg)
                    # Store the file path mapping for the raw flowgroup
                    self._flowgroup_file_paths[fg.flowgroup] = yaml_file_path

            self._flowgroups = processed_flowgroups

        if pipeline_filter:
            return [fg for fg in self._flowgroups if fg.pipeline == pipeline_filter]
        return self._flowgroups

    def _create_empty_graphs(self) -> DependencyGraphs:
        """Create empty dependency graphs."""
        return DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

    def _build_action_graph(self, flowgroups: List[FlowGroup]) -> nx.DiGraph:
        """Build action-level dependency graph."""
        graph = nx.DiGraph()
        target_to_action = defaultdict(
            list
        )  # Map of target -> list of action_names for dependency resolution

        # First pass: collect all actions and their targets
        for flowgroup in flowgroups:
            for action in flowgroup.actions:
                action_id = f"{flowgroup.flowgroup}.{action.name}"

                # Add node with metadata
                graph.add_node(
                    action_id,
                    type=action.type.value,
                    flowgroup=flowgroup.flowgroup,
                    pipeline=flowgroup.pipeline,
                    target=action.target,
                    action_name=action.name,
                )

                # Track targets for dependency resolution
                if action.target:
                    target_to_action[action.target].append(action_id)

                # Track write action outputs (catalog.schema.table format)
                if action.type == ActionType.WRITE and action.write_target:
                    if isinstance(action.write_target, dict):
                        catalog = action.write_target.get("catalog", "")
                        schema = action.write_target.get("schema", "")
                        table = action.write_target.get("table", "")
                        if catalog and schema and table:
                            produced_table = f"{catalog}.{schema}.{table}"
                            target_to_action[produced_table].append(action_id)
                    # Handle WriteTarget object as well
                    elif hasattr(action.write_target, "catalog") and hasattr(
                        action.write_target, "table"
                    ):
                        catalog = getattr(action.write_target, "catalog", "")
                        schema = getattr(action.write_target, "schema", "")
                        table = getattr(action.write_target, "table", "")
                        if catalog and schema and table:
                            produced_table = f"{catalog}.{schema}.{table}"
                            target_to_action[produced_table].append(action_id)

        # Second pass: build dependencies based on source/target relationships
        for flowgroup in flowgroups:
            for action in flowgroup.actions:
                action_id = f"{flowgroup.flowgroup}.{action.name}"
                sources = self._extract_action_sources(action, flowgroup.flowgroup)

                for source in sources:
                    if source in target_to_action:
                        # Internal dependency - create edges to ALL actions that produce this source
                        for source_action_id in target_to_action[source]:
                            graph.add_edge(
                                source_action_id, action_id, dependency_type="internal"
                            )
                    else:
                        # External dependency - add as node attribute
                        if "external_sources" not in graph.nodes[action_id]:
                            graph.nodes[action_id]["external_sources"] = []
                        graph.nodes[action_id]["external_sources"].append(source)

        return graph

    def _build_flowgroup_graph(
        self, flowgroups: List[FlowGroup], action_graph: nx.DiGraph
    ) -> nx.DiGraph:
        """Build flowgroup-level dependency graph derived from action dependencies."""
        graph = nx.DiGraph()

        # Add flowgroup nodes
        for flowgroup in flowgroups:
            action_count = len(flowgroup.actions)
            external_sources = set()

            # Collect external sources from all actions in this flowgroup
            for action in flowgroup.actions:
                action_id = f"{flowgroup.flowgroup}.{action.name}"
                if action_id in action_graph.nodes:
                    node_external_sources = action_graph.nodes[action_id].get(
                        "external_sources", []
                    )
                    external_sources.update(node_external_sources)

            graph.add_node(
                flowgroup.flowgroup,
                pipeline=flowgroup.pipeline,
                action_count=action_count,
                external_sources=list(external_sources),
            )

        # Add dependencies between flowgroups based on action dependencies
        flowgroup_deps = set()

        for source_action, target_action in action_graph.edges():
            source_fg = action_graph.nodes[source_action]["flowgroup"]
            target_fg = action_graph.nodes[target_action]["flowgroup"]

            # Only add dependency if flowgroups are different (avoid self-loops)
            if source_fg != target_fg:
                flowgroup_deps.add((source_fg, target_fg))

        # Add edges to graph
        for source_fg, target_fg in flowgroup_deps:
            graph.add_edge(source_fg, target_fg, dependency_type="flowgroup")

        return graph

    def _build_pipeline_graph(
        self, flowgroups: List[FlowGroup], flowgroup_graph: nx.DiGraph
    ) -> nx.DiGraph:
        """Build pipeline-level dependency graph derived from flowgroup dependencies."""
        graph = nx.DiGraph()

        # Add pipeline nodes
        pipeline_info = defaultdict(
            lambda: {"flowgroups": 0, "actions": 0, "external_sources": set()}
        )

        for flowgroup in flowgroups:
            pipeline = flowgroup.pipeline
            pipeline_info[pipeline]["flowgroups"] += 1
            pipeline_info[pipeline]["actions"] += len(flowgroup.actions)

            # Collect external sources from flowgroup
            if flowgroup.flowgroup in flowgroup_graph.nodes:
                fg_external_sources = flowgroup_graph.nodes[flowgroup.flowgroup].get(
                    "external_sources", []
                )
                pipeline_info[pipeline]["external_sources"].update(fg_external_sources)

        # Add pipeline nodes with metadata
        for pipeline, info in pipeline_info.items():
            graph.add_node(
                pipeline,
                flowgroup_count=info["flowgroups"],
                action_count=info["actions"],
                external_sources=list(info["external_sources"]),
            )

        # Add dependencies between pipelines based on flowgroup dependencies
        pipeline_deps = set()

        for source_fg, target_fg in flowgroup_graph.edges():
            source_pipeline = flowgroup_graph.nodes[source_fg]["pipeline"]
            target_pipeline = flowgroup_graph.nodes[target_fg]["pipeline"]

            # Only add dependency if pipelines are different (avoid self-loops)
            if source_pipeline != target_pipeline:
                pipeline_deps.add((source_pipeline, target_pipeline))

        # Add edges to graph
        for source_pipeline, target_pipeline in pipeline_deps:
            graph.add_edge(source_pipeline, target_pipeline, dependency_type="pipeline")

        return graph

    def _write_target_as_dict(self, action: Action) -> Optional[Dict[str, Any]]:
        """Normalize ``action.write_target`` to a ``dict`` for uniform lookup.

        Returns ``None`` when the action has no write target. Handles both the
        Pydantic ``WriteTarget`` form and a raw dict (some code paths pre-dump
        it before reaching the analyzer).
        """
        wt = getattr(action, "write_target", None)
        if wt is None:
            return None
        if isinstance(wt, dict):
            return wt
        # Pydantic model — serialize to a plain dict for uniform key lookup.
        return wt.model_dump()

    def _iter_sql_bodies(
        self, action: Action
    ) -> Iterator[Tuple[Optional[str], Optional[str]]]:
        """Yield ``(inline_sql, sql_path)`` for every known SQL location.

        Covers:
          - ``action.sql`` / ``action.sql_path``
          - ``action.source["sql"]`` / ``action.source["sql_path"]`` (when
            ``source["type"] == "sql"``)
          - ``write_target["sql"]`` / ``write_target["sql_path"]``
            (materialized-view SQL)

        Either element of a yielded tuple may be ``None``; callers should
        treat each independently.
        """
        yield (
            getattr(action, "sql", None),
            getattr(action, "sql_path", None),
        )

        source = getattr(action, "source", None)
        if isinstance(source, dict) and source.get("type") == "sql":
            yield source.get("sql"), source.get("sql_path")

        wt = self._write_target_as_dict(action)
        if wt is not None:
            yield wt.get("sql"), wt.get("sql_path")

    def _iter_python_bodies(
        self, action: Action
    ) -> Iterator[Tuple[Optional[str], Optional[str]]]:
        """Yield ``(inline_python, file_path)`` for every known Python location.

        Covers:
          - top-level ``action.module_path`` (Python transforms / custom sources)
          - ``write_target["module_path"]`` (custom sinks)
          - ``write_target["batch_handler"]`` (inline ForEachBatch code)
          - ``write_target["snapshot_cdc_config"]["source_function"]["file"]``
            (CDC snapshot functions, existing behavior)
        """
        module_path = getattr(action, "module_path", None)
        if module_path:
            yield None, module_path

        wt = self._write_target_as_dict(action)
        if wt is None:
            return

        wt_module_path = wt.get("module_path")
        if wt_module_path:
            yield None, wt_module_path

        batch_handler = wt.get("batch_handler")
        if batch_handler:
            yield batch_handler, None

        cdc = wt.get("snapshot_cdc_config") or {}
        source_function = cdc.get("source_function") if isinstance(cdc, dict) else None
        if isinstance(source_function, dict):
            fn_file = source_function.get("file")
            if fn_file:
                yield None, fn_file

    def _resolve_and_parse_file(
        self,
        file_path_str: str,
        flowgroup_name: str,
        action: Action,
        parser_fn: Callable[[str], List[str]],
        file_type_label: str,
        code_number: str,
        path_context_key: str,
    ) -> List[str]:
        """Resolve a relative file path, read it, and parse via ``parser_fn``.

        Resolution order: first relative to the flowgroup YAML file (if the
        mapping is available), then relative to the project root. Both SQL
        and Python paths go through this — this matches the pre-refactor
        behavior for SQL and extends Python paths to also honor YAML-relative
        paths, which is consistent with how generators resolve
        ``write_target`` file references.

        Raises ``LHPError(IO, code_number)`` when no candidate resolves to an
        existing file. Parse-time failures log a warning and return ``[]``
        instead of raising (preserving pre-refactor behavior).
        """
        yaml_file_path = self._flowgroup_file_paths.get(flowgroup_name)

        candidate_paths: List[Path] = []
        if yaml_file_path is not None:
            candidate_paths.append(yaml_file_path.parent / file_path_str)
        candidate_paths.append(self.project_root / file_path_str)

        resolved_path = next((p for p in candidate_paths if p.exists()), None)

        if resolved_path is None:
            # Use the last candidate as the "expected" path in the error.
            reported_path = candidate_paths[-1]
            context: Dict[str, str] = {
                "Action": action.name,
                "Flowgroup": flowgroup_name,
                path_context_key: file_path_str,
                "Full Path": str(reported_path),
            }
            if yaml_file_path is not None:
                context["YAML File"] = str(yaml_file_path)
            else:
                context["Project Root"] = str(self.project_root)

            raise LHPError(
                category=ErrorCategory.IO,
                code_number=code_number,
                title=f"{file_type_label} file not found for action '{action.name}'",
                details=(
                    f"{file_type_label} file '{file_path_str}' referenced by "
                    f"action '{action.name}' does not exist."
                ),
                suggestions=[
                    f"Check that the {file_type_label} file exists at: {reported_path}",
                    f"Verify the {path_context_key.lower()} is correct "
                    f"relative to the YAML file or project root",
                    "Ensure the file has proper read permissions",
                ],
                context=context,
            )

        try:
            content = resolved_path.read_text(encoding="utf-8")
            parsed = list(parser_fn(content))
            self.logger.debug(
                f"Extracted {len(parsed)} sources from {file_type_label} file "
                f"{resolved_path} for {flowgroup_name}.{action.name}"
            )
            return parsed
        except Exception as e:
            self.logger.warning(
                f"Could not analyze {file_type_label} file {resolved_path} for "
                f"{flowgroup_name}.{action.name}: {e}"
            )
            return []

    def _extract_action_sources(self, action: Action, flowgroup_name: str) -> List[str]:
        """
        Extract source names from action.

        Precedence:
          1. SQL parsing — reliable; if it yields sources, return only those.
          2. Python parsing — best-effort; union parser output with explicit
             `source:` so users can patch unresolvable cases.
          3. Explicit source declaration (fallback).

        Args:
            action: The action to analyze
            flowgroup_name: Name of the flowgroup containing this action

        Returns:
            List of source table references
        """
        # Priority 1: SQL parsing is reliable — parser wins.
        sql_sources = self._extract_sql_sources(action, flowgroup_name)
        if sql_sources:
            self.logger.debug(
                f"Using {len(sql_sources)} SQL sources for {flowgroup_name}.{action.name}: {sql_sources}"
            )
            return sql_sources

        # Priority 2: Python parsing is best-effort — union with explicit source.
        python_sources = self._extract_python_sources(action, flowgroup_name)
        if python_sources:
            explicit = extract_action_sources(action)
            merged = sorted(set(python_sources) | set(explicit))
            self.logger.debug(
                f"Using {len(merged)} Python sources for {flowgroup_name}.{action.name} "
                f"(parser: {python_sources}, explicit: {explicit})"
            )
            return merged

        # Priority 3: Fall back to shared explicit source extraction
        sources = extract_action_sources(action)

        if sources:
            self.logger.debug(
                f"Using {len(sources)} explicit sources for {flowgroup_name}.{action.name}: {sources}"
            )
        else:
            self.logger.debug(f"No sources found for {flowgroup_name}.{action.name}")

        return sources

    def _extract_sql_sources(self, action: Action, flowgroup_name: str) -> List[str]:
        """
        Extract table references from SQL content in actions.

        Iterates over every known SQL location (inline, sql_path, source dict,
        and write_target) and collects table references from each.

        Args:
            action: The action to analyze
            flowgroup_name: Name of the flowgroup containing this action

        Returns:
            List of table references found in SQL content
        """
        from ...utils.sql_parser import extract_tables_from_sql

        sources: List[str] = []
        for inline_sql, sql_path in self._iter_sql_bodies(action):
            if inline_sql:
                try:
                    parsed = extract_tables_from_sql(inline_sql)
                    sources.extend(parsed)
                    self.logger.debug(
                        f"Extracted {len(parsed)} sources from inline SQL in {flowgroup_name}.{action.name}"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Could not parse inline SQL in {flowgroup_name}.{action.name}: {e}"
                    )

            if sql_path:
                sources.extend(
                    self._resolve_and_parse_file(
                        sql_path,
                        flowgroup_name,
                        action,
                        extract_tables_from_sql,
                        file_type_label="SQL",
                        code_number="002",
                        path_context_key="SQL Path",
                    )
                )

        return sources

    def _extract_python_sources(self, action: Action, flowgroup_name: str) -> List[str]:
        """
        Extract table references from Python code in actions.

        Iterates over every known Python location and collects table references
        from each:
          - top-level ``action.module_path`` (Python transforms / custom sources)
          - ``write_target.module_path`` (custom sinks)
          - ``write_target.batch_handler`` (inline ForEachBatch Python)
          - ``write_target.snapshot_cdc_config.source_function.file``
            (CDC snapshot functions)

        Args:
            action: The action to analyze
            flowgroup_name: Name of the flowgroup containing this action

        Returns:
            List of table references found in Python code
        """
        from ...utils.python_parser import extract_tables_from_python

        sources: List[str] = []
        for inline_python, file_path in self._iter_python_bodies(action):
            if inline_python:
                try:
                    parsed = extract_tables_from_python(inline_python)
                    sources.extend(parsed)
                    self.logger.debug(
                        f"Extracted {len(parsed)} sources from inline Python in {flowgroup_name}.{action.name}"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Could not parse inline Python in {flowgroup_name}.{action.name}: {e}"
                    )

            if file_path:
                sources.extend(
                    self._resolve_and_parse_file(
                        file_path,
                        flowgroup_name,
                        action,
                        extract_tables_from_python,
                        file_type_label="Python",
                        code_number="003",
                        path_context_key="Module Path",
                    )
                )

        return sources

    def _is_external_source(self, source: str, all_targets: Set[str]) -> bool:
        """
        Check if a source is external using explicit tracking.

        A source is external if it's not produced by any action in the project.
        """
        return source not in all_targets

    def _build_metadata(
        self,
        flowgroups: List[FlowGroup],
        action_graph: nx.DiGraph,
        flowgroup_graph: nx.DiGraph,
        pipeline_graph: nx.DiGraph,
    ) -> Dict[str, Any]:
        """Build metadata about the dependency analysis."""
        return {
            "total_flowgroups": len(flowgroups),
            "total_actions": sum(len(fg.actions) for fg in flowgroups),
            "total_pipelines": len(set(fg.pipeline for fg in flowgroups)),
            "graph_sizes": {
                "actions": len(action_graph.nodes),
                "flowgroups": len(flowgroup_graph.nodes),
                "pipelines": len(pipeline_graph.nodes),
            },
            "edge_counts": {
                "action_dependencies": len(action_graph.edges),
                "flowgroup_dependencies": len(flowgroup_graph.edges),
                "pipeline_dependencies": len(pipeline_graph.edges),
            },
        }

    def _analyze_pipeline_dependencies(
        self, graphs: DependencyGraphs
    ) -> Dict[str, PipelineDependency]:
        """Analyze dependencies for each pipeline."""
        pipeline_deps = {}

        for pipeline in graphs.pipeline_graph.nodes():
            node_data = graphs.pipeline_graph.nodes[pipeline]

            # Get direct dependencies
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
            # Use topological generations to get stages of parallel execution
            return list(nx.topological_generations(pipeline_graph))
        except nx.NetworkXError as e:
            # This should not happen if circular dependencies are handled properly
            self.logger.error(f"Error in topological sorting: {e}")
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
                cycle_path = list(cycle_nodes) + [cycle_nodes[0]]
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

        # Collect from action graph
        for node in graphs.action_graph.nodes():
            node_external_sources = graphs.action_graph.nodes[node].get(
                "external_sources", []
            )
            external_sources.update(node_external_sources)

        return sorted(list(external_sources))

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
