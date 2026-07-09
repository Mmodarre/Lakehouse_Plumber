"""Dependency graph builder: graph construction from a given flowgroup set.

Owns the construction pipeline that turns a *given* flowgroup set (plus the
file-path index that maps each flowgroup to its source YAML) into a
``DependencyGraphs`` triple (action / flowgroup / pipeline). Discovery,
processing, and blueprint view-mode live on the
:class:`DependencyAnalysisService` composition root; the builder is handed
``(flowgroups, file_paths)`` and never touches the filesystem to discover them.

Per-action source extraction (parsing SQL/Python bodies into upstream table
references) lives in :mod:`.source_parsing`; ``_build_action_graph``
constructs a :class:`SourceParser` over the *passed-in* file-path index and
delegates to it.
"""

import logging
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx

from lhp.models import FlowGroup

from ...models.dependencies import (
    AffectedAction,
    DependencyGraphs,
    DependencyWarning,
)
from ._parse_cache import ParseCache
from ._producers import build_producer_indexes, match_table_producers
from .source_parsing import SourceParser


def _aggregate_warnings(
    leaves: List[DependencyWarning],
) -> List[DependencyWarning]:
    """Aggregate leaf advisories by warning SITE.

    One unresolvable read lives at exactly one source location, but every
    action referencing that file re-parses it and re-emits the leaf — a
    shared helper can fan a single site out to thousands of near-identical
    records. Grouping by ``(code, file_path, line, message)`` folds each
    site back to ONE record carrying the distinct affected actions (sorted
    by flowgroup/action; the first becomes the record's representative
    ``flowgroup``/``action``/``edit_yaml_path``) and their count. Site
    order follows first appearance, so output is deterministic.
    """
    grouped: Dict[
        Tuple[str, Optional[str], Optional[int], str], List[DependencyWarning]
    ] = {}
    for leaf in leaves:
        key = (leaf.code, leaf.file_path, leaf.line, leaf.message)
        grouped.setdefault(key, []).append(leaf)

    aggregated: List[DependencyWarning] = []
    for group in grouped.values():
        affected = sorted(
            {
                AffectedAction(
                    flowgroup=w.flowgroup,
                    action=w.action,
                    edit_yaml_path=w.edit_yaml_path,
                )
                for w in group
            },
            key=lambda a: (a.flowgroup, a.action),
        )
        representative = affected[0]
        aggregated.append(
            replace(
                group[0],
                flowgroup=representative.flowgroup,
                action=representative.action,
                edit_yaml_path=representative.edit_yaml_path,
                affected_actions=tuple(affected),
                affected_count=len(affected),
            )
        )
    return aggregated


class DependencyGraphBuilder:
    """Build action/flowgroup/pipeline dependency graphs from a flowgroup set.

    Owns the source-extraction + graph construction pipeline only. The
    caller (the :class:`DependencyAnalysisService` composition root)
    performs discovery/processing and threads the resulting
    ``(flowgroups, file_paths)`` into :meth:`build_from_flowgroups`.
    """

    def __init__(self, project_root: Path) -> None:
        """Wire up the builder with the project root.

        Args:
            project_root: Root directory of the LakehousePlumber project,
                used as the fallback resolution base for the
                :class:`SourceParser`.
        """
        self.project_root = project_root
        # Per-builder read/parse memo: a helper file referenced by N actions
        # is read + parsed once, not N times (content-keyed, so sharing it
        # across this builder's builds stays correct).
        self._parse_cache = ParseCache()
        self.logger = logging.getLogger(__name__)

    def build_from_flowgroups(
        self, flowgroups: List[FlowGroup], file_paths: Dict[str, Path]
    ) -> DependencyGraphs:
        """Build the action/flowgroup/pipeline graph triple from a given set.

        Args:
            flowgroups: The (already discovered + processed) flowgroups to
                build the graph from.
            file_paths: Mapping of flowgroup name -> the YAML file it was
                discovered in, used by the :class:`SourceParser` to resolve
                relative file references. Produced by the service's
                discovery pass and threaded in here.
        """
        self.logger.info("Building dependency graphs...")

        if not flowgroups:
            self.logger.warning("No flowgroups found for analysis")
            return self._create_empty_graphs()

        action_graph, extraction_warnings = self._build_action_graph(
            flowgroups, file_paths
        )
        flowgroup_graph = self._build_flowgroup_graph(flowgroups, action_graph)
        pipeline_graph = self._build_pipeline_graph(flowgroups, flowgroup_graph)
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
            extraction_warnings=extraction_warnings,
        )

    def _create_empty_graphs(self) -> DependencyGraphs:
        """Create empty dependency graphs."""
        return DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        )

    def _build_action_graph(
        self, flowgroups: List[FlowGroup], file_paths: Dict[str, Path]
    ) -> Tuple[nx.DiGraph, List[DependencyWarning]]:
        """Build the action-level dependency graph + stamped extraction warnings."""
        graph = nx.DiGraph()
        # View targets are pipeline-scoped to mirror Lakeflow runtime semantics:
        # `action.target` produces a temporary view visible only within its own
        # pipeline. Keying on `(pipeline, target)` prevents same-named views in
        # sibling pipelines (common with blueprint expansion) from generating
        # phantom cross-pipeline edges. Views are NOT canonicalized and are NOT
        # subject to the 2-part<->3-part table reconciliation below: they are
        # pipeline-scoped names, not global catalog objects.
        view_target_to_action: Dict[Tuple[str, str], List[str]] = defaultdict(list)

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

                if action.target:
                    view_target_to_action[(flowgroup.pipeline, action.target)].append(
                        action_id
                    )

        # CHOKE POINT (i) PRODUCER REGISTRATION lives in `_producers`. It owns
        # the canonical global table indexes (ref canonicalization + delta
        # sink `options.tableName` destinations). Views above stay pipeline-
        # scoped here; tables are global.
        table_producers, table_short_to_catalogs = build_producer_indexes(flowgroups)

        # The source parser resolves relative file references against the
        # passed-in file-path index. The service populates that index during
        # its discovery pass (get_flowgroups) and threads it in here; an empty
        # mapping silently yields project-root-only resolution (see
        # source_parsing docstring).
        #
        # NOTE: substitution tokens are intentionally NOT resolved here. A
        # source written as a literal `${catalog}.schema.table` cannot match a
        # producer whose write_target resolved to a concrete catalog — this is a
        # documented authoring constraint, NOT fixed by ref canonicalization.
        source_parser = SourceParser(
            file_paths, self.project_root, parse_cache=self._parse_cache
        )
        extraction_warnings: List[DependencyWarning] = []

        for flowgroup in flowgroups:
            for action in flowgroup.actions:
                action_id = f"{flowgroup.flowgroup}.{action.name}"
                action_sources = source_parser.extract_action_sources(
                    action, flowgroup.flowgroup
                )
                extraction_warnings.extend(action_sources.warnings)

                for source in action_sources.sources:
                    # CHOKE POINT (ii) EDGE-MATCH LOOKUP. View path first
                    # (pipeline-scoped, raw key, NO canonicalization); fall back
                    # to the global, canonicalized table path.
                    producers = view_target_to_action.get(
                        (flowgroup.pipeline, source)
                    ) or match_table_producers(
                        source, table_producers, table_short_to_catalogs
                    )

                    if producers:
                        for source_action_id in producers:
                            graph.add_edge(
                                source_action_id, action_id, dependency_type="internal"
                            )
                    else:
                        if "external_sources" not in graph.nodes[action_id]:
                            graph.nodes[action_id]["external_sources"] = []
                        graph.nodes[action_id]["external_sources"].append(source)

        return graph, _aggregate_warnings(extraction_warnings)

    def _build_flowgroup_graph(
        self, flowgroups: List[FlowGroup], action_graph: nx.DiGraph
    ) -> nx.DiGraph:
        """Build flowgroup-level dependency graph derived from action dependencies."""
        graph = nx.DiGraph()

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
                external_sources=sorted(external_sources),
            )

        flowgroup_deps = set()

        for source_action, target_action in action_graph.edges():
            source_fg = action_graph.nodes[source_action]["flowgroup"]
            target_fg = action_graph.nodes[target_action]["flowgroup"]

            if source_fg != target_fg:
                flowgroup_deps.add((source_fg, target_fg))

        for source_fg, target_fg in sorted(flowgroup_deps):
            graph.add_edge(source_fg, target_fg, dependency_type="flowgroup")

        return graph

    def _build_pipeline_graph(
        self, flowgroups: List[FlowGroup], flowgroup_graph: nx.DiGraph
    ) -> nx.DiGraph:
        """Build pipeline-level dependency graph derived from flowgroup dependencies."""
        graph = nx.DiGraph()

        pipeline_info = defaultdict(
            lambda: {"flowgroups": 0, "actions": 0, "external_sources": set()}
        )

        for flowgroup in flowgroups:
            pipeline = flowgroup.pipeline
            pipeline_info[pipeline]["flowgroups"] += 1
            pipeline_info[pipeline]["actions"] += len(flowgroup.actions)

            if flowgroup.flowgroup in flowgroup_graph.nodes:
                fg_external_sources = flowgroup_graph.nodes[flowgroup.flowgroup].get(
                    "external_sources", []
                )
                pipeline_info[pipeline]["external_sources"].update(fg_external_sources)

        for pipeline, info in pipeline_info.items():
            graph.add_node(
                pipeline,
                flowgroup_count=info["flowgroups"],
                action_count=info["actions"],
                external_sources=sorted(info["external_sources"]),
            )

        pipeline_deps = set()

        for source_fg, target_fg in flowgroup_graph.edges():
            source_pipeline = flowgroup_graph.nodes[source_fg]["pipeline"]
            target_pipeline = flowgroup_graph.nodes[target_fg]["pipeline"]

            if source_pipeline != target_pipeline:
                pipeline_deps.add((source_pipeline, target_pipeline))

        for source_pipeline, target_pipeline in sorted(pipeline_deps):
            graph.add_edge(source_pipeline, target_pipeline, dependency_type="pipeline")

        return graph

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
            "total_pipelines": len({fg.pipeline for fg in flowgroups}),
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
