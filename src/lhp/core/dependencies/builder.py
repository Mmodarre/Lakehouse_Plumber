"""Dependency graph builder: discovery, source extraction, graph construction.

Owns the construction pipeline that turns a project (or a given flowgroup
set) into a ``DependencyGraphs`` triple (action / flowgroup / pipeline).

The discovery + source-extraction sub-graph is intentionally co-located
with the graph-construction passes — see the ``# JUSTIFIED:`` block below
for the cohesion rationale.
"""

# JUSTIFIED: This module is ~770 lines because the graph construction pipeline
# is a single cohesive unit. `_build_action_graph` is a two-pass algorithm
# that (a) registers every action's target as a producer keyed by
# `(pipeline, target)` for views and `catalog.schema.table` for tables, then
# (b) resolves every action's sources to producers via the same maps. Both
# passes call `_extract_action_sources`, which dispatches to
# `_extract_sql_sources` / `_extract_python_sources`; those in turn drive
# `_iter_sql_bodies` / `_iter_python_bodies` over every SQL/Python location
# (inline `action.sql`, `sql_path`, `source["sql"]`, `write_target["sql"]`,
# `module_path`, `batch_handler`, `snapshot_cdc_config.source_function.file`)
# and feed the discovered paths through `_resolve_and_parse_file`, which
# resolves them against `_flowgroup_file_paths` — state populated by
# `_discover_and_process_all_flowgroups` / `_process_one`.
#
# Splitting source-extraction (`_iter_sql_bodies`, `_iter_python_bodies`,
# `_extract_sql_sources`, `_extract_python_sources`, `_resolve_and_parse_file`,
# `_write_target_as_dict`, `_extract_action_sources`) into its own module
# requires either duplicating the `_flowgroup_file_paths` dict + view-target
# lookup helpers, or threading them through every function as parameters —
# ~80L of glue with no offsetting clarity benefit. The discovery cluster
# (`get_flowgroups`, `_discover_and_process_all_flowgroups`, `_process_one`,
# `set_blueprint_view_mode`) shares the same `_flowgroups` / `_blueprint_*`
# state, so it cannot move out without the same trade-off.
#
# `_build_flowgroup_graph` / `_build_pipeline_graph` / `_build_metadata`
# (~115L combined) are downstream consumers of `_build_action_graph` output
# and only meaningful as a sequence; isolating them in a separate file
# would force the caller to make four imports for one logical step.
# TODO(Phase 9.5): extract source-extraction + discovery clusters into builder sub-modules once the file-path index can be passed as a parameter without N+1 glue; see LOCAL/REMAINING_WORK.md §9.5.

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

import networkx as nx

from lhp.models import Action, ActionType, FlowGroup, FlowGroupContext

from ...errors import ErrorCategory, LHPError
from ...models.dependencies import DependencyGraphs
from ...utils.source_extractor import extract_action_sources
from ..discovery.blueprint_discoverer import BlueprintDiscoverer
from ..discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from ..processing.blueprint_expander import BlueprintExpander, BlueprintProvenance
from ..processing.flowgroup_resolver import FlowgroupResolutionService


class DependencyGraphBuilder:
    """Build action/flowgroup/pipeline dependency graphs from flowgroups.

    Owns the discovery + processing + source-extraction + graph
    construction pipeline. Collaborators are injected by the
    composition root (``service.py``).
    """

    def __init__(
        self,
        project_root: Path,
        flowgroup_discoverer: FlowgroupDiscoveryService,
        blueprint_discoverer: BlueprintDiscoverer,
        blueprint_expander: BlueprintExpander,
        flowgroup_resolver: FlowgroupResolutionService,
    ) -> None:
        """Wire up the builder with its discovery/processing collaborators.

        Args:
            project_root: Root directory of the LakehousePlumber project
            flowgroup_discoverer: On-disk flowgroup discovery
            blueprint_discoverer: Blueprint + instance discovery
            blueprint_expander: Blueprint expansion (specs -> synthetic flowgroups)
            flowgroup_resolver: Template/preset resolution per flowgroup
        """
        self.project_root = project_root
        self.flowgroup_discoverer = flowgroup_discoverer
        self.blueprint_discoverer = blueprint_discoverer
        self.blueprint_expander = blueprint_expander
        self.flowgroup_processor = flowgroup_resolver
        self.logger = logging.getLogger(__name__)

        # View-mode controls: set via `set_blueprint_view_mode` and consumed
        # in get_flowgroups. Defaults to dedupe ON (one representative per
        # spec + count annotation) to keep 32k-flowgroup output usable.
        self._expand_blueprints_view: bool = False
        self._blueprint_filter: Optional[str] = None

        self._flowgroups: Optional[List[FlowGroup]] = None
        self._flowgroup_file_paths: Dict[str, Path] = {}
        self._blueprint_provenance: Dict[Tuple[str, str], BlueprintProvenance] = {}

    def build(self, pipeline_filter: Optional[str] = None) -> DependencyGraphs:
        """Discover flowgroups (optionally filtered) and build all three graphs.

        Args:
            pipeline_filter: Optional pipeline name to restrict the graph to.

        Returns:
            DependencyGraphs containing action/flowgroup/pipeline levels.
        """
        flowgroups = self.get_flowgroups(pipeline_filter)
        return self.build_from_flowgroups(flowgroups)

    def build_from_flowgroups(self, flowgroups: List[FlowGroup]) -> DependencyGraphs:
        """Build the action/flowgroup/pipeline graph triple from a given set.

        Skips discovery — used by the ABC-required ``build_graphs`` entry
        point on the service, which receives the flowgroup sequence
        directly.
        """
        self.logger.info("Building dependency graphs...")

        if not flowgroups:
            self.logger.warning("No flowgroups found for analysis")
            return self._create_empty_graphs()

        action_graph = self._build_action_graph(flowgroups)
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
        )

    def get_flowgroups(self, pipeline_filter: Optional[str] = None) -> List[FlowGroup]:
        """Get flowgroups, optionally filtered by pipeline.

        Includes synthetic flowgroups expanded from blueprints, deduplicated
        by `(blueprint_name, spec_index)` unless `set_blueprint_view_mode(
        expand=True)` was called. With a blueprint filter, only synthetics
        from that blueprint are emitted (and on-disk flowgroups are excluded).
        """
        if self._flowgroups is None:
            self._flowgroups = self._discover_and_process_all_flowgroups()

        flowgroups = self._flowgroups

        if self._blueprint_filter is not None:
            allowed_keys = {
                key
                for key, prov in self._blueprint_provenance.items()
                if prov.blueprint_name == self._blueprint_filter
            }
            flowgroups = [
                fg for fg in flowgroups if (fg.pipeline, fg.flowgroup) in allowed_keys
            ]
        elif not self._expand_blueprints_view and self._blueprint_provenance:
            # Dedupe synthetics by (blueprint_name, spec_index); first wins.
            # Non-synthetic flowgroups pass through.
            seen_specs: Set[Tuple[str, int]] = set()
            deduped: List[FlowGroup] = []
            for fg in flowgroups:
                prov = self._blueprint_provenance.get((fg.pipeline, fg.flowgroup))
                if prov is None:
                    deduped.append(fg)
                    continue
                spec_key = (prov.blueprint_name, prov.spec_index)
                if spec_key in seen_specs:
                    continue
                seen_specs.add(spec_key)
                deduped.append(fg)
            flowgroups = deduped

        if pipeline_filter:
            return [fg for fg in flowgroups if fg.pipeline == pipeline_filter]
        return flowgroups

    def _discover_and_process_all_flowgroups(self) -> List[FlowGroup]:
        """Discover flowgroups (on-disk + synthetic) and process them once.

        The substitution manager is constructed once and reused for every
        flowgroup; per-flowgroup processing errors fall back to the raw
        flowgroup so the dependency graph never crashes on a single bad file.
        """
        from ..processing.substitution import EnhancedSubstitutionManager

        substitution_mgr = EnhancedSubstitutionManager(
            substitution_file=None,
            env="dev",
            skip_validation=True,
        )

        processed: List[FlowGroup] = []
        for (
            fg,
            yaml_file_path,
        ) in self.flowgroup_discoverer.discover_all_flowgroups_with_paths():
            processed.append(self._process_one(fg, yaml_file_path, substitution_mgr))

        try:
            blueprints = self.blueprint_discoverer.discover_blueprints()
            if not blueprints:
                return processed
            instances = self.blueprint_discoverer.discover_instances(blueprints)
            if not instances:
                return processed
            synthetic_ctxs, provenance = self.blueprint_expander.expand(
                blueprints, instances
            )
            self._blueprint_provenance = provenance
            for ctx in synthetic_ctxs:
                fg = ctx.flowgroup
                bp_path = provenance[(fg.pipeline, fg.flowgroup)].blueprint_path
                processed.append(self._process_one(fg, bp_path, substitution_mgr))
        except Exception as e:
            # Blueprint expansion errors surface via `lhp validate` / `generate`;
            # `deps` is read-only and degrades gracefully rather than blocking.
            self.logger.warning(
                f"Blueprint expansion skipped during dependency analysis: {e}"
            )

        return processed

    def _process_one(
        self, fg: FlowGroup, file_path: Path, substitution_mgr
    ) -> FlowGroup:
        """Process one flowgroup; fall back to raw fg on template/preset error."""
        try:
            ctx_in = FlowGroupContext(flowgroup=fg, source_yaml=file_path)
            ctx_out = self.flowgroup_processor.process_flowgroup(
                ctx_in, substitution_mgr
            )
            processed_fg = ctx_out.flowgroup
            self._flowgroup_file_paths[processed_fg.flowgroup] = file_path
            return processed_fg
        except Exception as e:
            self.logger.warning(f"Could not process flowgroup {fg.flowgroup}: {e}")
            self._flowgroup_file_paths[fg.flowgroup] = file_path
            return fg

    def set_blueprint_view_mode(
        self,
        expand_blueprints: bool = False,
        blueprint: Optional[str] = None,
    ) -> None:
        """Configure how synthetic flowgroups are surfaced in the deps graph.

        Args:
            expand_blueprints: When True, emit one flowgroup per (blueprint x
                instance x spec) — the literal expansion. Default False
                dedupes by (blueprint_name, spec_index) for readable graphs at
                32k-flowgroup scale.
            blueprint: When set, restrict the graph to the named blueprint's
                flowgroups (still deduped unless expand_blueprints=True).
        """
        self._expand_blueprints_view = expand_blueprints
        self._blueprint_filter = blueprint
        # Invalidate cached flowgroup list so the new view is rebuilt next call
        # (cheap because the underlying discovery/expansion is also cached on
        # the discoverer's side — the cache invalidation here only affects the
        # processed/filtered list assembled in get_flowgroups).
        self._flowgroups = None
        self._flowgroup_file_paths = {}

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
        # View targets are pipeline-scoped to mirror Lakeflow runtime semantics:
        # `action.target` produces a temporary view visible only within its own
        # pipeline. Keying on `(pipeline, target)` prevents same-named views in
        # sibling pipelines (common with blueprint expansion) from generating
        # phantom cross-pipeline edges.
        view_target_to_action: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        table_target_to_action: Dict[str, List[str]] = defaultdict(list)

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

                if action.type == ActionType.WRITE and action.write_target:
                    if isinstance(action.write_target, dict):
                        catalog = action.write_target.get("catalog", "")
                        schema = action.write_target.get("schema", "")
                        table = action.write_target.get("table", "")
                        if catalog and schema and table:
                            produced_table = f"{catalog}.{schema}.{table}"
                            table_target_to_action[produced_table].append(action_id)
                    elif hasattr(action.write_target, "catalog") and hasattr(
                        action.write_target, "table"
                    ):
                        catalog = getattr(action.write_target, "catalog", "")
                        schema = getattr(action.write_target, "schema", "")
                        table = getattr(action.write_target, "table", "")
                        if catalog and schema and table:
                            produced_table = f"{catalog}.{schema}.{table}"
                            table_target_to_action[produced_table].append(action_id)

        for flowgroup in flowgroups:
            for action in flowgroup.actions:
                action_id = f"{flowgroup.flowgroup}.{action.name}"
                sources = self._extract_action_sources(action, flowgroup.flowgroup)

                for source in sources:
                    producers = view_target_to_action.get(
                        (flowgroup.pipeline, source)
                    ) or table_target_to_action.get(source)

                    if producers:
                        for source_action_id in producers:
                            graph.add_edge(
                                source_action_id, action_id, dependency_type="internal"
                            )
                    else:
                        if "external_sources" not in graph.nodes[action_id]:
                            graph.nodes[action_id]["external_sources"] = []
                        graph.nodes[action_id]["external_sources"].append(source)

        return graph

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
                external_sources=list(external_sources),
            )

        flowgroup_deps = set()

        for source_action, target_action in action_graph.edges():
            source_fg = action_graph.nodes[source_action]["flowgroup"]
            target_fg = action_graph.nodes[target_action]["flowgroup"]

            if source_fg != target_fg:
                flowgroup_deps.add((source_fg, target_fg))

        for source_fg, target_fg in flowgroup_deps:
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
                external_sources=list(info["external_sources"]),
            )

        pipeline_deps = set()

        for source_fg, target_fg in flowgroup_graph.edges():
            source_pipeline = flowgroup_graph.nodes[source_fg]["pipeline"]
            target_pipeline = flowgroup_graph.nodes[target_fg]["pipeline"]

            if source_pipeline != target_pipeline:
                pipeline_deps.add((source_pipeline, target_pipeline))

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
        """Precedence: SQL parsing (reliable, wins alone) > Python parsing (union with
        explicit source:) > explicit source declaration (fallback)."""
        sql_sources = self._extract_sql_sources(action, flowgroup_name)
        if sql_sources:
            self.logger.debug(
                f"Using {len(sql_sources)} SQL sources for {flowgroup_name}.{action.name}: {sql_sources}"
            )
            return sql_sources

        python_sources = self._extract_python_sources(action, flowgroup_name)
        if python_sources:
            explicit = extract_action_sources(action)
            merged = sorted(set(python_sources) | set(explicit))
            self.logger.debug(
                f"Using {len(merged)} Python sources for {flowgroup_name}.{action.name} "
                f"(parser: {python_sources}, explicit: {explicit})"
            )
            return merged

        sources = extract_action_sources(action)

        if sources:
            self.logger.debug(
                f"Using {len(sources)} explicit sources for {flowgroup_name}.{action.name}: {sources}"
            )
        else:
            self.logger.debug(f"No sources found for {flowgroup_name}.{action.name}")

        return sources

    def _extract_sql_sources(self, action: Action, flowgroup_name: str) -> List[str]:
        """Extract table references from every SQL location via ``_iter_sql_bodies``."""
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
        """Extract table references from every Python location via ``_iter_python_bodies``."""
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
