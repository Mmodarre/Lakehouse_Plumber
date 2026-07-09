"""Dependency resolution for LakehousePlumber actions."""

import logging
from collections import defaultdict
from typing import Dict, List, Tuple

import networkx as nx

from lhp.models import Action, ActionType

from ...errors import (
    ErrorFactory,
    codes,
)
from ._graph_ops import find_cycle, topological_generations
from .source_extractor import extract_action_sources, is_cdc_write_action


class DependencyResolver:
    """Resolve action dependencies and validate relationships."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def resolve_dependencies(self, actions: List[Action]) -> List[Action]:
        """Order actions based on dependencies using topological sort.

        Args:
            actions: List of actions to resolve dependencies for

        Returns:
            List of actions in dependency order

        Raises:
            ValueError: If circular dependencies detected
        """
        self.logger.debug(f"Resolving dependencies for {len(actions)} action(s)")
        # Build dependency graph
        graph, targets = self._build_dependency_graph(actions)

        # Implement topological sort
        return self._topological_sort(actions, graph, targets)

    def validate_relationships(self, actions: List[Action]) -> List[str]:
        """Validate action relationships - cycles, required actions, orphans.

        Sources not produced by any action are treated as legitimate external
        tables/views, so "missing source" is not detectable at this level.

        Args:
            actions: List of actions to validate

        Returns:
            List of validation error messages
        """
        self.logger.debug(f"Validating relationships for {len(actions)} action(s)")
        errors = []

        graph, targets = self._build_dependency_graph(actions)

        cycle = find_cycle(self._to_digraph(actions, graph))
        if cycle:
            errors.append(f"Circular dependency detected: {' -> '.join(cycle)}")

        load_actions = [a for a in actions if a.type == ActionType.LOAD]
        write_actions = [a for a in actions if a.type == ActionType.WRITE]
        test_actions = [a for a in actions if a.type == ActionType.TEST]

        # Check if there are self-contained actions that provide their own data
        has_self_contained_snapshot_cdc = any(
            self._is_self_contained_snapshot_cdc(action) for action in actions
        )
        has_self_contained_mv = any(
            self._is_self_contained_materialized_view(action) for action in actions
        )

        # Test-only flowgroups are allowed (for data quality testing)
        is_test_only_flowgroup = test_actions and not (load_actions or write_actions)

        if not is_test_only_flowgroup:
            if (
                not load_actions
                and not has_self_contained_snapshot_cdc
                and not has_self_contained_mv
            ):
                errors.append("FlowGroup must have at least one Load action")

            if not write_actions:
                errors.append("FlowGroup must have at least one Write action")

        orphaned = self._find_orphaned_actions(actions, graph, targets)
        for action in orphaned:
            if action.type == ActionType.TRANSFORM:
                # Append orphaned transform as error instead of raising
                errors.append(
                    f"Unused transform action: '{action.name}' produces view "
                    f"'{action.target}' but no other action references it"
                )

        return errors

    def _build_dependency_graph(
        self, actions: List[Action]
    ) -> Tuple[Dict[str, List[str]], Dict[str, Action]]:
        """Build dependency graph from actions.

        Returns:
            Tuple of (dependency graph, targets map)
            - graph: Maps action name to list of dependent action names
            - targets: Maps target name to action that produces it
        """
        graph = defaultdict(list)
        targets = {}

        for action in actions:
            if action.target:
                targets[action.target] = action

        self.logger.debug(
            f"Dependency graph: {len(targets)} target(s), {len(actions)} action(s)"
        )
        for action in actions:
            sources = self._get_action_sources(action)
            for source in sources:
                if source in targets:
                    source_action = targets[source]
                    graph[source_action.name].append(action.name)

        return dict(graph), targets

    def _get_action_sources(self, action: Action) -> List[str]:
        return extract_action_sources(action)

    def _is_self_contained_snapshot_cdc(self, action: Action) -> bool:
        """True when a snapshot CDC action provides its own data via source_function
        (no Load action required)."""
        if not is_cdc_write_action(action):
            return False
        if action.write_target.get("mode") != "snapshot_cdc":
            return False
        snapshot_config = action.write_target.get("snapshot_cdc_config", {})
        return bool(snapshot_config.get("source_function"))

    def _is_self_contained_materialized_view(self, action: Action) -> bool:
        """True when a materialized-view write action defines its own SQL (no Load action required)."""
        if action.type != ActionType.WRITE:
            return False
        wt = action.write_target
        if not isinstance(wt, dict):
            return False
        if wt.get("type") != "materialized_view":
            return False
        return bool(wt.get("sql") or wt.get("sql_path"))

    def _topological_sort(
        self,
        actions: List[Action],
        graph: Dict[str, List[str]],
        targets: Dict[str, Action],
    ) -> List[Action]:
        """Topological sort via the shared nx.DiGraph contract."""
        action_map = {action.name: action for action in actions}

        digraph = self._to_digraph(actions, graph)

        cycle = find_cycle(digraph)
        if cycle is not None:
            self._raise_circular_dependency(actions, digraph)

        result = [
            action_map[name]
            for generation in topological_generations(digraph)
            for name in generation
        ]

        self.logger.debug(f"Topological sort complete: {[a.name for a in result]}")
        return result

    def _to_digraph(
        self, actions: List[Action], graph: Dict[str, List[str]]
    ) -> "nx.DiGraph":
        """Build an nx.DiGraph (producer -> dependent) from the resolver adjacency.

        Every action is added as a node so isolated actions survive the sort.
        """
        digraph = nx.DiGraph()
        digraph.add_nodes_from(action.name for action in actions)
        for producer, dependents in graph.items():
            for dependent in dependents:
                digraph.add_edge(producer, dependent)
        return digraph

    def _raise_circular_dependency(
        self, actions: List[Action], digraph: "nx.DiGraph"
    ) -> None:
        """Raise DEP_001 with the residual cyclic node set (in input order)."""
        cyclic: set[str] = set()
        for component in nx.strongly_connected_components(digraph):
            if len(component) > 1:
                cyclic |= component
            else:
                (node,) = tuple(component)
                if digraph.has_edge(node, node):
                    cyclic.add(node)

        residual = set(cyclic)
        for node in cyclic:
            residual |= nx.descendants(digraph, node)

        unprocessed = [action.name for action in actions if action.name in residual]
        cycle_visual = " -> ".join([*unprocessed, unprocessed[0]])
        raise ErrorFactory.dependency_error(
            codes.DEP_001,
            title="Circular dependency detected",
            details=f"Circular dependency detected involving: {unprocessed}\n\n{cycle_visual}",
            suggestions=[
                "Review the dependency chain and remove one of the dependencies",
                "Consider splitting complex transformations into separate stages",
                "Use materialized views to break dependency cycles",
            ],
            context={"Cycle": cycle_visual, "Components": ", ".join(unprocessed)},
        )

    def _find_orphaned_actions(
        self,
        actions: List[Action],
        graph: Dict[str, List[str]],
        targets: Dict[str, Action],
    ) -> List[Action]:
        """Find actions that are not connected to the dependency graph."""
        orphaned = []

        for action in actions:
            has_dependents = action.name in graph and len(graph[action.name]) > 0
            has_dependencies = len(self._get_action_sources(action)) > 0

            if (
                not has_dependents
                and not has_dependencies
                and action.type != ActionType.WRITE
            ):
                orphaned.append(action)
            elif not has_dependents and action.type == ActionType.TRANSFORM:
                orphaned.append(action)

        return orphaned
