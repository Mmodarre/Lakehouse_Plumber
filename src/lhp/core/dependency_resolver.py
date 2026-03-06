"""Dependency resolution for LakehousePlumber actions."""

import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple

from ..models.config import Action, ActionType
from ..utils.error_formatter import (
    ErrorCategory,
    LHPConfigError,
    LHPValidationError,
)
from ..utils.source_extractor import extract_action_sources, is_cdc_write_action


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
        """Validate action relationships - check for cycles, missing sources.

        Args:
            actions: List of actions to validate

        Returns:
            List of validation error messages
        """
        self.logger.debug(f"Validating relationships for {len(actions)} action(s)")
        errors = []

        # Build graphs
        graph, targets = self._build_dependency_graph(actions)

        # Check for missing dependencies
        for action in actions:
            sources = self._get_action_sources(action)
            for source in sources:
                if source not in targets:
                    # Check if source is an external table/view (not produced by any action)
                    if not self._is_external_source(source, targets):
                        errors.append(
                            f"Action '{action.name}' depends on '{source}' which is not produced by any action"
                        )

        # Add cycle detection
        cycle = self._detect_cycle(graph)
        if cycle:
            errors.append(f"Circular dependency detected: {' -> '.join(cycle)}")

        # Validate relationships
        # Validate action type constraints
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

        # Check for orphaned actions (no dependencies and not depended upon)
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
        graph = defaultdict(list)  # action_name -> [dependent_action_names]
        targets = {}  # target_name -> action

        # Build targets map - what each action produces
        for action in actions:
            if action.target:
                targets[action.target] = action

        self.logger.debug(
            f"Dependency graph: {len(targets)} target(s), {len(actions)} action(s)"
        )
        # Build dependency graph - who depends on whom
        for action in actions:
            sources = self._get_action_sources(action)
            for source in sources:
                if source in targets:
                    source_action = targets[source]
                    # source_action must run before action
                    graph[source_action.name].append(action.name)

        return dict(graph), targets

    def _get_action_sources(self, action: Action) -> List[str]:
        """Extract source names from action.

        Delegates to the shared extract_action_sources() function in source_extractor.py.
        """
        return extract_action_sources(action)

    def _is_self_contained_snapshot_cdc(self, action: Action) -> bool:
        """Check if action is a self-contained snapshot CDC action with source_function.

        These actions provide their own data via source functions and don't require
        external load actions. Uses the shared is_cdc_write_action check, then
        verifies the snapshot_cdc source_function configuration.
        """
        if not is_cdc_write_action(action):
            return False
        if action.write_target.get("mode") != "snapshot_cdc":
            return False
        snapshot_config = action.write_target.get("snapshot_cdc_config", {})
        return bool(snapshot_config.get("source_function"))

    def _is_self_contained_materialized_view(self, action: Action) -> bool:
        """Check if action is a self-contained materialized view with SQL.

        Materialized views that define their own SQL query (inline or via sql_path)
        don't require external load actions since they are self-contained.
        """
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
        """Perform topological sort of actions.

        Uses Kahn's algorithm for topological sorting.
        """
        # Create action name to action mapping
        action_map = {action.name: action for action in actions}

        # Calculate in-degrees (number of dependencies)
        in_degree = {action.name: 0 for action in actions}

        # Count dependencies
        for action_name in graph:
            for dependent in graph[action_name]:
                if dependent in in_degree:
                    in_degree[dependent] += 1

        # Initialize queue with actions that have no dependencies
        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            current = queue.popleft()
            result.append(action_map[current])

            # Reduce in-degree of dependent actions
            for dependent in graph.get(current, []):
                if dependent in in_degree:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        # Check if all actions were processed
        if len(result) != len(actions):
            unprocessed = [name for name, degree in in_degree.items() if degree > 0]
            cycle_visual = " -> ".join(unprocessed + [unprocessed[0]])
            raise LHPValidationError(
                category=ErrorCategory.DEPENDENCY,
                code_number="001",
                title="Circular dependency detected",
                details=f"Circular dependency detected involving: {unprocessed}\n\n{cycle_visual}",
                suggestions=[
                    "Review the dependency chain and remove one of the dependencies",
                    "Consider splitting complex transformations into separate stages",
                    "Use materialized views to break dependency cycles",
                ],
                context={"Cycle": cycle_visual, "Components": ", ".join(unprocessed)},
            )

        self.logger.debug(f"Topological sort complete: {[a.name for a in result]}")
        return result

    def _detect_cycle(self, graph: Dict[str, List[str]]) -> Optional[List[str]]:
        """Detect cycles in dependency graph using DFS."""
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle
                elif neighbor in rec_stack:
                    # Found cycle
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]

            rec_stack.remove(node)
            path.pop()
            return None

        # Check all nodes
        for node in graph:
            if node not in visited:
                cycle = dfs(node)
                if cycle:
                    return cycle

        return None

    def _is_external_source(self, source: str, targets: Dict[str, Action]) -> bool:
        """Check if a source is external (not produced by any action).

        A source is external if it's not in the targets registry - meaning it's
        a database table, materialized view, or external view that exists outside
        the current flowgroup's actions.

        Args:
            source: The source name to check
            targets: Registry mapping target names to actions that produce them

        Returns:
            True if source is external (not produced by any action in this flowgroup)
        """
        return source not in targets

    def _find_orphaned_actions(
        self,
        actions: List[Action],
        graph: Dict[str, List[str]],
        targets: Dict[str, Action],
    ) -> List[Action]:
        """Find actions that are not connected to the dependency graph."""
        orphaned = []

        for action in actions:
            # Check if action produces anything used by others
            has_dependents = action.name in graph and len(graph[action.name]) > 0

            # Check if action depends on anything
            has_dependencies = len(self._get_action_sources(action)) > 0

            # Write actions don't need dependents
            if (
                not has_dependents
                and not has_dependencies
                and action.type != ActionType.WRITE
            ):
                orphaned.append(action)
            elif not has_dependents and action.type == ActionType.TRANSFORM:
                # Transform actions should always have dependents
                orphaned.append(action)

        return orphaned

    def get_execution_stages(self, actions: List[Action]) -> List[List[Action]]:
        """Group actions into execution stages based on dependencies.

        Actions in the same stage can be executed in parallel.

        Returns:
            List of stages, where each stage is a list of actions
        """
        # First, get ordered actions
        ordered_actions = self.resolve_dependencies(actions)

        # Build reverse dependency graph (who depends on me)
        graph, targets = self._build_dependency_graph(actions)
        reverse_graph = defaultdict(list)

        for action_name, dependents in graph.items():
            for dependent in dependents:
                reverse_graph[dependent].append(action_name)

        # Assign stages
        stages = []
        processed = set()

        for action in ordered_actions:
            if action.name in processed:
                continue

            # Find all actions that can run at this stage
            current_stage = []

            for candidate in ordered_actions:
                if candidate.name in processed:
                    continue

                # Check if all dependencies are processed
                dependencies = reverse_graph.get(candidate.name, [])
                if all(dep in processed for dep in dependencies):
                    current_stage.append(candidate)

            if current_stage:
                stages.append(current_stage)
                for action in current_stage:
                    processed.add(action.name)

        return stages
