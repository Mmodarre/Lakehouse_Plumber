"""Dependency resolution for LakehousePlumber actions."""

import logging
from typing import List, Dict, Tuple, Optional
from collections import defaultdict, deque
from ..models.config import Action, ActionType
from ..utils.error_formatter import LHPError, ErrorCategory


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

        # Check if there are self-contained snapshot CDC actions that provide data
        has_self_contained_snapshot_cdc = any(
            self._is_self_contained_snapshot_cdc(action)
            for action in actions
        )

        # Test-only flowgroups are allowed (for data quality testing)
        is_test_only_flowgroup = test_actions and not (load_actions or write_actions)
        
        if not is_test_only_flowgroup:
            if not load_actions and not has_self_contained_snapshot_cdc:
                errors.append("FlowGroup must have at least one Load action")

            if not write_actions:
                errors.append("FlowGroup must have at least one Write action")

        # Check for orphaned actions (no dependencies and not depended upon)
        orphaned = self._find_orphaned_actions(actions, graph, targets)
        for action in orphaned:
            if action.type == ActionType.TRANSFORM:
                # Create a proper LHPError for orphaned transform actions
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="003",
                    title=f"Unused transform action: '{action.name}'",
                    details=f"Transform action '{action.name}' produces view '{action.target}' but no other action references it.",
                    suggestions=[
                        "Add a write action that uses this transform's output",
                        "Reference this view in another transform action's source",
                        "Remove this transform action if it's not needed",
                        "Check for typos in view names that reference this transform",
                    ],
                    example=f"""Fix this by adding a write action that uses the transform output:

actions:
  - name: {action.name}
    type: transform
    transform_type: sql
    source: v_source_data
    target: {action.target}   # ← This view is produced
    sql: |
      SELECT * FROM v_source_data WHERE active = true

  - name: write_result
    type: write
    source: {action.target}   # ← Reference the transform output here
    write_target:
      type: streaming_table
      database: "catalog.schema"
      table: result_table
      create_table: true

Or reference it in another transform:

  - name: further_transform
    type: transform
    transform_type: sql
    source: {action.target}   # ← Use as source for another transform
    target: v_final_result
    sql: |
      SELECT processed_field FROM {action.target}""",
                    context={
                        "Transform Action": action.name,
                        "Produced View": action.target,
                        "Transform Type": getattr(action, "transform_type", "unknown"),
                        "Available Actions": [a.name for a in actions],
                        "Actions that have sources": [
                            a.name for a in actions if self._get_action_sources(a)
                        ],
                    },
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

        Different action types have sources in different places:
        - Load: Usually no sources (external data)
        - Transform: source field contains view names
        - Write: source field contains view to write from
        - CDC Write Actions: source comes from CDC config, not action.source

        For CDC modes, the precedence order is:
        1. source_function (snapshot_cdc only) -> no external dependencies
        2. cdc_config.source / snapshot_cdc_config.source -> explicit CDC source
        3. action.source -> fallback for malformed CDC configs
        """
        sources = []

        # Special handling for CDC write actions
        if self._is_cdc_write_action(action):
            cdc_sources = self._extract_cdc_sources(action)
            if cdc_sources is not None:  # None means fallback to action.source
                return cdc_sources
        
        # Standard source extraction for all other cases
        if action.source:
            if isinstance(action.source, str):
                # Simple string source
                sources.append(action.source)
            elif isinstance(action.source, list):
                # List of sources
                for source in action.source:
                    if isinstance(source, str):
                        sources.append(source)
            elif isinstance(action.source, dict):
                # For dict sources, look for view/source keys
                if "view" in action.source:
                    sources.append(action.source["view"])
                elif "source" in action.source:
                    source_val = action.source["source"]
                    if isinstance(source_val, str):
                        sources.append(source_val)
                    elif isinstance(source_val, list):
                        sources.extend(source_val)
                # For transform actions with multiple sources
                elif "sources" in action.source:
                    sources.extend(action.source["sources"])

        return sources

    def _is_cdc_write_action(self, action: Action) -> bool:
        """Check if action is a CDC write action (cdc or snapshot_cdc mode)."""
        return (action.type == ActionType.WRITE and 
                action.write_target and 
                isinstance(action.write_target, dict) and
                action.write_target.get("mode") in ["cdc", "snapshot_cdc"])

    def _extract_cdc_sources(self, action: Action) -> List[str] | None:
        """Extract sources from CDC write actions.
        
        Returns:
            List of source names, empty list for self-contained actions,
            or None to fallback to action.source
        """
        mode = action.write_target.get("mode")
        
        if mode == "cdc":
            # For CDC mode, source comes from cdc_config
            cdc_config = action.write_target.get("cdc_config", {})
            if cdc_config.get("source"):
                return [cdc_config["source"]]
        
        elif mode == "snapshot_cdc":
            # For snapshot CDC mode, check source configuration precedence
            snapshot_config = action.write_target.get("snapshot_cdc_config", {})
            
            # Priority 1: source_function (self-contained, no external dependencies)
            if snapshot_config.get("source_function"):
                return []  # Source function is internal - no external dependencies
            
            # Priority 2: snapshot_cdc_config.source (explicit CDC source reference)
            elif snapshot_config.get("source"):
                return [snapshot_config["source"]]
        
        # No CDC-specific source found, fallback to action.source
        return None

    def _is_self_contained_snapshot_cdc(self, action: Action) -> bool:
        """Check if action is a self-contained snapshot CDC action with source_function.
        
        These actions provide their own data via source functions and don't require
        external load actions.
        """
        return (action.type == ActionType.WRITE and
                action.write_target and
                isinstance(action.write_target, dict) and
                action.write_target.get("mode") == "snapshot_cdc" and
                action.write_target.get("snapshot_cdc_config", {}).get("source_function"))

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
            raise ValueError(f"Circular dependency detected involving: {unprocessed}")

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
