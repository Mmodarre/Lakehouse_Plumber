"""Write-action grouping: bucket by target table + synthesize combined actions.

:stability: internal
"""

from collections import defaultdict
from typing import Any, Dict, List

from lhp.models import Action
from ...utils.source_extractor import extract_source_views_from_action


class WriteActionGrouper:
    """Group write actions by target table and synthesize combined actions.

    The "combined" action carries per-flow append metadata in a private
    ``_action_metadata`` attribute on the Pydantic model so the streaming
    table template can emit one base table + N append flows.

    :stability: internal
    """

    def __init__(self) -> None:
        pass

    def group_write_actions_by_target(
        self, write_actions: List[Action]
    ) -> Dict[str, List[Action]]:
        """
        Group write actions by their target table.

        Args:
            write_actions: List of write actions

        Returns:
            Dictionary mapping target table names to lists of actions
        """
        grouped = defaultdict(list)

        for action in write_actions:
            target_config = action.write_target
            if not target_config:
                # Handle legacy structure
                target_config = action.source if isinstance(action.source, dict) else {}

            # Build full table name
            catalog = target_config.get("catalog", "")
            schema = target_config.get("schema", "")
            table = target_config.get("table") or target_config.get("name", "")

            if catalog and schema and table:
                full_table_name = f"{catalog}.{schema}.{table}"
            elif table:
                full_table_name = table
            else:
                # Use action name as fallback
                full_table_name = action.name

            grouped[full_table_name].append(action)

        return dict(grouped)

    def create_combined_write_action(
        self, actions: List[Action], target_table: str
    ) -> Action:
        """
        Create a combined write action with individual action metadata preserved.

        Args:
            actions: List of write actions targeting the same table
            target_table: Full target table name

        Returns:
            Combined action with individual action metadata
        """
        # Determine which action should create the table.
        # Uses the public free function (D3a Option A — promoted out of the
        # validator's private surface so codegen no longer reaches into it).
        from ..validators import action_creates_table  # lazy import, method scope

        table_creator = next(
            (action for action in actions if action_creates_table(action)),
            None,
        )
        if table_creator is None:
            table_creator = actions[0]

        # Build individual action metadata for each append flow
        action_metadata = []
        for action in actions:
            # Extract source views (can be multiple per action)
            source_views_for_action = self._extract_source_views_from_action(
                action.source
            )

            # Generate base flow name from action name
            base_flow_name = action.name.replace("-", "_").replace(" ", "_")
            if base_flow_name.startswith("write_"):
                base_flow_name = base_flow_name[6:]  # Remove "write_" prefix
            base_flow_name = (
                f"f_{base_flow_name}"
                if not base_flow_name.startswith("f_")
                else base_flow_name
            )

            # Per-flow CDC params (only populated for CDC-mode contributors)
            flow_cdc_config = self._build_flow_cdc_config(action)

            if len(source_views_for_action) > 1:
                # Multiple sources in this action: create separate append flow for each
                for i, source_view in enumerate(source_views_for_action):
                    flow_name = f"{base_flow_name}_{i + 1}"
                    action_metadata.append(
                        {
                            "action_name": f"{action.name}_{i + 1}",
                            "source_view": source_view,
                            "once": action.once or False,
                            "readMode": action.readMode,  # Preserve individual readMode
                            "flow_name": flow_name,
                            "description": action.description
                            or f"Append flow to {target_table} from {source_view}",
                            "flow_cdc_config": flow_cdc_config,
                        }
                    )
            else:
                # Single source in this action: create one append flow
                source_view = (
                    source_views_for_action[0] if source_views_for_action else ""
                )
                action_metadata.append(
                    {
                        "action_name": action.name,
                        "source_view": source_view,
                        "once": action.once or False,
                        "readMode": action.readMode,  # Preserve individual readMode
                        "flow_name": base_flow_name,
                        "description": action.description
                        or f"Append flow to {target_table}",
                        "flow_cdc_config": flow_cdc_config,
                    }
                )

        # Create combined action using the table creator as the base
        combined_action = table_creator.model_copy(deep=True)

        # Store metadata as private attribute (Pydantic compatible)
        object.__setattr__(combined_action, "_action_metadata", action_metadata)
        object.__setattr__(combined_action, "_table_creator", table_creator)

        return combined_action

    def _extract_source_views_from_action(self, source) -> List[str]:
        """
        Extract all source views from an action source configuration.

        Delegates to utility function for consistency across codebase.

        Args:
            source: Source configuration (string, list, or dict)

        Returns:
            List of source view names
        """
        return extract_source_views_from_action(source)

    def _build_flow_cdc_config(self, action: Action) -> Dict[str, Any]:
        """Build the per-flow CDC config dict for a contributing action.

        Returns an empty dict for non-CDC actions so the template can safely
        index into it without branching on mode.
        """
        wt = action.write_target
        if not isinstance(wt, dict) or wt.get("mode") != "cdc":
            return {}
        ac = wt.get("cdc_config", {}) or {}
        return {
            "ignore_null_updates": ac.get("ignore_null_updates"),
            "apply_as_deletes": ac.get("apply_as_deletes"),
            "apply_as_truncates": ac.get("apply_as_truncates"),
            "column_list": ac.get("column_list"),
            "except_column_list": ac.get("except_column_list"),
        }
