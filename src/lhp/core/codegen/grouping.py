"""Write-action grouping: bucket by target table + synthesize combined actions.

:stability: internal
"""

from collections import defaultdict
from typing import Any, Dict, List, Union

from lhp.models import Action


def extract_source_views_from_action(source: Union[str, List, Dict]) -> List[str]:
    """Extract all source views from an action source configuration.

    For sources without explicit table names (e.g., Kafka topics), returns
    ``["source"]`` as a placeholder to maintain consistency in code generation.
    """
    if isinstance(source, str):
        return [source]
    if isinstance(source, list):
        result = []
        for item in source:
            if isinstance(item, str):
                result.append(item)
            elif isinstance(item, dict):
                catalog = item.get("catalog")
                schema = item.get("schema")
                table = item.get("table") or item.get("view") or item.get("name", "")
                if catalog and schema and table:
                    result.append(f"{catalog}.{schema}.{table}")
                elif table:
                    result.append(table)
            else:
                result.append(str(item))
        return result
    if isinstance(source, dict):
        catalog = source.get("catalog")
        schema = source.get("schema")
        table = source.get("table") or source.get("view") or source.get("name", "")
        if catalog and schema and table:
            return [f"{catalog}.{schema}.{table}"]
        if table:
            return [table]
        # Return generic "source" for non-table sources (e.g., Kafka, custom sources)
        # This prevents empty lists that could cause issues in code generation
        return ["source"]
    return ["source"]  # Fallback for unknown types


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
        grouped = defaultdict(list)

        for action in write_actions:
            target_config = action.write_target
            if not target_config:
                target_config = action.source if isinstance(action.source, dict) else {}

            catalog = target_config.get("catalog", "")
            schema = target_config.get("schema", "")
            table = target_config.get("table") or target_config.get("name", "")

            if catalog and schema and table:
                full_table_name = f"{catalog}.{schema}.{table}"
            elif table:
                full_table_name = table
            else:
                full_table_name = action.name

            grouped[full_table_name].append(action)

        return dict(grouped)

    def create_combined_write_action(
        self, actions: List[Action], target_table: str
    ) -> Action:
        # Uses the public free function (D3a Option A — promoted out of the
        # validator's private surface so codegen no longer reaches into it).
        from ..validators import action_creates_table  # lazy import, method scope

        table_creator = next(
            (action for action in actions if action_creates_table(action)),
            None,
        )
        if table_creator is None:
            table_creator = actions[0]

        action_metadata = []
        for action in actions:
            source_views_for_action = self._extract_source_views_from_action(
                action.source
            )

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

        combined_action = table_creator.model_copy(deep=True)

        object.__setattr__(combined_action, "_action_metadata", action_metadata)
        object.__setattr__(combined_action, "_table_creator", table_creator)

        return combined_action

    def _extract_source_views_from_action(self, source) -> List[str]:
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
