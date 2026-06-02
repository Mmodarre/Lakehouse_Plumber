"""Table creation validation for write actions."""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

from lhp.models import Action, ActionType, FlowGroup, WriteTargetType

logger = logging.getLogger(__name__)


def action_creates_table(action: Action) -> bool:
    """Return True if this write action creates its target table.

    Public-API counterpart to ``TableCreationValidator._action_creates_table``;
    used by the codegen ``grouping`` module to pick the table-creator action
    in a combined-write group without reaching into the validator's private
    surface.

    Rules (identical to the private predicate):
    - ``MaterializedView`` write targets always create their table.
    - ``snapshot_cdc`` mode always creates its table.
    - Otherwise, falls back to the explicit ``create_table`` field
      (default True if absent).
    """
    if not action.write_target:
        return False

    if isinstance(action.write_target, dict):
        write_type = action.write_target.get("type")
        if write_type == "materialized_view":
            return True
        mode = action.write_target.get("mode", "standard")
        if mode == "snapshot_cdc":
            return True
        return action.write_target.get("create_table", True)
    else:
        if action.write_target.type == WriteTargetType.MATERIALIZED_VIEW:
            return True
        mode = getattr(action.write_target, "mode", "standard")
        if mode == "snapshot_cdc":
            return True
        return action.write_target.create_table


class TableCreationValidator:
    def validate(self, flowgroups: List[FlowGroup]) -> List[str]:
        """Validate table creation rules across the entire pipeline.

        Rules:
        1. Each streaming table must have exactly one creator (create_table: true)
        2. All other actions writing to the same table must have create_table: false
        """
        logger.debug(
            f"Validating table creation rules across {len(flowgroups)} flowgroup(s)"
        )
        errors = []

        table_creators = defaultdict(list)  # table_name -> List[creator_action_info]
        table_users = defaultdict(list)  # table_name -> List[user_action_info]

        for flowgroup in flowgroups:
            for action in flowgroup.actions:
                if action.type == ActionType.WRITE and action.write_target:
                    table_name = self._get_full_table_name(action.write_target)
                    if not table_name:
                        continue

                    creates_table = self._action_creates_table(action)

                    action_info = {
                        "flowgroup": flowgroup.flowgroup,
                        "action": action.name,
                        "table": table_name,
                    }

                    if creates_table:
                        table_creators[table_name].append(action_info)
                    else:
                        table_users[table_name].append(action_info)

        all_tables = set(table_creators.keys()) | set(table_users.keys())
        logger.debug(f"Found {len(all_tables)} unique table(s) across write actions")

        for table_name in all_tables:
            creators = table_creators.get(table_name, [])
            users = table_users.get(table_name, [])

            # Rule 1: Each table must have exactly one creator
            if len(creators) == 0:
                user_list = [f"{u['flowgroup']}.{u['action']}" for u in users]
                errors.append(
                    f"Table '{table_name}' has no creator. "
                    f"One action must have 'create_table: true'. "
                    f"Used by: {', '.join(user_list)}"
                )
            elif len(creators) > 1:
                creator_names = [f"{c['flowgroup']}.{c['action']}" for c in creators]

                from ....errors import ErrorFactory, codes

                parts = table_name.split(".")
                catalog_name = parts[0] if len(parts) >= 3 else ""
                schema_name = parts[1] if len(parts) >= 3 else parts[0]
                table_part = parts[2] if len(parts) >= 3 else parts[-1]
                example_text = (
                    "Fix by updating your configuration:\n\n"
                    "# Table Creator (keeps create_table: true)\n"
                    f"- name: {creators[0]['action']}\n"
                    "  type: write\n"
                    "  source: v_source_data\n"
                    "  write_target:\n"
                    "    type: streaming_table\n"
                    f'    catalog: "{catalog_name}"\n'
                    f'    schema: "{schema_name}"\n'
                    f'    table: "{table_part}"\n'
                    "    create_table: true    # ← Only ONE action should have this\n\n"
                    "# Table Users (set create_table: false)\n"
                    f"- name: {creators[1]['action']}\n"
                    "  type: write\n"
                    "  source: v_other_data\n"
                    "  write_target:\n"
                    "    type: streaming_table\n"
                    f'    catalog: "{catalog_name}"\n'
                    f'    schema: "{schema_name}"\n'
                    f'    table: "{table_part}"\n'
                    "    create_table: false   # ← All others should have this"
                )

                raise ErrorFactory.config_error(
                    codes.CFG_004,
                    title=f"Multiple table creators detected: '{table_name}'",
                    details=f"Table '{table_name}' has multiple actions with 'create_table: true'. Only one action can create a table.",
                    suggestions=[
                        "Choose one action to create the table (keep 'create_table: true')",
                        "Set 'create_table: false' for all other actions writing to this table",
                        "Use the Append Flow API for actions that don't create the table",
                        "Consider using different table names if actions need separate tables",
                    ],
                    example=example_text,
                    context={
                        "Table Name": table_name,
                        "Conflicting Actions": creator_names,
                        "Total Creators": len(creators),
                        "Total Users": len(users),
                        "Flowgroups": list(set(c["flowgroup"] for c in creators)),
                    },
                )

            # Rule 2: All other actions must be users (create_table: false)
            # This is implicitly validated by the separation above

        return errors

    def _get_full_table_name(
        self, write_target: Union[Dict[str, Any], Any]
    ) -> Optional[str]:
        if isinstance(write_target, dict):
            catalog = write_target.get("catalog")
            schema = write_target.get("schema")
            table = write_target.get("table") or write_target.get("name")
        else:
            catalog = getattr(write_target, "catalog", None)
            schema = getattr(write_target, "schema", None)
            table = write_target.table

        if not catalog or not schema or not table:
            return None

        return f"{catalog}.{schema}.{table}"

    def _action_creates_table(self, action: Action) -> bool:
        """Back-compat forwarder to module-level ``action_creates_table``."""
        return action_creates_table(action)
