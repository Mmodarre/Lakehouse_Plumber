"""Table creation validation for write actions."""

from typing import List, Dict, Any, Optional, Union
from collections import defaultdict
from ...models.config import FlowGroup, Action, ActionType, WriteTargetType


class TableCreationValidator:
    """Validator for table creation rules across flowgroups."""

    def validate(self, flowgroups: List[FlowGroup]) -> List[str]:
        """Validate table creation rules across the entire pipeline.

        Rules:
        1. Each streaming table must have exactly one creator (create_table: true)
        2. All other actions writing to the same table must have create_table: false

        Args:
            flowgroups: List of all flowgroups in the pipeline

        Returns:
            List of validation error messages
        """
        errors = []

        # Track table creators and users
        table_creators = defaultdict(list)  # table_name -> List[creator_action_info]
        table_users = defaultdict(list)  # table_name -> List[user_action_info]

        # Collect all write actions across flowgroups
        for flowgroup in flowgroups:
            for action in flowgroup.actions:
                if action.type == ActionType.WRITE and action.write_target:
                    # Get full table name
                    table_name = self._get_full_table_name(action.write_target)
                    if not table_name:
                        continue  # Skip if we can't determine table name

                    # Check if this action creates the table
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

        # Validate rules
        all_tables = set(table_creators.keys()) | set(table_users.keys())

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

                # Create a proper LHPError for multiple table creators
                from ...utils.error_formatter import LHPError, ErrorCategory

                # Build example configuration string
                db_name = table_name.split('.')[0]
                table_part = table_name.split('.')[1]
                example_text = (
                    "Fix by updating your configuration:\n\n"
                    "# Table Creator (keeps create_table: true)\n"
                    f"- name: {creators[0]['action']}\n"
                    "  type: write\n"
                    "  source: v_source_data\n"
                    "  write_target:\n"
                    "    type: streaming_table\n"
                    f'    database: "{db_name}"\n'
                    f'    table: "{table_part}"\n'
                    "    create_table: true    # ← Only ONE action should have this\n\n"
                    "# Table Users (set create_table: false)\n"
                    f"- name: {creators[1]['action']}\n"
                    "  type: write\n"
                    "  source: v_other_data\n"
                    "  write_target:\n"
                    "    type: streaming_table\n"
                    f'    database: "{db_name}"\n'
                    f'    table: "{table_part}"\n'
                    "    create_table: false   # ← All others should have this"
                )

                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="004",
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
        """Extract the full table name from write target configuration."""
        if isinstance(write_target, dict):
            database = write_target.get("database")
            table = write_target.get("table") or write_target.get("name")
        else:
            database = write_target.database
            table = write_target.table

        if not database or not table:
            return None

        return f"{database}.{table}"

    def _action_creates_table(self, action: Action) -> bool:
        """Check if an action creates the table (create_table: true)."""
        if not action.write_target:
            return False

        # MaterializedView uses @dp.materialized_view() decorator, so it always creates its own table
        if isinstance(action.write_target, dict):
            write_type = action.write_target.get("type")
            if write_type == "materialized_view":
                return True

            # CDC modes always create their own tables
            mode = action.write_target.get("mode", "standard")
            if mode in ["cdc", "snapshot_cdc"]:
                return True
            return action.write_target.get("create_table", True)
        else:
            # For WriteTarget objects, check type first
            if action.write_target.type == WriteTargetType.MATERIALIZED_VIEW:
                return True

            # CDC modes always create their own tables
            mode = getattr(action.write_target, "mode", "standard")
            if mode in ["cdc", "snapshot_cdc"]:
                return True
            return action.write_target.create_table
