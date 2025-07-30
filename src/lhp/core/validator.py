"""Configuration validator for LakehousePlumber."""

from __future__ import annotations

import logging
from typing import List, Dict, Any, Optional, Union, TYPE_CHECKING

from collections import defaultdict

from ..models.config import FlowGroup, Action, ActionType, WriteTargetType
from .action_registry import ActionRegistry
from .dependency_resolver import DependencyResolver
from .config_field_validator import ConfigFieldValidator
from .action_validators import (
    LoadActionValidator,
    TransformActionValidator,
    WriteActionValidator,
)

if TYPE_CHECKING:
    from ..models.config import WriteTarget


class ConfigValidator:
    """Validate LakehousePlumber configurations."""

    def __init__(self, project_root=None):
        self.logger = logging.getLogger(__name__)
        self.project_root = project_root
        self.action_registry = ActionRegistry()
        self.dependency_resolver = DependencyResolver()
        self.field_validator = ConfigFieldValidator()

        # Initialize action validators
        self.load_validator = LoadActionValidator(
            self.action_registry, self.field_validator
        )
        self.transform_validator = TransformActionValidator(
            self.action_registry, self.field_validator, self.project_root
        )
        self.write_validator = WriteActionValidator(
            self.action_registry, self.field_validator, self.logger
        )

    def validate_flowgroup(self, flowgroup: FlowGroup) -> List[str]:
        """Step 4.4.2: Validate flowgroups and actions.

        Args:
            flowgroup: FlowGroup to validate

        Returns:
            List of validation error messages
        """
        errors = []

        # Validate basic fields
        if not flowgroup.pipeline:
            errors.append("FlowGroup must have a 'pipeline' name")

        if not flowgroup.flowgroup:
            errors.append("FlowGroup must have a 'flowgroup' name")

        if not flowgroup.actions:
            errors.append("FlowGroup must have at least one action")

        # Validate each action
        action_names = set()
        target_names = set()

        for i, action in enumerate(flowgroup.actions):
            action_errors = self.validate_action(action, i)
            errors.extend(action_errors)

            # Check for duplicate action names
            if action.name in action_names:
                errors.append(f"Duplicate action name: '{action.name}'")
            action_names.add(action.name)

            # Check for duplicate target names
            if action.target and action.target in target_names:
                errors.append(
                    f"Duplicate target name: '{action.target}' in action '{action.name}'"
                )
            if action.target:
                target_names.add(action.target)

        # Validate dependencies
        if flowgroup.actions:
            try:
                dependency_errors = self.dependency_resolver.validate_relationships(
                    flowgroup.actions
                )
                errors.extend(dependency_errors)
            except Exception as e:
                errors.append(str(e))

        # Validate template usage
        if flowgroup.use_template and not flowgroup.template_parameters:
            self.logger.warning(
                f"FlowGroup uses template '{flowgroup.use_template}' but no parameters provided"
            )

        return errors

    def validate_action(self, action: Action, index: int) -> List[str]:
        """Step 4.4.2: Validate action types and required fields.

        Args:
            action: Action to validate
            index: Action index in the flowgroup

        Returns:
            List of validation error messages
        """
        errors = []
        prefix = f"Action[{index}] '{action.name}'"

        # Basic validation
        if not action.name:
            errors.append(f"Action[{index}]: Missing 'name' field")
            return errors  # Can't continue without name

        if not action.type:
            errors.append(f"{prefix}: Missing 'type' field")
            return errors  # Can't continue without type

        # Strict field validation - validate action-level fields
        try:
            action_dict = action.model_dump()
            self.field_validator.validate_action_fields(action_dict, action.name)
        except Exception as e:
            errors.append(str(e))
            return errors  # Stop validation if field validation fails

        # Type-specific validation using action validators
        if action.type == ActionType.LOAD:
            errors.extend(self.load_validator.validate(action, prefix))

        elif action.type == ActionType.TRANSFORM:
            errors.extend(self.transform_validator.validate(action, prefix))

        elif action.type == ActionType.WRITE:
            errors.extend(self.write_validator.validate(action, prefix))

        else:
            errors.append(f"{prefix}: Unknown action type '{action.type}'")

        return errors

    def validate_action_references(self, actions: List[Action]) -> List[str]:
        """Validate that all action references are valid."""
        errors = []

        # Build set of all available views/targets
        available_views = set()
        for action in actions:
            if action.target:
                available_views.add(action.target)

        # Check all references
        for action in actions:
            sources = self._extract_all_sources(action)
            for source in sources:
                # Skip external sources
                if not source.startswith("v_") and "." in source:
                    continue  # Likely an external table like bronze.customers

                if source.startswith("v_") and source not in available_views:
                    errors.append(
                        f"Action '{action.name}' references view '{source}' which is not defined"
                    )

        return errors

    def _extract_all_sources(self, action: Action) -> List[str]:
        """Extract all source references from an action."""
        sources = []

        if isinstance(action.source, str):
            sources.append(action.source)
        elif isinstance(action.source, list):
            sources.extend(action.source)
        elif isinstance(action.source, dict):
            # Check various fields that might contain source references
            for field in ["view", "source", "views", "sources"]:
                value = action.source.get(field)
                if isinstance(value, str):
                    sources.append(value)
                elif isinstance(value, list):
                    sources.extend(value)

        return sources

    def validate_table_creation_rules(self, flowgroups: List[FlowGroup]) -> List[str]:
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
                from ..utils.error_formatter import LHPError, ErrorCategory

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
                    example=f"""Fix by updating your configuration:

# Table Creator (keeps create_table: true)
- name: {creators[0]['action']}
  type: write
  source: v_source_data
  write_target:
    type: streaming_table
    database: "{table_name.split('.')[0]}"
    table: "{table_name.split('.')[1]}"
    create_table: true    # ← Only ONE action should have this

# Table Users (set create_table: false)
- name: {creators[1]['action']}
  type: write
  source: v_other_data
  write_target:
    type: streaming_table
    database: "{table_name.split('.')[0]}"
    table: "{table_name.split('.')[1]}"
    create_table: false   # ← All others should have this""",
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

    def validate_duplicate_pipeline_flowgroup(self, flowgroups: List[FlowGroup]) -> List[str]:
        """Validate that there are no duplicate pipeline+flowgroup combinations.
        
        Args:
            flowgroups: List of all flowgroups to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        seen_combinations = set()
        
        for flowgroup in flowgroups:
            # Create a unique key from pipeline and flowgroup
            combination_key = f"{flowgroup.pipeline}.{flowgroup.flowgroup}"
            
            if combination_key in seen_combinations:
                errors.append(
                    f"Duplicate pipeline+flowgroup combination: '{combination_key}'. "
                    f"Each pipeline+flowgroup combination must be unique across all YAML files."
                )
            else:
                seen_combinations.add(combination_key)
                
        return errors

    def _get_full_table_name(
        self, write_target: Union[Dict[str, Any], WriteTarget]
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

        # MaterializedView uses @dlt.table() decorator, so it always creates its own table
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
