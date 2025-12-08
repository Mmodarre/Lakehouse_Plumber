"""Configuration validator for LakehousePlumber."""

from __future__ import annotations

import logging
from typing import List, Dict, Any, Optional, Union, TYPE_CHECKING

from collections import defaultdict

from ..models.config import FlowGroup, Action, ActionType, WriteTargetType
from .action_registry import ActionRegistry
from .dependency_resolver import DependencyResolver
from .config_field_validator import ConfigFieldValidator
from .validators import (
    LoadActionValidator,
    TransformActionValidator,
    WriteActionValidator,
    TestActionValidator,
    TableCreationValidator,
)

if TYPE_CHECKING:
    from ..models.config import WriteTarget


class ConfigValidator:
    """Validate LakehousePlumber configurations."""

    def __init__(self, project_root=None, project_config=None):
        self.logger = logging.getLogger(__name__)
        self.project_root = project_root
        self.project_config = project_config
        self.action_registry = ActionRegistry()
        self.dependency_resolver = DependencyResolver()
        self.field_validator = ConfigFieldValidator()

        # Initialize action validators
        self.load_validator = LoadActionValidator(
            self.action_registry, self.field_validator
        )
        self.transform_validator = TransformActionValidator(
            self.action_registry, self.field_validator, self.project_root, self.project_config
        )
        self.write_validator = WriteActionValidator(
            self.action_registry, self.field_validator, self.logger
        )
        self.test_validator = TestActionValidator(
            self.action_registry, self.field_validator
        )
        self.table_creation_validator = TableCreationValidator()

    def validate_flowgroup(self, flowgroup: FlowGroup) -> List[str]:
        """Validate flowgroups and actions.

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
        """Validate action types and required fields.

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

        elif action.type == ActionType.TEST:
            errors.extend(self.test_validator.validate(action, prefix))

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

        Delegates to TableCreationValidator for the actual validation logic.

        Args:
            flowgroups: List of all flowgroups in the pipeline

        Returns:
            List of validation error messages
        """
        return self.table_creation_validator.validate(flowgroups)

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

