"""Validator for test actions."""

from typing import List
from ...models.config import Action, TestActionType, ViolationAction
from ...utils.error_formatter import ErrorFormatter


class TestActionValidator:
    """Validator for test actions."""
    
    def __init__(self, action_registry, field_validator):
        """Initialize the test action validator.
        
        Args:
            action_registry: The action registry for checking valid generators
            field_validator: The field validator for strict field validation
        """
        self.action_registry = action_registry
        self.field_validator = field_validator
    
    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate test action configuration.
        
        Args:
            action: The test action to validate
            prefix: Prefix for error messages (e.g., "Action[0] 'test_name'")
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # 1. Validate test_type
        test_type = action.test_type
        if not test_type:
            errors.append(f"{prefix}: Test actions must have a 'test_type' field")
            return errors  # Can't continue without test_type
        
        # Check if test_type is valid
        if test_type not in [t.value for t in TestActionType]:
            valid_types = [t.value for t in TestActionType]
            errors.append(
                f"{prefix}: Invalid test_type '{test_type}'. Valid values are: {', '.join(valid_types)}"
            )
            return errors  # Can't continue with invalid test_type
        
        # 2. Validate on_violation if present
        if action.on_violation:
            if action.on_violation not in ['fail', 'warn', 'drop']:
                errors.append(
                    f"{prefix}: Invalid on_violation '{action.on_violation}'. Valid values are: fail, warn, drop"
                )
        
        # 3. Validate test type specific requirements
        errors.extend(self._validate_test_type_requirements(action, prefix, test_type))
        
        return errors
    
    def _validate_test_type_requirements(self, action: Action, prefix: str, test_type: str) -> List[str]:
        """Validate requirements specific to each test type.
        
        Args:
            action: The test action to validate
            prefix: Prefix for error messages
            test_type: The type of test being validated
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        if test_type == 'row_count':
            # Row count needs two sources to compare
            if not action.source:
                errors.append(f"{prefix}: Row count test requires 'source' field")
            elif not isinstance(action.source, list):
                errors.append(f"{prefix}: Row count test requires source to be a list of two tables")
            elif len(action.source) != 2:
                errors.append(f"{prefix}: Row count test requires exactly 2 sources to compare, got {len(action.source)}")
        
        elif test_type == 'uniqueness':
            # Uniqueness needs columns to check
            if not action.source:
                errors.append(f"{prefix}: Uniqueness test requires 'source' field")
            if not action.columns:
                errors.append(f"{prefix}: Uniqueness test requires 'columns' field specifying which columns to check")
        
        elif test_type == 'referential_integrity':
            # Referential integrity needs source, reference, and columns
            if not action.source:
                errors.append(f"{prefix}: Referential integrity test requires 'source' field")
            if not action.reference:
                errors.append(f"{prefix}: Referential integrity test requires 'reference' field")
            if not action.source_columns:
                errors.append(f"{prefix}: Referential integrity test requires 'source_columns' field")
            if not action.reference_columns:
                errors.append(f"{prefix}: Referential integrity test requires 'reference_columns' field")
        
        elif test_type == 'completeness':
            # Completeness needs source and required columns
            if not action.source:
                errors.append(f"{prefix}: Completeness test requires 'source' field")
            if not action.required_columns:
                errors.append(f"{prefix}: Completeness test requires 'required_columns' field")
        
        elif test_type == 'range':
            # Range needs column and at least one bound
            if not action.source:
                errors.append(f"{prefix}: Range test requires 'source' field")
            if not action.column:
                errors.append(f"{prefix}: Range test requires 'column' field")
            if action.min_value is None and action.max_value is None:
                errors.append(f"{prefix}: Range test requires at least one of 'min_value' or 'max_value'")
        
        elif test_type == 'schema_match':
            # Schema match needs source and reference
            if not action.source:
                errors.append(f"{prefix}: Schema match test requires 'source' field")
            if not action.reference:
                errors.append(f"{prefix}: Schema match test requires 'reference' field to compare schemas")
        
        elif test_type == 'all_lookups_found':
            # All lookups found needs source, lookup table, and columns
            if not action.source:
                errors.append(f"{prefix}: All lookups found test requires 'source' field")
            if not action.lookup_table:
                errors.append(f"{prefix}: All lookups found test requires 'lookup_table' field")
            if not action.lookup_columns:
                errors.append(f"{prefix}: All lookups found test requires 'lookup_columns' field")
            if not action.lookup_result_columns:
                errors.append(f"{prefix}: All lookups found test requires 'lookup_result_columns' field")
        
        elif test_type == 'custom_sql':
            # Custom SQL needs SQL and optionally expectations
            if not action.source and not action.sql:
                errors.append(f"{prefix}: Custom SQL test requires either 'source' or 'sql' field")
            if not action.sql:
                errors.append(f"{prefix}: Custom SQL test requires 'sql' field with the query")
        
        elif test_type == 'custom_expectations':
            # Custom expectations needs source and expectations
            if not action.source:
                errors.append(f"{prefix}: Custom expectations test requires 'source' field")
            if not action.expectations:
                errors.append(f"{prefix}: Custom expectations test requires 'expectations' field")
        
        return errors

