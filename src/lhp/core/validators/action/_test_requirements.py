"""Test-type-specific requirement validation for test actions.

Module-level function split out of :class:`TestActionValidator` to keep
that class small (constitution §3.3).
"""

import logging
from typing import List

from lhp.models import Action

from ._name_checks import require_equal_length, require_three_part_name

logger = logging.getLogger(__name__)


def validate_test_type_requirements(
    action: Action, prefix: str, test_type: str
) -> List[str]:
    """Validate requirements specific to each test type."""
    logger.debug(
        f"Validating test type '{test_type}' requirements for action '{action.name}'"
    )
    errors = []

    if test_type == "row_count":
        if not action.source:
            errors.append(f"{prefix}: Row count test requires 'source' field")
        elif not isinstance(action.source, list):
            errors.append(
                f"{prefix}: Row count test requires source to be a list of two tables"
            )
        elif len(action.source) != 2:
            errors.append(
                f"{prefix}: Row count test requires exactly 2 sources to compare, got {len(action.source)}"
            )

    elif test_type == "uniqueness":
        if not action.source:
            errors.append(f"{prefix}: Uniqueness test requires 'source' field")
        if not action.columns:
            errors.append(
                f"{prefix}: Uniqueness test requires 'columns' field specifying which columns to check"
            )

    elif test_type == "referential_integrity":
        if not action.source:
            errors.append(
                f"{prefix}: Referential integrity test requires 'source' field"
            )
        if not action.reference:
            errors.append(
                f"{prefix}: Referential integrity test requires 'reference' field"
            )
        else:
            err = require_three_part_name(action.reference, "'reference'", prefix)
            if err:
                errors.append(err)
        if not action.source_columns:
            errors.append(
                f"{prefix}: Referential integrity test requires 'source_columns' field"
            )
        if not action.reference_columns:
            errors.append(
                f"{prefix}: Referential integrity test requires 'reference_columns' field"
            )
        err = require_equal_length(
            action.source_columns,
            action.reference_columns,
            "'source_columns'",
            "'reference_columns'",
            prefix,
        )
        if err:
            errors.append(err)

    elif test_type == "completeness":
        if not action.source:
            errors.append(f"{prefix}: Completeness test requires 'source' field")
        if not action.required_columns:
            errors.append(
                f"{prefix}: Completeness test requires 'required_columns' field"
            )

    elif test_type == "range":
        if not action.source:
            errors.append(f"{prefix}: Range test requires 'source' field")
        if not action.column:
            errors.append(f"{prefix}: Range test requires 'column' field")
        if action.min_value is None and action.max_value is None:
            errors.append(
                f"{prefix}: Range test requires at least one of 'min_value' or 'max_value'"
            )

    elif test_type == "schema_match":
        if not action.source:
            errors.append(f"{prefix}: Schema match test requires 'source' field")
        else:
            err = require_three_part_name(action.source, "'source'", prefix)
            if err:
                errors.append(err)
        if not action.reference:
            errors.append(
                f"{prefix}: Schema match test requires 'reference' field to compare schemas"
            )
        else:
            err = require_three_part_name(action.reference, "'reference'", prefix)
            if err:
                errors.append(err)

    elif test_type == "all_lookups_found":
        if not action.source:
            errors.append(f"{prefix}: All lookups found test requires 'source' field")
        if not action.lookup_table:
            errors.append(
                f"{prefix}: All lookups found test requires 'lookup_table' field"
            )
        else:
            err = require_three_part_name(action.lookup_table, "'lookup_table'", prefix)
            if err:
                errors.append(err)
        if not action.lookup_columns:
            errors.append(
                f"{prefix}: All lookups found test requires 'lookup_columns' field"
            )
        if not action.lookup_result_columns:
            errors.append(
                f"{prefix}: All lookups found test requires 'lookup_result_columns' field"
            )
        err = require_equal_length(
            action.lookup_columns,
            action.lookup_result_columns,
            "'lookup_columns'",
            "'lookup_result_columns'",
            prefix,
        )
        if err:
            errors.append(err)

    elif test_type == "custom_sql":
        if not action.source and not action.sql:
            errors.append(
                f"{prefix}: Custom SQL test requires either 'source' or 'sql' field"
            )
        if not action.sql:
            errors.append(
                f"{prefix}: Custom SQL test requires 'sql' field with the query"
            )

    elif test_type == "custom_expectations":
        if not action.source:
            errors.append(f"{prefix}: Custom expectations test requires 'source' field")
        if not action.expectations:
            errors.append(
                f"{prefix}: Custom expectations test requires 'expectations' field"
            )

    return errors
