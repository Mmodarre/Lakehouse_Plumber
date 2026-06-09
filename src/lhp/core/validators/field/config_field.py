"""Strict field validation for configuration objects."""

import logging
from typing import Any, Dict

from ....errors import ErrorFactory, codes
from ._field_catalog import ACTION_FIELDS, LOAD_SOURCE_FIELDS, WRITE_TARGET_FIELDS
from ._field_suggestions import raise_unknown_fields_error

logger = logging.getLogger(__name__)


class ConfigFieldValidator:
    def __init__(self):
        self.load_source_fields = LOAD_SOURCE_FIELDS
        self.write_target_fields = WRITE_TARGET_FIELDS
        self.action_fields = ACTION_FIELDS

    def validate_load_source(
        self, source_config: Dict[str, Any], action_name: str
    ) -> None:
        logger.debug(f"Validating load source fields for action '{action_name}'")

        if not isinstance(source_config, dict):
            logger.debug(
                f"Skipping field validation for action '{action_name}': source is not a dict"
            )
            return

        source_type = source_config.get("type")
        if not source_type:
            logger.debug(
                f"Skipping field validation for action '{action_name}': no source type specified"
            )
            return  # No type specified, will be caught by other validation

        if source_type not in self.load_source_fields:
            logger.debug(
                f"Skipping field validation for action '{action_name}': unknown source type '{source_type}'"
            )
            return  # Unknown source type, will be caught by other validation

        expected_fields = self.load_source_fields[source_type]
        actual_fields = set(source_config.keys())
        unknown_fields = actual_fields - expected_fields

        if unknown_fields:
            logger.debug(
                f"Unknown fields detected in action '{action_name}' load source ({source_type}): {sorted(unknown_fields)}"
            )
            raise_unknown_fields_error(
                action_name=action_name,
                config_type=f"load source ({source_type})",
                unknown_fields=unknown_fields,
                expected_fields=expected_fields,
                config_section="source",
            )
        else:
            logger.debug(
                f"All fields valid for action '{action_name}' load source ({source_type})"
            )

    def validate_write_target(
        self, write_target: Dict[str, Any], action_name: str
    ) -> None:
        logger.debug(f"Validating write target fields for action '{action_name}'")

        if not isinstance(write_target, dict):
            logger.debug(
                f"Skipping field validation for action '{action_name}': write_target is not a dict"
            )
            return

        target_type = write_target.get("type")
        if not target_type:
            logger.debug(
                f"Skipping field validation for action '{action_name}': no target type specified"
            )
            return  # No type specified, will be caught by other validation

        if target_type not in self.write_target_fields:
            logger.debug(
                f"Skipping field validation for action '{action_name}': unknown target type '{target_type}'"
            )
            return  # Unknown target type, will be caught by other validation

        expected_fields = self.write_target_fields[target_type]
        actual_fields = set(write_target.keys())
        unknown_fields = actual_fields - expected_fields

        if unknown_fields:
            logger.debug(
                f"Unknown fields detected in action '{action_name}' write target ({target_type}): {sorted(unknown_fields)}"
            )
            raise_unknown_fields_error(
                action_name=action_name,
                config_type=f"write target ({target_type})",
                unknown_fields=unknown_fields,
                expected_fields=expected_fields,
                config_section="write_target",
            )
        else:
            logger.debug(
                f"All fields valid for action '{action_name}' write target ({target_type})"
            )

    def validate_action_fields(
        self, action_dict: Dict[str, Any], action_name: str
    ) -> None:
        logger.debug(f"Validating action-level fields for action '{action_name}'")
        actual_fields = set(action_dict.keys())
        unknown_fields = actual_fields - self.action_fields

        if unknown_fields:
            logger.debug(
                f"Unknown action-level fields in '{action_name}': {sorted(unknown_fields)}"
            )
            raise_unknown_fields_error(
                action_name=action_name,
                config_type="action",
                unknown_fields=unknown_fields,
                expected_fields=self.action_fields,
                config_section="action",
            )

        self._validate_depends_on(action_dict.get("depends_on"), action_name)

    def _validate_depends_on(self, depends_on: Any, action_name: str) -> None:
        """Validate the optional ``depends_on`` escape-hatch field's shape.

        Runs for every action type. ``None`` / ``[]`` are valid (no edges).
        Each declared entry must be a well-formed table reference: a non-empty
        string of at most three dot-separated parts
        (``catalog.schema.table`` / ``schema.table`` / ``table``) with no blank
        parts. Malformed entries raise ``LHP-VAL-063`` via the field-validator
        path, matching :func:`raise_unknown_fields_error`'s convention.
        """
        if not depends_on:
            return

        if not isinstance(depends_on, list):
            self._raise_depends_on_error(
                action_name,
                f"'depends_on' must be a list of table references, got {type(depends_on).__name__}",
                depends_on,
            )

        for entry in depends_on:
            if not isinstance(entry, str) or not entry.strip():
                self._raise_depends_on_error(
                    action_name,
                    "each 'depends_on' entry must be a non-empty string",
                    entry,
                )

            parts = entry.split(".")
            if len(parts) > 3 or any(not part.strip() for part in parts):
                self._raise_depends_on_error(
                    action_name,
                    (
                        "each 'depends_on' entry must be a table reference of at "
                        "most three dot-separated parts "
                        "(catalog.schema.table, schema.table, or table) with no "
                        "blank parts"
                    ),
                    entry,
                )

    def _raise_depends_on_error(
        self, action_name: str, reason: str, entry: Any
    ) -> None:
        """Raise a config error for a malformed ``depends_on`` entry (LHP-VAL-063)."""
        logger.debug(
            f"Invalid depends_on entry in action '{action_name}': {entry!r} ({reason})"
        )
        raise ErrorFactory.config_error(
            codes.VAL_063,
            title=f"Invalid 'depends_on' in action '{action_name}'",
            details=f"Action '{action_name}' has an invalid 'depends_on' entry: {reason}.",
            suggestions=[
                "Use a fully-qualified table reference: catalog.schema.table",
                "Or a two-part schema.table / single-part table name",
                "Remove empty strings and entries with blank dotted parts",
            ],
            example="depends_on:\n  - my_catalog.my_schema.my_table\n  - my_schema.my_table",
            context={
                "Action": action_name,
                "Field": "depends_on",
                "Invalid Entry": repr(entry),
            },
        )
