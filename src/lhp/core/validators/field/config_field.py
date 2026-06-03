"""Strict field validation for configuration objects."""

import logging
from typing import Any, Dict

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
