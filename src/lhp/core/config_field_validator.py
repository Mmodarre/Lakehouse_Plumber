"""Strict field validation for configuration objects."""

from typing import Dict, Set, Any, Optional
from ..utils.error_formatter import LHPError, ErrorCategory


class ConfigFieldValidator:
    """Validator for strict field validation across all configuration types."""

    def __init__(self):
        """Initialize with known field definitions for all config types."""

        # Load source type field definitions
        self.load_source_fields = {
            "cloudfiles": {
                "type",
                "path",
                "format",
                "options",
                "reader_options",
                "format_options",
                "schema",
                "schema_file",
                "readMode",
                "schema_location",
                "schema_infer_column_types",
                "max_files_per_trigger",
                "schema_evolution_mode",
                "rescue_data_column",
            },
            "delta": {
                "type",
                "path",
                "table",
                "catalog",
                "database",
                "readMode",
                "cdf_enabled",
                "read_change_feed",
                "cdc_options",
                "where_clause",
                "select_columns",
                "reader_options",
            },
            "sql": {"type", "sql", "sql_path"},
            "jdbc": {"type", "url", "user", "password", "driver", "query", "table"},
            "python": {"type", "module_path", "function_name", "parameters"},
        }

        # Transform type field definitions
        self.transform_fields = {
            "sql": {
                # No additional fields - uses action.sql or action.sql_path
            },
            "python": {
                # No additional fields - uses action.module_path, action.function_name, action.parameters
            },
            "data_quality": {
                # No additional fields - uses action.expectations_file
            },
            "schema": {
                # No additional fields - uses action.schema_file or action.schema_path
            },
            "temp_table": {
                # No additional fields - basic temp table configuration
            },
        }

        # Write target type field definitions
        self.write_target_fields = {
            "streaming_table": {
                "type",
                "database",
                "table",
                "create_table",
                "comment",
                "description",
                "table_properties",
                "partition_columns",
                "cluster_columns",
                "spark_conf",
                "schema",
                "row_filter",
                "temporary",
                "path",
                "mode",
                "cdc_config",
                "snapshot_cdc_config",
            },
            "materialized_view": {
                "type",
                "database",
                "table",
                "create_table",
                "comment",
                "description",
                "table_properties",
                "partition_columns",
                "cluster_columns",
                "spark_conf",
                "schema",
                "row_filter",
                "temporary",
                "path",
                "refresh_schedule",
                "sql",
            },
        }

        # Action-level fields that are always valid
        self.action_fields = {
            "name",
            "type",
            "source",
            "target",
            "description",
            "readMode",
            "write_target",
            "transform_type",
            "sql",
            "sql_path",
            "operational_metadata",
            "expectations_file",
            "once",
        }

    def validate_load_source(
        self, source_config: Dict[str, Any], action_name: str
    ) -> None:
        """Validate load source configuration for unknown fields.

        Args:
            source_config: Source configuration dictionary
            action_name: Name of the action for error reporting

        Raises:
            LHPError: If unknown fields are found
        """
        if not isinstance(source_config, dict):
            return  # Not a dict source, skip validation

        source_type = source_config.get("type")
        if not source_type:
            return  # No type specified, will be caught by other validation

        if source_type not in self.load_source_fields:
            return  # Unknown source type, will be caught by other validation

        expected_fields = self.load_source_fields[source_type]
        actual_fields = set(source_config.keys())
        unknown_fields = actual_fields - expected_fields

        if unknown_fields:
            self._raise_unknown_fields_error(
                action_name=action_name,
                config_type=f"load source ({source_type})",
                unknown_fields=unknown_fields,
                expected_fields=expected_fields,
                config_section="source",
            )

    def validate_write_target(
        self, write_target: Dict[str, Any], action_name: str
    ) -> None:
        """Validate write target configuration for unknown fields.

        Args:
            write_target: Write target configuration dictionary
            action_name: Name of the action for error reporting

        Raises:
            LHPError: If unknown fields are found
        """
        if not isinstance(write_target, dict):
            return  # Not a dict target, skip validation

        target_type = write_target.get("type")
        if not target_type:
            return  # No type specified, will be caught by other validation

        if target_type not in self.write_target_fields:
            return  # Unknown target type, will be caught by other validation

        expected_fields = self.write_target_fields[target_type]
        actual_fields = set(write_target.keys())
        unknown_fields = actual_fields - expected_fields

        if unknown_fields:
            self._raise_unknown_fields_error(
                action_name=action_name,
                config_type=f"write target ({target_type})",
                unknown_fields=unknown_fields,
                expected_fields=expected_fields,
                config_section="write_target",
            )

    def validate_action_fields(
        self, action_dict: Dict[str, Any], action_name: str
    ) -> None:
        """Validate action-level fields for unknown fields.

        Args:
            action_dict: Action configuration dictionary
            action_name: Name of the action for error reporting

        Raises:
            LHPError: If unknown fields are found
        """
        actual_fields = set(action_dict.keys())
        unknown_fields = actual_fields - self.action_fields

        if unknown_fields:
            self._raise_unknown_fields_error(
                action_name=action_name,
                config_type="action",
                unknown_fields=unknown_fields,
                expected_fields=self.action_fields,
                config_section="action",
            )

    def _raise_unknown_fields_error(
        self,
        action_name: str,
        config_type: str,
        unknown_fields: Set[str],
        expected_fields: Set[str],
        config_section: str,
    ) -> None:
        """Raise a user-friendly error for unknown configuration fields.

        Args:
            action_name: Name of the action
            config_type: Type of configuration (e.g., "load source (cloudfiles)")
            unknown_fields: Set of unknown field names
            expected_fields: Set of expected field names
            config_section: Configuration section name (e.g., "source", "write_target")
        """
        unknown_list = sorted(list(unknown_fields))

        # Smart suggestions for common mistakes
        suggestions = []
        for unknown_field in unknown_list:
            if unknown_field == "mode":
                suggestions.append(f"'{unknown_field}' → 'readMode'")
            elif unknown_field == "read_mode":
                suggestions.append(f"'{unknown_field}' → 'readMode'")
            elif unknown_field == "partitions":
                suggestions.append(f"'{unknown_field}' → 'partition_columns'")
            elif unknown_field == "sub_type":
                suggestions.append(
                    f"'{unknown_field}' → use 'type' field in {config_section}"
                )
            else:
                # Find the best match using similarity scoring
                best_match = self._find_best_match(unknown_field, expected_fields)
                if best_match:
                    suggestions.append(f"'{unknown_field}' → '{best_match}'")

        # Create concise error message
        if len(unknown_list) == 1:
            unknown_text = f"Unknown field '{unknown_list[0]}'"
        else:
            unknown_text = f"Unknown fields: {', '.join(unknown_list)}"

        # Build compact example
        example_lines = []
        if suggestions:
            example_lines.append("Fix:")
            for suggestion in suggestions[:3]:  # Show max 3 suggestions
                example_lines.append(f"  {suggestion}")

        if len(suggestions) > 3:
            example_lines.append(f"  ... and {len(suggestions) - 3} more")

        example = (
            "\n".join(example_lines)
            if example_lines
            else "Check field names in documentation"
        )

        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title=f"{unknown_text} in {config_type}",
            details=f"Action '{action_name}' has invalid configuration",
            suggestions=[
                "Use the field name corrections shown below",
                "Check the documentation for valid field names",
            ],
            example=example,
            context={
                "Action": action_name,
                "Section": config_section,
                "Unknown": unknown_list,
                "Type": config_type,
            },
        )

    def _find_best_match(
        self, unknown_field: str, expected_fields: Set[str]
    ) -> Optional[str]:
        """Find the best matching field name using similarity scoring."""
        best_match = None
        best_score = 0

        for field in expected_fields:
            # Calculate similarity score
            score = self._calculate_similarity(unknown_field, field)
            if score > best_score and score > 0.6:  # Minimum threshold
                best_score = score
                best_match = field

        return best_match

    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings."""
        # Simple similarity: longer common substring ratio
        if not str1 or not str2:
            return 0.0

        # Check if one is contained in the other
        if str1 in str2 or str2 in str1:
            return 0.8

        # Calculate longest common subsequence ratio
        common_chars = sum(1 for c1, c2 in zip(str1, str2) if c1 == c2)
        max_len = max(len(str1), len(str2))
        return common_chars / max_len if max_len > 0 else 0.0
