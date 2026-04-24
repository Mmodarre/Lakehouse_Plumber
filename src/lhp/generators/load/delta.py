"""Delta load generator"""

import logging
from typing import Any, Dict

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import (
    ErrorCategory,
    ErrorFormatter,
    LHPValidationError,
)

logger = logging.getLogger(__name__)


class DeltaLoadGenerator(BaseActionGenerator):
    """Generate Delta table load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate Delta load code."""
        source_config = action.source if isinstance(action.source, dict) else {}
        logger.debug(
            f"Generating Delta load for target '{action.target}' with source config keys: {list(source_config.keys())}"
        )

        # Check for removed fields and raise errors
        removed_fields = {
            "cdf_enabled": "Use 'options: {readChangeFeed: \"true\"}' instead",
            "read_change_feed": "Use 'options: {readChangeFeed: \"true\"}' instead",
            "reader_options": "Use 'options' field instead",
            "cdc_options": 'Use \'options: {startingVersion: "X", startingTimestamp: "Y"}\' instead',
        }

        for field, message in removed_fields.items():
            if field in source_config:
                raise ErrorFormatter.deprecated_field(
                    action_name=action.name,
                    field_name=field,
                    replacement=message,
                    example="""source:
  type: delta
  table: my_table
  options:
    readChangeFeed: "true"
    startingVersion: "5" """,
                )

        # Extract configuration
        table = source_config.get("table")
        catalog = source_config.get("catalog")
        schema = source_config.get("schema")

        # Build table reference (normalizer guarantees catalog/schema are present for delta)
        if catalog and schema:
            table_ref = f"{catalog}.{schema}.{table}"
        else:
            table_ref = table

        # Process options first to check for CDC requirements
        reader_options = {}
        if source_config.get("options"):
            options = source_config["options"]
            # Validate options is a dictionary
            if not isinstance(options, dict):
                raise ErrorFormatter.invalid_field_type(
                    action_name=action.name,
                    field_name="options",
                    expected_type="a dictionary (mapping)",
                    actual_type=type(options).__name__,
                    example="""options:
  readChangeFeed: "true"
  startingVersion: "5" """,
                )
            for key, value in options.items():
                # Validate option values
                if value is None or value == "":
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="010",
                        title=f"Invalid option value in action '{action.name}'",
                        details=(
                            f"Delta load action '{action.name}': option '{key}' has an "
                            f"invalid value (None or empty string). All options must have "
                            f"non-empty values."
                        ),
                        suggestions=[
                            f"Provide a valid value for option '{key}'",
                            "Remove the option if it is not needed",
                        ],
                        context={
                            "Action": action.name,
                            "Option": key,
                            "Value": repr(value),
                        },
                    )
                reader_options[key] = value

        # Determine readMode
        readMode = action.readMode or source_config.get("readMode", "batch")
        logger.debug(
            f"Delta load '{action.target}': table_ref='{table_ref}', readMode='{readMode}', options_count={len(reader_options)}"
        )

        # Validate Delta option combinations
        has_cdf = reader_options.get("readChangeFeed") in ("true", "True", True)
        has_skip = reader_options.get("skipChangeCommits") in ("true", "True", True)
        has_starting_version = "startingVersion" in reader_options
        has_starting_timestamp = "startingTimestamp" in reader_options
        has_ending_version = "endingVersion" in reader_options
        has_ending_timestamp = "endingTimestamp" in reader_options
        has_version_as_of = "versionAsOf" in reader_options
        has_timestamp_as_of = "timestampAsOf" in reader_options
        is_stream = readMode == "stream"

        # readChangeFeed + skipChangeCommits: contradictory
        if has_cdf and has_skip:
            raise ErrorFormatter.incompatible_options(
                action_name=action.name,
                option_a="readChangeFeed",
                option_b="skipChangeCommits",
                reason="readChangeFeed reads all changes while skipChangeCommits skips them.",
                suggestion="Use readChangeFeed to consume changes, or skipChangeCommits to ignore them, but not both",
            )

        # readChangeFeed + versionAsOf: CDF vs time travel
        if has_cdf and has_version_as_of:
            raise ErrorFormatter.incompatible_options(
                action_name=action.name,
                option_a="readChangeFeed",
                option_b="versionAsOf",
                reason="readChangeFeed reads a stream of changes while versionAsOf reads a point-in-time snapshot.",
                suggestion="Use readChangeFeed for change tracking or versionAsOf for snapshots, not both",
            )

        # readChangeFeed + timestampAsOf: CDF vs time travel
        if has_cdf and has_timestamp_as_of:
            raise ErrorFormatter.incompatible_options(
                action_name=action.name,
                option_a="readChangeFeed",
                option_b="timestampAsOf",
                reason="readChangeFeed reads a stream of changes while timestampAsOf reads a point-in-time snapshot.",
                suggestion="Use readChangeFeed for change tracking or timestampAsOf for snapshots, not both",
            )

        # startingVersion + startingTimestamp: ambiguous start
        if has_starting_version and has_starting_timestamp:
            raise ErrorFormatter.incompatible_options(
                action_name=action.name,
                option_a="startingVersion",
                option_b="startingTimestamp",
                reason="Both specify a starting point for reading changes but are ambiguous together.",
                suggestion="Use either startingVersion or startingTimestamp, not both",
            )

        # versionAsOf + timestampAsOf: ambiguous snapshot
        if has_version_as_of and has_timestamp_as_of:
            raise ErrorFormatter.incompatible_options(
                action_name=action.name,
                option_a="versionAsOf",
                option_b="timestampAsOf",
                reason="Both specify a snapshot point but are ambiguous together.",
                suggestion="Use either versionAsOf or timestampAsOf, not both",
            )

        # endingVersion/endingTimestamp + stream: ending bounds are batch-only
        if is_stream and has_ending_version:
            raise ErrorFormatter.incompatible_options(
                action_name=action.name,
                option_a="endingVersion",
                option_b="readMode: stream",
                reason="endingVersion is only supported in batch mode.",
                suggestion="Use readMode: batch with endingVersion, or remove endingVersion for streaming",
            )

        if is_stream and has_ending_timestamp:
            raise ErrorFormatter.incompatible_options(
                action_name=action.name,
                option_a="endingTimestamp",
                option_b="readMode: stream",
                reason="endingTimestamp is only supported in batch mode.",
                suggestion="Use readMode: batch with endingTimestamp, or remove endingTimestamp for streaming",
            )

        # Batch CDF requires a starting bound
        if has_cdf and not is_stream:
            if not has_starting_version and not has_starting_timestamp:
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="013",
                    title=f"Batch CDF requires a starting bound in action '{action.name}'",
                    details=(
                        f"Delta load action '{action.name}': readChangeFeed in batch mode "
                        f"requires either 'startingVersion' or 'startingTimestamp' to define "
                        f"the range of changes to read."
                    ),
                    suggestions=[
                        "Add 'startingVersion' to specify the starting Delta version",
                        "Add 'startingTimestamp' to specify the starting timestamp",
                        "Use readMode: stream for continuous CDF consumption",
                    ],
                    context={
                        "Action": action.name,
                        "readMode": readMode,
                    },
                    example="""options:
  readChangeFeed: "true"
  startingVersion: "0"
  # or: startingTimestamp: "2024-01-01" """,
                )

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        # Apply additional context substitutions for Delta source
        # Replace ${source_table} placeholder with actual table reference
        for col_name, expression in metadata_columns.items():
            metadata_columns[col_name] = expression.replace(
                "${source_table}", table_ref
            )

        template_context = {
            "target": action.target,
            "table_ref": table_ref,
            "readMode": readMode,
            "reader_options": reader_options,
            "where_clauses": source_config.get("where_clause", []),
            "select_columns": source_config.get("select_columns"),
            "description": action.description or f"Delta source: {table_ref}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("load/delta.py.j2", template_context)
