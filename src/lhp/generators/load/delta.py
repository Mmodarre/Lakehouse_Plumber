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
        database = source_config.get("database")

        # Build table reference
        if catalog and database:
            table_ref = f"{catalog}.{database}.{table}"
        elif database:
            table_ref = f"{database}.{table}"
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

        # Validate: readChangeFeed requires streaming mode
        if (
            reader_options.get("readChangeFeed") in ("true", "True", True)
            and readMode != "stream"
        ):
            raise ErrorFormatter.invalid_read_mode(
                action_name=action.name,
                action_type="delta (with readChangeFeed)",
                provided=readMode,
                valid_modes=["stream"],
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
