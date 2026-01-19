"""Delta load generator """

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from typing import Dict, Any


class DeltaLoadGenerator(BaseActionGenerator):
    """Generate Delta table load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate Delta load code."""
        source_config = action.source if isinstance(action.source, dict) else {}

        # Check for removed fields and raise errors
        removed_fields = {
            "cdf_enabled": "Use 'options: {readChangeFeed: \"true\"}' instead",
            "read_change_feed": "Use 'options: {readChangeFeed: \"true\"}' instead",
            "reader_options": "Use 'options' field instead",
            "cdc_options": "Use 'options: {startingVersion: \"X\", startingTimestamp: \"Y\"}' instead"
        }
        
        for field, message in removed_fields.items():
            if field in source_config:
                raise ValueError(
                    f"Delta load action '{action.name}': Field '{field}' is no longer supported. "
                    f"{message}"
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
                raise ValueError(
                    f"Delta load action '{action.name}': 'options' must be a dictionary, "
                    f"got {type(options).__name__}. "
                    f"Use YAML dictionary syntax: options:\\n  key: value"
                )
            for key, value in options.items():
                # Validate option values
                if value is None or value == "":
                    raise ValueError(
                        f"Delta load action '{action.name}': Option '{key}' has invalid value. "
                        f"Value cannot be None or empty string."
                    )
                reader_options[key] = value

        # Determine readMode
        readMode = action.readMode or source_config.get("readMode", "batch")

        # Validate: readChangeFeed requires streaming mode
        if reader_options.get("readChangeFeed") in ("true", "True", True) and readMode != "stream":
            raise ValueError(
                f"Delta load action '{action.name}': Option 'readChangeFeed' requires "
                f"readMode='stream', but got readMode='{readMode}'. "
                f"Add 'readMode: stream' to your action configuration."
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
