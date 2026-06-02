"""Schema parser for converting YAML schema files to Spark formats."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Union

from ..errors import ErrorFactory, LHPError, codes
from ..parsers.yaml_parser import YAMLParser


class SchemaParser:
    """Parse YAML schema files and convert to Spark formats."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.yaml_parser = YAMLParser()

    def parse_schema_file(
        self, schema_file_path: Path, spec_dir: Path | None = None
    ) -> Dict[str, Any]:
        """Parse a YAML schema file.

        Args:
            schema_file_path: Path to schema file
            spec_dir: Base directory for relative paths

        Returns:
            Parsed schema dictionary
        """
        # Handle relative paths
        if not schema_file_path.is_absolute() and spec_dir:
            schema_file_path = spec_dir / schema_file_path

        if not schema_file_path.exists():
            raise ErrorFactory.file_not_found(
                file_path=str(schema_file_path),
                search_locations=[str(schema_file_path.parent)]
                + ([str(spec_dir)] if spec_dir else []),
                file_type="schema file",
            )

        try:
            schema_data = self.yaml_parser.parse_file(schema_file_path)
            self.logger.debug(f"Parsed schema file: {schema_file_path}")
            return schema_data
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            raise ErrorFactory.config_error(
                codes.CFG_007,
                title="Schema file parsing error",
                details=f"Error parsing schema file {schema_file_path}: {e}",
                suggestions=[
                    "Check the schema YAML syntax",
                    "Ensure the file contains 'name' and 'columns' keys",
                    "Verify column definitions have 'name' and 'type' fields",
                ],
                context={"file": str(schema_file_path)},
            ) from e

    def to_schema_hints(self, schema_data: Dict[str, Any]) -> str:
        """Convert schema data to cloudFiles.schemaHints string.

        Args:
            schema_data: Parsed schema dictionary

        Returns:
            Schema hints string for Auto Loader
        """
        if "columns" not in schema_data:
            raise ErrorFactory.validation_error(
                codes.VAL_016,
                title="Missing 'columns' field in schema",
                details="Schema must have a 'columns' field to generate schema hints.",
                suggestions=[
                    "Add a 'columns' key with a list of column definitions",
                    "Each column needs 'name' and 'type' fields",
                ],
                example="columns:\n  - name: id\n    type: BIGINT\n  - name: name\n    type: STRING",
                context={"schema": str(schema_data.get("name", "<unknown>"))},
            )

        hints = []
        for column in schema_data["columns"]:
            name = column["name"]
            col_type = column["type"]
            nullable = column.get("nullable", True)

            # Add NOT NULL constraint if column is not nullable
            constraint = "" if nullable else " NOT NULL"
            hints.append(f"{name} {col_type}{constraint}")

        return ", ".join(hints)

    def validate_schema(
        self, schema_data: Dict[str, Any]
    ) -> List[Union[str, LHPError]]:
        """Validate schema structure.

        Args:
            schema_data: Parsed schema dictionary

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Check required fields
        if "name" not in schema_data:
            errors.append("Schema must have 'name' field")

        if "columns" not in schema_data:
            errors.append("Schema must have 'columns' field")
            return errors  # Can't continue without columns

        if not isinstance(schema_data["columns"], list):
            errors.append("Schema 'columns' must be a list")
            return errors

        if not schema_data["columns"]:
            errors.append("Schema must have at least one column")

        # Validate each column
        for i, column in enumerate(schema_data["columns"]):
            if not isinstance(column, dict):
                errors.append(f"Column {i} must be a dictionary")
                continue

            if "name" not in column:
                errors.append(f"Column {i} must have 'name' field")

            if "type" not in column:
                errors.append(f"Column {i} must have 'type' field")

            # Check nullable field if present
            if "nullable" in column and not isinstance(column["nullable"], bool):
                errors.append(f"Column {i} 'nullable' must be boolean")

        return errors
