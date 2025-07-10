"""Schema parser for converting YAML schema files to Spark formats."""

import logging
from pathlib import Path
from typing import Dict, List, Any, Tuple
from ..parsers.yaml_parser import YAMLParser
from .error_formatter import LHPError


class SchemaParser:
    """Parse YAML schema files and convert to Spark formats."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.yaml_parser = YAMLParser()

        # Mapping of our schema types to Spark types
        self.type_mapping = {
            "STRING": "StringType()",
            "BIGINT": "LongType()",
            "INT": "IntegerType()",
            "INTEGER": "IntegerType()",
            "LONG": "LongType()",
            "DOUBLE": "DoubleType()",
            "FLOAT": "FloatType()",
            "BOOLEAN": "BooleanType()",
            "DATE": "DateType()",
            "TIMESTAMP": "TimestampType()",
            "BINARY": "BinaryType()",
            "BYTE": "ByteType()",
            "SHORT": "ShortType()",
        }

        # For decimal types, we need special handling
        self.decimal_pattern = r"DECIMAL\((\d+),(\d+)\)"

    def parse_schema_file(
        self, schema_file_path: Path, spec_dir: Path = None
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
            raise FileNotFoundError(f"Schema file not found: {schema_file_path}")

        try:
            schema_data = self.yaml_parser.parse_file(schema_file_path)
            self.logger.debug(f"Parsed schema file: {schema_file_path}")
            return schema_data
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            raise ValueError(f"Error parsing schema file {schema_file_path}: {e}")

    def to_struct_type_code(self, schema_data: Dict[str, Any]) -> Tuple[str, List[str]]:
        """Convert schema data to Spark StructType code.

        Args:
            schema_data: Parsed schema dictionary

        Returns:
            Tuple of (variable_name, code_lines)
        """
        if "columns" not in schema_data:
            raise ValueError("Schema must have 'columns' field")

        schema_name = schema_data.get("name", "schema")
        variable_name = f"{schema_name}_schema"

        imports = [
            "from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, FloatType, BooleanType, DateType, TimestampType, DecimalType, BinaryType, ByteType, ShortType"
        ]

        code_lines = [f"{variable_name} = StructType(["]

        for column in schema_data["columns"]:
            field_code = self._generate_struct_field(column)
            code_lines.append(f"    {field_code},")

        code_lines.append("])")

        return variable_name, imports + [""] + code_lines

    def to_schema_hints(self, schema_data: Dict[str, Any]) -> str:
        """Convert schema data to cloudFiles.schemaHints string.

        Args:
            schema_data: Parsed schema dictionary

        Returns:
            Schema hints string for Auto Loader
        """
        if "columns" not in schema_data:
            raise ValueError("Schema must have 'columns' field")

        hints = []
        for column in schema_data["columns"]:
            name = column["name"]
            col_type = column["type"]
            hints.append(f"{name} {col_type}")

        return ", ".join(hints)

    def _generate_struct_field(self, column: Dict[str, Any]) -> str:
        """Generate StructField code for a column.

        Args:
            column: Column definition dictionary

        Returns:
            StructField code string
        """
        name = column["name"]
        col_type = column["type"]
        nullable = column.get("nullable", True)
        comment = column.get("comment", "")

        # Convert type to Spark type
        spark_type = self._convert_to_spark_type(col_type)

        # Build metadata if comment exists
        metadata = "{}" if not comment else f'{{"comment": "{comment}"}}'

        return f'StructField("{name}", {spark_type}, {nullable}, {metadata})'

    def _convert_to_spark_type(self, col_type: str) -> str:
        """Convert schema type to Spark type code.

        Args:
            col_type: Column type string (e.g., 'STRING', 'DECIMAL(18,2)')

        Returns:
            Spark type code string
        """
        import re

        col_type = col_type.upper().strip()

        # Handle DECIMAL types specially
        decimal_match = re.match(self.decimal_pattern, col_type)
        if decimal_match:
            precision, scale = decimal_match.groups()
            return f"DecimalType({precision}, {scale})"

        # Handle regular types
        if col_type in self.type_mapping:
            return self.type_mapping[col_type]

        # Default to StringType for unknown types
        self.logger.warning(f"Unknown type '{col_type}', defaulting to StringType")
        return "StringType()"

    def validate_schema(self, schema_data: Dict[str, Any]) -> List[str]:
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


def create_schema_parser() -> SchemaParser:
    """Factory function to create a schema parser."""
    return SchemaParser()
