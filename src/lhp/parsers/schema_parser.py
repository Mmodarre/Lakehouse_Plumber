import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Union

from ..errors import ErrorFactory, LHPError, codes
from ..parsers.yaml_parser import YAMLParser
from . import unified_schema_format


class SchemaParser:
    # A column name that is safe to emit unquoted in Databricks SQL DDL
    _SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.yaml_parser = YAMLParser()

    def parse_schema_file(
        self, schema_file_path: Path, spec_dir: Path | None = None
    ) -> Dict[str, Any]:
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
            unified_schema_format.validate(schema_data, schema_file_path)
            return schema_data
        except LHPError:
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

        schema_name = schema_data.get("name", "<unknown>")
        hints = []
        for column in schema_data["columns"]:
            self._require_column_fields(column, schema_name)
            name = self._quote_identifier(column["name"])
            col_type = column["type"]
            nullable = column.get("nullable", True)

            constraint = "" if nullable else " NOT NULL"
            hints.append(f"{name} {col_type}{constraint}")

        return ", ".join(hints)

    @classmethod
    def _quote_identifier(cls, name: Any) -> str:
        """Render a column name as a well-formed Databricks SQL DDL identifier.

        Plain identifiers are returned unchanged so existing schema hints stay stable.
        Anything else — whitespace, ``$``, or other punctuation — is wrapped in
        backticks with any embedded backtick doubled, per Spark SQL quoting rules.
        """
        name = str(name)
        if cls._SAFE_IDENTIFIER.match(name):
            return name
        escaped = name.replace("`", "``")
        return f"`{escaped}`"

    @staticmethod
    def _require_column_fields(column, schema_name) -> None:
        """Guard that ``column`` carries the ``name`` and ``type`` fields
        ``to_schema_hints`` consumes. Raise a clean ``LHPError`` if either is
        missing, rather than letting subscript access blow up with a ``KeyError``
        during generation.
        """
        missing = [f for f in ("name", "type") if f not in column]
        if not missing:
            return

        column_name = column.get("name", "<unknown>")
        raise ErrorFactory.validation_error(
            codes.VAL_016,
            title="Invalid column definition",
            details=(
                f"Column '{column_name}' in schema '{schema_name}' is missing "
                f"required field(s): {', '.join(missing)}."
            ),
            suggestions=[
                "Give every column a 'name'",
                "Give every column a 'type' (e.g. STRING, BIGINT)",
            ],
            example=(
                "columns:\n"
                "  - name: id\n"
                "    type: BIGINT\n"
                "  - name: email\n"
                "    type: STRING"
            ),
            context={"schema": str(schema_name), "column": str(column_name)},
        )

    def validate_schema(
        self, schema_data: Dict[str, Any]
    ) -> List[Union[str, LHPError]]:
        errors = []

        if "columns" not in schema_data:
            errors.append("Schema must have 'columns' field")
            return errors  # Can't continue without columns

        if not isinstance(schema_data["columns"], list):
            errors.append("Schema 'columns' must be a list")
            return errors

        if not schema_data["columns"]:
            errors.append("Schema must have at least one column")

        for i, column in enumerate(schema_data["columns"]):
            if not isinstance(column, dict):
                errors.append(f"Column {i} must be a dictionary")
                continue

            if "name" not in column:
                errors.append(f"Column {i} must have 'name' field")

            if "type" not in column:
                errors.append(f"Column {i} must have 'type' field")

            if "nullable" in column and not isinstance(column["nullable"], bool):
                errors.append(f"Column {i} 'nullable' must be boolean")

        return errors
