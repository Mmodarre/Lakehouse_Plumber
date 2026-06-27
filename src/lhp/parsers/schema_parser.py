import logging
from pathlib import Path
from typing import Any, Dict, List, Union

from ..errors import ErrorFactory, LHPError, codes
from ..parsers.yaml_parser import YAMLParser


class SchemaParser:
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

        hints = []
        for column in schema_data["columns"]:
            name = column["name"]
            col_type = column["type"]
            nullable = column.get("nullable", True)

            constraint = "" if nullable else " NOT NULL"
            hints.append(f"{name} {col_type}{constraint}")

        return ", ".join(hints)

    def to_column_tags(
        self, schema_data: Dict[str, Any]
    ) -> Dict[str, Dict[str, str]]:
        """Extract Unity Catalog column-level tags from a parsed schema.

        Returns ``{column_name: {tag_key: tag_value}}`` for every column whose
        definition contains a ``tags`` key — including an explicit empty
        ``tags: {}`` (preserved as ``{}``, the managed-with-empty-set signal).
        Columns without a ``tags`` key are omitted entirely (unmanaged).

        Tag values are normalized to strings: ``None``/``~``/omitted become
        ``""`` (key-only tags); non-string scalars are coerced via ``str()``.
        """
        column_tags: Dict[str, Dict[str, str]] = {}

        for column in schema_data.get("columns", []) or []:
            if not isinstance(column, dict) or "tags" not in column:
                continue
            raw = column["tags"] or {}
            column_tags[column["name"]] = {
                str(key): "" if value is None else str(value)
                for key, value in raw.items()
            }

        return column_tags

    def validate_schema(
        self, schema_data: Dict[str, Any]
    ) -> List[Union[str, LHPError]]:
        errors = []

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

            if "tags" in column:
                tags = column["tags"]
                if tags is not None and not isinstance(tags, dict):
                    errors.append(f"Column {i} 'tags' must be a mapping")
                elif isinstance(tags, dict):
                    for key in tags:
                        if not isinstance(key, str):
                            errors.append(
                                f"Column {i} tags key '{key}' must be a string"
                            )

        return errors
