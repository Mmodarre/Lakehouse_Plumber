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

        schema_name = schema_data.get("name", "<unknown>")
        hints = []
        for column in schema_data["columns"]:
            self._require_column_fields(column, schema_name, require_type=True)
            name = column["name"]
            col_type = column["type"]
            nullable = column.get("nullable", True)

            constraint = "" if nullable else " NOT NULL"
            hints.append(f"{name} {col_type}{constraint}")

        return ", ".join(hints)

    @staticmethod
    def _require_tag_mapping(raw, column, schema_name) -> Dict[str, Any]:
        """Return ``raw`` as a tag mapping; a present-but-``None`` ``tags`` becomes
        ``{}`` (the managed-with-empty-set signal). Raise a clean ``LHPError`` if it
        is neither ``None`` nor a dict, rather than letting ``.items()`` blow up with
        an ``AttributeError`` during generation.
        """
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw

        column_name = column.get("name", "<unknown>")
        raise ErrorFactory.validation_error(
            codes.VAL_016,
            title="Invalid column 'tags'",
            details=(
                f"Column '{column_name}' in schema '{schema_name}' has 'tags' of "
                f"type {type(raw).__name__}; 'tags' must be a mapping of tag key to "
                f"tag value."
            ),
            suggestions=[
                "Define column tags as a mapping (key: value)",
                "Use an empty value ('', ~, or omitted) for a key-only tag",
            ],
            example=(
                "columns:\n"
                "  - name: email\n"
                "    type: STRING\n"
                "    tags:\n"
                "      classification: pii\n"
                "      masked: ~"
            ),
            context={"schema": str(schema_name), "column": str(column_name)},
        )

    @staticmethod
    def _require_column_fields(column, schema_name, *, require_type: bool) -> None:
        """Guard that ``column`` carries the fields needed to consume it. A
        column always needs ``name``; ``to_schema_hints`` additionally needs
        ``type``. Raise a clean ``LHPError`` if a required field is missing,
        rather than letting subscript access blow up with a ``KeyError`` during
        generation.
        """
        missing = [
            f
            for f in (("name", "type") if require_type else ("name",))
            if f not in column
        ]
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

    def to_column_tags(self, schema_data: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """Extract Unity Catalog column-level tags from a parsed schema.

        Returns ``{column_name: {tag_key: tag_value}}`` for every column whose
        definition contains a ``tags`` key — including an explicit empty
        ``tags: {}`` (preserved as ``{}``, the managed-with-empty-set signal).
        Columns without a ``tags`` key are omitted entirely (unmanaged).

        Tag values are normalized to strings: ``None``/``~``/omitted become
        ``""`` (key-only tags); non-string scalars are coerced via ``str()``.
        """
        column_tags: Dict[str, Dict[str, str]] = {}
        schema_name = schema_data.get("name", "<unknown>")

        for column in schema_data.get("columns", []) or []:
            if not isinstance(column, dict) or "tags" not in column:
                continue
            self._require_column_fields(column, schema_name, require_type=False)
            raw = self._require_tag_mapping(column["tags"], column, schema_name)
            column_tags[column["name"]] = {
                str(key): "" if value is None else str(value)
                for key, value in raw.items()
            }

        return column_tags

    @staticmethod
    def _validate_column_tags(index: int, tags: Any) -> List[str]:
        """Validation messages for a column's ``tags``. ``None`` is allowed
        (managed-with-empty-set); otherwise it must be a dict with string keys.
        """
        if tags is None:
            return []
        if not isinstance(tags, dict):
            return [f"Column {index} 'tags' must be a mapping"]
        return [
            f"Column {index} tags key '{key}' must be a string"
            for key in tags
            if not isinstance(key, str)
        ]

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
                errors.extend(self._validate_column_tags(i, column["tags"]))

        return errors
