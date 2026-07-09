import logging
import re
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..errors import ErrorFactory, codes
from ..models.deprecations import record_deprecation
from ..parsers.yaml_parser import YAMLParser
from .yaml_loader import SAFE_LOADER

logger = logging.getLogger(__name__)

# `$` is legal wherever a name *references* an existing source column, but a
# freshly *minted* target name must stay a plain identifier (reference-vs-mint
# invariant — see issue #142).
_SOURCE_COLUMN_CHARS = r"[a-zA-Z_$][a-zA-Z0-9_$]*"
_TARGET_COLUMN_CHARS = r"[a-zA-Z_][a-zA-Z0-9_]*"


class SchemaTransformParser:
    """Parse schema transform files supporting arrow and legacy formats.

    Arrow format (recommended):
        columns:
          - old_col -> new_col: TYPE    # Rename and cast
          - old_col -> new_col           # Rename only
          - col: TYPE                    # Cast only
          - col                          # Pass-through (strict mode only)

    Legacy format (deprecated):
        column_mapping:
          old_col: new_col
        type_casting:
          col: TYPE
    """

    def __init__(self):
        self.yaml_parser = YAMLParser()
        # Regex pattern for arrow syntax: "old -> new: TYPE" or variations
        self.arrow_pattern = re.compile(
            rf"^\s*({_SOURCE_COLUMN_CHARS})\s*->\s*({_TARGET_COLUMN_CHARS})\s*(?::\s*(.+?))?\s*$"
        )
        # Regex pattern for type cast only: "col: TYPE"
        self.cast_pattern = re.compile(rf"^\s*({_SOURCE_COLUMN_CHARS})\s*:\s*(.+?)\s*$")
        # Regex pattern for pass-through: "col"
        self.passthrough_pattern = re.compile(rf"^\s*({_SOURCE_COLUMN_CHARS})\s*$")

    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        if not file_path.exists():
            raise ErrorFactory.file_not_found(
                file_path=str(file_path),
                search_locations=[str(file_path.parent)],
                file_type="schema transform file",
            )

        logger.debug(f"Parsing schema transform file: {file_path}")
        data = self.yaml_parser.parse_file(file_path)
        return self.parse_file_data(data)

    def parse_inline_schema(self, schema_str: str) -> Dict[str, Any]:
        """Parse inline schema from ``action.schema_inline``.

        Supports two formats:
        1. Plain arrow format (lines of arrow syntax):
           old_col -> new_col: TYPE
           col: TYPE

        2. Full YAML format:
           columns:
             - "old_col -> new_col: TYPE"
        """
        if not schema_str or not schema_str.strip():
            raise ErrorFactory.schema_syntax_error(
                file_path="<inline>",
                line_content=None,
                expected_format="Non-empty schema with column definitions",
                example="schema_inline: |\n  old_col -> new_col: TYPE\n  col: TYPE",
            )

        try:
            parsed = yaml.load(schema_str, Loader=SAFE_LOADER)
        except yaml.YAMLError as e:
            # Likely plain arrow format with inconsistent colons — fall back
            logger.debug(
                f"YAML parsing failed for inline schema, falling back to arrow format: {e}"
            )
            return self._parse_arrow_lines(schema_str)

        if isinstance(parsed, dict):
            has_columns_key = "columns" in parsed
            has_legacy_keys = "column_mapping" in parsed or "type_casting" in parsed

            if has_columns_key or has_legacy_keys:
                if "enforcement" in parsed:
                    record_deprecation(
                        codes.DEPR_003,
                        title="The schema-transform 'enforcement' key is deprecated.",
                        details=(
                            "The 'enforcement' field in an inline 'schema_inline' is "
                            "ignored. Specify 'enforcement' at the action level only."
                        ),
                    )
                return self.parse_file_data(parsed)
            return self._parse_arrow_lines(schema_str)
        return self._parse_arrow_lines(schema_str)

    def _parse_arrow_lines(self, text: str) -> Dict[str, Any]:
        lines = [line.strip() for line in text.strip().split("\n") if line.strip()]

        if not lines:
            raise ErrorFactory.schema_syntax_error(
                file_path="<inline>",
                line_content=None,
                expected_format="One or more column definition lines",
                example="old_col -> new_col: TYPE\ncol: TYPE",
            )

        data = {"columns": lines}

        result = self.parse_arrow_format(data)

        result.pop("enforcement", None)

        return result

    def parse_file_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse schema transform from a dict (already loaded YAML)."""
        if "enforcement" in data:
            record_deprecation(
                codes.DEPR_003,
                title="The schema-transform 'enforcement' key is deprecated.",
                details=(
                    "The 'enforcement' field in an external schema file is deprecated. "
                    "Use the action-level 'enforcement' field instead. "
                    "This value will be ignored."
                ),
            )

        has_columns = "columns" in data
        has_legacy = "column_mapping" in data or "type_casting" in data

        if has_columns and has_legacy:
            raise ErrorFactory.schema_syntax_error(
                file_path="<schema>",
                line_content="Found both 'columns' and 'column_mapping'/'type_casting'",
                expected_format="Either 'columns' (arrow format) OR 'column_mapping'/'type_casting' (legacy format), not both",
                example="# Arrow format (recommended):\ncolumns:\n  - old_col -> new_col: TYPE\n\n# Legacy format:\ncolumn_mapping:\n  old_col: new_col\ntype_casting:\n  col: TYPE",
            )

        if has_columns:
            logger.debug(
                f"Detected arrow format schema with {len(data.get('columns', []))} column(s)"
            )
            return self.parse_arrow_format(data)
        if has_legacy:
            return self.parse_legacy_format(data)
        raise ErrorFactory.schema_syntax_error(
            file_path="<schema>",
            line_content=str(list(data.keys())),
            expected_format="'columns' (arrow format) or 'column_mapping'/'type_casting' (legacy format)",
            example="columns:\n  - old_col -> new_col: TYPE\n  - col: TYPE",
        )

    def parse_arrow_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse arrow format schema transform.

        Arrow format examples:
            - "c_custkey -> customer_id: BIGINT"  # Rename + cast
            - "c_name -> customer_name"            # Rename only
            - "account_balance: DECIMAL(18,2)"     # Cast only
            - "address"                            # Pass-through (strict only)
        """
        columns = data.get("columns", [])

        if not columns:
            raise ErrorFactory.schema_syntax_error(
                file_path="<schema>",
                line_content=None,
                expected_format="At least one column definition in 'columns' list",
                example='columns:\n  - "old_col -> new_col: TYPE"',
            )

        column_mapping: Dict[str, str] = {}
        type_casting: Dict[str, str] = {}
        pass_through_columns: List[str] = []

        source_columns_seen: set[str] = set()
        target_columns_seen: set[str] = set()

        for col_def in columns:
            if not isinstance(col_def, str):
                raise ErrorFactory.schema_syntax_error(
                    file_path="<schema>",
                    line_content=str(col_def),
                    expected_format="String column definition",
                    example='columns:\n  - "old_col -> new_col: TYPE"\n  - "col: TYPE"',
                )

            arrow_match = self.arrow_pattern.match(col_def)
            if arrow_match:
                source_col = arrow_match.group(1)
                target_col = arrow_match.group(2)
                col_type = arrow_match.group(3)  # May be None

                if source_col in source_columns_seen:
                    raise ErrorFactory.schema_syntax_error(
                        file_path="<schema>",
                        line_content=col_def,
                        expected_format="Each source column can only be mapped once",
                        example=f"# Remove the duplicate mapping for '{source_col}'",
                    )
                source_columns_seen.add(source_col)

                if target_col in target_columns_seen:
                    raise ErrorFactory.schema_syntax_error(
                        file_path="<schema>",
                        line_content=col_def,
                        expected_format="Each target column must be unique",
                        example=f"# Remove or rename the duplicate target column '{target_col}'",
                    )
                target_columns_seen.add(target_col)

                column_mapping[source_col] = target_col

                if col_type:
                    type_casting[target_col] = col_type

                continue

            cast_match = self.cast_pattern.match(col_def)
            if cast_match:
                col_name = cast_match.group(1)
                col_type = cast_match.group(2)

                if col_name in type_casting:
                    raise ErrorFactory.schema_syntax_error(
                        file_path="<schema>",
                        line_content=col_def,
                        expected_format="Each column can only have one type cast",
                        example=f"# Remove duplicate type cast for '{col_name}'",
                    )

                if col_name in source_columns_seen:
                    raise ErrorFactory.schema_syntax_error(
                        file_path="<schema>",
                        line_content=col_def,
                        expected_format="Cannot cast a column that is used as source in a rename operation",
                        example=f"# '{col_name}' was already renamed above; cast the target column instead",
                    )

                # Allows casting a previously renamed column
                target_columns_seen.add(col_name)

                type_casting[col_name] = col_type
                continue

            passthrough_match = self.passthrough_pattern.match(col_def)
            if passthrough_match:
                col_name = passthrough_match.group(1)

                if col_name in target_columns_seen:
                    raise ErrorFactory.schema_syntax_error(
                        file_path="<schema>",
                        line_content=col_def,
                        expected_format="Each column must appear only once",
                        example=f"# Remove the duplicate reference to '{col_name}'",
                    )
                target_columns_seen.add(col_name)

                pass_through_columns.append(col_name)
                continue

            raise ErrorFactory.schema_syntax_error(
                file_path="<schema>",
                line_content=col_def,
                expected_format=(
                    "'old -> new: TYPE', 'old -> new', 'col: TYPE', or 'col' (pass-through). "
                    "A rename target (the name after '->') must be a plain identifier "
                    "— letters, digits, and underscores only, no '$'. "
                    "'$' is allowed only in source/reference column names."
                ),
                example='columns:\n  - "c_custkey -> customer_id: BIGINT"\n  - "c_name -> customer_name"\n  - "account_balance: DECIMAL(18,2)"\n  - "address"',
            )

        return {
            "column_mapping": column_mapping,
            "type_casting": type_casting,
            "pass_through_columns": pass_through_columns,
        }

    def parse_legacy_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        column_mapping = data.get("column_mapping", {})
        type_casting = data.get("type_casting", {})

        return {
            "column_mapping": column_mapping,
            "type_casting": type_casting,
            "pass_through_columns": [],  # Not supported in legacy format
        }
