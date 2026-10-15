import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..errors import ErrorFactory, codes
from ..models.deprecations import record_deprecation
from ..parsers.yaml_parser import YAMLParser
from ..utils.spark_identifiers import backtick_quote
from .yaml_loader import SAFE_LOADER

logger = logging.getLogger(__name__)

# `$` is legal wherever a name *references* an existing source column, but a
# freshly *minted* target name must stay a plain identifier (reference-vs-mint
# invariant — see issue #142).
_SOURCE_COLUMN_CHARS = r"[a-zA-Z_$][a-zA-Z0-9_$]*"
_TARGET_COLUMN_CHARS = r"[a-zA-Z_][a-zA-Z0-9_]*"
# Any other source name is written in backticks, as in Spark SQL; two
# backticks inside stand for one literal backtick (issue #324).
_QUOTED_COLUMN = r"`(?:[^`]|``)+`"
_SOURCE_COLUMN = rf"(?:{_QUOTED_COLUMN}|{_SOURCE_COLUMN_CHARS})"


def _unquote_column(token: str) -> str:
    """Return the column name a source token refers to, without backtick quoting."""
    if token.startswith("`"):
        return token[1:-1].replace("``", "`")
    return token


_INLINE_SOURCE = "schema_inline"
_ARROW_FORMS = "'old -> new: TYPE', 'old -> new', 'col: TYPE', or 'col' (pass-through)"
_ARROW_EXAMPLE = (
    "columns:\n"
    '  - "c_custkey -> customer_id: BIGINT"\n'
    '  - "c_name -> customer_name"\n'
    '  - "account_balance: DECIMAL(18,2)"\n'
    '  - "address"\n'
    '  - "`customer name` -> customer_name"'
)
_SOURCE_TOKEN = re.compile(_SOURCE_COLUMN)
_SOURCE_THEN_ARROW = re.compile(rf"\s*{_SOURCE_COLUMN}\s*->(?P<rest>.*)")
_TARGET_COLUMN = re.compile(_TARGET_COLUMN_CHARS)
_EMPTY_QUOTED_COLUMN = re.compile(r"``(?!`)")
# A ``columns:`` header alone on its line. In plain-line form ``columns: TYPE``
# is a cast of a column named ``columns``, so only the bare header counts.
_COLUMNS_HEADER = re.compile(r"\s*columns\s*:\s*(?:#[^\n]*)?(?:\n|$)")


def _explain_invalid_line(line: str) -> tuple[str, str]:
    """Return the expected-format text and example for a line no form matched."""
    text = line.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "\"'":
        return (
            f"{_ARROW_FORMS}. The line is wrapped in quotes; write it without "
            "them — a plain schema_inline line is not a YAML string.",
            _ARROW_EXAMPLE,
        )
    arrow = _SOURCE_THEN_ARROW.fullmatch(text)
    if arrow:
        target = arrow.group("rest").partition(":")[0].strip()
        if not _TARGET_COLUMN.fullmatch(target):
            return (
                f"{_ARROW_FORMS}. A rename target (the name after '->') must be a "
                "plain identifier — letters, digits, and underscores only, with no "
                "'$', spaces or backticks.",
                _ARROW_EXAMPLE,
            )
        return f"{_ARROW_FORMS}.", _ARROW_EXAMPLE
    if text.startswith("`"):
        token = _SOURCE_TOKEN.match(text)
        if token is None:
            reason = (
                "A backtick-quoted column name cannot be empty."
                if _EMPTY_QUOTED_COLUMN.match(text)
                else "A backtick-quoted column name must end with a backtick; "
                "write a backtick inside the name as two backticks."
            )
            return f"{_ARROW_FORMS}. {reason}", _ARROW_EXAMPLE
        rest = text[token.end() :].strip()
        if rest and not rest.startswith(("->", ":")):
            return (
                f"{_ARROW_FORMS}. A backtick-quoted column name must be followed "
                f"by '->', ':' or the end of the line, not '{rest}'.",
                _ARROW_EXAMPLE,
            )
        return f"{_ARROW_FORMS}.", _ARROW_EXAMPLE
    name = re.split(r"->|:", text, maxsplit=1)[0].strip()
    if name and not _SOURCE_TOKEN.fullmatch(name):
        quoted = backtick_quote(name)
        fixed = text.replace(name, quoted, 1)
        return (
            f"{_ARROW_FORMS}. A source column name other than letters, digits, '_' "
            f"and '$' must be written in backticks; if '{name}' is a column name, "
            f"write it as {quoted}.",
            "columns:\n  - " + json.dumps(fixed, ensure_ascii=False),
        )
    return f"{_ARROW_FORMS}.", _ARROW_EXAMPLE


class SchemaTransformParser:
    """Parse schema transform files supporting arrow and legacy formats.

    Arrow format (recommended):
        columns:
          - old_col -> new_col: TYPE    # Rename and cast
          - old_col -> new_col           # Rename only
          - col: TYPE                    # Cast only
          - col                          # Pass-through (strict mode only)
          - `odd name` -> new_col        # Backticks quote any source name

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
            rf"^\s*({_SOURCE_COLUMN})\s*->\s*({_TARGET_COLUMN_CHARS})\s*(?::\s*(.+?))?\s*$"
        )
        # Regex pattern for type cast only: "col: TYPE"
        self.cast_pattern = re.compile(rf"^\s*({_SOURCE_COLUMN})\s*:\s*(.+?)\s*$")
        # Regex pattern for pass-through: "col"
        self.passthrough_pattern = re.compile(rf"^\s*({_SOURCE_COLUMN})\s*$")

    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        if not file_path.exists():
            raise ErrorFactory.file_not_found(
                file_path=str(file_path),
                search_locations=[str(file_path.parent)],
                file_type="schema transform file",
            )

        logger.debug(f"Parsing schema transform file: {file_path}")
        data = self.yaml_parser.parse_file(file_path)
        return self.parse_file_data(data, source=str(file_path))

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
                file_path=_INLINE_SOURCE,
                line_content=None,
                expected_format="Non-empty schema with column definitions",
                example="schema_inline: |\n  old_col -> new_col: TYPE\n  col: TYPE",
            )

        try:
            # SAFE_LOADER is yaml.CSafeLoader/SafeLoader (see yaml_loader);
            # bandit cannot resolve the alias to a safe loader.
            parsed = yaml.load(schema_str, Loader=SAFE_LOADER)  # nosec B506
        except yaml.YAMLError as e:
            if _COLUMNS_HEADER.match(schema_str):
                mark = getattr(e, "problem_mark", None)
                raise ErrorFactory.schema_syntax_error(
                    file_path=_INLINE_SOURCE,
                    line_content=(
                        schema_str.splitlines()[mark.line].strip() if mark else None
                    ),
                    expected_format=(
                        "A 'columns:' list must be valid YAML; quote each entry, "
                        "and always quote one that starts with a backtick."
                    ),
                    example='columns:\n  - "`customer name` -> customer_name"',
                ) from e
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
                return self.parse_file_data(parsed, source=_INLINE_SOURCE)
            return self._parse_arrow_lines(schema_str)
        return self._parse_arrow_lines(schema_str)

    def _parse_arrow_lines(self, text: str) -> Dict[str, Any]:
        lines = [line.strip() for line in text.strip().split("\n") if line.strip()]

        if not lines:
            raise ErrorFactory.schema_syntax_error(
                file_path=_INLINE_SOURCE,
                line_content=None,
                expected_format="One or more column definition lines",
                example="old_col -> new_col: TYPE\ncol: TYPE",
            )

        data = {"columns": lines}

        result = self.parse_arrow_format(data, source=_INLINE_SOURCE)

        result.pop("enforcement", None)

        return result

    def parse_file_data(
        self, data: Dict[str, Any], *, source: str = "<schema>"
    ) -> Dict[str, Any]:
        """Parse schema transform from a dict (already loaded YAML).

        ``source`` names the schema file (or ``schema_inline``) in error messages.
        """
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
                file_path=source,
                line_content="Found both 'columns' and 'column_mapping'/'type_casting'",
                expected_format="Either 'columns' (arrow format) OR 'column_mapping'/'type_casting' (legacy format), not both",
                example="# Arrow format (recommended):\ncolumns:\n  - old_col -> new_col: TYPE\n\n# Legacy format:\ncolumn_mapping:\n  old_col: new_col\ntype_casting:\n  col: TYPE",
            )

        if has_columns:
            logger.debug(
                f"Detected arrow format schema with {len(data.get('columns', []))} column(s)"
            )
            return self.parse_arrow_format(data, source=source)
        if has_legacy:
            return self.parse_legacy_format(data)
        raise ErrorFactory.schema_syntax_error(
            file_path=source,
            line_content=str(list(data.keys())),
            expected_format="'columns' (arrow format) or 'column_mapping'/'type_casting' (legacy format)",
            example="columns:\n  - old_col -> new_col: TYPE\n  - col: TYPE",
        )

    def parse_arrow_format(
        self, data: Dict[str, Any], *, source: str = "<schema>"
    ) -> Dict[str, Any]:
        """Parse arrow format schema transform.

        Arrow format examples:
            - "c_custkey -> customer_id: BIGINT"  # Rename + cast
            - "c_name -> customer_name"            # Rename only
            - "account_balance: DECIMAL(18,2)"     # Cast only
            - "address"                            # Pass-through (strict only)
            - "`customer name` -> customer_name"   # Backtick-quoted source

        ``source`` names the schema file (or ``schema_inline``) in error messages.
        """
        columns = data.get("columns", [])

        if not columns:
            raise ErrorFactory.schema_syntax_error(
                file_path=source,
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
                    file_path=source,
                    line_content=str(col_def),
                    expected_format="String column definition",
                    example='columns:\n  - "old_col -> new_col: TYPE"\n  - "col: TYPE"',
                )

            arrow_match = self.arrow_pattern.match(col_def)
            if arrow_match:
                source_col = _unquote_column(arrow_match.group(1))
                target_col = arrow_match.group(2)
                col_type = arrow_match.group(3)  # May be None

                if source_col in source_columns_seen:
                    raise ErrorFactory.schema_syntax_error(
                        file_path=source,
                        line_content=col_def,
                        expected_format="Each source column can only be mapped once",
                        example=f"# Remove the duplicate mapping for '{source_col}'",
                    )
                source_columns_seen.add(source_col)

                if target_col in target_columns_seen:
                    raise ErrorFactory.schema_syntax_error(
                        file_path=source,
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
                col_name = _unquote_column(cast_match.group(1))
                col_type = cast_match.group(2)

                if col_name in type_casting:
                    raise ErrorFactory.schema_syntax_error(
                        file_path=source,
                        line_content=col_def,
                        expected_format="Each column can only have one type cast",
                        example=f"# Remove duplicate type cast for '{col_name}'",
                    )

                if col_name in source_columns_seen:
                    raise ErrorFactory.schema_syntax_error(
                        file_path=source,
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
                col_name = _unquote_column(passthrough_match.group(1))

                if col_name in target_columns_seen:
                    raise ErrorFactory.schema_syntax_error(
                        file_path=source,
                        line_content=col_def,
                        expected_format="Each column must appear only once",
                        example=f"# Remove the duplicate reference to '{col_name}'",
                    )
                target_columns_seen.add(col_name)

                pass_through_columns.append(col_name)
                continue

            expected_format, example = _explain_invalid_line(col_def)
            raise ErrorFactory.schema_syntax_error(
                file_path=source,
                line_content=col_def,
                expected_format=expected_format,
                example=example,
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
