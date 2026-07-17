"""Strict-format parser for external UC table-tags sidecar files.

Loads a ``tags_file`` sidecar and returns the parsed ``ParsedTagsFile`` after
validating the strict format. A tags file carries table-level tags
(``tags:``) and/or per-column tags (``columns:``). Format problems raise
LHP-CFG-067; missing files and YAML/document problems surface from the shared
``yaml_loader`` unchanged (LHP-IO-001 missing, LHP-IO-003 zero/multiple
documents, LHP-CFG-009 syntax).
"""

import logging
from pathlib import Path
from typing import Any, NamedTuple, Optional

from ..errors import ErrorFactory, LHPConfigError, codes
from .yaml_loader import load_yaml_file

logger = logging.getLogger(__name__)

_SUPPORTED_VERSIONS = {"1.0", "1.0.0"}
_ALLOWED_KEYS = {"version", "table", "tags", "columns"}

_FORMAT_EXAMPLE = """version: "1.0"
table: catalog.schema.my_table
tags:
  team: platform
  cost_center: "1234"
columns:
  email:
    pii: high
  region:
    classification: public
"""


class ParsedTagsFile(NamedTuple):
    """A validated UC tags file.

    ``tags`` is ``None`` when the ``tags:`` key is absent and ``{}`` for an
    explicit ``tags: {}`` (absent ≠ empty). ``columns`` maps each column name
    to its ``{tag_key: tag_value}`` mapping and defaults to ``{}`` when absent.
    """

    table: str
    tags: Optional[dict[str, Any]]
    columns: dict[str, dict[str, Any]]


def _invalid(file_path: Path, details: str, suggestion: str) -> LHPConfigError:
    return ErrorFactory.config_error(
        codes.CFG_067,
        title="Invalid UC tags file",
        details=details,
        suggestions=[suggestion],
        example=_FORMAT_EXAMPLE,
        context={"File": str(file_path)},
    )


def parse_tags_file(file_path: Path) -> ParsedTagsFile:
    """Load and validate a strict-format UC tags file.

    Caller resolves the path first (resolve_external_file_path) so a missing
    file raises LHP-IO-001 with search locations. Format problems raise
    LHP-CFG-067.
    """
    data = load_yaml_file(file_path, allow_empty=True, error_context="UC tags file")

    if not isinstance(data, dict):
        raise _invalid(
            file_path,
            "A UC tags file must be a mapping with 'version', 'table', and "
            "'tags' and/or 'columns'.",
            "Use the strict tags-file format shown below.",
        )

    unknown = set(data) - _ALLOWED_KEYS
    if unknown:
        raise _invalid(
            file_path,
            f"UC tags file has unknown key(s): {', '.join(sorted(unknown))}.",
            "Remove any key other than 'version', 'table', 'tags', and 'columns'.",
        )

    for key in ("version", "table"):
        if key not in data:
            raise _invalid(
                file_path,
                f"UC tags file is missing required key '{key}'.",
                "Provide the 'version' and 'table' keys.",
            )

    if "tags" not in data and "columns" not in data:
        raise _invalid(
            file_path,
            "UC tags file must declare 'tags' and/or 'columns'.",
            "Add a 'tags' and/or 'columns' block.",
        )

    version = str(data["version"]).strip()
    if version not in _SUPPORTED_VERSIONS:
        raise _invalid(
            file_path,
            f"UC tags file has unsupported version '{version}' "
            "(expected 1.0 or 1.0.0).",
            'Set "version" to "1.0".',
        )

    table = data["table"]
    if not isinstance(table, str) or not table.strip():
        raise _invalid(
            file_path,
            "UC tags file 'table' must be a non-empty string.",
            "Set 'table' to the fully-qualified write target.",
        )

    tags: Optional[dict[str, Any]] = None
    if "tags" in data:
        if not isinstance(data["tags"], dict):
            raise _invalid(
                file_path,
                "UC tags file 'tags' must be a mapping.",
                "Provide 'tags' as a mapping of key: value pairs (may be empty).",
            )
        tags = data["tags"]

    columns: dict[str, dict[str, Any]] = {}
    if "columns" in data:
        raw_columns = data["columns"]
        if not isinstance(raw_columns, dict):
            raise _invalid(
                file_path,
                "UC tags file 'columns' must be a mapping of column name to a "
                "tag mapping.",
                "Provide 'columns' as column_name: {key: value} mappings.",
            )
        for col_name, col_tags in raw_columns.items():
            if not isinstance(col_name, str):
                raise _invalid(
                    file_path,
                    f"UC tags file 'columns' has a non-string column name "
                    f"'{col_name!r}'.",
                    "Quote column names so each is a string.",
                )
            if not isinstance(col_tags, dict):
                raise _invalid(
                    file_path,
                    f"UC tags file column '{col_name}' must map to a tag mapping.",
                    "Provide each column as column_name: {key: value} (may be empty).",
                )
        columns = raw_columns

    return ParsedTagsFile(table.strip(), tags, columns)
