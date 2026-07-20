"""Strict-format parser for external UC table-tags sidecar files.

Loads a ``tags_file`` sidecar and returns the parsed ``ParsedTagsFile`` after
validating the strict format. A tags file carries table-level tags
(``tags:``) and/or per-column tags (``columns:`` — a list of ``{name, tags}``
entries). It is identified by ``table`` (or its accepted alias ``name``) and may
carry an optional ``version`` (absent ⇒ 1.0). Format problems raise LHP-CFG-067;
missing files and YAML/document problems surface from the shared ``yaml_loader``
unchanged (LHP-IO-001 missing, LHP-IO-003 zero/multiple documents, LHP-CFG-009
syntax).
"""

import logging
from pathlib import Path
from typing import Any, NamedTuple, Optional

from ..errors import ErrorFactory, LHPConfigError, codes
from .yaml_loader import load_yaml_file

logger = logging.getLogger(__name__)

_SUPPORTED_VERSIONS = {"1.0", "1.0.0"}
_ALLOWED_KEYS = {"version", "table", "name", "tags", "columns"}

_FORMAT_EXAMPLE = """version: "1.0"          # optional (absent ⇒ 1.0)
table: orders           # the write target's (unqualified) table name; 'name' is an accepted alias
tags:
  team: platform
  cost_center: "1234"
columns:
  - name: email
    tags:
      pii: high
  - name: region
    tags:
      classification: public
"""


class ParsedTagsFile(NamedTuple):
    """A validated UC tags file.

    ``table`` carries the resolved identifier — whichever of ``table``/``name``
    was provided (``table`` wins when both are present). ``tags`` is ``None``
    when the ``tags:`` key is absent and ``{}`` for an explicit ``tags: {}``
    (absent ≠ empty). ``columns`` is parsed from the ``columns:`` list of
    ``{name, tags}`` entries and returned as a mapping of column name to its
    ``{tag_key: tag_value}`` mapping, in entry order; defaults to ``{}``.
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
    LHP-CFG-067. A tags file declaring both ``table`` and ``name`` with
    differing values logs an LHP-CFG-068 warning and uses ``table``.
    """
    data = load_yaml_file(file_path, allow_empty=True, error_context="UC tags file")

    if not isinstance(data, dict):
        raise _invalid(
            file_path,
            "A UC tags file must be a mapping with a table identifier "
            "('table' or 'name') and 'tags' and/or 'columns'.",
            "Use the strict tags-file format shown below.",
        )

    unknown = set(data) - _ALLOWED_KEYS
    if unknown:
        raise _invalid(
            file_path,
            f"UC tags file has unknown key(s): {', '.join(sorted(unknown))}.",
            "Remove any key other than 'version', 'table', 'name', 'tags', "
            "and 'columns'.",
        )

    if "table" not in data and "name" not in data:
        raise _invalid(
            file_path,
            "UC tags file is missing a table identifier ('table' or its alias 'name').",
            "Add a 'table:' key set to the write target's table name.",
        )

    if "tags" not in data and "columns" not in data:
        raise _invalid(
            file_path,
            "UC tags file must declare 'tags' and/or 'columns'.",
            "Add a 'tags' and/or 'columns' block.",
        )

    if "version" in data:
        version = str(data["version"]).strip()
        if version not in _SUPPORTED_VERSIONS:
            raise _invalid(
                file_path,
                f"UC tags file has unsupported version '{version}' "
                "(expected 1.0 or 1.0.0).",
                'Set "version" to "1.0", or omit it (defaults to 1.0).',
            )

    resolved: dict[str, str] = {}
    for key in ("table", "name"):
        if key in data:
            value = data[key]
            if not isinstance(value, str) or not value.strip():
                raise _invalid(
                    file_path,
                    f"UC tags file '{key}' must be a non-empty string.",
                    "Set the table identifier to the write target's table name.",
                )
            resolved[key] = value.strip()

    if (
        "table" in resolved
        and "name" in resolved
        and resolved["table"] != resolved["name"]
    ):
        logger.warning(
            f"{codes.CFG_068.code}: UC tags file '{file_path}' declares both "
            f"'table' ({resolved['table']!r}) and 'name' ({resolved['name']!r}) "
            "with differing values; using 'table'."
        )
    table = resolved["table"] if "table" in resolved else resolved["name"]

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
        if not isinstance(raw_columns, list):
            raise _invalid(
                file_path,
                "UC tags file 'columns' must be a list of column entries.",
                "Provide 'columns' as a list of {name, tags} entries.",
            )
        for entry in raw_columns:
            if not isinstance(entry, dict):
                raise _invalid(
                    file_path,
                    "UC tags file 'columns' entries must be mappings with "
                    "'name' and 'tags'.",
                    "Write each entry as '- name: <column>' with a nested 'tags:' mapping.",
                )
            unknown_entry = set(entry) - {"name", "tags"}
            if unknown_entry:
                raise _invalid(
                    file_path,
                    f"UC tags file column entry has unknown key(s): "
                    f"{', '.join(sorted(unknown_entry))}.",
                    "Each 'columns' entry allows only 'name' and 'tags'.",
                )
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip():
                raise _invalid(
                    file_path,
                    "UC tags file column entry 'name' must be a non-empty string.",
                    "Set 'name' to the column name.",
                )
            name = name.strip()
            if name in columns:
                raise _invalid(
                    file_path,
                    f"UC tags file 'columns' declares column '{name}' more than once.",
                    "Merge duplicate entries into one per column.",
                )
            if "tags" not in entry or not isinstance(entry["tags"], dict):
                raise _invalid(
                    file_path,
                    f"UC tags file column '{name}' 'tags' must be a mapping "
                    "(may be empty).",
                    "Give each column entry a 'tags:' mapping of key: value pairs.",
                )
            columns[name] = entry["tags"]

    return ParsedTagsFile(table, tags, columns)
