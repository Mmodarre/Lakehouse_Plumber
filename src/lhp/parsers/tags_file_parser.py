"""Tags reader for the unified schema/tags file format.

Loads a ``tags_file`` and returns the parsed ``ParsedTagsFile`` after validating
it against the shared unified format (:mod:`lhp.parsers.unified_schema_format`).
The tags reader consumes the top-level ``tags:`` block and the per-column
``tags:`` under ``columns:``; it ignores the schema-only fields
``type``/``nullable``/``comment`` (so one file can serve as both ``table_schema``
and ``tags_file``). The file is identified by an optional ``table`` (or its alias
``name``) — canonical ``table`` wins on conflict. A file that carries no table or
column tags is a no-op, not an error. Whitelist problems raise LHP-CFG-067;
missing files and YAML/document problems surface from the shared ``yaml_loader``
unchanged (LHP-IO-001 missing, LHP-IO-003 zero/multiple documents, LHP-CFG-009
syntax).
"""

import logging
from pathlib import Path
from typing import Any, NamedTuple, Optional

from ..errors import codes
from . import unified_schema_format
from .yaml_loader import load_yaml_file

logger = logging.getLogger(__name__)


class ParsedTagsFile(NamedTuple):
    """A validated set of UC tags read from a unified schema/tags file.

    ``table`` carries the resolved identifier — whichever of ``table``/``name``
    was provided (``table`` wins when both are present) — or ``None`` when the
    file declares no identifier (identifier is optional). ``tags`` is ``None``
    when the ``tags:`` key is absent and ``{}`` for an explicit ``tags: {}``
    (absent ≠ empty). ``columns`` maps each column that carries a ``tags:`` key to
    its ``{tag_key: tag_value}`` mapping, in entry order; columns with no ``tags``
    contribute nothing, and it defaults to ``{}``.
    """

    table: Optional[str]
    tags: Optional[dict[str, Any]]
    columns: dict[str, dict[str, Any]]


def parse_tags_file(file_path: Path) -> ParsedTagsFile:
    """Load and validate a ``tags_file`` under the unified schema/tags format.

    Caller resolves the path first (resolve_external_file_path) so a missing
    file raises LHP-IO-001 with search locations. Whitelist problems raise
    LHP-CFG-067. A file declaring both ``table`` and ``name`` with differing
    values logs an LHP-CFG-068 warning and uses ``table``.
    """
    data = load_yaml_file(file_path, allow_empty=True, error_context="UC tags file")

    unified_schema_format.validate(data, file_path)

    # Resolve the optional identifier — canonical ``table`` wins over ``name``.
    resolved: dict[str, str] = {}
    for key in ("table", "name"):
        if key in data:
            value = data[key]
            if not isinstance(value, str) or not value.strip():
                raise unified_schema_format.invalid(
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
    table: Optional[str] = resolved.get("table") or resolved.get("name")

    tags: Optional[dict[str, Any]] = None
    if "tags" in data:
        if not isinstance(data["tags"], dict):
            raise unified_schema_format.invalid(
                file_path,
                "UC tags file 'tags' must be a mapping.",
                "Provide 'tags' as a mapping of key: value pairs (may be empty).",
            )
        tags = data["tags"]

    columns: dict[str, dict[str, Any]] = {}
    if "columns" in data:
        # ``validate`` has already confirmed ``columns`` is a list of mappings with
        # only whitelisted keys; here we read ``name`` + ``tags`` and ignore the
        # schema-only ``type``/``nullable``/``comment``.
        for entry in data["columns"]:
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip():
                raise unified_schema_format.invalid(
                    file_path,
                    "UC tags file column entry 'name' must be a non-empty string.",
                    "Set 'name' to the column name.",
                )
            name = name.strip()
            if "tags" not in entry:
                # A schema-only column (no ``tags:``) contributes no column tags.
                continue
            if not isinstance(entry["tags"], dict):
                raise unified_schema_format.invalid(
                    file_path,
                    f"UC tags file column '{name}' 'tags' must be a mapping "
                    "(may be empty).",
                    "Give each column entry a 'tags:' mapping of key: value pairs.",
                )
            if name in columns:
                raise unified_schema_format.invalid(
                    file_path,
                    f"UC tags file 'columns' declares column '{name}' more than once.",
                    "Merge duplicate entries into one per column.",
                )
            columns[name] = entry["tags"]

    if tags is None and not columns:
        logger.debug(
            f"UC tags file '{file_path}' declares no table or column tags; "
            "nothing to apply."
        )

    return ParsedTagsFile(table, tags, columns)
