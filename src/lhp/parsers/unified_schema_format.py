"""Shared strict format for the unified schema/tags file.

One YAML file under ``schemas/`` can serve a write action as BOTH its
``table_schema`` (column types → schema hints / StructType) and its ``tags_file``
(table- and column-level Unity Catalog tags). This module is the single source
of truth for that file's shape: a strict top-level and per-column whitelist
enforced identically by the schema reader (``schema_parser``) and the tags reader
(``tags_file_parser``). Whitelist violations raise LHP-CFG-067.

The three legacy schema keys ``version``/``description``/``primary_key`` are
tolerated-and-ignored — they appear in nearly every schema file LHP ships (incl.
``init --sample``'s ``orders_hints.yaml``), so a closed whitelist without them
would break ``lhp generate`` for existing, follow-the-docs files.

This is parse-time input validation (it raises), not a semantic
``core/validators/`` validator — hence it lives in ``parsers/`` alongside the two
readers, and is deliberately NOT named ``*_validator.py`` (constitution §2.6/§9.1).
Each reader layers its own requirement on top of this shared structural gate — a
schema column needs ``type``; a tags column needs ``tags`` — and ignores the
fields it does not consume.
"""

from pathlib import Path
from typing import Any

from ..errors import ErrorFactory, LHPConfigError, codes

_ALLOWED_TOP_LEVEL = {
    "table",
    "name",
    "tags",
    "columns",
    # Tolerated-and-ignored legacy schema keys (Path A).
    "version",
    "description",
    "primary_key",
}
_ALLOWED_COLUMN = {"name", "type", "nullable", "comment", "tags"}

_FORMAT_EXAMPLE = """table: orders           # optional identifier (table name); 'name' is an accepted alias
tags:                   # optional — table-level UC tags (tags_file reader only)
  team: platform
columns:
  - name: email         # required
    type: STRING        # required for table_schema use; optional when tags-only
    nullable: false     # optional (schema)
    comment: "PII"      # optional (schema)
    tags:               # optional — column UC tags (tags_file reader only)
      pii: high
"""


def invalid(file_path: Path, details: str, suggestion: str) -> LHPConfigError:
    """Build the shared LHP-CFG-067 error for a malformed schema/tags file."""
    return ErrorFactory.config_error(
        codes.CFG_067,
        title="Invalid schema/tags file",
        details=details,
        suggestions=[suggestion],
        example=_FORMAT_EXAMPLE,
        context={"File": str(file_path)},
    )


def validate(data: Any, file_path: Path) -> None:
    """Enforce the strict unified whitelist shared by both readers.

    Structural gate only: the file must be a mapping whose top-level keys are all
    whitelisted and whose ``columns:`` (when present) is a list of mappings, each
    carrying only whitelisted per-column keys. Reader-specific requirements are
    enforced by the caller (a schema column needs ``type``; a tags column needs
    ``tags``). Raises LHP-CFG-067 on any violation.
    """
    if not isinstance(data, dict):
        raise invalid(
            file_path,
            "A schema/tags file must be a mapping.",
            "Use the unified schema/tags format shown below.",
        )

    unknown = set(data) - _ALLOWED_TOP_LEVEL
    if unknown:
        raise invalid(
            file_path,
            "schema/tags file has unknown top-level key(s): "
            f"{', '.join(sorted(unknown))}.",
            "Remove any key other than 'table', 'name', 'tags', and 'columns' "
            "(the legacy 'version'/'description'/'primary_key' keys are ignored).",
        )

    if "columns" in data:
        columns = data["columns"]
        if not isinstance(columns, list):
            raise invalid(
                file_path,
                "schema/tags file 'columns' must be a list of column entries.",
                "Provide 'columns' as a list of '- name: ...' entries.",
            )
        for entry in columns:
            if not isinstance(entry, dict):
                raise invalid(
                    file_path,
                    "schema/tags file 'columns' entries must be mappings.",
                    "Write each entry as '- name: <column>' with optional "
                    "'type'/'nullable'/'comment'/'tags'.",
                )
            unknown_col = set(entry) - _ALLOWED_COLUMN
            if unknown_col:
                raise invalid(
                    file_path,
                    "schema/tags file column entry has unknown key(s): "
                    f"{', '.join(sorted(unknown_col))}.",
                    "Each 'columns' entry allows only 'name', 'type', "
                    "'nullable', 'comment', and 'tags'.",
                )


def schema_has_tags(schema_data: Any) -> bool:
    """True if a parsed schema/tags file carries any UC tags.

    Detects the CFG-069 silent-drop footgun: a ``table_schema`` file whose
    top-level ``tags:`` key is present, or whose ``columns:`` carry a per-column
    ``tags:`` key. Purely structural — presence of the key, not non-emptiness.
    """
    if not isinstance(schema_data, dict):
        return False
    if "tags" in schema_data:
        return True
    columns = schema_data.get("columns")
    if isinstance(columns, list):
        return any(isinstance(entry, dict) and "tags" in entry for entry in columns)
    return False
