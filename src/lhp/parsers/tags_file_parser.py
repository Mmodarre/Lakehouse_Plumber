"""Strict-format parser for external UC table-tags sidecar files.

Loads a ``tags_file`` sidecar and returns the ``(table, tags)`` pair after
validating the strict format. Format problems raise LHP-CFG-067; missing files
and YAML/document problems surface from the shared ``yaml_loader`` unchanged
(LHP-IO-001 missing, LHP-IO-003 zero/multiple documents, LHP-CFG-009 syntax).
"""

import logging
from pathlib import Path
from typing import Any

from ..errors import ErrorFactory, LHPConfigError, codes
from .yaml_loader import load_yaml_file

logger = logging.getLogger(__name__)

_SUPPORTED_VERSIONS = {"1.0", "1.0.0"}
_ALLOWED_KEYS = {"version", "table", "tags"}

_FORMAT_EXAMPLE = """version: "1.0"
table: catalog.schema.my_table
tags:
  team: platform
  cost_center: "1234"
"""


def _invalid(file_path: Path, details: str, suggestion: str) -> LHPConfigError:
    return ErrorFactory.config_error(
        codes.CFG_067,
        title="Invalid UC tags file",
        details=details,
        suggestions=[suggestion],
        example=_FORMAT_EXAMPLE,
        context={"File": str(file_path)},
    )


def parse_tags_file(file_path: Path) -> tuple[str, dict[str, Any]]:
    """Load and validate a strict-format UC tags file. Returns (table, tags).

    Caller resolves the path first (resolve_external_file_path) so a missing
    file raises LHP-IO-001 with search locations. Format problems raise
    LHP-CFG-067.
    """
    data = load_yaml_file(file_path, allow_empty=True, error_context="UC tags file")

    if not isinstance(data, dict):
        raise _invalid(
            file_path,
            "A UC tags file must be a mapping with 'version', 'table', 'tags'.",
            "Use the strict tags-file format shown below.",
        )

    unknown = set(data) - _ALLOWED_KEYS
    if unknown:
        raise _invalid(
            file_path,
            f"UC tags file has unknown key(s): {', '.join(sorted(unknown))}.",
            "Remove any key other than 'version', 'table', and 'tags'.",
        )

    for key in ("version", "table", "tags"):
        if key not in data:
            raise _invalid(
                file_path,
                f"UC tags file is missing required key '{key}'.",
                "Provide the 'version', 'table', and 'tags' keys.",
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

    if not isinstance(data["tags"], dict):
        raise _invalid(
            file_path,
            "UC tags file 'tags' must be a mapping.",
            "Provide 'tags' as a mapping of key: value pairs (may be empty).",
        )

    return table.strip(), data["tags"]
