"""Resolve a write-target ``table_schema`` value to a DDL hint / schema string.

A ``write_target.table_schema`` may be authored four ways, all funnelled here so
the streaming-table and materialized-view generators share one resolution path:

1. an inline structured schema (a dict with a ``columns`` list) -> DDL hints,
2. an external ``.yaml``/``.yml``/``.json`` file -> parsed then DDL hints,
3. an external ``.ddl``/``.sql`` (or other) file -> raw text,
4. an inline DDL string -> used verbatim.
"""

from pathlib import Path
from typing import Any, Dict, Union

from lhp.parsers import SchemaParser

from .external_file_loader import (
    is_file_path,
    load_external_file_text,
    resolve_external_file_path,
)

_STRUCTURED_SCHEMA_EXTENSIONS = (".yaml", ".yml", ".json")


def resolve_table_schema(
    schema_value: Union[str, Dict[str, Any]],
    project_root: Path,
    schema_parser: SchemaParser,
) -> str:
    """Resolve ``table_schema`` to the schema string emitted by the generators.

    :param schema_value: the raw ``table_schema`` value (inline dict, inline DDL
        string, or a path to an external schema file).
    :param project_root: base directory for resolving relative file paths.
    :param schema_parser: shared parser used for YAML/dict -> DDL-hint conversion.
    :returns: the schema string (DDL hints or raw file text) for the generators.
    :raises LHPError: if a structured schema lacks ``columns`` or a referenced
        file cannot be found or read.
    """
    if isinstance(schema_value, dict):
        return schema_parser.to_schema_hints(schema_value)

    if not is_file_path(schema_value):
        return schema_value

    if Path(schema_value).suffix.lower() in _STRUCTURED_SCHEMA_EXTENSIONS:
        resolved_path = resolve_external_file_path(
            schema_value, project_root, file_type="table schema file"
        )
        schema_data = schema_parser.parse_schema_file(resolved_path)
        return schema_parser.to_schema_hints(schema_data)

    return load_external_file_text(
        schema_value, project_root, file_type="table schema file"
    ).strip()
