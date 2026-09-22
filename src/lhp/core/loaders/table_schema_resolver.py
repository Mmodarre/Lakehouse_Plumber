"""Shared resolver for a write target's ``table_schema`` value.

A ``table_schema`` may be inline DDL / a StructType string, or a path to a
``.yaml``/``.yml``/``.json``/``.ddl``/``.sql`` file. This one resolver turns any of
those into a schema string, so the streaming-table and materialized-view generators
and :class:`CdcSchemaValidator` share a single resolution path (no drift). It
mirrors the branching the generators previously duplicated.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from .external_file_loader import (
    is_file_path,
    load_external_file_text,
    resolve_external_file_path,
)

_YAML_JSON_EXTS = (".yaml", ".yml", ".json")


@dataclass
class ResolvedTableSchema:
    """Outcome of resolving a ``table_schema`` value.

    ``text`` is the schema string used for code generation and for the CDC
    ``__START_AT``/``__END_AT`` presence check; it is ``None`` when the value is a
    file path that could not be resolved (no ``project_root``). ``schema_data`` and
    ``resolved_path`` are populated only for parsed ``.yaml``/``.yml``/``.json``
    files, so the generator can run its UC-tag-drop warning.
    """

    text: Optional[str]
    schema_data: Optional[Dict[str, Any]] = None
    resolved_path: Optional[Path] = None


def resolve_table_schema(
    schema_value: Optional[str], project_root: Optional[Path]
) -> ResolvedTableSchema:
    """Resolve a ``table_schema`` value to a :class:`ResolvedTableSchema`.

    - inline (not a file path) → ``text`` is the value unchanged.
    - ``.yaml``/``.yml``/``.json`` file → parsed to schema hints; ``schema_data`` and
      ``resolved_path`` are populated.
    - ``.ddl``/``.sql`` file → the file's text, stripped.
    - a file path with ``project_root`` ``None`` → ``text`` is ``None`` (unresolvable;
      the caller decides whether to skip).

    Raises :class:`~lhp.errors.LHPError` (e.g. ``LHP-IO-001``) for a referenced but
    missing/invalid file when ``project_root`` is provided — preserving the
    generator's error-surfacing. Callers that must not fail wrap this and skip.
    """
    if not schema_value:
        return ResolvedTableSchema(text=None)

    if not is_file_path(schema_value):
        return ResolvedTableSchema(text=schema_value)

    if project_root is None:
        return ResolvedTableSchema(text=None)

    if Path(schema_value).suffix.lower() in _YAML_JSON_EXTS:
        # Deferred, package-level import keeps the loaders package __init__ free of
        # an eager cross-package (lhp.parsers) import (repo cycle-avoidance convention).
        from lhp.parsers import SchemaParser

        parser = SchemaParser()
        resolved_path = resolve_external_file_path(
            schema_value, project_root, file_type="table schema file"
        )
        schema_data = parser.parse_schema_file(resolved_path)
        return ResolvedTableSchema(
            text=parser.to_schema_hints(schema_data),
            schema_data=schema_data,
            resolved_path=resolved_path,
        )

    text = load_external_file_text(
        schema_value, project_root, file_type="table schema file"
    ).strip()
    return ResolvedTableSchema(text=text)
