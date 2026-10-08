"""Locate file-valued action fields for project editor navigation.

The declared value is preserved; this index never invents generator fallback
paths or treats unresolved substitution tokens as existing files.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, Tuple

ReferenceKind = Literal[
    "sql", "python", "schema", "expectations", "config", "template", "blueprint"
]


@dataclass(frozen=True)
class ActionFileReference:
    kind: ReferenceKind
    path: str
    field_path: Tuple[str, ...]
    action_name: str
    exists: bool


_FIELDS: Tuple[tuple[Tuple[str, ...], ReferenceKind, bool], ...] = (
    (("sql_path",), "sql", False),
    (("expectations_file",), "expectations", False),
    (("schema_file",), "schema", False),
    (("module_path",), "python", False),
    (("source", "sql_path"), "sql", False),
    (("source", "module_path"), "python", False),
    (("source", "schema_file"), "schema", False),
    (("source", "schema"), "schema", True),
    (("source", "options", "cloudFiles.schemaHints"), "schema", True),
    (("write_target", "table_schema"), "schema", True),
    (("write_target", "sql_path"), "sql", False),
    (("write_target", "module_path"), "python", False),
    (("write_target", "tags_file"), "schema", False),
    (
        ("write_target", "snapshot_cdc_config", "source_function", "file"),
        "python",
        False,
    ),
)


def _value_at(action: Mapping[str, Any], field_path: Tuple[str, ...]) -> Any:
    value: Any = action
    for part in field_path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value


def _looks_like_path(value: str) -> bool:
    lower = value.lower()
    return (
        "/" in value
        or "\\" in value
        or lower.endswith((".yaml", ".yml", ".json", ".ddl", ".sql", ".py"))
    )


def _contained_file(root: Path, value: str) -> bool:
    if "${" in value or "{{" in value:
        return False
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        return False
    resolved_root = root.resolve()
    target = (resolved_root / path).resolve()
    return target.is_relative_to(resolved_root) and target.is_file()


def action_file_references(
    action: Mapping[str, Any], project_root: Path
) -> Tuple[ActionFileReference, ...]:
    """Return exact declared file references and whether they resolve locally."""
    name = action.get("name")
    action_name = name if isinstance(name, str) else ""
    found: list[ActionFileReference] = []
    for field_path, kind, ambiguous in _FIELDS:
        value = _value_at(action, field_path)
        if not isinstance(value, str) or not value.strip():
            continue
        if ambiguous and not _looks_like_path(value):
            continue
        declared = value.replace("\\", "/")
        found.append(
            ActionFileReference(
                kind=kind,
                path=declared,
                field_path=field_path,
                action_name=action_name,
                exists=_contained_file(project_root, declared),
            )
        )
    return tuple(found)
