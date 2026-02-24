"""Extract external file references from a resolved FlowGroup.

Inspects all 14 field paths where a FlowGroup's actions can reference
external files (SQL, Python, schema, expectations) and returns a
deduplicated, sorted list of RelatedFile records.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from lhp.models.config import FlowGroup
from lhp.utils.external_file_loader import is_file_path

logger = logging.getLogger(__name__)

# Category sort order: sql first, then schema, expectations, python
_CATEGORY_ORDER = {"sql": 0, "schema": 1, "expectations": 2, "python": 3}


@dataclass
class RelatedFile:
    """A file referenced by a FlowGroup action."""

    path: str  # Relative to project root
    category: str  # "sql" | "python" | "schema" | "expectations"
    action_name: str  # Which action references this file
    field: str  # e.g. "sql_path", "source.module_path"
    exists: bool  # Whether file exists on disk


def extract_related_files(
    flowgroup: FlowGroup, project_root: Path
) -> List[RelatedFile]:
    """Extract all external file references from a FlowGroup.

    Iterates over all actions, checking the 14 known field paths that can
    reference external files. Uses ``is_file_path()`` for ambiguous fields
    that accept both inline content and file paths.

    Args:
        flowgroup: A resolved FlowGroup (after template/preset/substitution).
        project_root: Project root for resolving relative paths.

    Returns:
        Deduplicated list sorted by category then alphabetically by path.
    """
    seen_paths: set[str] = set()
    results: List[RelatedFile] = []

    for action in flowgroup.actions:
        _extract_from_action(action, project_root, seen_paths, results)

    # Sort: category order, then alphabetical within each category
    results.sort(key=lambda r: (_CATEGORY_ORDER.get(r.category, 99), r.path))
    return results


def _extract_from_action(
    action: Any,
    project_root: Path,
    seen: set[str],
    results: List[RelatedFile],
) -> None:
    """Extract file references from a single action."""
    name = action.name

    # --- Action-level fields ---
    _check_field(action.sql_path, name, "sql_path", "sql", project_root, seen, results)
    _check_field(
        action.expectations_file,
        name,
        "expectations_file",
        "expectations",
        project_root,
        seen,
        results,
    )
    _check_field(
        action.schema_file, name, "schema_file", "schema", project_root, seen, results
    )
    _check_field(
        action.module_path, name, "module_path", "python", project_root, seen, results
    )

    # --- Source dict fields ---
    source = action.source
    if isinstance(source, dict):
        _check_field(
            source.get("sql_path"),
            name,
            "source.sql_path",
            "sql",
            project_root,
            seen,
            results,
        )
        _check_field(
            source.get("module_path"),
            name,
            "source.module_path",
            "python",
            project_root,
            seen,
            results,
        )
        _check_field(
            source.get("schema_file"),
            name,
            "source.schema_file",
            "schema",
            project_root,
            seen,
            results,
        )

        # source.schema — ambiguous: inline DDL or file path
        schema_val = source.get("schema")
        if schema_val and isinstance(schema_val, str) and is_file_path(schema_val):
            _check_field(
                schema_val,
                name,
                "source.schema",
                "schema",
                project_root,
                seen,
                results,
            )

        # source.options["cloudFiles.schemaHints"] — ambiguous
        options = source.get("options")
        if isinstance(options, dict):
            hints_val = options.get("cloudFiles.schemaHints")
            if hints_val and isinstance(hints_val, str) and is_file_path(hints_val):
                _check_field(
                    hints_val,
                    name,
                    "source.options.cloudFiles.schemaHints",
                    "schema",
                    project_root,
                    seen,
                    results,
                )

    # --- Write target fields ---
    wt = action.write_target
    if isinstance(wt, dict):
        _extract_from_write_target_dict(wt, name, project_root, seen, results)
    elif wt is not None:
        # WriteTarget model object
        _extract_from_write_target_model(wt, name, project_root, seen, results)


def _extract_from_write_target_dict(
    wt: Dict[str, Any],
    action_name: str,
    project_root: Path,
    seen: set[str],
    results: List[RelatedFile],
) -> None:
    """Extract file references from a write_target dict."""
    # table_schema — ambiguous
    ts_val = wt.get("table_schema")
    if ts_val and isinstance(ts_val, str) and is_file_path(ts_val):
        _check_field(
            ts_val,
            action_name,
            "write_target.table_schema",
            "schema",
            project_root,
            seen,
            results,
        )

    # schema — ambiguous (legacy alias)
    schema_val = wt.get("schema")
    if schema_val and isinstance(schema_val, str) and is_file_path(schema_val):
        _check_field(
            schema_val,
            action_name,
            "write_target.schema",
            "schema",
            project_root,
            seen,
            results,
        )

    _check_field(
        wt.get("sql_path"),
        action_name,
        "write_target.sql_path",
        "sql",
        project_root,
        seen,
        results,
    )
    _check_field(
        wt.get("module_path"),
        action_name,
        "write_target.module_path",
        "python",
        project_root,
        seen,
        results,
    )

    # snapshot_cdc_config.source_function.file
    cdc = wt.get("snapshot_cdc_config")
    if isinstance(cdc, dict):
        src_func = cdc.get("source_function")
        if isinstance(src_func, dict):
            _check_field(
                src_func.get("file"),
                action_name,
                "write_target.snapshot_cdc_config.source_function.file",
                "python",
                project_root,
                seen,
                results,
            )


def _extract_from_write_target_model(
    wt: Any,
    action_name: str,
    project_root: Path,
    seen: set[str],
    results: List[RelatedFile],
) -> None:
    """Extract file references from a WriteTarget model object."""
    # table_schema — ambiguous
    ts_val = getattr(wt, "table_schema", None)
    if ts_val and isinstance(ts_val, str) and is_file_path(ts_val):
        _check_field(
            ts_val,
            action_name,
            "write_target.table_schema",
            "schema",
            project_root,
            seen,
            results,
        )

    _check_field(
        getattr(wt, "module_path", None),
        action_name,
        "write_target.module_path",
        "python",
        project_root,
        seen,
        results,
    )

    # sql_path and snapshot_cdc_config not on WriteTarget model,
    # but could be in the dict form — handled by dict extractor.


def _check_field(
    value: Optional[str],
    action_name: str,
    field: str,
    category: str,
    project_root: Path,
    seen: set[str],
    results: List[RelatedFile],
) -> None:
    """Add a RelatedFile if the value is a non-empty string path not yet seen."""
    if not value or not isinstance(value, str):
        return

    # Normalize the path
    normalized = value.replace("\\", "/")

    if normalized in seen:
        return
    seen.add(normalized)

    exists = (project_root / normalized).exists()
    results.append(
        RelatedFile(
            path=normalized,
            category=category,
            action_name=action_name,
            field=field,
            exists=exists,
        )
    )
