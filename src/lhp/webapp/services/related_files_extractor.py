"""Extract external file references from a raw flowgroup YAML dict.

Inspects the known field paths where a flowgroup's actions can reference
external files (SQL, Python, schema, expectations) and returns a
deduplicated, sorted list of :class:`RelatedFile` records.

The input is the plain ``yaml.safe_load`` dict of a *raw* flowgroup YAML
file (not a resolved internal ``FlowGroup`` model). Field names match the
current flowgroup schema (``src/lhp/schemas/flowgroup.schema.json``); the
JSON-facing record fields match what the web IDE frontend consumes.

KNOWN LIMITATION (v1): template-driven flowgroups (``use_template``) carry
their action/file references on the template, not inline. Because this
walker only reads raw YAML, template-side file references are NOT resolved
in v1. This is an accepted deferral; a ``use_template`` flowgroup simply
yields the file refs present in its own inline ``actions`` list (often none).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Category sort order: sql first, then schema, expectations, python.
_CATEGORY_ORDER = {"sql": 0, "schema": 1, "expectations": 2, "python": 3}

# File-path heuristic for ambiguous fields (inline DDL vs path). Mirrors
# ``lhp.utils.external_file_loader.is_file_path``, inlined here because
# ``lhp.webapp`` may not import ``lhp.utils`` (import-linter boundary).
_FILE_EXTENSIONS = (".yaml", ".yml", ".json", ".ddl", ".sql")


@dataclass
class RelatedFile:
    """A file referenced by a flowgroup action."""

    path: str  # Relative to project root
    category: str  # "sql" | "python" | "schema" | "expectations"
    action_name: str  # Which action references this file
    field: str  # e.g. "sql_path", "source.module_path"
    exists: bool  # Whether file exists on disk


def _is_file_path(value: str) -> bool:
    """Detect if a string is a file path vs inline content.

    Used for ambiguous fields (e.g. ``source.schema``,
    ``cloudFiles.schemaHints``, ``write_target.table_schema``) that accept
    both inline DDL and a path. A value is treated as a path when it has a
    known file extension or contains a path separator.
    """
    if not value:
        return False
    lowered = value.lower()
    if any(ext in lowered for ext in _FILE_EXTENSIONS):
        return True
    return "/" in value or "\\" in value


def extract_related_files(
    flowgroup: dict[str, Any], project_root: Path
) -> list[RelatedFile]:
    """Extract all external file references from a raw flowgroup YAML dict.

    Iterates over the dict's ``actions``, checking the known field paths
    that can reference external files. Uses :func:`_is_file_path` for
    ambiguous fields that accept both inline content and file paths.

    Args:
        flowgroup: ``yaml.safe_load`` dict of a raw flowgroup YAML file.
        project_root: Project root for resolving relative paths.

    Returns:
        Deduplicated list sorted by category then alphabetically by path.
    """
    seen_paths: set[str] = set()
    results: list[RelatedFile] = []

    actions = flowgroup.get("actions") if isinstance(flowgroup, dict) else None
    if isinstance(actions, list):
        for action in actions:
            if isinstance(action, dict):
                _extract_from_action(action, project_root, seen_paths, results)

    # Sort: category order, then alphabetical within each category.
    results.sort(key=lambda r: (_CATEGORY_ORDER.get(r.category, 99), r.path))
    return results


def _extract_from_action(
    action: dict[str, Any],
    project_root: Path,
    seen: set[str],
    results: list[RelatedFile],
) -> None:
    """Extract file references from a single action dict."""
    name = action.get("name") or ""

    # --- Action-level fields ---
    _check_field(
        action.get("sql_path"), name, "sql_path", "sql", project_root, seen, results
    )
    _check_field(
        action.get("expectations_file"),
        name,
        "expectations_file",
        "expectations",
        project_root,
        seen,
        results,
    )
    _check_field(
        action.get("schema_file"),
        name,
        "schema_file",
        "schema",
        project_root,
        seen,
        results,
    )
    _check_field(
        action.get("module_path"),
        name,
        "module_path",
        "python",
        project_root,
        seen,
        results,
    )

    # --- Source dict fields (load actions; transforms use a string/array) ---
    source = action.get("source")
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

        # source.schema — ambiguous: inline DDL or file path.
        schema_val = source.get("schema")
        if isinstance(schema_val, str) and _is_file_path(schema_val):
            _check_field(
                schema_val, name, "source.schema", "schema", project_root, seen, results
            )

        # source.options["cloudFiles.schemaHints"] — ambiguous.
        options = source.get("options")
        if isinstance(options, dict):
            hints_val = options.get("cloudFiles.schemaHints")
            if isinstance(hints_val, str) and _is_file_path(hints_val):
                _check_field(
                    hints_val,
                    name,
                    "source.options.cloudFiles.schemaHints",
                    "schema",
                    project_root,
                    seen,
                    results,
                )

    # --- Write target dict fields ---
    wt = action.get("write_target")
    if isinstance(wt, dict):
        _extract_from_write_target(wt, name, project_root, seen, results)


def _extract_from_write_target(
    wt: dict[str, Any],
    action_name: str,
    project_root: Path,
    seen: set[str],
    results: list[RelatedFile],
) -> None:
    """Extract file references from a write_target dict."""
    # table_schema — ambiguous (inline DDL or path).
    ts_val = wt.get("table_schema")
    if isinstance(ts_val, str) and _is_file_path(ts_val):
        _check_field(
            ts_val,
            action_name,
            "write_target.table_schema",
            "schema",
            project_root,
            seen,
            results,
        )

    # schema — ambiguous (legacy alias).
    schema_val = wt.get("schema")
    if isinstance(schema_val, str) and _is_file_path(schema_val):
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


def _resolve_schema_ref(value: str, project_root: Path) -> tuple[str, bool]:
    """Resolve a schema reference to a project-relative path + on-disk existence.

    This is a webapp navigation aid, deliberately MORE permissive than generator
    resolution. A *bare* schema name — no file extension and no path separator,
    e.g. ``nation_schema`` — is resolved to ``schemas/<name>.<ext>`` for editor
    convenience: probe ``schemas/<name>.yaml`` then ``schemas/<name>.yml`` and
    return the first that exists (else the ``.yaml`` candidate with
    ``exists=False``). This does NOT reflect generator resolution, which treats
    an action-level ``schema_file`` as a literal relative path with no
    ``schemas/`` bare-name fallback. Anything that already reads as a path (has
    an extension or separator) or carries a substitution token (``${...}``) is
    returned unchanged, existence-checked as a literal path.
    """
    if _is_file_path(value) or any(ch in value for ch in "${}"):
        return value, (project_root / value).exists()

    for ext in (".yaml", ".yml"):
        candidate = f"schemas/{value}{ext}"
        if (project_root / candidate).exists():
            return candidate, True
    return f"schemas/{value}.yaml", False


def _check_field(
    value: Any,
    action_name: str,
    field: str,
    category: str,
    project_root: Path,
    seen: set[str],
    results: list[RelatedFile],
) -> None:
    """Add a RelatedFile if the value is a non-empty string path not yet seen."""
    if not value or not isinstance(value, str):
        return

    # Normalize the path (Windows backslashes -> forward slashes).
    normalized = value.replace("\\", "/")

    # Nav aid only: a bare schema name (no extension/separator) is resolved to
    # schemas/<name>.<ext> for editor convenience. This is more permissive than
    # generator resolution, which treats schema_file as a literal path.
    if category == "schema":
        normalized, exists = _resolve_schema_ref(normalized, project_root)
    else:
        exists = (project_root / normalized).exists()

    if normalized in seen:
        return
    seen.add(normalized)

    results.append(
        RelatedFile(
            path=normalized,
            category=category,
            action_name=action_name,
            field=field,
            exists=exists,
        )
    )
