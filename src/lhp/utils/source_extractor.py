"""Utilities for extracting source view information from action configurations.

This module is the single source of truth for extracting source references
from actions. It is used by:
- Code generators (extract_source_views_from_action, extract_single_source_view)
- DependencyResolver (extract_action_sources)
- DependencyAnalyzer (extract_action_sources for explicit source fallback)
"""

import logging
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


def extract_single_source_view(source: Union[str, List, Dict]) -> str:
    """Extract a single source view from various source formats.

    Args:
        source: Source configuration (string, list, or dict)

    Returns:
        Source view name as string, or empty string if not found
    """
    if isinstance(source, str):
        return source
    elif isinstance(source, list) and source:
        first_item = source[0]
        if isinstance(first_item, str):
            return first_item
        elif isinstance(first_item, dict):
            catalog = first_item.get("catalog")
            schema = first_item.get("schema")
            table = (
                first_item.get("table")
                or first_item.get("view")
                or first_item.get("name", "")
            )
            if catalog and schema and table:
                return f"{catalog}.{schema}.{table}"
            return table
        else:
            return str(first_item)
    elif isinstance(source, dict):
        catalog = source.get("catalog")
        schema = source.get("schema")
        table = source.get("table") or source.get("view") or source.get("name", "")
        if catalog and schema and table:
            return f"{catalog}.{schema}.{table}"
        return table
    else:
        return ""


def extract_source_views_from_action(source: Union[str, List, Dict]) -> List[str]:
    """Extract all source views from an action source configuration.

    This function handles various source formats and always returns a list.
    For sources without explicit table names (e.g., Kafka topics), returns
    a generic "source" placeholder to maintain consistency in code generation.

    Args:
        source: Source configuration (string, list, or dict)

    Returns:
        List of source view names, or ["source"] as fallback for non-table sources
    """
    if isinstance(source, str):
        return [source]
    elif isinstance(source, list):
        result = []
        for item in source:
            if isinstance(item, str):
                result.append(item)
            elif isinstance(item, dict):
                catalog = item.get("catalog")
                schema = item.get("schema")
                table = item.get("table") or item.get("view") or item.get("name", "")
                if catalog and schema and table:
                    result.append(f"{catalog}.{schema}.{table}")
                elif table:
                    result.append(table)
            else:
                result.append(str(item))
        return result
    elif isinstance(source, dict):
        catalog = source.get("catalog")
        schema = source.get("schema")
        table = source.get("table") or source.get("view") or source.get("name", "")
        if catalog and schema and table:
            return [f"{catalog}.{schema}.{table}"]
        elif table:
            return [table]
        else:
            # Return generic "source" for non-table sources (e.g., Kafka, custom sources)
            # This prevents empty lists that could cause issues in code generation
            return ["source"]
    else:
        return ["source"]  # Fallback for unknown types


def is_cdc_write_action(action: Any) -> bool:
    """Check if action is a CDC write action (cdc or snapshot_cdc mode).

    Args:
        action: An Action model instance

    Returns:
        True if the action is a write action with CDC or snapshot_cdc mode
    """
    if not hasattr(action, "type") or not hasattr(action, "write_target"):
        return False
    # ActionType is a str enum — compare with == "write" (works via str.__eq__)
    if action.type != "write":
        return False
    return (
        action.write_target
        and isinstance(action.write_target, dict)
        and action.write_target.get("mode") in ["cdc", "snapshot_cdc"]
    )


def extract_cdc_sources(action: Any) -> Optional[List[str]]:
    """Extract sources from CDC write actions.

    For CDC modes, the precedence order is:
    1. source_function (snapshot_cdc only) -> no external dependencies (returns [])
    2. cdc_config.source / snapshot_cdc_config.source -> explicit CDC source
    3. Returns None to signal fallback to action.source

    Args:
        action: An Action model instance with CDC write_target

    Returns:
        List of source names, empty list for self-contained actions,
        or None to signal fallback to action.source
    """
    if not action.write_target or not isinstance(action.write_target, dict):
        return None

    mode = action.write_target.get("mode")

    if mode == "cdc":
        cdc_config = action.write_target.get("cdc_config", {})
        if cdc_config.get("source"):
            return [cdc_config["source"]]

    elif mode == "snapshot_cdc":
        snapshot_config = action.write_target.get("snapshot_cdc_config", {})

        # Priority 1: source_function (self-contained, no external dependencies)
        if snapshot_config.get("source_function"):
            return []  # Source function is internal - no external dependencies

        # Priority 2: snapshot_cdc_config.source (explicit CDC source reference)
        elif snapshot_config.get("source"):
            return [snapshot_config["source"]]

    # No CDC-specific source found, fallback to action.source
    return None


def extract_action_sources(action: Any) -> List[str]:
    """Extract source names from an action for dependency analysis.

    This is the shared extraction function used by both DependencyResolver
    and DependencyAnalyzer (as explicit source fallback). It handles:
    - CDC write actions (cdc_config, snapshot_cdc_config)
    - String, list, and dict source formats
    - view/source/sources dict keys
    - database.table format from load actions

    Args:
        action: An Action model instance

    Returns:
        List of source dependency names
    """
    sources: List[str] = []

    # Special handling for CDC write actions
    if is_cdc_write_action(action):
        cdc_sources = extract_cdc_sources(action)
        if cdc_sources is not None:  # None means fallback to action.source
            return cdc_sources

    # Standard source extraction
    if not hasattr(action, "source") or not action.source:
        return sources

    source = action.source

    if isinstance(source, str):
        sources.append(source)
    elif isinstance(source, list):
        for item in source:
            if isinstance(item, str):
                sources.append(item)
    elif isinstance(source, dict):
        # Priority order for dict sources
        if "view" in source:
            sources.append(source["view"])
        elif "source" in source:
            source_val = source["source"]
            if isinstance(source_val, str):
                sources.append(source_val)
            elif isinstance(source_val, list):
                sources.extend(source_val)
        elif "sources" in source:
            sources.extend(source["sources"])
        elif "table" in source:
            catalog = source.get("catalog", "")
            schema = source.get("schema", "")
            table = source.get("table", "")
            if catalog and schema and table:
                sources.append(f"{catalog}.{schema}.{table}")
            elif table:
                sources.append(table)

    return sources
