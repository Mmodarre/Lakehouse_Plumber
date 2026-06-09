"""Source extraction for dependency analysis.

This module is the single source of truth for extracting source references
from actions for dependency analysis. It is used by:
- DependencyResolver (extract_action_sources)
- DependencyAnalysisService (extract_action_sources for explicit source fallback)

:stability: internal
"""

from typing import Any, List, Optional


def is_cdc_write_action(action: Any) -> bool:
    """Check if action is a CDC write action (cdc or snapshot_cdc mode)."""
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

    Precedence: source_function (snapshot_cdc only) → [] (no external deps);
    cdc_config.source / snapshot_cdc_config.source → explicit CDC source;
    returns None to signal fallback to action.source.
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
        if snapshot_config.get("source"):
            return [snapshot_config["source"]]

    # No CDC-specific source found, fallback to action.source
    return None


def extract_action_sources(action: Any) -> List[str]:
    """Extract source names from an action for dependency analysis.

    Shared by DependencyResolver and DependencyAnalysisService (explicit source fallback).
    Handles CDC write actions, string/list/dict source formats, and view/source/sources/table dict keys.
    """
    sources: List[str] = []

    if is_cdc_write_action(action):
        cdc_sources = extract_cdc_sources(action)
        if cdc_sources is not None:  # None means fallback to action.source
            return cdc_sources

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
