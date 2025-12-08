"""Utilities for extracting source view information from action configurations."""

from typing import List, Union, Dict, Any


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
            database = first_item.get("database")
            table = first_item.get("table") or first_item.get("view") or first_item.get("name", "")
            return f"{database}.{table}" if database and table else table
        else:
            return str(first_item)
    elif isinstance(source, dict):
        database = source.get("database")
        table = source.get("table") or source.get("view") or source.get("name", "")
        return f"{database}.{table}" if database and table else table
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
                database = item.get("database")
                table = item.get("table") or item.get("view") or item.get("name", "")
                if database and table:
                    result.append(f"{database}.{table}")
                elif table:
                    result.append(table)
            else:
                result.append(str(item))
        return result
    elif isinstance(source, dict):
        database = source.get("database")
        table = source.get("table") or source.get("view") or source.get("name", "")
        if database and table:
            return [f"{database}.{table}"]
        elif table:
            return [table]
        else:
            # Return generic "source" for non-table sources (e.g., Kafka, custom sources)
            # This prevents empty lists that could cause issues in code generation
            return ["source"]
    else:
        return ["source"]  # Fallback for unknown types

