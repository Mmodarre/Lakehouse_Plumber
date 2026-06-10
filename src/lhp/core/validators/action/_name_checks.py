"""Shared name-shape checks for action validators.

Module-level free functions split out so multiple action validators can
share a single implementation of the 3-part-name check (constitution §3.3).
"""

from typing import Optional


def require_three_part_name(value: object, label: str, prefix: str) -> Optional[str]:
    """Error string if value is a non-3-part str name; None otherwise (non-str passes)."""
    if isinstance(value, str) and value.count(".") != 2:
        return (
            f"{prefix}: {label} must be a 3-part name "
            f"(catalog.schema.table), got '{value}'"
        )
    return None


def require_equal_length(
    left: object,
    right: object,
    left_label: str,
    right_label: str,
    prefix: str,
) -> Optional[str]:
    """Error string if both lists are non-empty and lengths differ; None otherwise."""
    if left and right and len(left) != len(right):
        return (
            f"{prefix}: {left_label} ({len(left)}) and {right_label} "
            f"({len(right)}) must have the same number of columns"
        )
    return None
