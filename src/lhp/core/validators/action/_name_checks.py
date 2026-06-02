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
