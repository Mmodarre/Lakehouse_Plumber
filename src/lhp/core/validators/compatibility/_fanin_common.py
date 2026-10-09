"""Shared helpers for write-action fan-in validators.

Both ``CdcFanInCompatibilityValidator`` and ``ReplaceFanInCompatibilityValidator``
group write actions by their fully-qualified target table and resolve that name
the same way. This module is the single home for that logic so the two
validators differ only in their per-table compatibility rules and diagnostics.
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

from lhp.models import Action, ActionType, FlowGroup


def full_target_name(write_target: Union[Dict[str, Any], Any]) -> Optional[str]:
    """Return ``catalog.schema.table`` for a write target, or ``None`` if incomplete."""
    if isinstance(write_target, dict):
        catalog = write_target.get("catalog")
        schema = write_target.get("schema")
        table = write_target.get("table") or write_target.get("name")
    else:
        catalog = getattr(write_target, "catalog", None)
        schema = getattr(write_target, "schema", None)
        table = getattr(write_target, "table", None)
    if not catalog or not schema or not table:
        return None
    return f"{catalog}.{schema}.{table}"


def group_write_actions_by_table(
    flowgroups: List[FlowGroup],
) -> Dict[str, List[Tuple[FlowGroup, Action]]]:
    """Group every write action across ``flowgroups`` by its target table name."""
    by_table: Dict[str, List[Tuple[FlowGroup, Action]]] = defaultdict(list)
    for fg in flowgroups:
        for action in fg.actions:
            if action.type != ActionType.WRITE or not action.write_target:
                continue
            name = full_target_name(action.write_target)
            if name:
                by_table[name].append((fg, action))
    return by_table
