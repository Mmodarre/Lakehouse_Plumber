"""The combined write each write action of a flowgroup is generated into.

Code generation renders the write actions of a flowgroup that target one
table as one write built from the group's lead: the write that creates the
table, else the first of the group in dependency order (the order
``DependencyResolver.resolve_dependencies`` gives the flowgroup). A
streaming-table flow with no ``readMode`` reads with the lead's ``readMode``
(the ``streaming_table.py.j2`` flow loop).

``WriteActionGrouper`` (code generation) and ``effective_read_mode`` share
these rules, so a write's read cannot be worked out two ways.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Sequence

from lhp.models import Action, ActionType

from ...errors import LHPError
from ..validators import action_creates_table
from .dependency_resolver import DependencyResolver

logger = logging.getLogger(__name__)


def write_table_key(action: Action) -> str:
    """The table ``action``'s flow is grouped by: ``catalog.schema.table``.

    From ``write_target`` (or a mapping ``source`` when there is none): the
    3-part name when catalog, schema and table (or ``name``) are all set, else
    the table, else the action's name.
    """
    target: Any = action.write_target
    if not target:
        target = action.source if isinstance(action.source, dict) else {}
    config: Dict[str, Any] = target if isinstance(target, dict) else {}
    catalog = config.get("catalog", "")
    schema = config.get("schema", "")
    table = config.get("table") or config.get("name", "")
    if catalog and schema and table:
        return f"{catalog}.{schema}.{table}"
    return table or action.name


def combined_write_lead(group: Sequence[Action]) -> Action:
    """The write a non-empty group of one table's writes is built from.

    The first that creates the table (``action_creates_table``), else the
    first of the group; ``group`` is in dependency order.
    """
    return next((a for a in group if action_creates_table(a)), group[0])


def combined_flow(action: Action, actions: Sequence[Action]) -> Optional[Action]:
    """``action`` with the ``readMode`` its flow reads with in its combined write.

    A write with no ``readMode`` takes its lead's, when another write of
    ``actions`` leads its table's group. ``None`` when the dependency order
    of ``actions`` cannot be resolved (a cycle), since the lead is then
    unknown.
    """
    if action.readMode or action.type != ActionType.WRITE:
        return action
    key = write_table_key(action)
    if sum(1 for a in actions if _same_table(a, key)) < 2:
        return action
    try:
        ordered = DependencyResolver().resolve_dependencies(list(actions))
    except (LHPError, ValueError):
        logger.debug(f"No dependency order for '{action.name}': read unknown")
        return None
    lead = combined_write_lead([a for a in ordered if _same_table(a, key)])
    if lead is action or not lead.readMode:
        return action
    return action.model_copy(update={"readMode": lead.readMode})


def _same_table(action: Action, key: str) -> bool:
    return action.type == ActionType.WRITE and write_table_key(action) == key
