"""Shared-DLQ ``source_table`` uniqueness validation (``LHP-VAL-069``).

Quarantine actions (``data_quality`` transforms with ``mode: quarantine``) may
share one DLQ table. Every generated inbox and outbox MERGE, and the recycle and
outbox reads, filter on ``_dlq_source_table == source_table``. Two actions that
share a DLQ *and* a ``source_table`` therefore read and write the same slice:
their MERGEs can conflict, and each action's recycle stream picks up the other's
fixed rows.

``dlq_table`` is compared case-insensitively because Unity Catalog names are;
``source_table`` is compared exactly because the generated filters compare it
as a string literal.

Composed only by ``ValidationService.validate_cross_flowgroup`` (§9.24) on a
pipeline's resolved flowgroups; its errors form their own ``LHP-VAL-069``
family.
"""

import logging
from typing import Dict, List, Optional, Tuple

from lhp.models import Action, ActionType, FlowGroup, TransformType

logger = logging.getLogger(__name__)


def _quarantine_tables(action: Action) -> Optional[Tuple[str, str]]:
    """Return ``(dlq_table, source_table)`` for a complete quarantine action.

    Incomplete configs return ``None``: the per-flowgroup data-quality
    validator already reports a missing or empty field.
    """
    if (
        action.type != ActionType.TRANSFORM
        or action.transform_type != TransformType.DATA_QUALITY
        or action.mode != "quarantine"
        or action.quarantine is None
    ):
        return None
    dlq_table = getattr(action.quarantine, "dlq_table", None)
    source_table = getattr(action.quarantine, "source_table", None)
    if not isinstance(dlq_table, str) or not isinstance(source_table, str):
        return None
    if not dlq_table or not source_table:
        return None
    return dlq_table, source_table


class SharedDlqSourceTableValidator:
    def validate(self, flowgroups: List[FlowGroup]) -> List[str]:
        logger.debug(
            f"Validating shared-DLQ source_table uniqueness across "
            f"{len(flowgroups)} flowgroup(s)"
        )
        groups: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        dlq_spelling: Dict[Tuple[str, str], str] = {}
        for fg in flowgroups:
            for action in fg.actions:
                tables = _quarantine_tables(action)
                if tables is None:
                    continue
                dlq_table, source_table = tables
                key = (dlq_table.casefold(), source_table)
                dlq_spelling.setdefault(key, dlq_table)
                groups.setdefault(key, []).append((fg.flowgroup, action.name))

        errors: List[str] = []
        for key, members in groups.items():
            if len(members) < 2:
                continue
            offenders = ", ".join(f"{fg}.{name}" for fg, name in members)
            errors.append(
                f"Quarantine actions {offenders} share DLQ table "
                f"'{dlq_spelling[key]}' and source_table '{key[1]}'. Every "
                f"generated DLQ MERGE and recycle read filters on source_table, "
                f"so these actions would write the same DLQ and outbox rows and "
                f"each would recycle the others' fixed rows. Give each action a "
                f"distinct source_table."
            )
        logger.debug(f"Found {len(errors)} shared-DLQ source_table conflict(s)")
        return errors
