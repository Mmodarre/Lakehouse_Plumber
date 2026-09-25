"""REPLACE USING (``mode: replace``) fan-in compatibility validation.

A REPLACE USING target must be created within the pipeline and served by
*exactly one* replace flow — it cannot be combined with any other flow on the
same ``catalog.schema.table``. Because ``mode: replace`` forces
``create_table=True``, two replace actions (or a replace plus another creator)
already trip ``TableCreationValidator`` (CFG_004); this validator closes the
remaining gap: a ``replace`` action sharing its target with a
``create_table: false`` user of any mode.

Errors fold into the existing CDC fan-in error family (LHP-VAL-010) at the
single cross-flowgroup composition site.
"""

import logging
from typing import List

from lhp.models import Action, FlowGroup

from ._fanin_common import group_write_actions_by_table
from .table_creation import action_creates_table

logger = logging.getLogger(__name__)


class ReplaceFanInCompatibilityValidator:
    def validate(self, flowgroups: List[FlowGroup]) -> List[str]:
        logger.debug(
            f"Validating replace fan-in compatibility across "
            f"{len(flowgroups)} flowgroup(s)"
        )
        errors: List[str] = []

        for table_name, contributors in group_write_actions_by_table(
            flowgroups
        ).items():
            if not any(self._is_replace(a) for _, a in contributors):
                continue
            # A replace flow must be the sole flow for its target. Two creators
            # (e.g. two replace flows, or replace + another create_table:true
            # writer) are already reported by TableCreationValidator (CFG_004),
            # which runs first; reporting them here too would double up. So we
            # only flag the gap CFG_004 misses: create_table:false users sharing
            # the replace target, which would otherwise be silently combined
            # into the single replace flow.
            non_creators = [
                (fg, a) for fg, a in contributors if not action_creates_table(a)
            ]
            if non_creators:
                offenders = [f"{fg.flowgroup}.{a.name}" for fg, a in non_creators]
                errors.append(
                    f"Table '{table_name}' is served by a replace (REPLACE USING) "
                    f"flow, which must be the sole flow for the table, but these "
                    f"write actions also target it: {', '.join(offenders)}. A "
                    f"REPLACE USING target cannot be combined with other flows — "
                    f"give it its own table."
                )

        return errors

    def _is_replace(self, action: Action) -> bool:
        wt = action.write_target
        if isinstance(wt, dict):
            return wt.get("mode") == "replace"
        return getattr(wt, "mode", None) == "replace"
