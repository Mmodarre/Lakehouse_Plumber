"""REPLACE USING / REPLACE WHERE fan-in compatibility validation.

A REPLACE USING (``mode: replace``) or REPLACE WHERE (``mode: replace_where``)
target must be created within the pipeline and served by *exactly one* flow —
it cannot be combined with any other flow on the same ``catalog.schema.table``.
Because both modes force ``create_table=True``, two such actions (or one plus
another creator) already trip ``TableCreationValidator`` (CFG_004); this
validator closes the remaining gap: a replace-family action sharing its target
with a ``create_table: false`` user of any mode.

Errors fold into the existing CDC fan-in error family (LHP-VAL-010) at the
single cross-flowgroup composition site.
"""

import logging
from typing import Any, List

from lhp.models import Action, FlowGroup

from ._fanin_common import group_write_actions_by_table
from .table_creation import action_creates_table

logger = logging.getLogger(__name__)

# Each replace-family mode and the Databricks flow it generates.
_REPLACE_FLOW_KINDS = {"replace": "REPLACE USING", "replace_where": "REPLACE WHERE"}


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
            replace_modes = [
                _write_mode(a) for _, a in contributors if self._is_replace(a)
            ]
            if not replace_modes:
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
                mode = replace_modes[0]
                flow_kind = _REPLACE_FLOW_KINDS[mode]
                offenders = [f"{fg.flowgroup}.{a.name}" for fg, a in non_creators]
                errors.append(
                    f"Table '{table_name}' is served by a {mode} ({flow_kind}) "
                    f"flow, which must be the sole flow for the table, but these "
                    f"write actions also target it: {', '.join(offenders)}. A "
                    f"{flow_kind} target cannot be combined with other flows — "
                    f"give it its own table."
                )

        return errors

    def _is_replace(self, action: Action) -> bool:
        mode = _write_mode(action)
        return isinstance(mode, str) and mode in _REPLACE_FLOW_KINDS


def _write_mode(action: Action) -> Any:
    wt = action.write_target
    if isinstance(wt, dict):
        return wt.get("mode")
    return getattr(wt, "mode", None)
