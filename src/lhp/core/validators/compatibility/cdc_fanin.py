"""CDC fan-in compatibility validation for write actions.

When multiple write actions in ``mode: cdc`` target the same
``catalog.schema.table``, the resulting file emits one
``dp.create_streaming_table`` plus N ``dp.create_auto_cdc_flow`` calls. All
contributors must agree on table-level and CDC-key fields; they may differ
only on per-flow fields (``ignore_null_updates``, ``apply_as_deletes``,
``apply_as_truncates``, ``column_list``, ``except_column_list``, ``once``).

This validator fires after ``TableCreationValidator`` and is narrower in
scope: it only looks at ``mode == "cdc"`` contributors. Snapshot CDC is
deliberately excluded — it uses its own single-flow primitive and is
governed by ``TableCreationValidator`` alone.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

from lhp.models import Action, ActionType, FlowGroup

from ._cdc_fanin_messages import mismatch_error, mode_mix_message

logger = logging.getLogger(__name__)


# cdc_config fields that must agree across all CDC contributors to one table.
# These render at the table level (table-scoped or single-emission):
#   - keys / sequence_by / stored_as_scd_type drive table schema (__START_AT/__END_AT)
#   - track_history_* are rendered on create_auto_cdc_flow but must match
#     across flows because they define a single history-tracking semantic
_SHARED_CDC_CONFIG_FIELDS: Tuple[str, ...] = (
    "keys",
    "sequence_by",
    "stored_as_scd_type",
    "scd_type",  # Legacy alias
    "track_history_column_list",
    "track_history_except_column_list",
)

# write_target fields that must agree across all CDC contributors.
# These are table-level properties rendered once in dp.create_streaming_table.
_SHARED_TARGET_FIELDS: Tuple[str, ...] = (
    "partition_columns",
    "cluster_columns",
    "cluster_by_auto",
    "table_properties",
    "spark_conf",
    "table_schema",
    "comment",
    "path",
    "row_filter",
    "temporary",
)

# Default values so we can compare "missing" and "explicit default" as equal
# when deciding whether two contributors disagree.
_FIELD_DEFAULTS: Dict[str, Any] = {
    "stored_as_scd_type": 1,
    "scd_type": 1,
    "temporary": False,
}


class CdcFanInCompatibilityValidator:
    def validate(self, flowgroups: List[FlowGroup]) -> List[str]:
        """Validate CDC fan-in compatibility.

        Returns:
            List of error strings. May also raise ``LHPConfigError`` for
            rich, structured diagnostics on field mismatches.
        """
        logger.debug(
            f"Validating CDC fan-in compatibility across {len(flowgroups)} flowgroup(s)"
        )
        errors: List[str] = []

        by_table: Dict[str, List[Tuple[FlowGroup, Action]]] = defaultdict(list)
        for fg in flowgroups:
            for action in fg.actions:
                if action.type != ActionType.WRITE or not action.write_target:
                    continue
                name = self._full_name(action.write_target)
                if name:
                    by_table[name].append((fg, action))

        for table_name, contributors in by_table.items():
            cdc_contribs = [(fg, a) for fg, a in contributors if self._is_cdc(a)]
            if not cdc_contribs:
                continue

            # (a) mode uniformity: every contributor at this target must be CDC.
            non_cdc = [(fg, a) for fg, a in contributors if not self._is_cdc(a)]
            if non_cdc:
                errors.append(mode_mix_message(table_name, cdc_contribs, non_cdc))
                # Don't bother checking field mismatches when modes collide.
                continue

            for field in _SHARED_CDC_CONFIG_FIELDS:
                mismatch = self._check_equal(cdc_contribs, "cdc_config", field)
                if mismatch:
                    raise mismatch_error(table_name, "cdc_config", field, mismatch)

            for field in _SHARED_TARGET_FIELDS:
                mismatch = self._check_equal(cdc_contribs, "write_target", field)
                if mismatch:
                    raise mismatch_error(table_name, "write_target", field, mismatch)

        return errors

    def _is_cdc(self, action: Action) -> bool:
        """Return True only for CDC (non-snapshot) write actions."""
        wt = action.write_target
        if isinstance(wt, dict):
            return wt.get("mode") == "cdc"
        return getattr(wt, "mode", None) == "cdc"

    def _full_name(self, write_target: Union[Dict[str, Any], Any]) -> Optional[str]:
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

    def _get_field_value(self, action: Action, scope: str, field: str) -> Any:
        """Extract a field from either cdc_config or the flat write_target.

        Applies sentinel defaults for fields that have a documented implicit
        default so an unset value and an explicit default compare equal.
        """
        wt = action.write_target
        wt_dict = wt if isinstance(wt, dict) else {}

        if scope == "cdc_config":
            container = wt_dict.get("cdc_config", {}) or {}
        else:
            container = wt_dict

        value = container.get(field)
        if value is None and field in _FIELD_DEFAULTS:
            value = _FIELD_DEFAULTS[field]
        return value

    def _check_equal(
        self,
        contributors: List[Tuple[FlowGroup, Action]],
        scope: str,
        field: str,
    ) -> Optional[Dict[str, Any]]:
        """Return a dict of action_id -> value if contributors disagree.

        Returns ``None`` when all contributors either agree or all omit the
        field (so no mismatch to report).
        """
        values: Dict[str, Any] = {}
        distinct: List[Any] = []
        for fg, a in contributors:
            val = self._get_field_value(a, scope, field)
            values[f"{fg.flowgroup}.{a.name}"] = val
            if not any(self._values_equal(val, d) for d in distinct):
                distinct.append(val)

        # All values agree (including all being None) — no mismatch.
        if len(distinct) <= 1:
            return None
        return values

    @staticmethod
    def _values_equal(a: Any, b: Any) -> bool:
        return a == b
