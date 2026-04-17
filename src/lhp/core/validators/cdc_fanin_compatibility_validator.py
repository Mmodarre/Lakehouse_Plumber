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

from ...models.config import Action, ActionType, FlowGroup
from ...utils.error_formatter import ErrorCategory, LHPConfigError

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
    """Cross-action compatibility for CDC fan-in.

    Groups every write action by full target name, filters to the CDC
    contributors, and validates that the shared fields match. Also rejects
    mode-mixing (CDC action + non-CDC action both targeting the same table).
    """

    def validate(self, flowgroups: List[FlowGroup]) -> List[str]:
        """Validate CDC fan-in compatibility.

        Args:
            flowgroups: All flowgroups in the pipeline.

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
                errors.append(self._mode_mix_message(table_name, cdc_contribs, non_cdc))
                # Don't bother checking field mismatches when modes collide.
                continue

            # (b) must-match cdc_config fields
            for field in _SHARED_CDC_CONFIG_FIELDS:
                mismatch = self._check_equal(cdc_contribs, "cdc_config", field)
                if mismatch:
                    raise self._mismatch_error(
                        table_name, "cdc_config", field, mismatch
                    )

            # (c) must-match write_target table-level fields
            for field in _SHARED_TARGET_FIELDS:
                mismatch = self._check_equal(cdc_contribs, "write_target", field)
                if mismatch:
                    raise self._mismatch_error(
                        table_name, "write_target", field, mismatch
                    )

        return errors

    def _is_cdc(self, action: Action) -> bool:
        """Return True only for CDC (non-snapshot) write actions."""
        wt = action.write_target
        if isinstance(wt, dict):
            return wt.get("mode") == "cdc"
        return getattr(wt, "mode", None) == "cdc"

    def _full_name(self, write_target: Union[Dict[str, Any], Any]) -> Optional[str]:
        """Build full ``catalog.schema.table`` name if fully specified."""
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
        """Equality that treats dicts/lists by value and handles None."""
        return a == b

    def _mode_mix_message(
        self,
        table_name: str,
        cdc_contribs: List[Tuple[FlowGroup, Action]],
        non_cdc_contribs: List[Tuple[FlowGroup, Action]],
    ) -> str:
        cdc_list = ", ".join(f"{fg.flowgroup}.{a.name}" for fg, a in cdc_contribs)
        other_list = ", ".join(
            f"{fg.flowgroup}.{a.name} (mode={self._mode_of(a)})"
            for fg, a in non_cdc_contribs
        )
        return (
            f"Table '{table_name}': cannot mix CDC and non-CDC write actions "
            f"targeting the same table. CDC: [{cdc_list}]. Non-CDC: [{other_list}]. "
            f"Either use CDC mode for all contributors or split the targets."
        )

    @staticmethod
    def _mode_of(action: Action) -> str:
        wt = action.write_target
        if isinstance(wt, dict):
            return wt.get("mode", "standard")
        return getattr(wt, "mode", "standard") or "standard"

    def _mismatch_error(
        self,
        table_name: str,
        scope: str,
        field: str,
        values_by_action: Dict[str, Any],
    ) -> LHPConfigError:
        """Build a rich ``LHPConfigError`` for a mismatched shared field."""
        readable_field = f"cdc_config.{field}" if scope == "cdc_config" else field
        example_text = (
            "All CDC actions targeting the same table must agree on table-\n"
            "level and CDC-key fields. For example:\n\n"
            "- name: write_flow_1\n"
            "  type: write\n"
            "  source: v_source_1\n"
            "  write_target:\n"
            "    type: streaming_table\n"
            "    mode: cdc\n"
            f"    # {readable_field}: <shared_value>   # ← Must match across flows\n"
            "    create_table: true\n\n"
            "- name: write_flow_2\n"
            "  type: write\n"
            "  source: v_source_2\n"
            "  write_target:\n"
            "    type: streaming_table\n"
            "    mode: cdc\n"
            f"    # {readable_field}: <shared_value>   # ← Same value as above\n"
            "    create_table: false"
        )
        return LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="010",
            title=(
                f"CDC fan-in mismatch on '{readable_field}' for table "
                f"'{table_name}'"
            ),
            details=(
                f"Table '{table_name}' has multiple CDC write actions that "
                f"disagree on '{readable_field}'. All CDC contributors must "
                f"agree on this field; only per-flow fields (source, once, "
                f"ignore_null_updates, apply_as_deletes, apply_as_truncates, "
                f"column_list, except_column_list) may differ."
            ),
            suggestions=[
                (
                    f"Reconcile '{readable_field}' across all CDC actions "
                    f"targeting '{table_name}'"
                ),
                (
                    "If different values are needed, route the flows to "
                    "separate target tables"
                ),
                "Run 'lhp validate --env <env>' for full diagnostics",
            ],
            example=example_text,
            context={
                "Table": table_name,
                "Field": readable_field,
                "Values by action": values_by_action,
            },
        )
