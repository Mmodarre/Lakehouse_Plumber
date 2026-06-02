"""Pure error/message builders for CDC fan-in compatibility validation.

Split out of ``CdcFanInCompatibilityValidator`` so ``cdc_fanin.py`` stays
within the §1 validator file-size budget. These are stateless builders: they
take only their arguments and produce a message string or a structured
``LHPConfigError`` — no instance state is read.
"""

from typing import Any, Dict, List, Tuple

from lhp.models import Action, FlowGroup

from ....errors import ErrorFactory, LHPConfigError, codes


def mode_of(action: Action) -> str:
    wt = action.write_target
    if isinstance(wt, dict):
        return wt.get("mode", "standard")
    return getattr(wt, "mode", "standard") or "standard"


def mode_mix_message(
    table_name: str,
    cdc_contribs: List[Tuple[FlowGroup, Action]],
    non_cdc_contribs: List[Tuple[FlowGroup, Action]],
) -> str:
    cdc_list = ", ".join(f"{fg.flowgroup}.{a.name}" for fg, a in cdc_contribs)
    other_list = ", ".join(
        f"{fg.flowgroup}.{a.name} (mode={mode_of(a)})" for fg, a in non_cdc_contribs
    )
    return (
        f"Table '{table_name}': cannot mix CDC and non-CDC write actions "
        f"targeting the same table. CDC: [{cdc_list}]. Non-CDC: [{other_list}]. "
        f"Either use CDC mode for all contributors or split the targets."
    )


def mismatch_error(
    table_name: str,
    scope: str,
    field: str,
    values_by_action: Dict[str, Any],
) -> LHPConfigError:
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
    return ErrorFactory.config_error(
        codes.CFG_010,
        title=(
            f"CDC fan-in mismatch on '{readable_field}' for table " f"'{table_name}'"
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
