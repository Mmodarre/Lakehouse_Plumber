"""Private project-statistics aggregation for the inspection API.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Builds :class:`lhp.api.responses.StatsResult` from a sequence of resolved
flowgroups. Aggregating a whole project is a different responsibility
from the per-type DTO projections in
:mod:`lhp.api._inspection_converters`, which this module depends on for
write-target normalisation.

:stability: internal
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Sequence, Set

from lhp.api._inspection_converters import _write_metadata, _write_target_as_dict
from lhp.api.responses import StatsResult
from lhp.api.views import PipelineStats

if TYPE_CHECKING:
    from lhp.models import Action, FlowGroup


# Allow-lists bounding the write / write-mode / test sub-keys. A value
# outside one of these is folded into the matching ``*_other`` key, so
# the number of distinct keys THOSE THREE families contribute is a
# property of LHP, not of the project being described. The
# ``load_<source type>`` and ``transform_<transform type>`` families are
# NOT bounded this way: they carry the raw string the project wrote, so
# an unrecognised source type becomes a key of its own.
#
# These mirror ``WriteTargetType``, the write-target ``mode`` enum in
# ``schemas/flowgroup.schema.json`` (plus the implicit ``standard``
# default), and ``TestActionType``; the enums are not imported because
# ``lhp.api`` projects domain types rather than depending on them at
# runtime, and ``tests/api/test_stats_builder.py`` asserts the two sets
# stay in step with their enums.
_WRITE_TARGET_TYPES = frozenset({"streaming_table", "materialized_view", "sink"})
_WRITE_MODES = frozenset({"standard", "cdc", "snapshot_cdc"})
_TEST_TYPES = frozenset(
    {
        "row_count",
        "uniqueness",
        "referential_integrity",
        "completeness",
        "range",
        "schema_match",
        "all_lookups_found",
        "custom_sql",
        "custom_expectations",
    }
)


def _bump(counts: Dict[str, int], key: str) -> None:
    """Increment ``key``, creating it at 1 when it is not yet present."""
    counts[key] = counts.get(key, 0) + 1


def _plain_value(value: object) -> str:
    """Render a field as its plain string, unwrapping a ``str``-mixin enum.

    A write target reaches here either as raw YAML (plain strings) or as
    a dumped :class:`WriteTarget` model (``WriteTargetType`` members).
    Those enums do not inherit :class:`enum.ReprEnum`, so ``str()`` and
    f-strings render them as ``"WriteTargetType.SINK"`` rather than
    ``"sink"``; unwrapping keeps one spelling per value in the counts.
    """
    return str(getattr(value, "value", value))


def _count_write_action(
    action: "Action", counts: Dict[str, int], targets: Set[str]
) -> None:
    """Count the write sub-keys for one write action.

    A write target whose ``type`` is outside :data:`_WRITE_TARGET_TYPES`
    — including a write action carrying no target at all — is counted as
    ``write_other`` and contributes neither a write mode nor a target:
    nothing else about it can be read with confidence. Sinks are not
    tables and carry no write mode.
    """
    write_type = _plain_value(_write_target_as_dict(action.write_target).get("type"))
    if write_type not in _WRITE_TARGET_TYPES:
        _bump(counts, "write_other")
        return

    _bump(counts, f"write_{write_type}")
    if write_type == "sink":
        return

    write_mode, _scd_type, target_full_name = _write_metadata(action)
    mode = _plain_value(write_mode)
    _bump(counts, f"write_mode_{mode if mode in _WRITE_MODES else 'other'}")
    if target_full_name:
        targets.add(target_full_name)


def _count_test_action(action: "Action", counts: Dict[str, int]) -> None:
    """Count the ``test_<type>`` sub-key for one test action."""
    test_type = _plain_value(action.test_type)
    _bump(counts, f"test_{test_type if test_type in _TEST_TYPES else 'other'}")


def _build_stats_result(flowgroups: Sequence["FlowGroup"]) -> StatsResult:
    """Aggregate a list of flowgroups into a :class:`StatsResult`.

    Walks every action exactly once. ``action_counts_by_type`` keys are
    the lowercase :class:`ActionType` enum values (``"load"``,
    ``"transform"``, ``"write"``, ``"test"``) plus the sub-keys tracked
    for load source types, transform subtypes, write target types, write
    modes and test types (e.g. ``"load_cloudfiles"``,
    ``"transform_sql"``, ``"write_streaming_table"``,
    ``"write_mode_cdc"``, ``"test_uniqueness"``), and ``"tables"`` — the
    number of DISTINCT non-sink targets written, so a table written by
    two actions counts once. No key is ever emitted with a zero value.
    """
    pipelines: Dict[str, Dict[str, int]] = {}
    action_counts: Dict[str, int] = {}
    templates_used: set[str] = set()
    presets_used: set[str] = set()
    write_targets: Set[str] = set()
    total_actions = 0

    for fg in flowgroups:
        pipeline_row = pipelines.setdefault(
            fg.pipeline, {"flowgroups": 0, "actions": 0}
        )
        pipeline_row["flowgroups"] += 1
        if fg.use_template:
            templates_used.add(fg.use_template)
        for preset in fg.presets or ():
            presets_used.add(preset)
        for action in fg.actions:
            type_value = action.type.value
            _bump(action_counts, type_value)
            pipeline_row["actions"] += 1
            total_actions += 1
            if type_value == "load" and isinstance(action.source, dict):
                subtype = str(action.source.get("type", "unknown"))
                _bump(action_counts, f"load_{subtype}")
            elif type_value == "transform" and action.transform_type:
                _bump(action_counts, f"transform_{action.transform_type}")
            elif type_value == "write":
                _count_write_action(action, action_counts, write_targets)
            elif type_value == "test":
                _count_test_action(action, action_counts)

    if write_targets:
        action_counts["tables"] = len(write_targets)

    breakdown = tuple(
        PipelineStats(
            pipeline_name=name,
            flowgroup_count=row["flowgroups"],
            total_actions=row["actions"],
        )
        for name, row in sorted(pipelines.items())
    )
    return StatsResult(
        pipeline_count=len(pipelines),
        flowgroup_count=sum(row["flowgroups"] for row in pipelines.values()),
        total_actions=total_actions,
        action_counts_by_type=action_counts,
        pipeline_breakdown=breakdown,
        templates_used=tuple(sorted(templates_used)),
        presets_used=tuple(sorted(presets_used)),
    )
