"""The bounded project-shape allowlist carried by ``cli.command`` events.

The shape is a FIXED set of counters and flags. Folding is what keeps it
fixed: a project that uses an unrecognised load source, transform type, write
target, write mode or test type contributes to its family's ``*_other``
counter instead of inventing a key, so the number of distinct keys reaching
the wire is a property of LHP, not of the project being described. Every key
outside the allowlist is dropped.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Dict, Mapping, Optional, Tuple, Union

# Families whose sub-key is open-ended at the source and therefore folded.
# ``write_mode_`` precedes ``write_`` because the longer prefix must win.
_FAMILY_PREFIXES: Tuple[str, ...] = (
    "write_mode_",
    "load_",
    "transform_",
    "write_",
    "test_",
)


@dataclass(frozen=True)
class ProjectShape:
    """Size and feature counters for one project.

    Counters default to 0 and flags to False so a caller that cannot read a
    given source omits the key rather than guessing a value.
    """

    pipelines: int = 0
    flowgroups: int = 0
    actions: int = 0
    tables: int = 0
    load_cloudfiles: int = 0
    load_delta: int = 0
    load_sql: int = 0
    load_python: int = 0
    load_jdbc: int = 0
    load_custom_datasource: int = 0
    load_kafka: int = 0
    load_other: int = 0
    transform_sql: int = 0
    transform_python: int = 0
    transform_data_quality: int = 0
    transform_temp_table: int = 0
    transform_schema: int = 0
    transform_other: int = 0
    write_streaming_table: int = 0
    write_materialized_view: int = 0
    write_sink: int = 0
    write_other: int = 0
    write_mode_standard: int = 0
    write_mode_cdc: int = 0
    write_mode_snapshot_cdc: int = 0
    write_mode_other: int = 0
    test_row_count: int = 0
    test_uniqueness: int = 0
    test_referential_integrity: int = 0
    test_completeness: int = 0
    test_range: int = 0
    test_schema_match: int = 0
    test_all_lookups_found: int = 0
    test_custom_sql: int = 0
    test_custom_expectations: int = 0
    test_other: int = 0
    templates: int = 0
    flowgroups_using_templates: int = 0
    presets: int = 0
    blueprints: int = 0
    blueprint_instances: int = 0
    environments: int = 0
    has_operational_metadata: bool = False
    has_event_log: bool = False
    has_monitoring: bool = False
    has_uc_tagging: bool = False
    has_test_reporting: bool = False
    has_wheel: bool = False
    has_sandbox: bool = False
    has_required_lhp_version: bool = False
    apply_formatting: bool = False


PROJECT_SHAPE_KEYS: Tuple[str, ...] = tuple(f.name for f in fields(ProjectShape))
_ALLOWED_KEYS = frozenset(PROJECT_SHAPE_KEYS)
_FLAG_KEYS = frozenset(
    f.name for f in fields(ProjectShape) if isinstance(f.default, bool)
)


def _canonical_key(key: str) -> Optional[str]:
    """Map a raw key onto its allowlisted name, or ``None`` to drop it.

    A sub-key rendered from an enum member arrives as ``TransformType.SQL``;
    the text after the last dot, lowercased, is the value LHP names it by.
    """
    for prefix in _FAMILY_PREFIXES:
        if key.startswith(prefix):
            suffix = key[len(prefix) :].rsplit(".", 1)[-1].lower()
            candidate = f"{prefix}{suffix}"
            return candidate if candidate in _ALLOWED_KEYS else f"{prefix}other"
    return key if key in _ALLOWED_KEYS else None


def fold_project_shape(raw: Mapping[str, Union[int, bool]]) -> ProjectShape:
    """Normalise, fold and drop ``raw`` into a :class:`ProjectShape`.

    Counters accumulate, so several unrecognised sub-keys of one family sum
    into that family's ``*_other``; flags are assigned. Keys the allowlist
    does not name — including the bare ``load``/``transform``/``write``/
    ``test`` totals — are dropped.
    """
    values: Dict[str, Union[int, bool]] = {}
    for key, value in raw.items():
        name = _canonical_key(key)
        if name is None:
            continue
        if name in _FLAG_KEYS:
            values[name] = bool(value)
        else:
            values[name] = int(values.get(name, 0)) + int(value)
    return ProjectShape(**values)  # type: ignore[arg-type]
