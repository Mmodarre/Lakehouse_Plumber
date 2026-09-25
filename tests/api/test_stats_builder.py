"""Behavioural tests for :mod:`lhp.api._stats_builder`.

``action_counts_by_type`` is the shape consumers read to describe a
project without naming anything in it. Its ``write_*`` / ``write_mode_*``
/ ``test_*`` sub-keys come from fixed allow-lists in the module under
test, and those lists are what the tests below pin. ``load_*`` is
open-ended — it carries the raw ``source.type`` string — while
``transform_*`` is bounded by the ``TransformType`` enum at model
validation, not by an allow-list here. These tests cover both halves of
the shape:

* the fixture-project anchor test asserts the COMPLETE mapping for
  ``tests/e2e/fixtures/testing_project`` — an isolated deep copy, never
  mutated — so a regression in any key, old or new, fails here;
* the synthetic tests drive :func:`_build_stats_result` directly with
  hand-built flowgroups to reach the fallback branches (``write_other``,
  ``test_other``) and the distinct-target de-duplication that no
  well-formed fixture exercises.

``enforce_version=False`` relaxes the ``required_lhp_version`` gate so
the fixture is independent of the installed package version.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest

from lhp.api import LakehousePlumberApplicationFacade
from lhp.api._stats_builder import (
    _TEST_TYPES,
    _WRITE_TARGET_TYPES,
    _build_stats_result,
)
from lhp.models import Action, ActionType, FlowGroup, TestActionType, WriteTargetType
from lhp.models._action import WriteTarget

pytestmark = pytest.mark.unit

_FIXTURE_PATH = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"

# The complete mapping the fixture project produces: 24 pipelines, 79
# flowgroups, 177 actions.
_EXPECTED_COUNTS = {
    "load": 66,
    "load_cloudfiles": 3,
    "load_custom_datasource": 2,
    "load_delta": 41,
    "load_jdbc": 2,
    "load_kafka": 3,
    "load_python": 2,
    "load_sql": 13,
    "test": 9,
    "transform": 30,
    "transform_data_quality": 6,
    "transform_python": 6,
    "transform_schema": 5,
    "transform_sql": 12,
    "transform_temp_table": 1,
    "write": 72,
    "tables": 58,
    "test_all_lookups_found": 1,
    "test_completeness": 1,
    "test_custom_expectations": 1,
    "test_custom_sql": 1,
    "test_range": 1,
    "test_referential_integrity": 1,
    "test_row_count": 1,
    "test_schema_match": 1,
    "test_uniqueness": 1,
    "write_materialized_view": 16,
    "write_mode_cdc": 4,
    "write_mode_replace": 1,
    "write_mode_snapshot_cdc": 7,
    "write_mode_standard": 55,
    "write_sink": 5,
    "write_streaming_table": 51,
}

# The core action-type keys plus the ``load_*`` / ``transform_*`` sub-keys
# are the long-standing ``action_counts_by_type`` contract that CLI and
# webapp consumers already read, so they are asserted on their own as well
# as inside the full anchor above.
_LEGACY_STATS_KEYS = frozenset(
    key
    for key in _EXPECTED_COUNTS
    if key in {"load", "transform", "write", "test"}
    or key.startswith(("load_", "transform_"))
)


@pytest.fixture
def fixture_project(tmp_path: Path) -> Iterator[Path]:
    """Isolated deep copy of the e2e fixture, with cwd swapped to its root.

    LHP resolves several relative paths off ``Path.cwd()``, so the copy is
    made the working directory for the duration of the test.
    """
    root = tmp_path / "testing_project"
    shutil.copytree(_FIXTURE_PATH, root)
    original_cwd = os.getcwd()
    os.chdir(root)
    try:
        yield root
    finally:
        os.chdir(original_cwd)


@pytest.fixture
def fixture_counts(fixture_project: Path) -> Dict[str, int]:
    facade = LakehousePlumberApplicationFacade.for_project(
        fixture_project, enforce_version=False
    )
    return dict(facade.inspection.compute_stats().action_counts_by_type)


def _action(name: str, action_type: str, **fields: Any) -> Action:
    return Action(name=name, type=ActionType(action_type), **fields)


def _flowgroup(actions: List[Action]) -> FlowGroup:
    return FlowGroup(pipeline="p", flowgroup="fg", actions=actions)


def _write(name: str, write_target: Any, source: str = "v_src") -> Action:
    return _action(name, "write", source=source, write_target=write_target)


def _table_target(table: str, **extra: Any) -> Dict[str, Any]:
    return {
        "type": "streaming_table",
        "catalog": "cat",
        "schema": "sch",
        "table": table,
        **extra,
    }


class TestFixtureProjectCounts:
    def test_legacy_stats_keys_match_the_fixture(
        self, fixture_counts: Dict[str, int]
    ) -> None:
        """Isolates the established contract, so a shift in a key consumers
        already read is unmistakable even when the full anchor moves too."""
        observed = {
            key: value
            for key, value in fixture_counts.items()
            if key in _LEGACY_STATS_KEYS
        }
        expected = {
            key: value
            for key, value in _EXPECTED_COUNTS.items()
            if key in _LEGACY_STATS_KEYS
        }
        assert observed == expected

    def test_counts_match_the_fixture_anchor_exactly(
        self, fixture_counts: Dict[str, int]
    ) -> None:
        assert fixture_counts == _EXPECTED_COUNTS

    def test_write_subkeys_partition_the_write_actions(
        self, fixture_counts: Dict[str, int]
    ) -> None:
        write_keys = {
            key: value
            for key, value in fixture_counts.items()
            if key.startswith("write_") and not key.startswith("write_mode_")
        }
        assert sum(write_keys.values()) == fixture_counts["write"]
        assert "write_other" not in fixture_counts

    def test_write_mode_subkeys_partition_the_non_sink_writes(
        self, fixture_counts: Dict[str, int]
    ) -> None:
        mode_total = sum(
            value
            for key, value in fixture_counts.items()
            if key.startswith("write_mode_")
        )
        assert mode_total == fixture_counts["write"] - fixture_counts["write_sink"]

    def test_test_subkeys_partition_the_test_actions(
        self, fixture_counts: Dict[str, int]
    ) -> None:
        test_keys = {
            key: value
            for key, value in fixture_counts.items()
            if key.startswith("test_")
        }
        assert sum(test_keys.values()) == fixture_counts["test"]
        assert "test_other" not in fixture_counts

    def test_table_count_does_not_exceed_the_non_sink_writes(
        self, fixture_counts: Dict[str, int]
    ) -> None:
        non_sink = fixture_counts["write"] - fixture_counts["write_sink"]
        assert 0 < fixture_counts["tables"] <= non_sink


class TestAllowListsTrackTheDomainEnums:
    """The allow-lists bound the write / write-mode / test key cardinality;
    a new enum value must be admitted deliberately, not leak in as a fresh
    key. They say nothing about ``load_*``, which is open-ended, or about
    ``transform_*``, which the ``TransformType`` enum bounds at model
    validation instead."""

    def test_write_target_types_match_the_enum(self) -> None:
        assert _WRITE_TARGET_TYPES == {member.value for member in WriteTargetType}

    def test_test_types_match_the_enum(self) -> None:
        assert _TEST_TYPES == {member.value for member in TestActionType}

    def test_load_sub_keys_carry_the_raw_source_type(self) -> None:
        """``Action.source`` is an unvalidated mapping, so the load family is
        open-ended: whatever ``source.type`` says becomes the key."""
        counts = _build_stats_result(
            [_flowgroup([_action("l1", "load", source={"type": "hologram"})])]
        ).action_counts_by_type

        assert counts["load_hologram"] == 1

    def test_transform_sub_keys_carry_the_enum_value(self) -> None:
        """``Action.transform_type`` is coerced to a ``TransformType`` member
        whose ``str()`` is ``"TransformType.SQL"`` — the key must be the enum
        VALUE, or one transform type would count under two spellings."""
        counts = _build_stats_result(
            [
                _flowgroup(
                    [
                        _action(
                            "t1",
                            "transform",
                            transform_type="sql",
                            source="v_src",
                            target="v_out",
                        )
                    ]
                )
            ]
        ).action_counts_by_type

        assert counts["transform_sql"] == 1
        assert not [key for key in counts if "TransformType" in key]


class TestWriteCounting:
    def test_distinct_targets_are_counted_once(self) -> None:
        counts = _build_stats_result(
            [
                _flowgroup(
                    [
                        _write("w1", _table_target("customers")),
                        _write("w2", _table_target("customers")),
                        _write("w3", _table_target("orders")),
                    ]
                )
            ]
        ).action_counts_by_type

        assert counts["tables"] == 2
        assert counts["write_streaming_table"] == 3

    def test_sinks_are_neither_tables_nor_write_modes(self) -> None:
        counts = _build_stats_result(
            [
                _flowgroup(
                    [
                        _write("w1", {"type": "sink", "sink_type": "kafka"}),
                        _write("w2", {"type": "sink", "sink_type": "delta"}),
                    ]
                )
            ]
        ).action_counts_by_type

        assert counts["write_sink"] == 2
        assert "tables" not in counts
        assert not [key for key in counts if key.startswith("write_mode_")]

    def test_unrecognised_target_type_falls_back_to_write_other(self) -> None:
        counts = _build_stats_result(
            [_flowgroup([_write("w1", {"type": "hologram", "table": "t"})])]
        ).action_counts_by_type

        assert counts["write_other"] == 1
        assert "tables" not in counts
        assert not [key for key in counts if key.startswith("write_mode_")]

    def test_missing_write_target_falls_back_to_write_other(self) -> None:
        counts = _build_stats_result(
            [_flowgroup([_write("w1", None)])]
        ).action_counts_by_type

        assert counts["write"] == 1
        assert counts["write_other"] == 1

    def test_write_mode_defaults_to_standard_for_table_targets(self) -> None:
        counts = _build_stats_result(
            [
                _flowgroup(
                    [
                        _write("w1", _table_target("a")),
                        _write("w2", _table_target("b", mode="cdc")),
                        _write("w3", _table_target("c", mode="snapshot_cdc")),
                    ]
                )
            ]
        ).action_counts_by_type

        assert counts["write_mode_standard"] == 1
        assert counts["write_mode_cdc"] == 1
        assert counts["write_mode_snapshot_cdc"] == 1

    def test_unrecognised_write_mode_falls_back_to_other(self) -> None:
        counts = _build_stats_result(
            [_flowgroup([_write("w1", _table_target("a", mode="teleport"))])]
        ).action_counts_by_type

        assert counts["write_mode_other"] == 1
        assert counts["tables"] == 1

    def test_coerced_write_target_model_is_counted_by_its_enum_value(self) -> None:
        """A dumped ``WriteTarget`` yields ``WriteTargetType`` members, whose
        ``str()`` is ``"WriteTargetType.STREAMING_TABLE"`` — the key must still
        be the enum VALUE, or one target would count under two spellings."""
        target = WriteTarget(
            type=WriteTargetType.STREAMING_TABLE,
            catalog="cat",
            schema="sch",
            table="customers",
        )
        counts = _build_stats_result(
            [_flowgroup([_write("w1", target)])]
        ).action_counts_by_type

        assert counts["write_streaming_table"] == 1
        assert "write_other" not in counts
        assert counts["write_mode_standard"] == 1
        assert counts["tables"] == 1

    def test_materialized_view_targets_are_counted_as_tables(self) -> None:
        counts = _build_stats_result(
            [
                _flowgroup(
                    [
                        _write(
                            "w1",
                            {
                                "type": "materialized_view",
                                "catalog": "cat",
                                "schema": "sch",
                                "table": "mv",
                            },
                        )
                    ]
                )
            ]
        ).action_counts_by_type

        assert counts["write_materialized_view"] == 1
        assert counts["tables"] == 1


class TestTestCounting:
    def test_each_known_test_type_gets_its_own_key(self) -> None:
        actions = [
            _action(f"t_{member.value}", "test", test_type=member.value)
            for member in TestActionType
        ]
        counts = _build_stats_result([_flowgroup(actions)]).action_counts_by_type

        assert counts["test"] == len(actions)
        for member in TestActionType:
            assert counts[f"test_{member.value}"] == 1

    def test_unknown_test_type_falls_back_to_test_other(self) -> None:
        counts = _build_stats_result(
            [
                _flowgroup(
                    [
                        _action("t1", "test", test_type="clairvoyance"),
                        _action("t2", "test"),
                    ]
                )
            ]
        ).action_counts_by_type

        assert counts["test_other"] == 2
