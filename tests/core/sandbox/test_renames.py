"""Tests for the sandbox table-rename engine (``core/sandbox/_renames.py``).

The rename set must be exactly the tables produced by the in-scope flowgroups
(MV/ST write targets + delta-sink ``tableName``), matched through the
canonical 2-part<->3-part rule, with the pattern applied to the table leaf
only on the per-site ORIGINAL spelling. The rename set and the
rewrite plan must survive a pickle round-trip (spawn boundary into pool
workers).
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Optional

import pytest

from lhp.core.sandbox import (
    SandboxRewritePlan,
    SandboxTableRenames,
    TableRenameStrategy,
    build_sandbox_table_renames,
    match_renamed_table,
    rename_parts,
)
from lhp.models import Action, ActionType, FlowGroup
from lhp.models.processing import SandboxWarningRecord


def _table_write_action(
    name: str, source: str, write_type: str, catalog: str, schema: str, table: str
) -> Action:
    """A streaming_table / materialized_view WRITE to catalog.schema.table."""
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": write_type,
            "catalog": catalog,
            "schema": schema,
            "table": table,
        },
    )


def _delta_sink_action(name: str, source: str, table_name: str) -> Action:
    """A delta-sink WRITE whose destination lives in ``options.tableName``."""
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "sink",
            "sink_type": "delta",
            "options": {"tableName": table_name},
        },
    )


def _in_scope_flowgroups() -> list[FlowGroup]:
    """In-scope set covering all three producer kinds.

    - ST -> ``dev.bronze.RawOrders`` (CamelCase leaf: canonicalization check).
    - MV -> ``dev.silver.orders``.
    - delta sink -> 2-part ``stg.events`` (no catalog in ``tableName``).
    - a kafka sink, which produces NO table and must NOT enter the rename set.
    """
    fg_ingest = FlowGroup(
        pipeline="p_ingest",
        flowgroup="fg_ingest",
        actions=[
            _table_write_action(
                "wr_raw", "v_raw", "streaming_table", "dev", "bronze", "RawOrders"
            ),
            _delta_sink_action("wr_sink", "v_sink", "stg.events"),
        ],
    )
    fg_curate = FlowGroup(
        pipeline="p_curate",
        flowgroup="fg_curate",
        actions=[
            _table_write_action(
                "wr_orders", "v_orders", "materialized_view", "dev", "silver", "orders"
            ),
            Action(
                name="wr_kafka",
                type=ActionType.WRITE,
                source="v_kafka",
                write_target={"type": "sink", "sink_type": "kafka", "options": {}},
            ),
        ],
    )
    return [fg_ingest, fg_curate]


def _strategy(pattern: str = "{namespace}_{table}") -> TableRenameStrategy:
    return TableRenameStrategy(namespace="alice", table_pattern=pattern)


def _renames() -> SandboxTableRenames:
    return build_sandbox_table_renames(_in_scope_flowgroups(), _strategy())


@pytest.mark.unit
class TestBuildSandboxTableRenames:
    def test_rename_set_is_exactly_the_produced_tables(self):
        """D7: MV + ST + delta-sink destinations, canonicalized; nothing else."""
        renames = _renames()

        assert set(renames.table_producers) == {
            "dev.bronze.raworders",
            "dev.silver.orders",
            "stg.events",
        }

    def test_carries_the_given_strategy(self):
        strategy = _strategy()

        renames = build_sandbox_table_renames(_in_scope_flowgroups(), strategy)

        assert renames.strategy == strategy

    def test_indexes_are_plain_dicts(self):
        """No defaultdict wrappers: lookups must never grow the frozen set."""
        renames = _renames()

        assert type(renames.table_producers) is dict
        assert type(renames.table_short_to_catalogs) is dict


# (read-site spelling, expected canonical rename-set key or None)
MATCH_MATRIX = [
    # 3-part exact (canonical) match.
    ("dev.bronze.raworders", "dev.bronze.raworders"),
    # Case-insensitive: UC matching ignores the per-site spelling.
    ("DEV.Bronze.RawOrders", "dev.bronze.raworders"),
    # Backticks and whitespace strip away.
    ("`dev`.`silver`.`orders`", "dev.silver.orders"),
    ("  dev . silver . orders  ", "dev.silver.orders"),
    # 2-part read -> unique 3-part producer.
    ("silver.orders", "dev.silver.orders"),
    ("Bronze.`RawOrders`", "dev.bronze.raworders"),
    # 3-part read -> 2-part (catalog-less delta sink) producer.
    ("anycat.stg.events", "stg.events"),
    # 2-part read -> 2-part producer.
    ("stg.events", "stg.events"),
    # Not produced in-scope -> exempt from renaming.
    ("dev.gold.summary", None),
    ("external.raw", None),
    # Not a 2- or 3-part table ref.
    ("orders", None),
    ("a.b.c.d", None),
]


@pytest.mark.unit
class TestMatchRenamedTable:
    @pytest.mark.parametrize(("ref", "expected_key"), MATCH_MATRIX)
    def test_canonical_matching(self, ref: str, expected_key: Optional[str]):
        renames = _renames()

        assert match_renamed_table(ref, renames) == expected_key

    def test_matched_key_is_a_real_rename_set_key(self):
        renames = _renames()

        key = match_renamed_table("SILVER.Orders", renames)

        assert key is not None
        assert renames.table_producers[key] == ["fg_curate.wr_orders"]


@pytest.mark.unit
class TestRenameParts:
    def test_rewrites_leaf_only_and_passes_catalog_schema_through(self):
        assert rename_parts(_strategy(), "dev", "silver", "orders") == (
            "dev",
            "silver",
            "alice_orders",
        )

    def test_preserves_original_leaf_spelling(self):
        """Matching is case-insensitive; the site's casing survives."""
        assert rename_parts(_strategy(), "dev", "bronze", "RawOrders") == (
            "dev",
            "bronze",
            "alice_RawOrders",
        )

    def test_passes_absent_catalog_and_schema_through(self):
        assert rename_parts(_strategy(), None, "stg", "events") == (
            None,
            "stg",
            "alice_events",
        )
        assert rename_parts(_strategy(), None, None, "events") == (
            None,
            None,
            "alice_events",
        )

    def test_suffix_style_pattern(self):
        strategy = _strategy("{table}_{namespace}")

        assert rename_parts(strategy, "dev", "bronze", "RawOrders") == (
            "dev",
            "bronze",
            "RawOrders_alice",
        )


@pytest.mark.unit
class TestPickleRoundTrip:
    """Both value objects cross the spawn boundary into pool workers."""

    def test_sandbox_table_renames_round_trips(self):
        renames = _renames()

        restored = pickle.loads(pickle.dumps(renames))

        assert restored == renames
        assert restored.strategy == renames.strategy
        assert restored.table_producers == renames.table_producers
        assert restored.table_short_to_catalogs == renames.table_short_to_catalogs

    def test_sandbox_rewrite_plan_round_trips(self):
        plan = SandboxRewritePlan(
            renames=_renames(),
            warnings=(
                SandboxWarningRecord(
                    code="LHP-VAL-065",
                    message="mixed-producer sink table rewritten",
                    file=Path("pipelines/curate/fg_curate.yaml"),
                    flowgroup="fg_curate",
                ),
                SandboxWarningRecord(
                    code="LHP-VAL-066",
                    message="unrewritable read",
                    file=None,
                    flowgroup=None,
                ),
            ),
        )

        restored = pickle.loads(pickle.dumps(plan))

        assert restored == plan
        assert restored.renames == plan.renames
        assert restored.warnings == plan.warnings

    def test_restored_renames_still_match(self):
        """The restored object is functionally intact, not just field-equal."""
        restored = pickle.loads(pickle.dumps(_renames()))

        assert match_renamed_table("silver.orders", restored) == "dev.silver.orders"
        assert match_renamed_table("dev.gold.summary", restored) is None
