"""Parity tests: ``match_produced_table`` key extraction vs ``match_table_producers``.

``match_produced_table`` is the single implementation of the LOCKED
2-part<->3-part reconciliation rule; ``match_table_producers`` is a thin
delegate over it. For every cell of the 2<->3-part matrix the two must agree:

    match_table_producers(x, tp, ts) == tp.get(match_produced_table(x, tp, ts), [])

Producer indexes are built from real in-memory flowgroups via
``build_producer_indexes`` so the tests exercise the same canonicalization
path (choke point (i)) as production.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pytest

from lhp.core.dependencies import match_produced_table
from lhp.core.dependencies._producers import (
    build_producer_indexes,
    match_table_producers,
)
from lhp.models import Action, ActionType, FlowGroup


def _streaming_table_action(
    name: str, source: str, catalog: str, schema: str, table: str
) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "streaming_table",
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


def _indexes() -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, List[str]]]]:
    """Producer indexes covering every cell of the 2<->3-part matrix.

    - ``cat.silver.orders``: 3-part producer, unique catalog for its short.
    - ``cat_a.gold.daily`` + ``cat_b.gold.daily``: same ``schema.table`` short
      registered under TWO catalogs -> ambiguous for a 2-part read.
    - ``stg.events``: 2-part producer (delta-sink ``tableName`` without a
      catalog) -> registered under the EMPTY catalog.
    """
    fg_main = FlowGroup(
        pipeline="p1",
        flowgroup="fg_main",
        actions=[
            _streaming_table_action("wr_orders", "v_a", "cat", "silver", "orders"),
            _streaming_table_action("wr_dup_a", "v_b", "cat_a", "gold", "daily"),
        ],
    )
    fg_other = FlowGroup(
        pipeline="p2",
        flowgroup="fg_other",
        actions=[
            _streaming_table_action("wr_dup_b", "v_c", "cat_b", "gold", "daily"),
            _delta_sink_action("wr_sink", "v_d", "stg.events"),
        ],
    )
    return build_producer_indexes([fg_main, fg_other])


# (source spelling, expected canonical producer-index key or None)
MATRIX = [
    # 3-part source -> 3-part producer: exact canonical equality.
    ("cat.silver.orders", "cat.silver.orders"),
    # 2-part source -> unique 3-part producer: matches via uniqueness guard.
    ("silver.orders", "cat.silver.orders"),
    # 2-part source -> MULTIPLE catalogs produce the short: ambiguous, no match.
    ("gold.daily", None),
    # 3-part source over the ambiguous short: exact equality still wins.
    ("cat_a.gold.daily", "cat_a.gold.daily"),
    ("cat_b.gold.daily", "cat_b.gold.daily"),
    # 3-part source, no exact producer, ambiguous short fallback: no match.
    ("cat_c.gold.daily", None),
    # 3-part source -> 2-part (empty-catalog) producer: suffix + uniqueness.
    ("anycat.stg.events", "stg.events"),
    # 2-part source -> 2-part producer.
    ("stg.events", "stg.events"),
    # 3-part source, unique producer under a DIFFERENT catalog: current LOCKED
    # behavior matches it (uniqueness guard ignores the source's catalog).
    ("dev.silver.orders", "cat.silver.orders"),
    # No producer at all.
    ("cat.nope.missing", None),
    ("nope.missing", None),
    # Not a 2- or 3-part table ref.
    ("orders", None),
    ("a.b.c.d", None),
    # Canonicalization: backticks, case, whitespace collapse to the same key.
    ("`cat`.`silver`.`orders`", "cat.silver.orders"),
    ("CAT.Silver.ORDERS", "cat.silver.orders"),
    ("  `Cat` . `Silver` . `Orders`  ", "cat.silver.orders"),
    ("SILVER.`Orders`", "cat.silver.orders"),
    ("`Stg`.EVENTS", "stg.events"),
]


@pytest.mark.unit
class TestMatchProducedTableParity:
    @pytest.mark.parametrize(("source", "expected_key"), MATRIX)
    def test_delegate_parity_across_matrix(
        self, source: str, expected_key: Optional[str]
    ):
        """Acceptance equation: the delegate equals key-lookup, cell by cell."""
        table_producers, table_short_to_catalogs = _indexes()

        key = match_produced_table(source, table_producers, table_short_to_catalogs)

        assert key == expected_key
        assert match_table_producers(
            source, table_producers, table_short_to_catalogs
        ) == table_producers.get(key, [])

    def test_matched_keys_resolve_to_expected_producers(self):
        """The keys are real ``table_producers`` keys with the right action ids."""
        table_producers, table_short_to_catalogs = _indexes()

        assert match_table_producers(
            "silver.orders", table_producers, table_short_to_catalogs
        ) == ["fg_main.wr_orders"]
        assert match_table_producers(
            "anycat.stg.events", table_producers, table_short_to_catalogs
        ) == ["fg_other.wr_sink"]
        assert match_table_producers(
            "cat_b.gold.daily", table_producers, table_short_to_catalogs
        ) == ["fg_other.wr_dup_b"]

    def test_no_match_semantics(self):
        """No match is ``None`` from the key fn and ``[]`` from the delegate."""
        table_producers, table_short_to_catalogs = _indexes()

        assert (
            match_produced_table("gold.daily", table_producers, table_short_to_catalogs)
            is None
        )
        assert (
            match_table_producers(
                "gold.daily", table_producers, table_short_to_catalogs
            )
            == []
        )
