"""Tests for the guarded text-level table-ref rewriter (``_text_rewriter.py``).

The primitive must rewrite every authoring spelling of a rename-set table
(case, per-part backticks, whitespace around dots, the 2-part spelling of a
unique 3-part key) while refusing partial/suffix matches, dot-adjacent
longer-name interiors, ambiguous 2-part spellings, and — when masking is on —
refs inside SQL string literals and comments. Only the table-leaf segment
changes; qualifier prefixes and any ``.column`` tail survive verbatim. A
second rewrite over already-rewritten text must be a no-op.

NOTE: imports the module directly (the package ``__init__`` export is owned
by a downstream task).
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from lhp.core.sandbox import (
    SandboxTableRenames,
    TableRenameStrategy,
    build_sandbox_table_renames,
)
from lhp.core.sandbox._text_rewriter import rewrite_table_refs_in_text
from lhp.models import Action, ActionType, FlowGroup


def _table_write_action(
    name: str, source: str, catalog: str, schema: str, table: str
) -> Action:
    """A streaming_table WRITE to catalog.schema.table."""
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


def _build_renames(
    tables: Sequence[tuple[str, str, str]],
    sink_tables: Sequence[str] = (),
    namespace: str = "alice",
    pattern: str = "{namespace}_{table}",
) -> SandboxTableRenames:
    """Rename set over one flowgroup producing the given destinations."""
    actions: list[Action] = [
        _table_write_action(f"wr_{i}", f"v_{i}", catalog, schema, table)
        for i, (catalog, schema, table) in enumerate(tables)
    ]
    actions += [
        _delta_sink_action(f"wr_sink_{i}", f"v_sink_{i}", table_name)
        for i, table_name in enumerate(sink_tables)
    ]
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=actions)
    strategy = TableRenameStrategy(namespace=namespace, table_pattern=pattern)
    return build_sandbox_table_renames([flowgroup], strategy)


def _renames() -> SandboxTableRenames:
    """Standard fixture: two 3-part keys + one catalog-less 2-part sink key.

    Keys: ``dev.bronze.raworders`` (leaf authored as RawOrders),
    ``dev.silver.orders``, ``stg.events``.
    """
    return _build_renames(
        tables=[("dev", "bronze", "RawOrders"), ("dev", "silver", "orders")],
        sink_tables=["stg.events"],
    )


@pytest.mark.unit
class TestSqlClauseRewriting:
    def test_from_and_join_clauses(self):
        text = (
            "SELECT o.id FROM dev.silver.orders o "
            "JOIN dev.bronze.RawOrders r ON o.id = r.id"
        )

        assert rewrite_table_refs_in_text(text, _renames()) == (
            "SELECT o.id FROM dev.silver.alice_orders o "
            "JOIN dev.bronze.alice_RawOrders r ON o.id = r.id"
        )

    def test_two_part_sink_key(self):
        text = "INSERT INTO stg.events SELECT * FROM src"

        assert rewrite_table_refs_in_text(text, _renames()) == (
            "INSERT INTO stg.alice_events SELECT * FROM src"
        )

    def test_multiple_distinct_keys_in_one_text(self):
        text = (
            "SELECT * FROM dev.silver.orders\n"
            "UNION ALL SELECT * FROM dev.bronze.RawOrders\n"
            "UNION ALL SELECT * FROM stg.events"
        )

        assert rewrite_table_refs_in_text(text, _renames()) == (
            "SELECT * FROM dev.silver.alice_orders\n"
            "UNION ALL SELECT * FROM dev.bronze.alice_RawOrders\n"
            "UNION ALL SELECT * FROM stg.alice_events"
        )

    def test_text_without_refs_unchanged(self):
        text = "SELECT 1 FROM other.schema.table WHERE x = 2"

        assert rewrite_table_refs_in_text(text, _renames()) == text


@pytest.mark.unit
class TestSpellings:
    """Per-site spelling is matched case-insensitively and re-emitted verbatim."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            # Case variants: prefix kept verbatim, leaf casing preserved
            # inside the renamed name.
            (
                "FROM DEV.SILVER.ORDERS",
                "FROM DEV.SILVER.alice_ORDERS",
            ),
            # Fully backticked: leaf re-wrapped in backticks.
            (
                "FROM `dev`.`silver`.`orders`",
                "FROM `dev`.`silver`.`alice_orders`",
            ),
            # Mixed backticks: each part keeps its own quoting.
            (
                "FROM dev.`silver`.orders",
                "FROM dev.`silver`.alice_orders",
            ),
            (
                "FROM `dev`.silver.`orders`",
                "FROM `dev`.silver.`alice_orders`",
            ),
            # Whitespace around dots survives in the prefix.
            (
                "FROM dev . silver . orders",
                "FROM dev . silver . alice_orders",
            ),
            # 2-part spelling of a UNIQUE 3-part key.
            (
                "FROM silver.orders",
                "FROM silver.alice_orders",
            ),
            (
                "FROM Bronze.`RawOrders`",
                "FROM Bronze.`alice_RawOrders`",
            ),
        ],
    )
    def test_spelling_variants(self, text: str, expected: str):
        assert rewrite_table_refs_in_text(text, _renames()) == expected


@pytest.mark.unit
class TestGuards:
    @pytest.mark.parametrize(
        "text",
        [
            # Suffix protection: a key must not match a strict prefix of a
            # longer leaf.
            "FROM dev.silver.orders_history",
            "FROM dev.silver.ordersx",
            "FROM myorders",
            "FROM silver.myorders",
            # Prefix protection: a key must not start mid-identifier.
            "FROM xsilver.orders",
            # Dot-lookbehind: `silver.orders` inside the LONGER qualified name
            # `a.silver.orders` (not itself a key) must not match.
            "FROM a.silver.orders",
            # Backtick adjacency: a bare-spelling match cannot start inside a
            # quoted part.
            "FROM `xsilver`.orders",
        ],
    )
    def test_protected_spans_unchanged(self, text: str):
        assert rewrite_table_refs_in_text(text, _renames()) == text

    def test_substitution_token_adjacency(self):
        # Unresolved-token text `${catalog}.silver.orders`: the ref after `}`
        # starts right after a dot, so the dot-lookbehind blocks it — the
        # token spelling stays whole for the substitution engine to resolve.
        text = "FROM ${catalog}.silver.orders"

        assert rewrite_table_refs_in_text(text, _renames()) == text

    def test_ambiguous_two_part_spelling_not_rewritten(self):
        # Two catalogs produce silver.orders: the bare 2-part spelling is
        # ambiguous (left external), but each full 3-part spelling still
        # rewrites.
        renames = _build_renames(
            tables=[("dev", "silver", "orders"), ("prod", "silver", "orders")]
        )
        text = (
            "SELECT * FROM silver.orders "
            "JOIN dev.silver.orders USING (id) "
            "JOIN prod.silver.orders USING (id)"
        )

        assert rewrite_table_refs_in_text(text, renames) == (
            "SELECT * FROM silver.orders "
            "JOIN dev.silver.alice_orders USING (id) "
            "JOIN prod.silver.alice_orders USING (id)"
        )

    def test_bare_one_part_key_never_rewritten(self):
        # A dotless delta-sink destination registers a 1-part key; a lone
        # word is never text-rewritten (the structured pass owns that write).
        renames = _build_renames(tables=[], sink_tables=["events"])
        text = "SELECT * FROM events WHERE events = 1"

        assert rewrite_table_refs_in_text(text, renames) == text

    def test_empty_rename_set_fast_path(self):
        renames = _build_renames(tables=[])
        text = "SELECT * FROM dev.silver.orders"

        assert rewrite_table_refs_in_text(text, renames) == text


@pytest.mark.unit
class TestQualifiedColumnSplice:
    def test_three_part_table_with_column_tail(self):
        text = (
            "SELECT dev.silver.orders.id, dev.silver.orders.name FROM dev.silver.orders"
        )

        assert rewrite_table_refs_in_text(text, _renames()) == (
            "SELECT dev.silver.alice_orders.id, dev.silver.alice_orders.name "
            "FROM dev.silver.alice_orders"
        )

    def test_two_part_key_with_column_tail(self):
        text = "SELECT stg.events.payload FROM stg.events"

        assert rewrite_table_refs_in_text(text, _renames()) == (
            "SELECT stg.alice_events.payload FROM stg.alice_events"
        )


@pytest.mark.unit
class TestSqlMasking:
    def test_string_literal_masked(self):
        text = "SELECT * FROM dev.silver.orders WHERE src = 'silver.orders'"

        masked = rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True)
        unmasked = rewrite_table_refs_in_text(text, _renames())

        assert masked == (
            "SELECT * FROM dev.silver.alice_orders WHERE src = 'silver.orders'"
        )
        assert unmasked == (
            "SELECT * FROM dev.silver.alice_orders WHERE src = 'silver.alice_orders'"
        )

    def test_doubled_quote_escape_inside_literal(self):
        # The '' escape must not end the literal early and expose the ref.
        text = "SELECT * FROM stg.events WHERE note = 'it''s stg.events'"

        assert rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True) == (
            "SELECT * FROM stg.alice_events WHERE note = 'it''s stg.events'"
        )

    def test_line_comment_masked(self):
        text = "-- refresh silver.orders nightly\nSELECT * FROM silver.orders"

        assert rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True) == (
            "-- refresh silver.orders nightly\nSELECT * FROM silver.alice_orders"
        )

    def test_block_comment_masked(self):
        text = "/* dev.silver.orders */ SELECT * FROM dev.silver.orders"

        assert rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True) == (
            "/* dev.silver.orders */ SELECT * FROM dev.silver.alice_orders"
        )

    def test_double_quoted_span_is_not_masked(self):
        # ANSI SQL double quotes delimit IDENTIFIERS, not strings: a ref
        # between them is still a table spelling and must be rewritten.
        text = 'SELECT * FROM "silver.orders"'

        assert rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True) == (
            'SELECT * FROM "silver.alice_orders"'
        )


@pytest.mark.unit
class TestTableRefFunctionArgs:
    """`table_changes(...)` / `IDENTIFIER(...)` hold a table ref in a quoted
    literal; masking un-masks that one argument so its leaf still rewrites."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            # table_changes: first arg, 3-part and its unique 2-part spelling.
            (
                "SELECT * FROM table_changes('dev.silver.orders', 0)",
                "SELECT * FROM table_changes('dev.silver.alice_orders', 0)",
            ),
            (
                "SELECT * FROM table_changes('silver.orders', 0)",
                "SELECT * FROM table_changes('silver.alice_orders', 0)",
            ),
            # 2-part sink key.
            (
                "SELECT * FROM table_changes('stg.events', 0)",
                "SELECT * FROM table_changes('stg.alice_events', 0)",
            ),
            # Double-quoted first arg: never masked, so it rewrites regardless.
            (
                'SELECT * FROM table_changes("dev.silver.orders", 0)',
                'SELECT * FROM table_changes("dev.silver.alice_orders", 0)',
            ),
            # Function-name case variants + table-name casing preserved.
            (
                "SELECT * FROM TABLE_CHANGES('DEV.SILVER.ORDERS', 0)",
                "SELECT * FROM TABLE_CHANGES('DEV.SILVER.alice_ORDERS', 0)",
            ),
            (
                "SELECT * FROM Table_Changes('dev.silver.orders', 0)",
                "SELECT * FROM Table_Changes('dev.silver.alice_orders', 0)",
            ),
            # Whitespace/newlines between name, paren, and the string.
            (
                "SELECT * FROM table_changes\n(\n  'dev.silver.orders' , 0)",
                "SELECT * FROM table_changes\n(\n  'dev.silver.alice_orders' , 0)",
            ),
            # Backticked parts inside the quotes: leaf re-wrapped.
            (
                "SELECT * FROM table_changes('`dev`.`silver`.`orders`', 0)",
                "SELECT * FROM table_changes('`dev`.`silver`.`alice_orders`', 0)",
            ),
            (
                "SELECT * FROM table_changes('dev.silver.`orders`', 0)",
                "SELECT * FROM table_changes('dev.silver.`alice_orders`', 0)",
            ),
            # IDENTIFIER in a FROM position (sole argument, followed by `)`).
            (
                "SELECT * FROM IDENTIFIER('dev.silver.orders')",
                "SELECT * FROM IDENTIFIER('dev.silver.alice_orders')",
            ),
            # IDENTIFIER in an INSERT INTO position, case-varied name.
            (
                "INSERT INTO Identifier('dev.silver.orders') SELECT * FROM src",
                "INSERT INTO Identifier('dev.silver.alice_orders') SELECT * FROM src",
            ),
            # IDENTIFIER 2-part sink key.
            (
                "INSERT INTO IDENTIFIER('stg.events') SELECT * FROM src",
                "INSERT INTO IDENTIFIER('stg.alice_events') SELECT * FROM src",
            ),
        ],
    )
    def test_construct_arg_rewritten_under_masking(self, text: str, expected: str):
        assert (
            rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True)
            == expected
        )

    def test_only_first_table_changes_arg_unmasks(self):
        # The first arg is a table ref (rewrites); an ordinary literal elsewhere
        # that merely LOOKS like a ref stays masked.
        text = (
            "SELECT * FROM table_changes('dev.silver.orders', 0) "
            "WHERE note = 'silver.orders'"
        )

        assert rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True) == (
            "SELECT * FROM table_changes('dev.silver.alice_orders', 0) "
            "WHERE note = 'silver.orders'"
        )

    @pytest.mark.parametrize(
        "text",
        [
            # Non-matching table name in the construct: untouched.
            "SELECT * FROM table_changes('other.schema.table', 0)",
            # A user function whose name merely ends in `table_changes`: the
            # guard rejects it, so its literal stays masked.
            "SELECT * FROM my_table_changes('dev.silver.orders', 0)",
            # Not the sole IDENTIFIER argument (concatenated fragment): masked.
            "SELECT * FROM IDENTIFIER('dev.silver.orders' || suffix)",
            # `table_changes(` inside a line comment: the whole line is masked,
            # so the exemption never opens.
            "-- table_changes('dev.silver.orders', 0)\nSELECT 1",
            # `table_changes(` inside a block comment: same.
            "/* table_changes('dev.silver.orders', 0) */ SELECT 1",
        ],
    )
    def test_negative_cases_stay_masked(self, text: str):
        assert rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True) == (
            text
        )

    def test_idempotent_under_masking(self):
        text = (
            "SELECT * FROM table_changes('dev.silver.orders', 0)\n"
            "UNION ALL SELECT * FROM IDENTIFIER('stg.events')"
        )

        once = rewrite_table_refs_in_text(text, _renames(), mask_sql_literals=True)
        twice = rewrite_table_refs_in_text(once, _renames(), mask_sql_literals=True)

        assert once == (
            "SELECT * FROM table_changes('dev.silver.alice_orders', 0)\n"
            "UNION ALL SELECT * FROM IDENTIFIER('stg.alice_events')"
        )
        assert twice == once


@pytest.mark.unit
class TestIdempotency:
    def test_double_rewrite_equals_single(self):
        renames = _renames()
        text = (
            "SELECT dev.silver.orders.id FROM dev.silver.orders\n"
            "JOIN `dev`.`bronze`.`RawOrders` USING (id)\n"
            "JOIN silver.orders s USING (id)\n"
            "WHERE src = 'stg.events' -- stg.events\n"
        )

        once = rewrite_table_refs_in_text(text, renames, mask_sql_literals=True)
        twice = rewrite_table_refs_in_text(once, renames, mask_sql_literals=True)

        assert twice == once

    def test_pattern_output_containing_key_text_stays_stable(self):
        # Pathological shape: namespace `cat` + suffix pattern emit the leaf
        # `tbl_cat`, so a spliced column tail yields the byte sequence
        # `tbl_cat.col` — which CONTAINS the other key `cat.col`. The
        # word-char lookbehind (`_` before `cat`) keeps a second pass from
        # matching inside the renamed leaf.
        renames = _build_renames(
            tables=[("cat", "sch", "tbl")],
            sink_tables=["cat.col"],
            namespace="cat",
            pattern="{table}_{namespace}",
        )
        text = "SELECT cat.sch.tbl.col FROM cat.sch.tbl"

        once = rewrite_table_refs_in_text(text, renames)
        twice = rewrite_table_refs_in_text(once, renames)

        assert once == "SELECT cat.sch.tbl_cat.col FROM cat.sch.tbl_cat"
        assert twice == once
