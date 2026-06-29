"""Tests for the AST-guided Python table-literal rewriter (``_python_rewriter.py``).

The rewriter must rewrite direct string-literal arguments of the recognized
read shapes (``spark.table``, ``read/readStream.table``, ``format(...).table``
chains) and ``spark.sql`` constant bodies — preserving quote style and, for
SQL bodies, all original formatting byte-for-byte except the renamed refs —
while reporting in-scope reads it cannot rewrite (variables, f-strings) as
one :class:`UnrewritableTableRead` per site (the worker folds them into one
``LHP-VAL-066`` per file). Unparseable source passes through unchanged; a
second rewrite over already-rewritten source is a no-op.
"""

from __future__ import annotations

import ast
from collections.abc import Sequence

import pytest

from lhp.core.sandbox import (
    SandboxTableRenames,
    TableRenameStrategy,
    UnrewritableTableRead,
    build_sandbox_table_renames,
    rewrite_python_table_literals,
)
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
    """Default rename set: two 3-part tables + one 2-part delta sink."""
    return _build_renames(
        [("dev", "silver", "orders"), ("dev", "bronze", "RawOrders")],
        sink_tables=["stg.events"],
    )


@pytest.mark.unit
class TestTableReadLiteralRewrite:
    def test_double_quoted_literal_rewrites_leaf_and_keeps_quotes(self):
        out, warnings = rewrite_python_table_literals(
            'df = spark.table("dev.silver.orders")\n', _renames()
        )

        assert out == 'df = spark.table("dev.silver.alice_orders")\n'
        assert warnings == ()

    def test_single_quoted_literal_keeps_quotes(self):
        out, warnings = rewrite_python_table_literals(
            "df = spark.table('stg.events')\n", _renames()
        )

        assert out == "df = spark.table('stg.alice_events')\n"
        assert warnings == ()

    def test_read_stream_table(self):
        """Case-insensitive match; the site's ORIGINAL leaf casing survives."""
        out, _ = rewrite_python_table_literals(
            'df = spark.readStream.table("dev.bronze.RawOrders")\n', _renames()
        )

        assert out == 'df = spark.readStream.table("dev.bronze.alice_RawOrders")\n'

    def test_format_table_chain(self):
        """The format("delta") literal is not a table site and stays put."""
        out, _ = rewrite_python_table_literals(
            'df = spark.read.format("delta").table("dev.silver.orders")\n',
            _renames(),
        )

        assert out == (
            'df = spark.read.format("delta").table("dev.silver.alice_orders")\n'
        )

    def test_backticked_leaf_keeps_backtick_style(self):
        out, _ = rewrite_python_table_literals(
            'df = spark.table("dev.silver.`orders`")\n', _renames()
        )

        assert out == 'df = spark.table("dev.silver.`alice_orders`")\n'

    def test_out_of_scope_ref_untouched(self):
        source = 'df = spark.table("dev.gold.summary")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_bare_view_name_untouched(self):
        """1-part refs are rejected by the LOCKED matcher — never renamed."""
        source = 'df = spark.table("orders")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_implicit_concatenation_collapses_to_single_quoted_literal(self):
        """Pinned: a concatenated table-name literal is re-emitted as ONE

        plain single-quoted literal (the parts cannot be spliced piecewise).
        """
        out, warnings = rewrite_python_table_literals(
            'df = spark.table("dev.silver" ".orders")\n', _renames()
        )

        assert out == "df = spark.table('dev.silver.alice_orders')\n"
        assert warnings == ()


@pytest.mark.unit
class TestSparkSqlBodyRewrite:
    def test_triple_quoted_body_preserved_byte_for_byte_except_ref(self):
        source = (
            "df = spark.sql(\n"
            '    """\n'
            "    SELECT o.id,   o.amount\n"
            "    FROM dev.silver.orders AS o\n"
            "    WHERE o.status = 1\n"
            '    """\n'
            ")\n"
        )

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source.replace("dev.silver.orders", "dev.silver.alice_orders")
        assert warnings == ()

    def test_sql_string_literal_containing_ref_is_masked(self):
        """Refs inside SQL '...' literals are data, not table reads."""
        source = (
            'df = spark.sql("SELECT * FROM dev.silver.orders '
            "WHERE src = 'dev.silver.orders'\")\n"
        )

        out, _ = rewrite_python_table_literals(source, _renames())

        assert out == (
            'df = spark.sql("SELECT * FROM dev.silver.alice_orders '
            "WHERE src = 'dev.silver.orders'\")\n"
        )

    def test_body_without_in_scope_refs_untouched(self):
        source = 'df = spark.sql("SELECT * FROM dev.gold.summary")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_implicit_concatenation_rewrites_decoded_value(self):
        """Pinned: a concatenated SQL body is re-emitted as ONE literal in

        the first part's quote style, holding the rewritten DECODED value.
        """
        source = 'df = spark.sql("SELECT * FROM dev.silver.orders " "WHERE 1=1")\n'

        out, _ = rewrite_python_table_literals(source, _renames())

        assert out == (
            'df = spark.sql("SELECT * FROM dev.silver.alice_orders WHERE 1=1")\n'
        )

    def test_escape_sequence_body_rewrites_decoded_value_as_triple_quoted(self):
        r"""Pinned: a body with ``\n`` escapes diverges raw-vs-decoded, so the

        DECODED value is rewritten and re-emitted triple-quoted (real
        newlines) — semantics preserved, formatting normalized.
        """
        source = 'df = spark.sql("SELECT *\\nFROM stg.events")\n'

        out, _ = rewrite_python_table_literals(source, _renames())

        assert out == 'df = spark.sql("""SELECT *\nFROM stg.alice_events""")\n'
        assert ast.literal_eval(
            ast.parse(out).body[0].value.args[0]  # type: ignore[attr-defined]
        ) == ("SELECT *\nFROM stg.alice_events")


@pytest.mark.unit
class TestUnrewritableWarnings:
    def test_variable_bound_read_warns_and_leaves_source_unchanged(self):
        source = 'tbl = "dev.silver.orders"\ndf = spark.table(tbl)\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(
                lineno=2, table="dev.silver.orders", kind="table_read"
            ),
        )

    def test_fstring_spark_sql_warns_with_spark_sql_kind(self):
        source = 'tbl = "stg.events"\ndf = spark.sql(f"SELECT * FROM {tbl}")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(lineno=2, table="stg.events", kind="spark_sql"),
        )

    def test_resolved_but_out_of_scope_read_is_silent(self):
        source = 'tbl = "dev.gold.summary"\ndf = spark.table(tbl)\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_one_record_per_site(self):
        """Granular records: per-FILE folding into VAL_066 is the worker's job."""
        source = (
            'tbl = "dev.silver.orders"\n'
            "df1 = spark.table(tbl)\n"
            "df2 = spark.readStream.table(tbl)\n"
        )

        _, warnings = rewrite_python_table_literals(source, _renames())

        assert [w.lineno for w in warnings] == [2, 3]
        assert {w.table for w in warnings} == {"dev.silver.orders"}
        assert {w.kind for w in warnings} == {"table_read"}


@pytest.mark.unit
class TestPassthroughAndFastPaths:
    def test_syntax_error_source_passes_through_unchanged(self):
        source = "def broken(:\n    spark.table('dev.silver.orders')\n"

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_empty_renames_is_a_no_op(self):
        """A scope producing no tables (kafka sink) yields an empty rename set."""
        kafka_only = FlowGroup(
            pipeline="p",
            flowgroup="fg",
            actions=[
                Action(
                    name="wr_kafka",
                    type=ActionType.WRITE,
                    source="v_kafka",
                    write_target={"type": "sink", "sink_type": "kafka", "options": {}},
                )
            ],
        )
        renames = build_sandbox_table_renames(
            [kafka_only],
            TableRenameStrategy(namespace="alice", table_pattern="{namespace}_{table}"),
        )
        source = 'df = spark.table("dev.silver.orders")\n'

        out, warnings = rewrite_python_table_literals(source, renames)

        assert out == source
        assert warnings == ()

    def test_empty_source_is_a_no_op(self):
        assert rewrite_python_table_literals("", _renames()) == ("", ())


@pytest.mark.unit
class TestSpanAndCompositionProperties:
    def test_unicode_earlier_on_the_same_line(self):
        """AST columns are UTF-8 BYTE offsets — multi-byte chars before the

        site must not skew the splice.
        """
        source = 'label = "café — ünïcode"; df = spark.table("dev.silver.orders")\n'

        out, _ = rewrite_python_table_literals(source, _renames())

        assert out == (
            'label = "café — ünïcode"; df = spark.table("dev.silver.alice_orders")\n'
        )

    def test_multiple_sites_on_one_line(self):
        source = (
            'a, b = spark.table("dev.silver.orders"), '
            'spark.sql("SELECT * FROM stg.events")\n'
        )

        out, _ = rewrite_python_table_literals(source, _renames())

        assert out == (
            'a, b = spark.table("dev.silver.alice_orders"), '
            'spark.sql("SELECT * FROM stg.alice_events")\n'
        )

    def test_rewrite_is_idempotent(self):
        source = (
            'df0 = spark.table("dev.silver.orders")\n'
            'df1 = spark.readStream.table("dev.bronze.RawOrders")\n'
            'df2 = spark.sql("""\n'
            "    SELECT * FROM stg.events\n"
            '""")\n'
            'tbl = "dev.silver.orders"\n'
            "df3 = spark.table(tbl)\n"
        )

        once, warnings_once = rewrite_python_table_literals(source, _renames())
        twice, warnings_twice = rewrite_python_table_literals(once, _renames())

        assert twice == once
        assert warnings_twice == warnings_once

    def test_rewritten_source_parses(self):
        source = (
            'df0 = spark.table("dev.silver.orders")\n'
            'df1 = spark.table("dev.silver" ".orders")\n'
            'df2 = spark.sql("SELECT *\\nFROM stg.events")\n'
            'df3 = spark.sql("""\n'
            "    SELECT * FROM dev.bronze.RawOrders\n"
            '""")\n'
        )

        out, _ = rewrite_python_table_literals(source, _renames())

        assert out != source
        ast.parse(out)
