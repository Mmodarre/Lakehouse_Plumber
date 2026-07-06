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
    UnverifiableSqlRead,
    build_sandbox_table_renames,
    rewrite_python_table_literals,
)
from lhp.models import Action, ActionType, FlowGroup

_HELPER_DEF = "def __lhp_sandbox_table("


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


@pytest.mark.unit
class TestSparkSqlFStringRewrite:
    """Dynamic ``spark.sql(f"...")`` bodies: in-scope refs in the literal

    segments are rewritten (leaf renamed, interpolations verbatim) whenever
    the SQL context around them is unambiguous.
    """

    def test_clean_fstring_rewrites_leaf_and_keeps_interpolation(self):
        source = (
            'df = spark.sql(f"SELECT * FROM dev.silver.orders WHERE x = {run_id}")\n'
        )

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == (
            'df = spark.sql(f"SELECT * FROM dev.silver.alice_orders '
            'WHERE x = {run_id}")\n'
        )
        assert warnings == ()

    def test_multiple_fqns_and_interpolations_all_rewrite(self):
        source = (
            'df = spark.sql(f"SELECT * FROM dev.silver.orders WHERE a = {p} '
            'UNION SELECT * FROM stg.events WHERE b = {q}")\n'
        )

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == (
            'df = spark.sql(f"SELECT * FROM dev.silver.alice_orders WHERE a = {p} '
            'UNION SELECT * FROM stg.alice_events WHERE b = {q}")\n'
        )
        assert warnings == ()

    def test_column_interpolation_does_not_block_table_rewrite(self):
        """A field that is not part of a qualified table name (``col_{i}``)

        neither blocks the in-scope table rewrite nor triggers a warning.
        """
        source = 'df = spark.sql(f"SELECT col_{i} FROM dev.silver.orders")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == 'df = spark.sql(f"SELECT col_{i} FROM dev.silver.alice_orders")\n'
        assert warnings == ()

    def test_triple_quoted_fstring_rewrites_and_parses(self):
        source = (
            "df = spark.sql(f'''\n"
            "    SELECT * FROM dev.silver.orders\n"
            "    WHERE x = {run_id}\n"
            "''')\n"
        )

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source.replace("dev.silver.orders", "dev.silver.alice_orders")
        assert warnings == ()
        ast.parse(out)

    def test_masked_ref_inside_fstring_sql_literal_not_rewritten(self):
        """A ref inside a self-contained SQL '...' literal (quote not crossing

        an interpolation) is masked; the real read on the same line rewrites.
        """
        source = (
            'df = spark.sql(f"SELECT * FROM dev.silver.orders '
            "WHERE s = 'dev.silver.orders' AND x = {y}\")\n"
        )

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == (
            'df = spark.sql(f"SELECT * FROM dev.silver.alice_orders '
            "WHERE s = 'dev.silver.orders' AND x = {y}\")\n"
        )
        assert warnings == ()

    def test_out_of_scope_fstring_untouched_no_warning(self):
        source = 'df = spark.sql(f"SELECT * FROM dev.gold.summary WHERE x = {y}")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_fstring_rewrite_is_idempotent(self):
        source = (
            'a = spark.sql(f"SELECT * FROM dev.silver.orders WHERE x = {p}")\n'
            'b = spark.sql(f"""\n    SELECT * FROM stg.events\n    WHERE y = {q}\n""")\n'
        )

        once, warnings_once = rewrite_python_table_literals(source, _renames())
        twice, warnings_twice = rewrite_python_table_literals(once, _renames())

        assert once != source
        assert twice == once
        assert warnings_twice == warnings_once


@pytest.mark.unit
class TestSparkSqlFStringAmbiguous:
    """Where interpolation makes a match ambiguous, the site is left untouched

    and a representative in-scope table is reported (kind ``spark_sql``); an
    ambiguous body with no in-scope table stays silent.
    """

    def test_quote_span_crossing_interpolation_warns_not_rewrites(self):
        """The second literal ``'dev.silver.orders'`` is inside a quote span,

        but the earlier ``'{x}'`` opened a quote across an interpolation, so
        the mask state is runtime-dependent → warn, do not rewrite.
        """
        source = (
            'df = spark.sql(f"SELECT * FROM t WHERE c = '
            "'{x}' AND t = 'dev.silver.orders'\")\n"
        )

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(
                lineno=1, table="dev.silver.orders", kind="spark_sql"
            ),
        )

    def test_split_leaf_across_interpolation_warns(self):
        source = 'df = spark.sql(f"SELECT * FROM dev.silver.{tbl}")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(lineno=1, table="dev.silver.{...}", kind="spark_sql"),
        )

    def test_split_schema_across_interpolation_warns(self):
        source = 'df = spark.sql(f"SELECT * FROM dev.{sch}.orders")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(lineno=1, table="dev.{...}.orders", kind="spark_sql"),
        )

    def test_split_out_of_scope_is_silent(self):
        """A split ref that could never resolve to an in-scope table is not

        rewritten and raises no warning.
        """
        source = 'df = spark.sql(f"SELECT * FROM dev.gold.{tbl}")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_raw_fstring_falls_back_to_warning(self):
        """A raw f-string prefix routes to the conservative warn path rather

        than risky byte surgery, even with a fully-literal in-scope ref.
        """
        source = 'df = spark.sql(rf"SELECT * FROM dev.silver.orders WHERE x = {y}")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(
                lineno=1, table="dev.silver.orders", kind="spark_sql"
            ),
        )


@pytest.mark.unit
class TestContainerBoundReads:
    """Reads through a name-bound dict / list literal are recognized as
    in-scope reads: they are NOT rewritten (the argument is not a plain literal
    and the container's own strings are not read sites) but reported as one
    :class:`UnrewritableTableRead` per site (kind ``table_read``)."""

    def test_dict_constant_key_read_warns_source_unchanged(self):
        source = 'TABLES = {"o": "dev.silver.orders"}\ndf = spark.table(TABLES["o"])\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(
                lineno=2, table="dev.silver.orders", kind="table_read"
            ),
        )

    def test_name_bound_list_for_loop_warns_source_unchanged(self):
        source = 'TS = ["dev.silver.orders"]\nfor t in TS:\n    spark.table(t)\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(
                lineno=3, table="dev.silver.orders", kind="table_read"
            ),
        )

    def test_name_bound_list_constant_index_warns_source_unchanged(self):
        source = 'TS = ["dev.silver.orders"]\ndf = spark.table(TS[0])\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (
            UnrewritableTableRead(
                lineno=2, table="dev.silver.orders", kind="table_read"
            ),
        )

    def test_out_of_scope_container_value_is_silent(self):
        source = 'TABLES = {"o": "dev.gold.summary"}\ndf = spark.table(TABLES["o"])\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_container_literal_strings_are_never_rewritten(self):
        """The rename-set table appears verbatim inside the dict literal — but
        the literal is an assignment RHS, not a read site, so it stays put even
        though the read through it is flagged."""
        source = 'TABLES = {"o": "dev.silver.orders"}\ndf = spark.table(TABLES["o"])\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert '"dev.silver.orders"' in out
        assert "alice_orders" not in out
        assert warnings[0].table == "dev.silver.orders"


@pytest.mark.unit
class TestOpaqueReadShim:
    """A fully-opaque recognized read (runtime-determined name) is wrapped in

    the ``__lhp_sandbox_table(...)`` shim; the helper is emitted once per module
    with any wrapped site and never warns.
    """

    def test_bare_name_read_wrapped(self):
        source = "name = fetch()\ndf = spark.read.table(name)\n"

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert "df = spark.read.table(__lhp_sandbox_table(name))\n" in out
        assert _HELPER_DEF in out
        assert warnings == ()

    def test_read_stream_fstring_arg_wrapped(self):
        source = "df = spark.readStream.table(f'{prefix}.{tbl}')\n"

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert "spark.readStream.table(__lhp_sandbox_table(f'{prefix}.{tbl}'))" in out
        assert warnings == ()

    def test_dict_dynamic_key_subscript_wrapped(self):
        source = "cfg = {'o': 'x'}\nk = pick()\ndf = spark.table(cfg[k])\n"

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert "df = spark.table(__lhp_sandbox_table(cfg[k]))\n" in out
        assert warnings == ()

    def test_catalog_exists_opaque_wrapped(self):
        # spark.catalog.tableExists is in the recognized table_read set; an
        # opaque arg is wrapped for consistency with wrapped reads (an existence
        # check must see the same sandbox name the read resolves to).
        source = "ok = spark.catalog.tableExists(name)\n"

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert "spark.catalog.tableExists(__lhp_sandbox_table(name))" in out
        assert warnings == ()

    def test_helper_emitted_once_for_multiple_sites(self):
        source = "a = spark.table(x)\nb = spark.readStream.table(y)\n"

        out, _ = rewrite_python_table_literals(source, _renames())

        assert out.count(_HELPER_DEF) == 1
        assert "spark.table(__lhp_sandbox_table(x))" in out
        assert "spark.readStream.table(__lhp_sandbox_table(y))" in out

    def test_no_helper_when_no_opaque_sites(self):
        source = 'df = spark.table("dev.silver.orders")\n'

        out, _ = rewrite_python_table_literals(source, _renames())

        assert _HELPER_DEF not in out
        assert out == 'df = spark.table("dev.silver.alice_orders")\n'

    def test_resolvable_in_scope_read_not_wrapped(self):
        # A variable that statically resolves to an in-scope table keeps its
        # warn-only VAL-066 behavior — never wrapped, no helper.
        source = 'tbl = "dev.silver.orders"\ndf = spark.table(tbl)\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert _HELPER_DEF not in out
        assert warnings == (
            UnrewritableTableRead(
                lineno=2, table="dev.silver.orders", kind="table_read"
            ),
        )

    def test_wrapped_output_is_ast_valid(self):
        source = "name = fetch()\ndf = spark.read.table(name)\n"

        out, _ = rewrite_python_table_literals(source, _renames())

        ast.parse(out)  # raises on corruption

    def test_double_run_does_not_double_wrap(self):
        source = "name = fetch()\ndf = spark.table(name)\n"

        once, warnings_once = rewrite_python_table_literals(source, _renames())
        twice, warnings_twice = rewrite_python_table_literals(once, _renames())

        assert twice == once
        assert once.count(_HELPER_DEF) == 1
        assert twice.count(_HELPER_DEF) == 1
        assert "__lhp_sandbox_table(__lhp_sandbox_table(" not in twice
        assert warnings_once == () and warnings_twice == ()

    def test_helper_placed_after_docstring_and_future_import(self):
        source = (
            '"""Module doc."""\n'
            "from __future__ import annotations\n"
            "\n"
            "df = spark.table(z)\n"
        )

        out, _ = rewrite_python_table_literals(source, _renames())

        ast.parse(out)
        # Docstring and future import stay first; the helper follows them and
        # precedes the wrapped read.
        assert out.index('"""Module doc."""') < out.index("from __future__")
        assert out.index("from __future__") < out.index(_HELPER_DEF)
        assert out.index(_HELPER_DEF) < out.index("spark.table(__lhp_sandbox_table(z))")

    def test_no_advisory_for_wrapped_read(self):
        source = "df = spark.table(fetch())\n"

        _, warnings = rewrite_python_table_literals(source, _renames())

        assert warnings == ()


@pytest.mark.unit
class TestOpaqueSqlAdvisory:
    """An opaque ``spark.sql`` body (runtime-built query, or an f-string whose

    table identity is fully runtime-determined) is left untouched and reported
    as one :class:`UnverifiableSqlRead` (LHP-VAL-067). SQL args are never
    wrapped — the shim is for a name argument, not SQL text.
    """

    def test_opaque_spark_sql_var_advises(self):
        source = "q = build_query()\ndf = spark.sql(q)\n"

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert _HELPER_DEF not in out
        assert warnings == (UnverifiableSqlRead(lineno=2),)

    def test_fully_runtime_fstring_advises(self):
        source = 'df = spark.sql(f"SELECT * FROM {sch}.{tbl}")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == (UnverifiableSqlRead(lineno=1),)

    def test_out_of_scope_literal_fqn_in_fstring_stays_silent(self):
        # A fully-literal (wildcard-free) out-of-scope FQN with an interpolation
        # only in a WHERE value has no >=2-wildcard window → silent.
        source = 'df = spark.sql(f"SELECT * FROM acme.gold.summary WHERE x = {y}")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert out == source
        assert warnings == ()

    def test_no_advisory_for_val066_site(self):
        # A statically-resolved in-scope f-string spark.sql is VAL-066, never
        # VAL-067.
        source = 'tbl = "stg.events"\ndf = spark.sql(f"SELECT * FROM {tbl}")\n'

        _, warnings = rewrite_python_table_literals(source, _renames())

        assert warnings == (
            UnrewritableTableRead(lineno=2, table="stg.events", kind="spark_sql"),
        )

    def test_resolvable_sql_body_not_advised(self):
        # A constant in-scope body is rewritten, not advised.
        source = 'df = spark.sql("SELECT * FROM dev.silver.orders")\n'

        out, warnings = rewrite_python_table_literals(source, _renames())

        assert "dev.silver.alice_orders" in out
        assert warnings == ()
