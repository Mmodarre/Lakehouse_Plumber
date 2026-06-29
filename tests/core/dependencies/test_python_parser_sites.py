"""Unit tests for table-site collection (``collect_python_table_sites``).

Pins the consumer contract of the sandbox python rewriter: REWRITABLE sites
(direct ``ast.Constant`` string arguments) carry the literal's exact span in
AST coordinates (1-based lines, UTF-8 *byte* columns) plus its value;
UNREWRITABLE-RESOLVED sites (statically resolved but not a plain constant)
carry only the candidate table names and the call's line; opaque sites are
never recorded. Every fixture also asserts legacy parity — the additive
recording must leave ``extract_tables_from_python`` outputs untouched.
"""

from __future__ import annotations

import pytest

from lhp.core.dependencies import (
    PythonTableSite,
    SourceSpan,
    collect_python_table_sites,
)
from lhp.core.dependencies._bindings import DictValue, ParameterBindings
from lhp.core.dependencies.python_parser import extract_tables_from_python
from lhp.utils.python_spans import line_start_byte_offsets

DEP_002_CODE = "LHP-DEP-002"


def _slice(code: str, span: SourceSpan) -> str:
    """Slice the UTF-8 bytes the span addresses — exactly what a rewriter does."""
    data = code.encode("utf-8")
    offsets = line_start_byte_offsets(data)
    start = offsets[span.lineno] + span.col_offset
    end = offsets[span.end_lineno] + span.end_col_offset
    return data[start:end].decode("utf-8")


@pytest.mark.unit
class TestRewritableTableReads:
    """Direct string-literal arguments are rewritable with exact spans."""

    def test_spark_table_literal_exact_span(self):
        code = 'df = spark.table("cat.sch.t")\n'
        sites = collect_python_table_sites(code).sites

        assert sites == (
            PythonTableSite(
                kind="table_read",
                rewritable=True,
                lineno=1,
                span=SourceSpan(
                    lineno=1, col_offset=17, end_lineno=1, end_col_offset=28
                ),
                value="cat.sch.t",
            ),
        )
        assert _slice(code, sites[0].span) == '"cat.sch.t"'
        # Legacy parity.
        result = extract_tables_from_python(code)
        assert result.tables == ["cat.sch.t"]
        assert result.warnings == []

    def test_read_stream_table_literal(self):
        code = 'df = spark.readStream.table("c.s.t")\n'
        sites = collect_python_table_sites(code).sites

        assert len(sites) == 1
        site = sites[0]
        assert site.kind == "table_read"
        assert site.rewritable is True
        assert site.value == "c.s.t"
        assert _slice(code, site.span) == '"c.s.t"'
        assert extract_tables_from_python(code).tables == ["c.s.t"]

    def test_format_chain_table_records_one_site(self):
        code = 'df = spark.read.format("delta").table("c.s.t2")\n'
        sites = collect_python_table_sites(code).sites

        # The inner .format("delta") call must NOT double-record.
        assert len(sites) == 1
        site = sites[0]
        assert site.kind == "table_read"
        assert site.rewritable is True
        assert site.value == "c.s.t2"
        assert _slice(code, site.span) == '"c.s.t2"'
        assert extract_tables_from_python(code).tables == ["c.s.t2"]

    def test_format_chain_load_literal(self):
        code = 'df = spark.readStream.format("delta").load("cat.s.t3")\n'
        sites = collect_python_table_sites(code).sites

        assert len(sites) == 1
        assert sites[0].rewritable is True
        assert sites[0].value == "cat.s.t3"
        assert _slice(code, sites[0].span) == '"cat.s.t3"'

    def test_non_allowlisted_format_records_nothing(self):
        code = 'df = spark.readStream.format("cloudFiles").load("/some/path")\n'
        assert collect_python_table_sites(code).sites == ()
        result = extract_tables_from_python(code)
        assert result.tables == []
        assert result.warnings == []

    def test_catalog_calls_are_table_read_sites(self):
        code = (
            'ok = spark.catalog.tableExists("cat.sch.t")\n'
            'spark.catalog.dropTempView("tmp_view")\n'
        )
        sites = collect_python_table_sites(code).sites

        assert [s.kind for s in sites] == ["table_read", "table_read"]
        assert [s.value for s in sites] == ["cat.sch.t", "tmp_view"]
        assert all(s.rewritable for s in sites)
        assert extract_tables_from_python(code).tables == ["cat.sch.t", "tmp_view"]

    def test_multiline_call_span(self):
        code = 'x = 1\ndf = (\n    spark.table(\n        "cat.sch.multi"\n    )\n)\n'
        sites = collect_python_table_sites(code).sites

        assert len(sites) == 1
        site = sites[0]
        assert site.lineno == 3  # The call starts at `spark` on line 3.
        assert site.span == SourceSpan(
            lineno=4, col_offset=8, end_lineno=4, end_col_offset=23
        )
        assert _slice(code, site.span) == '"cat.sch.multi"'

    def test_implicit_concatenation_is_one_constant(self):
        # Adjacent string literals fold into ONE ast.Constant; the span
        # covers both parts so a rewriter replaces the whole expression.
        code = 'df = spark.table("cat.sch" ".orders")\n'
        sites = collect_python_table_sites(code).sites

        assert len(sites) == 1
        assert sites[0].rewritable is True
        assert sites[0].value == "cat.sch.orders"
        assert _slice(code, sites[0].span) == '"cat.sch" ".orders"'
        assert extract_tables_from_python(code).tables == ["cat.sch.orders"]


@pytest.mark.unit
class TestUnrewritableResolvedTableReads:
    """Statically resolved non-constant arguments record values, not spans."""

    def test_variable_assigned_literal(self):
        # The arg node is an ast.Name even though it resolves to one literal
        # — pinned as UNREWRITABLE-RESOLVED, not rewritable.
        code = 'tbl = "cat.sch.orders"\ndf = spark.table(tbl)\n'
        sites = collect_python_table_sites(code).sites

        assert sites == (
            PythonTableSite(
                kind="table_read",
                rewritable=False,
                lineno=2,
                resolved_values=frozenset({"cat.sch.orders"}),
            ),
        )
        assert extract_tables_from_python(code).tables == ["cat.sch.orders"]

    def test_reassignment_union_carries_all_candidates(self):
        code = 'tbl = "cat.a.t1"\ntbl = "cat.b.t2"\ndf = spark.table(tbl)\n'
        sites = collect_python_table_sites(code).sites

        assert len(sites) == 1
        assert sites[0].rewritable is False
        assert sites[0].resolved_values == frozenset({"cat.a.t1", "cat.b.t2"})
        assert extract_tables_from_python(code).tables == ["cat.a.t1", "cat.b.t2"]

    def test_fstring_resolved_via_binding(self):
        code = 'def fn(*, schema):\n    return spark.table(f"{schema}.orders")\n'
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue({"schema": frozenset({"cat.bronze"})}),
        )
        sites = collect_python_table_sites(code, bindings=bindings).sites

        assert sites == (
            PythonTableSite(
                kind="table_read",
                rewritable=False,
                lineno=2,
                resolved_values=frozenset({"cat.bronze.orders"}),
            ),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["cat.bronze.orders"]
        assert result.warnings == []

    def test_bindings_passthrough(self):
        code = "def fn(*, table_name):\n    return spark.read.table(table_name)\n"
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue({"table_name": frozenset({"cat.sch.orders"})}),
        )

        # Without bindings the parameter is opaque: no site, one advisory.
        assert collect_python_table_sites(code).sites == ()
        unbound = extract_tables_from_python(code)
        assert unbound.tables == []
        assert [w.code for w in unbound.warnings] == [DEP_002_CODE]

        # With bindings the same call becomes a resolved site.
        sites = collect_python_table_sites(code, bindings=bindings).sites
        assert len(sites) == 1
        assert sites[0].rewritable is False
        assert sites[0].resolved_values == frozenset({"cat.sch.orders"})


@pytest.mark.unit
class TestOpaqueSitesExcluded:
    """Opaque arguments stay LHP-DEP-002 territory — never recorded."""

    def test_function_call_argument(self):
        code = "df = spark.table(get_name())\n"
        assert collect_python_table_sites(code).sites == ()
        result = extract_tables_from_python(code)
        assert result.tables == []
        assert [w.code for w in result.warnings] == [DEP_002_CODE]

    def test_keyword_argument_is_opaque(self):
        # Recognition only inspects positional args; a keyword-passed table
        # name is opaque for both extraction and site collection.
        code = 'df = spark.table(tableName="cat.sch.t")\n'
        assert collect_python_table_sites(code).sites == ()
        result = extract_tables_from_python(code)
        assert result.tables == []
        assert [w.code for w in result.warnings] == [DEP_002_CODE]

    def test_opaque_spark_sql(self):
        code = "df = spark.sql(build_query())\n"
        assert collect_python_table_sites(code).sites == ()
        result = extract_tables_from_python(code)
        assert result.tables == []
        assert [w.code for w in result.warnings] == [DEP_002_CODE]


@pytest.mark.unit
class TestSparkSqlSites:
    """spark.sql(...) sites: literal SQL is rewritable, resolved SQL is not."""

    def test_triple_quoted_constant(self):
        code = 'df = spark.sql("""\n    SELECT *\n    FROM cat.sch.orders\n""")\n'
        sites = collect_python_table_sites(code).sites

        assert len(sites) == 1
        site = sites[0]
        assert site.kind == "spark_sql"
        assert site.rewritable is True
        assert site.lineno == 1
        assert site.value == "\n    SELECT *\n    FROM cat.sch.orders\n"
        assert site.span == SourceSpan(
            lineno=1, col_offset=15, end_lineno=4, end_col_offset=3
        )
        assert (
            _slice(code, site.span) == '"""\n    SELECT *\n    FROM cat.sch.orders\n"""'
        )
        result = extract_tables_from_python(code)
        assert result.tables == ["cat.sch.orders"]
        assert result.warnings == []

    def test_fstring_resolvable_records_extracted_tables(self):
        code = (
            "def fn(*, schema):\n"
            '    return spark.sql(f"SELECT * FROM {schema}.orders '
            'JOIN {schema}.lines ON 1=1")\n'
        )
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue({"schema": frozenset({"cat.bronze"})}),
        )
        sites = collect_python_table_sites(code, bindings=bindings).sites

        assert len(sites) == 1
        site = sites[0]
        assert site.kind == "spark_sql"
        assert site.rewritable is False
        assert site.span is None and site.value is None
        assert site.resolved_values == frozenset(
            {"cat.bronze.orders", "cat.bronze.lines"}
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["cat.bronze.lines", "cat.bronze.orders"]


@pytest.mark.unit
class TestSpansAreByteAccurate:
    """AST col offsets are UTF-8 byte columns; spans index the raw input."""

    def test_unicode_before_site(self):
        code = (
            'name = "café"\n'
            'df = spark.table("cat.sch.t")\n'
            'pair = ("é", spark.table("cat.sch.u"))\n'
        )
        sites = collect_python_table_sites(code).sites
        assert [s.value for s in sites] == ["cat.sch.t", "cat.sch.u"]

        # Unicode on a PREVIOUS line never shifts a later site's coordinates.
        assert sites[0].span == SourceSpan(
            lineno=2, col_offset=17, end_lineno=2, end_col_offset=28
        )

        # Unicode EARLIER ON THE SAME LINE shifts the byte column past the
        # character column ("é" is 2 UTF-8 bytes).
        line3 = 'pair = ("é", spark.table("cat.sch.u"))'
        byte_col = line3.encode("utf-8").index(b'"cat.sch.u"')
        char_col = line3.index('"cat.sch.u"')
        assert byte_col == char_col + 1
        assert sites[1].span.col_offset == byte_col
        assert _slice(code, sites[1].span) == '"cat.sch.u"'

    def test_verbatim_parse_no_dedent(self):
        # Divergence pin: collect_python_table_sites parses the source
        # VERBATIM so spans index the input as given. An indented snippet
        # that only parses after dedenting yields an empty result here,
        # while legacy extraction (which dedents) still extracts.
        code = '    df = spark.table("cat.sch.t")\n'
        assert collect_python_table_sites(code).sites == ()
        assert extract_tables_from_python(code).tables == ["cat.sch.t"]


@pytest.mark.unit
class TestDegradedInputs:
    """Empty / unparseable input degrades to an empty result, never raises."""

    def test_empty_source(self):
        assert collect_python_table_sites("").sites == ()

    def test_syntax_error_source(self):
        assert collect_python_table_sites("def broken(:\n").sites == ()

    def test_source_order(self):
        code = (
            'a = spark.table("cat.s.one")\n'
            'b = spark.sql("SELECT * FROM cat.s.two")\n'
            'c = spark.read.table("cat.s.three")\n'
        )
        sites = collect_python_table_sites(code).sites
        assert [s.lineno for s in sites] == [1, 2, 3]
        assert [s.kind for s in sites] == ["table_read", "spark_sql", "table_read"]
