"""Unit tests for the scope-aware extraction visitor.

Covers YAML parameter-binding seeding (kwonly and positional-dict styles,
mirroring codegen's function lookup: module depth only, first match wins,
never speculative), static loop unrolling, scope-aware ``spark.sql``
resolution, and the LHP-DEP-002 opaque-read advisories. Token bytes like
``${env}`` must flow through extraction verbatim.
"""

from __future__ import annotations

import pytest

from lhp.core.dependencies._bindings import DictValue, ListValue, ParameterBindings
from lhp.core.dependencies.python_parser import extract_tables_from_python

DEP_002_CODE = "LHP-DEP-002"
DEP_003_CODE = "LHP-DEP-003"


@pytest.mark.unit
class TestKwonlySeeding:
    """Kwonly-style bindings seed the matching module-level function scope."""

    def test_kwonly_binding_resolves_read(self):
        code = """
def fn(*, table_name, other):
    return spark.read.table(table_name)
"""
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue(
                {
                    "table_name": frozenset({"cat.sch.orders"}),
                    "other": frozenset({"unrelated"}),
                }
            ),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["cat.sch.orders"]
        assert result.warnings == []

    def test_token_bytes_flow_through_verbatim(self):
        code = """
def fn(*, table_name):
    return spark.read.table(table_name)
"""
        raw = "${catalog}.${bronze_schema}.orders${suffix}"
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue({"table_name": frozenset({raw})}),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == [raw]

    def test_leftover_entries_bind_to_kwargs(self):
        code = """
def fn(*, table_name, **kwargs):
    spark.read.table(table_name)
    spark.table(kwargs["extra_table"])
"""
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue(
                {
                    "table_name": frozenset({"c.s.main"}),
                    "extra_table": frozenset({"c.s.extra"}),
                }
            ),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["c.s.extra", "c.s.main"]
        assert result.warnings == []

    def test_leftover_entries_without_kwargs_silently_unbound(self):
        code = """
def fn(*, table_name):
    return spark.read.table(table_name)
"""
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue(
                {
                    "table_name": frozenset({"c.s.main"}),
                    "unused": frozenset({"c.s.never_read"}),
                }
            ),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["c.s.main"]
        assert result.warnings == []

    def test_first_module_level_match_wins(self):
        # Mirrors codegen: only the FIRST module-level ``def fn`` is seeded.
        # The duplicate's read stays opaque and surfaces as one advisory.
        code = """
def fn(*, table_name):
    spark.read.table(table_name)

def fn(*, table_name):
    spark.read.table(table_name)
"""
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue({"table_name": frozenset({"c.s.first"})}),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["c.s.first"]
        assert len(result.warnings) == 1
        assert result.warnings[0].code == DEP_002_CODE

    def test_nested_function_not_seeded(self):
        # Codegen looks up the function in ``tree.body`` only — a nested def
        # never receives bindings, so its read is opaque.
        code = """
def outer():
    def fn(*, table_name):
        spark.read.table(table_name)
"""
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue({"table_name": frozenset({"c.s.t"})}),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == []
        assert len(result.warnings) == 1
        assert result.warnings[0].code == DEP_002_CODE

    def test_name_mismatch_not_seeded(self):
        code = """
def other(*, table_name):
    spark.read.table(table_name)
"""
        bindings = ParameterBindings(
            function_name="fn",
            kwonly=DictValue({"table_name": frozenset({"c.s.t"})}),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == []
        assert len(result.warnings) == 1
        assert result.warnings[0].code == DEP_002_CODE


@pytest.mark.unit
class TestDictStyleSeeding:
    """Positional-dict bindings seed the parameter at ``dict_arg_index``."""

    def test_transform_signature_index_2(self):
        # Python transform with source views: (spark, sources, parameters).
        # Both ``parameters["k"]`` and ``parameters.get("k")`` reads resolve.
        code = """
def f(spark, sources, parameters):
    a = spark.read.table(parameters["lookup_table"])
    b = spark.table(parameters.get("dim_table"))
    return a.join(b)
"""
        bindings = ParameterBindings(
            function_name="f",
            dict_arg_index=2,
            dict_value=DictValue(
                {
                    "lookup_table": frozenset({"c.s.lookup"}),
                    "dim_table": frozenset({"c.s.dim"}),
                }
            ),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["c.s.dim", "c.s.lookup"]
        assert result.warnings == []

    def test_python_load_signature_index_1(self):
        # Python load (no source views): (spark, parameters).
        code = """
def get_df(spark, parameters):
    return spark.read.table(parameters["src"])
"""
        bindings = ParameterBindings(
            function_name="get_df",
            dict_arg_index=1,
            dict_value=DictValue({"src": frozenset({"c.s.src"})}),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["c.s.src"]
        assert result.warnings == []

    def test_index_out_of_signature_range_binds_nothing(self):
        # Signature mismatch (index 1 but only one positional): NO binding —
        # never guess — so the routed read is opaque and emits LHP-DEP-002.
        code = """
def get_df(spark):
    return spark.read.table(parameters["src"])
"""
        bindings = ParameterBindings(
            function_name="get_df",
            dict_arg_index=1,
            dict_value=DictValue({"src": frozenset({"c.s.src"})}),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == []
        assert len(result.warnings) == 1
        assert result.warnings[0].code == DEP_002_CODE


@pytest.mark.unit
class TestStaticLoopUnrolling:
    """``for t in <static list>`` binds the target to all iterations' values."""

    def test_loop_over_bound_parameter_list(self):
        code = """
def f(spark, parameters):
    for t in parameters["tables"]:
        df = spark.read.table(t)
"""
        bindings = ParameterBindings(
            function_name="f",
            dict_arg_index=1,
            dict_value=DictValue({"tables": ListValue(("c.s.a", "c.s.b"))}),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["c.s.a", "c.s.b"]
        assert result.warnings == []

    def test_loop_over_literal_list_without_bindings(self):
        code = """
for t in ["c.s.x", "c.s.y"]:
    spark.table(t)
"""
        result = extract_tables_from_python(code)
        assert result.tables == ["c.s.x", "c.s.y"]
        assert result.warnings == []


@pytest.mark.unit
class TestOpaqueReadWarnings:
    """Recognized reads with unresolvable arguments emit one LHP-DEP-002."""

    def test_helper_call_argument_emits_single_dep_002(self):
        code = """
df = load_config("path")
df2 = spark.read.table(helper(x))
"""
        result = extract_tables_from_python(code)
        assert result.tables == []
        assert len(result.warnings) == 1
        warning = result.warnings[0]
        assert warning.code == DEP_002_CODE
        assert "spark.read.table" in warning.message
        assert "runtime" in warning.message
        assert warning.line == 2
        assert warning.flowgroup == ""
        assert warning.action == ""
        assert "depends_on" in warning.suggestion
        assert warning.file_path is None

    def test_unmatched_calls_emit_no_warning(self):
        code = """
process(data)
helper("foo")
spark.createDataFrame(rows)
"""
        result = extract_tables_from_python(code)
        assert result.tables == []
        assert result.warnings == []


@pytest.mark.unit
class TestSparkSqlResolution:
    """``spark.sql`` arguments resolve through scope + bindings."""

    def test_bound_name_sql_extracts_tables(self):
        code = """
q = "SELECT * FROM silver.users"
spark.sql(q)
"""
        result = extract_tables_from_python(code)
        assert result.tables == ["silver.users"]
        assert result.warnings == []

    def test_f_string_with_bound_parameter_extracts_tables(self):
        code = """
def f(spark, parameters):
    return spark.sql(f"SELECT * FROM {parameters['src']} WHERE x = 1")
"""
        bindings = ParameterBindings(
            function_name="f",
            dict_arg_index=1,
            dict_value=DictValue({"src": frozenset({"c.s.src"})}),
        )
        result = extract_tables_from_python(code, bindings=bindings)
        assert result.tables == ["c.s.src"]
        assert result.warnings == []

    def test_unresolvable_sql_argument_emits_dep_002(self):
        code = "spark.sql(build_query())"
        result = extract_tables_from_python(code)
        assert result.tables == []
        assert len(result.warnings) == 1
        assert result.warnings[0].code == DEP_002_CODE
        assert "spark.sql" in result.warnings[0].message

    def test_unparseable_literal_sql_emits_dep_003_at_python_line(self):
        """An unparseable resolved SQL string yields exactly ONE LHP-DEP-003
        (not DEP-002), re-stamped with the ``spark.sql`` call's Python line —
        the extractor's own line points inside the SQL string (line 1 here)."""
        code = 'x = 1\ndf = spark.sql("NOT VALID SQL !!!")\n'
        result = extract_tables_from_python(code)
        assert result.tables == []
        [warning] = result.warnings
        assert warning.code == DEP_003_CODE
        assert warning.line == 2  # the Python call line, not the SQL-internal line
