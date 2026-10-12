"""Regression tests for #296 — user strings in generated test-action code.

The test templates wrapped names and descriptions in hand-written ``"..."``
literals and spliced SQL (``custom_sql``, uniqueness ``filter``, table and
column names) line by line into a ``spark.sql(\"\"\"...\"\"\")`` block, so
quotes and backslashes produced invalid Python or silently altered SQL. Only
the Python layer is escaped; SQL-level quoting stays the user's responsibility.
"""

import ast

import pytest

from lhp.generators.test import (
    AllLookupsFoundTestGenerator,
    CompletenessTestGenerator,
    CustomExpectationsTestGenerator,
    CustomSqlTestGenerator,
    RangeTestGenerator,
    ReferentialIntegrityTestGenerator,
    RowCountTestGenerator,
    SchemaMatchTestGenerator,
    UniquenessTestGenerator,
)
from lhp.models import Action, ActionType
from lhp.utils.file_header import normalize_content

REGEX = "code RLIKE '^\\\\d+$'"
DESCRIPTION = 'Check "orders" in C:\\new """x"""'


def _parse(code: str) -> ast.Module:
    """Parse the generated code as it is written to disk."""
    return ast.parse(normalize_content(code))


def _sql(tree: ast.Module) -> str:
    (call,) = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "sql"
    ]
    value = ast.literal_eval(call.args[0])
    assert isinstance(value, str)
    return value


def _table_decorator(tree: ast.Module) -> dict[str, object]:
    function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef))
    (table,) = [
        d
        for d in function.decorator_list
        if isinstance(d, ast.Call) and getattr(d.func, "attr", "") == "table"
    ]
    return {kw.arg: ast.literal_eval(kw.value) for kw in table.keywords}


def _docstring(tree: ast.Module) -> str | None:
    function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef))
    return ast.get_docstring(function, clean=False)


def _test_action(test_type: str, **fields) -> Action:
    return Action(
        name="check_orders",
        type=ActionType.TEST,
        test_type=test_type,
        description=DESCRIPTION,
        **fields,
    )


_ALL_TESTS = {
    "row_count": (RowCountTestGenerator, {"source": ["a", "b"]}),
    "uniqueness": (UniquenessTestGenerator, {"source": "a", "columns": ["id"]}),
    "referential_integrity": (
        ReferentialIntegrityTestGenerator,
        {
            "source": "a",
            "reference": "b",
            "source_columns": ["id"],
            "reference_columns": ["id"],
        },
    ),
    "completeness": (
        CompletenessTestGenerator,
        {"source": "a", "required_columns": ["id"]},
    ),
    "range": (
        RangeTestGenerator,
        {"source": "a", "column": "x", "min_value": 0, "max_value": 1},
    ),
    "all_lookups_found": (
        AllLookupsFoundTestGenerator,
        {
            "source": "a",
            "lookup_table": "b",
            "lookup_columns": ["id"],
            "lookup_result_columns": ["r"],
        },
    ),
    "schema_match": (
        SchemaMatchTestGenerator,
        {"source": "c.s.a", "reference": "c.s.b"},
    ),
    "custom_sql": (CustomSqlTestGenerator, {"source": "a", "sql": "SELECT 1"}),
    "custom_expectations": (
        CustomExpectationsTestGenerator,
        {"source": "a", "expectations": [{"name": "e", "expression": "x > 0"}]},
    ),
}


@pytest.mark.unit
@pytest.mark.parametrize("test_type", sorted(_ALL_TESTS))
def test_name_comment_and_docstring_round_trip(test_type):
    generator, fields = _ALL_TESTS[test_type]
    target = "tmp_check_orders"

    code = generator().generate(
        _test_action(test_type, target=target, **fields), context={}
    )

    tree = _parse(code)
    assert _table_decorator(tree) == {
        "name": target,
        "comment": DESCRIPTION,
        "temporary": True,
    }
    assert _docstring(tree) == DESCRIPTION


@pytest.mark.unit
class TestSqlBodies:
    def test_custom_sql_keeps_backslashes(self):
        sql = f"SELECT *\nFROM t\nWHERE {REGEX}"

        tree = _parse(
            CustomSqlTestGenerator().generate(
                _test_action("custom_sql", source="t", sql=sql), context={}
            )
        )

        lines = [line.strip() for line in _sql(tree).strip().split("\n")]
        assert lines == sql.split("\n")

    @pytest.mark.parametrize(
        "line",
        ['WHERE note = "x"', 'WHERE note = """odd"""', "WHERE a = 'a\u2028b'"],
        ids=["ends-in-quote", "quote-run", "u2028"],
    )
    def test_custom_sql_line_round_trips(self, line):
        tree = _parse(
            CustomSqlTestGenerator().generate(
                _test_action("custom_sql", source="t", sql=f"SELECT 1\n{line}"),
                context={},
            )
        )

        assert _sql(tree).split("\n")[2] == f"        {line}"

    def test_uniqueness_filter_keeps_backslashes_and_quotes(self):
        filter_clause = f'{REGEX} AND note <> """'

        tree = _parse(
            UniquenessTestGenerator().generate(
                _test_action(
                    "uniqueness", source="t", columns=["id"], filter=filter_clause
                ),
                context={},
            )
        )

        assert f"WHERE {filter_clause}\n" in _sql(tree)

    def test_identifiers_in_sql_keep_backslashes(self):
        tree = _parse(
            SchemaMatchTestGenerator().generate(
                _test_action("schema_match", source="c\\x.s.a", reference='c.s."b"'),
                context={},
            )
        )

        sql = _sql(tree)
        assert "FROM c\\x.information_schema.columns" in sql
        assert "table_name = '\"b\"'" in sql

    def test_ordinary_sql_keeps_its_exact_bytes(self):
        code = CustomSqlTestGenerator().generate(
            _test_action(
                "custom_sql", source="t", sql='SELECT "" AS e,\t"a""b"\nFROM t'
            ),
            context={},
        )

        assert '        SELECT "" AS e,\t"a""b"\n        FROM t\n    """)' in code
