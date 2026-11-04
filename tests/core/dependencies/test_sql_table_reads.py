"""``sql_table_reads``: each table a SQL body reads, and whether as a stream.

The names are exactly those ``extract_tables_from_sql`` returns. The flag is
``True`` for the argument of ``STREAM(x)`` / ``STREAM x`` and ``False`` for a
plain reference, so a stream-static join reads one name of each. A body that
does not parse is ``None``.
"""

from __future__ import annotations

import pytest

from lhp.core.dependencies import sql_table_reads
from lhp.core.dependencies.sql_extraction import extract_tables_from_sql

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("sql", "reads"),
    [
        ("SELECT * FROM v", [("v", False)]),
        ("SELECT * FROM STREAM(v)", [("v", True)]),
        ("SELECT * FROM STREAM v", [("v", True)]),
        ("SELECT * FROM stream(`v`) AS a", [("v", True)]),
        (
            "SELECT * FROM STREAM(v_a) a JOIN v_b b ON a.id = b.id",
            [("v_a", True), ("v_b", False)],
        ),
        (
            "SELECT * FROM STREAM(v) a JOIN v b ON a.id = b.id",
            [("v", False), ("v", True)],
        ),
        (
            "WITH s AS (SELECT * FROM STREAM(v)) SELECT * FROM s JOIN w",
            [("v", True), ("w", False)],
        ),
        (
            "SELECT * FROM STREAM(${catalog}.${schema}.orders)",
            [("${catalog}.${schema}.orders", True)],
        ),
        ("SELECT * FROM STREAM read_files('/data', format => 'json')", []),
        ("SELECT * FROM $source", []),
        ("INSERT INTO t SELECT * FROM v", [("v", False)]),
        ("", []),
    ],
)
def test_reads_with_their_stream_flag(sql, reads):
    assert sql_table_reads(sql) == reads


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM STREAM(v_a) a JOIN v_b b ON a.id = b.id",
        "SELECT * FROM STREAM(v) a JOIN v b ON a.id = b.id",
        "SELECT * FROM `cat`.`sch`.t JOIN LIVE.x",
        "SELECT * FROM v WHERE id IN (SELECT id FROM ${catalog}.ref)",
    ],
)
def test_names_are_those_extraction_returns(sql):
    reads = sql_table_reads(sql)

    assert reads is not None
    assert sorted({name for name, _ in reads}) == extract_tables_from_sql(sql).tables


def test_unparseable_sql_is_none():
    assert sql_table_reads("SELECT * FROM v WHERE (") is None
