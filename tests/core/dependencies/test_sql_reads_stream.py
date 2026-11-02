"""``sql_reads_stream``: whether a SQL body reads a stream.

A ``spark.sql`` view is streaming exactly when its SQL reads ``STREAM(x)`` or
``STREAM x``. Without the word ``STREAM`` the answer is ``False`` and nothing is
parsed; a body that does not parse is ``None``. ``ParseCache.sql_reads_stream``
memoizes the probe per body.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from lhp.core.dependencies._parse_cache import ParseCache
from lhp.core.dependencies.sql_extraction import sql_reads_stream

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM STREAM(bronze.orders)",
        "select * from stream(bronze.orders)",
        "SELECT * FROM STREAM (v_orders)",
        "SELECT * FROM STREAM v_orders",
        "SELECT * FROM STREAM read_files('/data', format => 'json')",
        "SELECT a.* FROM STREAM(v_a) a JOIN v_b b ON a.id = b.id",
        "WITH s AS (SELECT * FROM STREAM(v)) SELECT * FROM s",
        "SELECT * FROM (SELECT * FROM STREAM(v))",
        "SELECT * FROM STREAM(${catalog}.${schema}.orders)",
        "SELECT * FROM STREAM(v)\nWHERE id IN (SELECT id FROM ${catalog}.ref)",
        "CREATE TEMPORARY VIEW x AS SELECT 1; SELECT * FROM STREAM(v)",
    ],
)
def test_stream_reads(sql: str) -> None:
    assert sql_reads_stream(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM bronze.orders",
        "SELECT stream FROM bronze.orders",
        "SELECT * FROM t WHERE stream = 1",
        "SELECT 'STREAM(x)' AS label FROM t",
        "SELECT * FROM t -- read a stream later",
        "SELECT * FROM LIVE.orders",
        "SELECT * FROM ${catalog}.bronze.orders",
        "",
    ],
)
def test_batch_reads(sql: str) -> None:
    assert sql_reads_stream(sql) is False


def test_no_stream_word_answers_without_parsing() -> None:
    with patch("lhp.core.dependencies.sql_extraction.sqlglot.parse") as parse:
        assert sql_reads_stream("SELECT stream_id FROM ((( not sql") is False
    parse.assert_not_called()


def test_unparseable_body_with_the_word_is_none() -> None:
    assert sql_reads_stream("SELECT * FROM STREAM(") is None


def test_parse_cache_memoizes_per_body() -> None:
    cache = ParseCache()
    with patch(
        "lhp.core.dependencies._parse_cache.sql_reads_stream", wraps=sql_reads_stream
    ) as probe:
        assert cache.sql_reads_stream("SELECT * FROM STREAM(v)") is True
        assert cache.sql_reads_stream("SELECT * FROM STREAM(v)") is True
        assert cache.sql_reads_stream("SELECT * FROM STREAM(") is None
        assert cache.sql_reads_stream("SELECT * FROM STREAM(") is None
    assert probe.call_count == 2
