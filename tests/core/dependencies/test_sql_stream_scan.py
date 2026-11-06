"""``scan_stream_reads``: the lexical STREAM scan of SQL sqlglot cannot parse.

Each case pins one classification branch of
:func:`lhp.core.dependencies.scan_stream_reads` (#346): string literals and
comments are blanked first; placeholders, 1-part names and table-valued
functions are not table reads; 2- and 3-part names are, spelled as written;
anything else is a read whose table is unknown (``None``, sorted last).
"""

from __future__ import annotations

from typing import Optional, Tuple

import pytest

from lhp.core.dependencies import scan_stream_reads

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        pytest.param(
            "SELECT 'STREAM(c.s.in_string)' AS x FROM STREAM(c.s.t)",
            ("c.s.t",),
            id="single-quoted-string-blanked",
        ),
        pytest.param(
            'SELECT "STREAM(c.s.in_string)" AS x FROM t',
            (),
            id="double-quoted-string-blanked",
        ),
        pytest.param(
            "SELECT 1 -- FROM STREAM(c.s.t)\nFROM t", (), id="line-comment-blanked"
        ),
        pytest.param(
            "SELECT 1 /* STREAM(c.s.t) */ FROM t", (), id="block-comment-blanked"
        ),
        pytest.param(
            "SELECT * FROM STREAM(`c`.`s`.`orders-raw`)",
            ("`c`.`s`.`orders-raw`",),
            id="backticked-name-as-written",
        ),
        pytest.param("SELECT * FROM STREAM($source)", (), id="source-placeholder"),
        pytest.param("SELECT * FROM STREAM(${view})", (), id="whole-token"),
        pytest.param(
            "SELECT * FROM STREAM(${catalog}.silver.t)",
            ("${catalog}.silver.t",),
            id="token-part-of-3-part-name",
        ),
        pytest.param("SELECT * FROM STREAM(v_orders)", (), id="1-part-is-a-view"),
        pytest.param("SELECT * FROM STREAM(silver.t)", ("silver.t",), id="2-part"),
        pytest.param("SELECT * FROM STREAM(c.silver.t)", ("c.silver.t",), id="3-part"),
        pytest.param(
            "SELECT * FROM STREAM read_files('/Volumes/x', format => 'json')",
            (),
            id="bare-table-valued-function",
        ),
        pytest.param(
            "SELECT * FROM STREAM(read_files('/Volumes/x'))",
            (),
            id="parenthesized-table-valued-function",
        ),
        pytest.param("SELECT * FROM STREAM(a.b.c.d)", (None,), id="4-part-unknown"),
        pytest.param("SELECT * FROM STREAM(c.s.t", (None,), id="unclosed-parenthesis"),
        pytest.param(
            "SELECT * FROM STREAM(SELECT * FROM c.s.t)", (None,), id="subquery"
        ),
        pytest.param("SELECT * FROM STREAM c.s.t", ("c.s.t",), id="bare-stream-form"),
        pytest.param("select * from stream(c.s.t)", ("c.s.t",), id="lowercase"),
        pytest.param(
            "SELECT * FROM STREAM(c.s.b) b JOIN STREAM(c.s.a) a ON a.id = b.id",
            ("c.s.a", "c.s.b"),
            id="two-reads-sorted",
        ),
        pytest.param(
            "SELECT * FROM STREAM(c.s.t) UNION ALL SELECT * FROM STREAM(c.s.t)",
            ("c.s.t",),
            id="repeated-read-once",
        ),
        pytest.param(
            "SELECT * FROM STREAM(c.s.a) JOIN STREAM(a.b.c.d) ON 1 = 1",
            ("c.s.a", None),
            id="unknown-read-sorted-last",
        ),
        pytest.param("SELECT x AS stream FROM t", (), id="the-word-stream"),
        pytest.param(
            "SELECT * FROM STREAM(c.s.t) WATERMARK ts DELAY OF INTERVAL 1 MINUTE",
            ("c.s.t",),
            id="watermark-clause",
        ),
        # Documented limit: IDENTIFIER('c.s.t') names its table in a string
        # literal, which is blanked, so the read reads as a function call and
        # is not detected.
        pytest.param(
            "SELECT * FROM STREAM(IDENTIFIER('c.s.t'))",
            (),
            id="identifier-clause-not-detected",
        ),
    ],
)
def test_classifies_each_stream_argument(
    sql: str, expected: Tuple[Optional[str], ...]
) -> None:
    assert scan_stream_reads(sql) == expected
