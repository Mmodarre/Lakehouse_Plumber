"""ReplaceWhereStreamReadValidator (LHP-VAL-072).

A ``mode: replace_where`` streaming table is rewritten by every refresh, so a
streaming read of it fails (``DELTA_SOURCE_TABLE_IGNORE_CHANGES``) or, with
``skipChangeCommits``, silently drops the rewritten rows. Facts are collected
per flowgroup (as the worker does) and joined project-wide here, so a reader
in any pipeline is checked against every producer.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from lhp.core.coordination._replace_where_facts import collect_flowgroup_facts
from lhp.core.registry import ActionRegistry
from lhp.core.validators.compatibility import ReplaceWhereStreamReadValidator
from lhp.models import Action, ActionType, FlowGroup, TransformType

pytestmark = pytest.mark.unit

_MESSAGE = (
    "{reader} streams from '{table}', a REPLACE WHERE table written by "
    "{producer}. Each refresh rewrites rows, which a streaming read cannot "
    "follow (skipChangeCommits silently drops data). Use readMode: batch, or "
    'stream its change feed with options.readChangeFeed: "true" (see docs).'
)


def _rw_write(
    table: str = "rw", *, catalog: str = "c", name: str = "write_rw"
) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source="v_src",
        write_target={
            "type": "streaming_table",
            "mode": "replace_where",
            "catalog": catalog,
            "schema": "s",
            "table": table,
            "replace_where_config": {"replace_where": "d >= '2024-01-01'"},
        },
    )


def _st_write(table: str, *, catalog: str = "c", name: str = "write_st") -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source="v_in",
        write_target={
            "type": "streaming_table",
            "catalog": catalog,
            "schema": "s",
            "table": table,
        },
    )


def _delta(
    table: str,
    *,
    catalog: str = "c",
    read_mode: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
    name: str = "load_rw",
    target: str = "v_in",
) -> Action:
    source: Dict[str, Any] = {
        "type": "delta",
        "catalog": catalog,
        "schema": "s",
        "table": table,
    }
    if options is not None:
        source["options"] = options
    return Action(
        name=name,
        type=ActionType.LOAD,
        source=source,
        target=target,
        readMode=read_mode,
    )


def _sql(sql: str, *, name: str = "transform_sql", target: str = "v_in") -> Action:
    return Action(
        name=name,
        type=ActionType.TRANSFORM,
        transform_type=TransformType.SQL,
        sql=sql,
        target=target,
    )


def _fg(pipeline: str, flowgroup: str, *actions: Action) -> FlowGroup:
    return FlowGroup(pipeline=pipeline, flowgroup=flowgroup, actions=list(actions))


def _producer_fg(pipeline: str = "p_rw", **rw: Any) -> FlowGroup:
    return _fg(
        pipeline,
        "fg_rw",
        _delta("src", read_mode="batch", name="load_src", target="v_src"),
        _rw_write(**rw),
    )


def _reader_fg(pipeline: str, *actions: Action) -> FlowGroup:
    return _fg(pipeline, "fg_reader", *actions, _st_write("out"))


def _check(*flowgroups: FlowGroup, match_two_part: bool = True) -> Dict[str, List[str]]:
    producers: list = []
    readers: list = []
    for fg in flowgroups:
        fg_producers, fg_readers = collect_flowgroup_facts(
            fg,
            fg.pipeline,
            registry=ActionRegistry(),
            substitution_mgr=None,
            project_root=None,
            table_renames=None,
        )
        producers.extend(fg_producers)
        readers.extend(fg_readers)
    return ReplaceWhereStreamReadValidator().validate(
        producers, readers, match_two_part=match_two_part
    )


def test_stream_delta_reader_flagged():
    errors = _check(
        _producer_fg(), _reader_fg("p_rw", _delta("rw", read_mode="stream"))
    )

    assert errors == {
        "p_rw": [
            _MESSAGE.format(
                reader="p_rw.fg_reader.load_rw",
                table="c.s.rw",
                producer="p_rw.fg_rw.write_rw",
            )
        ]
    }


def test_skip_change_commits_still_flagged():
    reader = _delta("rw", read_mode="stream", options={"skipChangeCommits": "true"})

    errors = _check(_producer_fg(), _reader_fg("p_reader", reader))

    assert list(errors) == ["p_reader"]
    assert "p_reader.fg_reader.load_rw streams from 'c.s.rw'" in errors["p_reader"][0]


def test_sql_stream_reader_flagged():
    reader = _sql("SELECT * FROM STREAM(c.s.rw) WHERE id > 0")

    errors = _check(_producer_fg(), _reader_fg("p_reader", reader))

    assert errors == {
        "p_reader": [
            _MESSAGE.format(
                reader="p_reader.fg_reader.transform_sql",
                table="c.s.rw",
                producer="p_rw.fg_rw.write_rw",
            )
        ]
    }


def test_cross_pipeline_reader_flagged():
    """The reader's pipeline owns the error; the producer's pipeline gets none."""
    errors = _check(
        _producer_fg("p_silver"),
        _reader_fg("p_gold", _delta("rw", read_mode="stream")),
    )

    assert list(errors) == ["p_gold"]
    assert "written by p_silver.fg_rw.write_rw" in errors["p_gold"][0]


@pytest.mark.parametrize("value", ["true", "True", "TRUE"])
def test_change_feed_reader_exempt_string(value: str):
    reader = _delta(
        "rw",
        read_mode="stream",
        options={"readChangeFeed": value, "startingVersion": "0"},
    )

    assert _check(_producer_fg(), _reader_fg("p_reader", reader)) == {}


def test_change_feed_reader_exempt_bool():
    reader = _delta("rw", read_mode="stream", options={"readChangeFeed": True})

    assert _check(_producer_fg(), _reader_fg("p_reader", reader)) == {}


@pytest.mark.parametrize(
    "reader",
    [
        _delta("rw"),
        _delta("rw", read_mode="batch"),
        _sql("SELECT * FROM c.s.rw"),
    ],
    ids=["delta-default", "delta-batch", "sql-plain-reference"],
)
def test_batch_reader_ok(reader: Action):
    assert _check(_producer_fg(), _reader_fg("p_reader", reader)) == {}


def test_mv_reader_in_other_pipeline_ok():
    mv_from_view = Action(
        name="write_mv",
        type=ActionType.WRITE,
        source="v_in",
        write_target={
            "type": "materialized_view",
            "catalog": "c",
            "schema": "s",
            "table": "mv_a",
        },
    )
    mv_from_sql = Action(
        name="write_mv_sql",
        type=ActionType.WRITE,
        write_target={
            "type": "materialized_view",
            "catalog": "c",
            "schema": "s",
            "table": "mv_b",
            "sql": "SELECT * FROM c.s.rw",
        },
    )
    gold = _fg("p_gold", "fg_gold", _delta("rw"), mv_from_view, mv_from_sql)

    assert _check(_producer_fg("p_silver"), gold) == {}


def test_same_table_other_catalog_ok():
    reader = _delta("rw", catalog="other", read_mode="stream")

    assert _check(_producer_fg(), _reader_fg("p_reader", reader)) == {}


def test_two_part_ref_ambiguous_with_non_rw_producer_ok():
    """``s.rw`` is produced in two catalogs, so the 2-part read matches neither."""
    other_catalog = _fg(
        "p_other", "fg_other", _delta("x"), _st_write("rw", catalog="c2")
    )
    reader = _sql("SELECT * FROM STREAM(s.rw)")

    errors = _check(_producer_fg(), other_catalog, _reader_fg("p_reader", reader))

    assert errors == {}


def test_two_part_ref_unique_to_rw_producer_flagged():
    """The contrast case: with one producing catalog the 2-part read matches."""
    reader = _sql("SELECT * FROM STREAM(s.rw)")

    errors = _check(_producer_fg(), _reader_fg("p_reader", reader))

    assert list(errors) == ["p_reader"]
    assert "streams from 's.rw'" in errors["p_reader"][0]


def test_custom_sql_test_action_stream_reader_flagged():
    test_action = Action(
        name="test_rw_rows",
        type=ActionType.TEST,
        test_type="custom_sql",
        sql="SELECT count(*) AS n FROM STREAM(c.s.rw)",
    )

    errors = _check(_producer_fg(), _fg("p_tests", "fg_tests", test_action))

    assert list(errors) == ["p_tests"]
    assert "p_tests.fg_tests.test_rw_rows streams from 'c.s.rw'" in errors["p_tests"][0]


_UNPARSEABLE_STREAM_SQL = "SELECT * FROM STREAM(c.s.rw) WITH (skipChangeCommits = true)"
_WATERMARK = "WATERMARK ts DELAY OF INTERVAL 5 MINUTES"
# sqlglot cannot parse the WATERMARK clause, nor classify a subquery argument.
_UNEXTRACTABLE_STREAM_SQL = f"SELECT * FROM STREAM((SELECT * FROM c.s.x)) {_WATERMARK}"

_UNVERIFIED_MESSAGE = (
    "p_reader.fg_reader.transform_sql may stream from a REPLACE WHERE table: "
    "LHP cannot tell which table a STREAM read in its SQL names (the SQL does "
    "not parse and the STREAM argument is not a plain table name, or the SQL "
    "cannot be read), and the project writes REPLACE WHERE tables (c.s.rw). "
    "Each refresh rewrites rows, which a streaming read cannot follow "
    "(skipChangeCommits silently drops data). Name the streamed table with its "
    "full catalog.schema.table, as STREAM(catalog.schema.table), or read it "
    "through a delta load action (see docs)."
)


def test_unextractable_stream_read_fails_closed_when_a_rw_table_exists():
    errors = _check(
        _producer_fg(), _reader_fg("p_reader", _sql(_UNEXTRACTABLE_STREAM_SQL))
    )

    assert errors == {"p_reader": [_UNVERIFIED_MESSAGE]}


@pytest.mark.parametrize(
    "sql",
    [
        f"SELECT a, ts FROM STREAM($source) {_WATERMARK}",
        f"SELECT a, ts FROM STREAM(v_view) {_WATERMARK}",
        f"SELECT * FROM STREAM read_files('/Volumes/c/s/v', format => 'json') {_WATERMARK}",
    ],
    ids=["source-placeholder", "view", "table-valued-function"],
)
def test_unparseable_stream_of_a_non_table_ok_with_a_rw_producer(sql: str):
    """The lexical fallback: a placeholder, a 1-part view name or a function
    is not a table read, so unparseable SQL streaming one is not flagged."""
    assert _check(_producer_fg(), _reader_fg("p_reader", _sql(sql))) == {}


@pytest.mark.parametrize(
    "sql",
    [
        f"SELECT a, ts FROM STREAM(c.s.rw) {_WATERMARK}",
        _UNPARSEABLE_STREAM_SQL,
        f"SELECT a, ts FROM STREAM `c`.`s`.`rw` {_WATERMARK}",
    ],
    ids=["watermark", "with-options", "unparenthesized-backticked"],
)
def test_unparseable_stream_of_the_rw_table_flagged_through_the_fallback(sql: str):
    errors = _check(_producer_fg(), _reader_fg("p_reader", _sql(sql)))

    assert list(errors) == ["p_reader"]
    (message,) = errors["p_reader"]
    assert message.startswith("p_reader.fg_reader.transform_sql streams from '")
    assert "a REPLACE WHERE table written by p_rw.fg_rw.write_rw" in message


def test_unparseable_stream_of_another_table_ok():
    reader = _sql(f"SELECT a, ts FROM STREAM(c.s.events) {_WATERMARK}")

    assert _check(_producer_fg(), _reader_fg("p_reader", reader)) == {}


def test_filtered_run_leaves_an_ambiguous_two_part_ref_unmatched():
    """Under ``-p`` the other catalog's producer is not in the run: ``s.rw``
    looks unique among the selected facts, yet is ambiguous project-wide."""
    selected = (
        _producer_fg(),
        _reader_fg("p_reader", _sql("SELECT * FROM STREAM(s.rw)")),
    )

    assert _check(*selected, match_two_part=False) == {}


def test_filtered_run_still_matches_a_three_part_cross_pipeline_read():
    reader = _reader_fg("p_gold", _delta("rw", read_mode="stream"))

    errors = _check(_producer_fg("p_silver"), reader, match_two_part=False)

    assert list(errors) == ["p_gold"]
    assert "streams from 'c.s.rw'" in errors["p_gold"][0]


@pytest.mark.parametrize(
    "sql",
    [
        _UNEXTRACTABLE_STREAM_SQL,
        "SELECT a FROM STREAM(c.s.t) WATERMARK ts DELAY OF INTERVAL 5 MINUTES",
        "SELECT * FROM $source",
    ],
    ids=["unextractable", "watermark", "source-placeholder"],
)
def test_unverifiable_sql_ok_without_a_rw_producer(sql: str):
    """No REPLACE WHERE table in the project: nothing can be streamed from one."""
    project = _fg("p_a", "fg_a", _delta("src"), _st_write("t"))

    assert _check(project, _reader_fg("p_reader", _sql(sql))) == {}


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT CAST(value AS STRING) AS v FROM $source",
        "SELECT * FROM STREAM($source)",
    ],
    ids=["no-stream", "stream-of-placeholder"],
)
def test_source_placeholder_sql_ok_with_a_rw_producer(sql: str):
    """``$source`` names the action's input view, never a table: a body using it
    (as the fixture's Kafka transform does) is not an unverified reader."""
    assert _check(_producer_fg(), _reader_fg("p_kafka", _sql(sql))) == {}


def test_source_placeholder_beside_a_stream_of_the_table_flagged():
    reader = _sql("SELECT * FROM $source s JOIN STREAM(c.s.rw) r USING (id)")

    errors = _check(_producer_fg(), _reader_fg("p_reader", reader))

    assert list(errors) == ["p_reader"]
    assert "streams from 'c.s.rw'" in errors["p_reader"][0]


def test_unparseable_sql_without_stream_reads_nothing():
    """A body without the word STREAM reads no stream, parseable or not."""
    reader = _sql("SELECT a FROM c.s.rw WATERMARK ts DELAY OF INTERVAL 5 MINUTES")

    assert _check(_producer_fg(), _reader_fg("p_reader", reader)) == {}
