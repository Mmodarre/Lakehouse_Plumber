"""ReadModeLineageValidator (LHP-VAL-070).

Databricks rejects a read of a pipeline view that does not match the view's
kind: ``spark.readStream`` (or ``STREAM(v)`` in SQL) of a batch view, and
``spark.read`` (or a plain SQL reference) of a streaming view. A batch view
appended to a streaming table must be read by a ``once`` flow. The validator
checks each such read inside one flowgroup.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import pytest

from lhp.core.processing import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.core.validators.compatibility import ReadModeLineageValidator
from lhp.errors import LHPError, codes
from lhp.models import Action, ActionType, FlowGroup, TestActionType, TransformType

pytestmark = pytest.mark.unit


def _delta(target: str, read_mode: Optional[str] = None) -> Action:
    return Action(
        name=f"load_{target}",
        type=ActionType.LOAD,
        source={"type": "delta", "catalog": "c", "schema": "s", "table": "t"},
        target=target,
        readMode=read_mode,
    )


def _cloudfiles(target: str) -> Action:
    return Action(
        name=f"load_{target}",
        type=ActionType.LOAD,
        source={"type": "cloudfiles", "path": "/p", "format": "json"},
        target=target,
    )


def _jdbc(target: str) -> Action:
    return Action(
        name=f"load_{target}",
        type=ActionType.LOAD,
        source={"type": "jdbc", "url": "jdbc:x", "driver": "d", "table": "t"},
        target=target,
    )


def _transform(
    transform_type: TransformType,
    source: Any,
    target: str,
    read_mode: Optional[str] = None,
    **fields: Any,
) -> Action:
    return Action(
        name=f"transform_{target}",
        type=ActionType.TRANSFORM,
        transform_type=transform_type,
        source=source,
        target=target,
        readMode=read_mode,
        **fields,
    )


def _sql(target: str, sql: str, source: Any = None) -> Action:
    return _transform(TransformType.SQL, source, target, sql=sql)


def _st(
    name: str,
    source: Any,
    *,
    read_mode: Optional[str] = None,
    once: Optional[bool] = None,
    table: str = "t_out",
    **target: Any,
) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        readMode=read_mode,
        once=once,
        write_target={
            "type": "streaming_table",
            "catalog": "c",
            "schema": "s",
            "table": table,
            **target,
        },
    )


def _mv(source: str) -> Action:
    return Action(
        name="write_mv",
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "materialized_view",
            "catalog": "c",
            "schema": "s",
            "table": "mv",
        },
    )


def _sink(source: str) -> Action:
    return Action(
        name="write_sink",
        type=ActionType.WRITE,
        source=source,
        write_target={"type": "sink", "sink_type": "delta", "sink_name": "s"},
    )


def _validate(
    *actions: Action,
    project_root: Optional[Path] = None,
    substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
) -> List[LHPError]:
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=list(actions))
    validator = ReadModeLineageValidator(ActionRegistry(), project_root)
    return validator.validate(flowgroup, substitution_mgr)


def _only(errors: List[LHPError]) -> LHPError:
    assert len(errors) == 1, [e.title for e in errors]
    assert errors[0].code == codes.VAL_070.code
    return errors[0]


# ---- mismatched kinds ------------------------------------------------------


def test_sink_reading_a_batch_view_is_val_070_naming_both_actions():
    error = _only(_validate(_delta("v_in"), _sink("v_in")))

    assert error.title == "Read of view 'v_in' does not match its kind"
    assert "'write_sink' reads view 'v_in' with spark.readStream" in error.details
    assert "'load_v_in' produces it as a batch view" in error.details
    assert error.context == {
        "FlowGroup": "fg",
        "Producer": "load_v_in",
        "Consumer": "write_sink",
        "View": "v_in",
        "View kind": "batch",
        "Read": "stream (with spark.readStream)",
    }
    # A sink always reads a stream, so only the producer can change.
    assert error.suggestions == [
        "Set `readMode: stream` on 'load_v_in' so 'v_in' is a streaming view"
    ]


def test_materialized_view_reading_a_streaming_view_is_val_070():
    error = _only(_validate(_delta("v_in", "stream"), _mv("v_in")))

    assert "'write_mv' reads view 'v_in' with spark.read" in error.details
    assert "as a streaming view" in error.details
    assert error.suggestions == [
        "Set `readMode: batch` on 'load_v_in' so 'v_in' is a batch view"
    ]


def test_fixed_producer_and_fixed_consumer_suggest_reading_a_table():
    error = _only(_validate(_cloudfiles("v_in"), _mv("v_in")))

    assert error.suggestions == ["Write 'v_in' to a table and read that table instead"]


def test_streaming_table_reading_a_batch_view_offers_a_once_backfill():
    error = _only(_validate(_delta("v_in"), _st("write_t", "v_in")))

    assert "with spark.readStream" in error.details
    assert error.suggestions == [
        "Set `readMode: stream` on 'load_v_in' so 'v_in' is a streaming view",
        "Set `readMode: batch` and `once: true` on 'write_t' (a backfill)",
    ]


def test_batch_flow_reading_a_streaming_view_is_val_070_even_once():
    errors = _validate(
        _delta("v_in", "stream"),
        _st("write_t", "v_in", read_mode="batch", once=True),
    )

    error = _only(errors)
    assert "with spark.read," in error.details
    assert "as a streaming view" in error.details
    assert error.suggestions[-1] == "Set `readMode: stream` on 'write_t'"


def test_test_action_reading_a_streaming_view_is_val_070():
    test = Action(
        name="test_count",
        type=ActionType.TEST,
        test_type=TestActionType.ROW_COUNT,
        source=["v_in", "c.s.other"],
    )

    error = _only(_validate(_delta("v_in", "stream"), test))
    assert error.context["Consumer"] == "test_count"


def test_explicit_dq_read_mode_against_its_source_is_val_070():
    dq = _transform(
        TransformType.DATA_QUALITY,
        "v_in",
        "v_dq",
        read_mode="stream",
        expectations_file="e.yaml",
    )

    error = _only(_validate(_delta("v_in"), dq))
    assert error.context["Consumer"] == "transform_v_dq"
    assert error.context["View"] == "v_in"


@pytest.mark.parametrize(
    ("producer", "suggestion"),
    [
        (
            _delta("v_in"),
            "Set `readMode: stream` on 'load_v_in' so 'v_in' is a streaming view",
        ),
        (_jdbc("v_in"), "Write 'v_in' to a table and read that table instead"),
    ],
    ids=["delta", "jdbc"],
)
def test_quarantine_dq_over_a_batch_view_is_val_070(producer, suggestion):
    # Quarantine reads only a stream, so it never inherits the batch kind and
    # is never told to read a batch.
    dq = _transform(
        TransformType.DATA_QUALITY,
        "v_in",
        "v_dq",
        mode="quarantine",
        quarantine={"dlq_table": "c.s.dlq", "source_table": "c.s.src"},
        expectations_file="e.yaml",
    )

    error = _only(_validate(producer, dq))

    assert error.context["Consumer"] == "transform_v_dq"
    assert error.context["Read"] == "stream (with spark.readStream)"
    assert error.suggestions == [suggestion]


@pytest.mark.parametrize("read_mode", ["batch", "stream"])
def test_python_load_read_mode_is_not_a_kind(read_mode: str):
    # The user's loader makes its own read; readMode is not rendered.
    load = Action(
        name="load_py",
        type=ActionType.LOAD,
        source={"type": "python", "module_path": "m.py", "function_name": "f"},
        target="v_in",
        readMode=read_mode,
    )

    assert _validate(load, _st("w", "v_in")) == []
    assert _validate(load, _mv("v_in")) == []


def test_dqe_inherits_a_case_differing_source_without_val_070():
    dq = _transform(TransformType.DATA_QUALITY, "V_IN", "v_dq", expectations_file="e")

    assert _validate(_delta("v_in"), dq, _mv("v_dq")) == []


def test_python_transform_checks_every_source_of_a_list():
    python = _transform(TransformType.PYTHON, ["v_a", "v_b"], "v_out", "stream")

    errors = _validate(_delta("v_a", "stream"), _delta("v_b"), python)
    assert [e.context["View"] for e in errors] == ["v_b"]


def test_mapping_source_view_is_an_edge():
    dq = _transform(
        TransformType.DATA_QUALITY,
        {"view": "v_in"},
        "v_dq",
        read_mode="batch",
        expectations_file="e.yaml",
    )

    error = _only(_validate(_delta("v_in", "stream"), dq))
    assert error.context["Read"] == "batch (with spark.read)"


def test_cdc_flow_reading_a_batch_view_is_val_070():
    cdc = _st("write_cdc", "v_in", mode="cdc", cdc_config={"keys": ["id"]})

    error = _only(_validate(_delta("v_in"), cdc))
    assert error.context["Consumer"] == "write_cdc"


def test_view_names_match_case_insensitively():
    error = _only(_validate(_delta("v_in"), _sink("V_IN")))
    assert error.context["View"] == "v_in"


def test_a_view_listed_twice_reports_once():
    python = _transform(TransformType.PYTHON, ["v_in", "v_in"], "v_out", "stream")

    _only(_validate(_delta("v_in"), python))


# ---- matching reads and skipped edges --------------------------------------


@pytest.mark.parametrize(
    "actions",
    [
        pytest.param((_delta("v_in", "stream"), _st("w", "v_in")), id="stream"),
        pytest.param((_delta("v_in"), _mv("v_in")), id="batch-mv"),
        pytest.param((_cloudfiles("v_in"), _sink("v_in")), id="cloudfiles-sink"),
        pytest.param(
            (_delta("v_in"), _st("w", "v_in", read_mode="batch", once=True)),
            id="batch-once-backfill",
        ),
        pytest.param(
            (
                _delta("v_in"),
                _transform(
                    TransformType.DATA_QUALITY, "v_in", "v_dq", expectations_file="e"
                ),
                _mv("v_dq"),
            ),
            id="dq-inherits-batch",
        ),
        pytest.param((_delta("v_in"), _sink("c.s.v_in")), id="external-table"),
        pytest.param((_sink("v_elsewhere"),), id="no-producer"),
        pytest.param(
            (
                Action(
                    name="load_py",
                    type=ActionType.LOAD,
                    source={"type": "python", "module_path": "m.py"},
                    target="v_in",
                ),
                _mv("v_in"),
            ),
            id="python-load-unknown",
        ),
        pytest.param(
            (
                _transform(TransformType.TEMP_TABLE, "c.s.t", "v_tmp"),
                _sink("v_tmp"),
            ),
            id="temp-table-unknown",
        ),
        pytest.param(
            (_delta("v_in"), _delta("v_in", "stream"), _sink("v_in")),
            id="ambiguous-producer",
        ),
        pytest.param(
            (
                _delta("v_in"),
                _st("w", "v_in", mode="cdc", once=True, cdc_config={"keys": ["k"]}),
            ),
            id="cdc-once-unknown-read",
        ),
    ],
)
def test_matching_or_uncertain_reads_pass(actions):
    assert _validate(*actions) == []


# ---- the once rule ---------------------------------------------------------


def test_batch_append_without_once_is_val_070():
    error = _only(_validate(_delta("v_in"), _st("w", "v_in", read_mode="batch")))

    assert error.title == (
        "Batch view 'v_in' appended to a streaming table without once"
    )
    assert "CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED" in error.details
    assert error.suggestions == [
        "Set `once: true` on 'w' to append 'v_in' once, as a backfill",
        "Or append it incrementally: set `readMode: stream` on 'load_v_in' so "
        "'v_in' is a streaming view, and `readMode: stream` on 'w'",
    ]


def test_combined_write_flow_inherits_the_creators_batch_read():
    creator = _st("w_main", "v_main", read_mode="batch", once=True)
    second = _st("w_extra", "v_extra", create_table=False)

    error = _only(_validate(_delta("v_main"), _delta("v_extra"), creator, second))
    assert error.context["Consumer"] == "w_extra"
    assert "without once" in error.title


def test_combined_write_lead_follows_dependency_order():
    # The table is created elsewhere (create_table: false), so each group is
    # led by its first write in dependency order, not in YAML order.
    backfill_leads = [
        _delta("v_hist"),
        _delta("v_s1", "stream"),
        _sql("v_stream", "SELECT * FROM STREAM(v_s1)", source="v_s1"),
        _st("w_inc", "v_stream", create_table=False),
        _st("w_backfill", "v_hist", read_mode="batch", once=True, create_table=False),
    ]
    incremental_leads = [
        _delta("v_s1", "stream"),
        _delta("v_hist"),
        _st("w_backfill", "v_hist", read_mode="batch", once=True, create_table=False),
        _st("w_inc", "v_s1", create_table=False),
    ]

    # w_inc reads with its lead's batch read, so its streaming view fails.
    error = _only(_validate(*backfill_leads))
    assert error.context["Consumer"] == "w_inc"
    assert error.context["View"] == "v_stream"
    # w_inc leads and reads its streaming view as a stream.
    assert _validate(*incremental_leads) == []


def test_once_rule_does_not_apply_to_materialized_views_or_replace():
    replace = _st("w", "v_in", mode="replace")

    assert _validate(_delta("v_in"), _mv("v_in")) == []
    assert _only(_validate(_delta("v_in"), replace)).title.startswith("Read of view")


# ---- SQL consumers: one read per referenced view ---------------------------


def test_sql_stream_static_join_matches_each_source():
    sql = "SELECT * FROM STREAM(v_s) s JOIN v_b b ON s.id = b.id"

    assert _validate(_delta("v_s", "stream"), _delta("v_b"), _sql("v_j", sql)) == []


@pytest.mark.parametrize(
    ("sql", "how", "tip"),
    [
        (
            "SELECT * FROM v_s",
            "without STREAM()",
            "Read it as STREAM(v_s) in the SQL of 'transform_v_j'",
        ),
        (
            "SELECT * FROM STREAM(v_b)",
            "as STREAM(v_b)",
            "Read v_b without STREAM() in the SQL of 'transform_v_j'",
        ),
    ],
)
def test_sql_read_against_the_view_kind_is_val_070(sql, how, tip):
    errors = _validate(_delta("v_s", "stream"), _delta("v_b"), _sql("v_j", sql))

    error = _only(errors)
    assert f"reads view '{error.context['View']}' {how}" in error.details
    assert error.suggestions[-1] == tip


def test_sql_reading_a_view_both_ways_flags_the_wrong_one():
    sql = "SELECT * FROM STREAM(v_s) a JOIN v_s b ON a.id = b.id"

    error = _only(_validate(_delta("v_s", "stream"), _sql("v_j", sql)))
    assert error.context["Read"] == "batch (without STREAM())"


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT * FROM $source", id="source-placeholder"),
        pytest.param("SELECT * FROM v_s WHERE (", id="unparseable"),
    ],
)
def test_unresolvable_sql_is_skipped(sql):
    assert _validate(_delta("v_s", "stream"), _sql("v_j", sql, "v_s")) == []


def test_sql_file_is_read_under_the_project_root(tmp_path: Path):
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "j.sql").write_text("SELECT * FROM v_s")
    consumer = _transform(TransformType.SQL, "v_s", "v_j", sql_path="sql/j.sql")

    assert _validate(_delta("v_s", "stream"), consumer) == []
    error = _only(_validate(_delta("v_s", "stream"), consumer, project_root=tmp_path))
    assert error.context["Consumer"] == "transform_v_j"


def test_sql_file_is_read_with_the_environments_substitutions(tmp_path: Path):
    # `${stage}` names the streaming view only once substituted, so the check
    # reads the file as code generation does.
    (tmp_path / "j.sql").write_text("SELECT * FROM ${stage}")
    consumer = _transform(TransformType.SQL, "v_s", "v_j", sql_path="j.sql")
    manager = EnhancedSubstitutionManager()
    manager.mappings["stage"] = "v_s"
    actions = (_delta("v_s", "stream"), consumer)

    assert _validate(*actions, project_root=tmp_path) == []
    error = _only(_validate(*actions, project_root=tmp_path, substitution_mgr=manager))
    assert error.context["Consumer"] == "transform_v_j"
    assert manager.secret_references == set()


def test_sql_load_reads_its_sql():
    load = Action(
        name="load_sql",
        type=ActionType.LOAD,
        source="SELECT * FROM v_b",
        target="v_sql",
    )

    error = _only(_validate(_delta("v_b", "stream"), load))
    assert error.context["Consumer"] == "load_sql"


def test_sql_producer_suggests_stream_in_its_sql():
    producer = _sql("v_in", "SELECT * FROM c.s.t")

    error = _only(_validate(producer, _sink("v_in")))
    assert error.suggestions == [
        "Read its source as STREAM(...) in the SQL of 'transform_v_in'"
    ]


def test_streaming_sql_view_feeds_a_streaming_table():
    producer = _sql("v_in", "SELECT * FROM STREAM(c.s.t)")

    assert _validate(producer, _st("w", "v_in")) == []
    error = _only(_validate(producer, _mv("v_in")))
    assert error.suggestions == ["Remove STREAM() from the SQL of 'transform_v_in'"]
