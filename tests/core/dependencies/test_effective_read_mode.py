"""``effective_read_mode``: the read an action's generated code makes.

A dqe data_quality action with no ``readMode`` reads the way its in-flowgroup
source view was made (#344: a data_quality action over a batch delta load used
to read it with ``spark.readStream`` and failed on Databricks). Quarantine mode
always reads a stream. A streaming-table flow with no ``readMode`` in a
combined write reads with the table creator's. Every other action reads what
its generator declares.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pytest

from lhp.core.codegen.action_dispatch import ActionDispatcher
from lhp.core.codegen.context import GenerationContextBuilder
from lhp.core.codegen.grouping import WriteActionGrouper
from lhp.core.dependencies import effective_read_mode, producer_kind
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.models import Action, ActionType, FlowGroup, TransformType

pytestmark = pytest.mark.unit


def _load(target: str, source: Any, read_mode: Optional[str] = None) -> Action:
    return Action(
        name=f"load_{target}",
        type=ActionType.LOAD,
        source=source,
        target=target,
        readMode=read_mode,
    )


def _transform(
    transform_type: TransformType,
    source: Any = "v_in",
    target: str = "v_out",
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


def _dq(source: Any = "v_in", read_mode: Optional[str] = None, **fields: Any):
    return _transform(
        TransformType.DATA_QUALITY,
        source=source,
        target="v_dq",
        read_mode=read_mode,
        expectations_file="e.yaml",
        **fields,
    )


def _write(
    name: str,
    *,
    read_mode: Optional[str] = None,
    create_table: bool = True,
    table: str = "t",
    **target: Any,
) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=f"v_{name}",
        readMode=read_mode,
        write_target={
            "type": "streaming_table",
            "catalog": "c",
            "schema": "s",
            "table": table,
            "create_table": create_table,
            **target,
        },
    )


_PRODUCERS = {
    "delta-batch": (_load("v_in", {"type": "delta", "table": "t"}), "batch"),
    "delta-readMode-stream": (
        _load("v_in", {"type": "delta", "table": "t"}, "stream"),
        "stream",
    ),
    "delta-source-readMode-stream": (
        _load("v_in", {"type": "delta", "table": "t", "readMode": "stream"}),
        "stream",
    ),
    "cloudfiles": (
        _load("v_in", {"type": "cloudfiles", "path": "/p", "format": "json"}),
        "stream",
    ),
    "kafka": (_load("v_in", {"type": "kafka", "bootstrap_servers": "b"}), "stream"),
    "jdbc": (_load("v_in", {"type": "jdbc", "url": "u"}), "batch"),
    "sql-load-batch": (_load("v_in", "SELECT * FROM c.s.t"), "batch"),
    "sql-load-stream": (_load("v_in", "SELECT * FROM STREAM(c.s.t)"), "stream"),
    "sql-transform-batch": (
        _transform(TransformType.SQL, target="v_in", sql="SELECT * FROM v_raw"),
        "batch",
    ),
    "sql-transform-stream": (
        _transform(TransformType.SQL, target="v_in", sql="SELECT * FROM STREAM(v_raw)"),
        "stream",
    ),
    "python-transform-batch": (
        _transform(TransformType.PYTHON, target="v_in"),
        "batch",
    ),
    "schema-stream": (_transform(TransformType.SCHEMA, target="v_in"), "stream"),
}


class TestDataQualityInheritance:
    @pytest.mark.parametrize(
        ("producer", "expected"), _PRODUCERS.values(), ids=_PRODUCERS.keys()
    )
    def test_dqe_reads_its_producers_kind(self, producer: Action, expected: str):
        dq = _dq()

        assert effective_read_mode(dq, [producer, dq]) == expected

    @pytest.mark.parametrize(
        "producer",
        [
            _load("v_in", {"type": "python", "module_path": "m.py"}),
            _transform(TransformType.SQL, target="v_in", sql="SELECT * FROM $source"),
            _transform(TransformType.SQL, target="v_in", sql_path="missing.sql"),
            _transform(TransformType.TEMP_TABLE, target="v_in"),
        ],
        ids=["python-load", "sql-source-placeholder", "sql-unreadable", "temp_table"],
    )
    def test_unknown_producer_kind_reads_a_stream(self, producer: Action):
        dq = _dq()

        assert producer_kind(producer, [producer, dq]) == "unknown"
        assert effective_read_mode(dq, [producer, dq]) == "stream"

    def test_external_source_reads_a_stream(self):
        # No action of the flowgroup produces the source: it is a table.
        dq = _dq(source="c.s.bronze_orders")
        delta = _load("v_other", {"type": "delta", "table": "t"})

        assert effective_read_mode(dq, [delta, dq]) == "stream"

    def test_dqe_mode_set_explicitly_inherits(self):
        dq = _dq(mode="dqe")
        delta, _ = _PRODUCERS["delta-batch"]

        assert effective_read_mode(dq, [delta, dq]) == "batch"

    def test_source_mapping_view_inherits(self):
        delta, _ = _PRODUCERS["delta-batch"]
        by_view = _dq(source={"view": "v_in"})
        by_source = _dq(source={"source": "v_in"})

        assert effective_read_mode(by_view, [delta, by_view]) == "batch"
        assert effective_read_mode(by_source, [delta, by_source]) == "batch"

    def test_chained_data_quality_inherits_through(self):
        delta, _ = _PRODUCERS["delta-batch"]
        first = _transform(
            TransformType.DATA_QUALITY,
            source="v_in",
            target="v_first",
            expectations_file="e.yaml",
        )
        second = _dq(source="v_first")
        actions = [delta, first, second]

        assert producer_kind(first, actions) == "batch"
        assert effective_read_mode(second, actions) == "batch"

    def test_ambiguous_producer_reads_a_stream(self):
        delta, _ = _PRODUCERS["delta-batch"]
        jdbc, _ = _PRODUCERS["jdbc"]
        dq = _dq()

        assert effective_read_mode(dq, [delta, jdbc, dq]) == "stream"

    def test_cycle_of_data_quality_views_ends(self):
        one = _transform(TransformType.DATA_QUALITY, source="v_two", target="v_one")
        two = _transform(TransformType.DATA_QUALITY, source="v_one", target="v_two")

        assert effective_read_mode(one, [one, two]) == "stream"
        assert producer_kind(two, [one, two]) == "stream"

    def test_self_source_ends(self):
        dq = _transform(TransformType.DATA_QUALITY, source="v_dq", target="v_dq")

        assert effective_read_mode(dq, [dq]) == "stream"

    def test_writes_are_not_producers(self):
        dq = _dq()
        write = Action(
            name="w",
            type=ActionType.WRITE,
            source="v_x",
            target="v_in",
            write_target={"type": "streaming_table", "table": "t"},
        )

        assert effective_read_mode(dq, [write, dq]) == "stream"

    def test_producer_sql_file_is_read_under_project_root(self, tmp_path: Path):
        (tmp_path / "q.sql").write_text("SELECT * FROM ${orders_read}")
        (tmp_path / "dev.yaml").write_text("dev:\n  orders_read: STREAM(v_orders)\n")
        manager = EnhancedSubstitutionManager(tmp_path / "dev.yaml", env="dev")
        producer = _transform(TransformType.SQL, target="v_in", sql_path="q.sql")
        dq = _dq()
        actions = [producer, dq]

        # Unreadable without the project root: the default stream.
        assert effective_read_mode(dq, actions) == "stream"
        assert effective_read_mode(dq, actions, project_root=tmp_path) == "batch"
        assert (
            effective_read_mode(
                dq, actions, project_root=tmp_path, substitution_mgr=manager
            )
            == "stream"
        )


class TestExplicitReadMode:
    @pytest.mark.parametrize(
        ("read_mode", "producer_key", "expected"),
        [
            ("stream", "delta-batch", "stream"),
            ("batch", "cloudfiles", "batch"),
            ("batch", "delta-batch", "batch"),
        ],
        ids=["stream-over-batch", "batch-over-stream", "batch-over-batch"],
    )
    def test_explicit_read_mode_wins(
        self, read_mode: str, producer_key: str, expected: str
    ):
        producer, _ = _PRODUCERS[producer_key]
        dq = _dq(read_mode=read_mode)

        assert effective_read_mode(dq, [producer, dq]) == expected

    def test_unrecognised_read_mode_is_not_certain(self):
        # The validator rejects it (LHP-VAL-068).
        producer, _ = _PRODUCERS["delta-batch"]
        dq = _dq(read_mode="Stream")

        assert effective_read_mode(dq, [producer, dq]) is None


class TestQuarantine:
    @pytest.mark.parametrize(
        ("read_mode", "producer_key"),
        [(None, "delta-batch"), ("stream", "delta-batch"), ("batch", "cloudfiles")],
        ids=["no-readMode-over-batch", "stream", "batch-requested"],
    )
    def test_quarantine_always_reads_a_stream(
        self, read_mode: Optional[str], producer_key: str
    ):
        producer, _ = _PRODUCERS[producer_key]
        dq = _dq(read_mode=read_mode, mode="quarantine")

        assert effective_read_mode(dq, [producer, dq]) == "stream"
        assert producer_kind(dq, [producer, dq]) == "stream"


class TestOtherActions:
    @pytest.mark.parametrize(
        ("action", "expected"),
        [
            (_load("v", {"type": "cloudfiles", "path": "/p", "format": "j"}), "stream"),
            (_load("v", {"type": "delta", "table": "t"}), "batch"),
            (_load("v", {"type": "delta", "table": "t"}, "stream"), "stream"),
            (_load("v", {"type": "jdbc", "url": "u"}), "batch"),
            (_load("v", {"type": "custom_datasource"}), "stream"),
            (_load("v", {"type": "python", "module_path": "m.py"}), None),
            (_transform(TransformType.PYTHON), "batch"),
            (_transform(TransformType.SCHEMA), "stream"),
            (_transform(TransformType.TEMP_TABLE), "batch"),
            (_transform(TransformType.TEMP_TABLE, read_mode="stream"), "stream"),
            (_write("w"), "stream"),
            (_write("w", read_mode="batch", once=True), "batch"),
            (_write("w", mode="replace"), "stream"),
            (_write("w", mode="cdc"), "stream"),
            (
                Action(
                    name="w",
                    type=ActionType.WRITE,
                    source="v",
                    once=True,
                    write_target={"type": "streaming_table", "mode": "cdc"},
                ),
                None,
            ),
            (_write("w", mode="snapshot_cdc"), None),
            (
                Action(
                    name="mv",
                    type=ActionType.WRITE,
                    source="v",
                    readMode="stream",
                    write_target={"type": "materialized_view", "table": "t"},
                ),
                "batch",
            ),
            (
                Action(
                    name="sink",
                    type=ActionType.WRITE,
                    source="v",
                    write_target={"type": "sink", "sink_type": "delta"},
                ),
                "stream",
            ),
            (
                Action(
                    name="test",
                    type=ActionType.TEST,
                    test_type="row_count",
                    source=["v_a", "v_b"],
                ),
                "batch",
            ),
        ],
        ids=[
            "cloudfiles",
            "delta",
            "delta-readMode",
            "jdbc",
            "custom_datasource",
            "python-load",
            "python-transform",
            "schema",
            "temp_table",
            "temp_table-readMode",
            "streaming_table",
            "streaming_table-batch",
            "streaming_table-replace",
            "streaming_table-cdc",
            "streaming_table-cdc-once",
            "streaming_table-snapshot_cdc",
            "materialized_view",
            "sink",
            "test",
        ],
    )
    def test_declared_read(self, action: Action, expected: Optional[str]):
        assert effective_read_mode(action, [action]) == expected
        assert effective_read_mode(action, [action]) == (
            ActionRegistry().consumer_read_mode(action)
        )

    @pytest.mark.parametrize(
        ("sql", "expected"),
        [
            ("SELECT * FROM v_a JOIN v_b USING (id)", "batch"),
            ("SELECT * FROM STREAM(v_a)", None),
            ("SELECT * FROM STREAM(v_a) JOIN v_b USING (id)", None),
            ("SELECT * FROM $source", None),
        ],
        ids=["no-stream", "stream", "stream-static-join", "source-placeholder"],
    )
    def test_sql_transform(self, sql: str, expected: Optional[str]):
        # A body with STREAM reads each STREAM argument as a stream and every
        # other reference as a batch, so it has no single read.
        action = _transform(TransformType.SQL, sql=sql)

        assert effective_read_mode(action, [action]) == expected

    def test_sql_load(self):
        batch = _load("v", "SELECT * FROM c.s.t")
        stream = _load("v", {"type": "sql", "sql": "SELECT * FROM STREAM(c.s.t)"})

        assert effective_read_mode(batch, [batch]) == "batch"
        assert effective_read_mode(stream, [stream]) is None


class TestCombinedWrite:
    """A flow with no readMode reads with its table creator's readMode."""

    def test_flow_inherits_the_creators_read_mode(self):
        creator = _write("w_creator", read_mode="batch", once=True)
        flow = _write("w_flow", create_table=False)
        actions = [flow, creator]

        assert effective_read_mode(flow, actions) == "batch"
        assert effective_read_mode(creator, actions) == "batch"

    def test_own_read_mode_wins(self):
        creator = _write("w_creator", read_mode="batch", once=True)
        flow = _write("w_flow", read_mode="stream", create_table=False)

        assert effective_read_mode(flow, [creator, flow]) == "stream"

    def test_creator_without_read_mode_reads_a_stream(self):
        creator = _write("w_creator")
        flow = _write("w_flow", create_table=False)

        assert effective_read_mode(flow, [creator, flow]) == "stream"

    def test_other_tables_do_not_combine(self):
        creator = _write("w_creator", read_mode="batch", once=True)
        other = _write("w_other", table="u")

        assert effective_read_mode(other, [creator, other]) == "stream"

    def test_without_creator_the_first_write_leads(self):
        # The table's creator is in another flowgroup.
        first = _write("w_first", read_mode="batch", once=True, create_table=False)
        second = _write("w_second", create_table=False)

        assert effective_read_mode(second, [first, second]) == "batch"

    @pytest.mark.parametrize(
        ("creator_read", "flow_read"),
        [("batch", None), (None, None), ("batch", "stream"), (None, "batch")],
        ids=["inherit-batch", "inherit-default", "own-stream", "own-batch"],
    )
    def test_matches_generated_code(
        self, creator_read: Optional[str], flow_read: Optional[str]
    ):
        actions = [
            _write("w_flow", read_mode=flow_read, create_table=False),
            _write("w_creator", read_mode=creator_read),
            _write("w_other", read_mode=None, table="u"),
        ]

        reads = _generated_reads(actions)

        for action in actions:
            assert reads[f"f_{action.name}"] == {effective_read_mode(action, actions)}


def _generated_reads(actions: List[Action]) -> Dict[str, Set[str]]:
    """Each generated function's reads (``spark.read`` / ``readStream``)."""
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=actions)
    dispatcher = ActionDispatcher(
        action_registry=ActionRegistry(),
        grouping=WriteActionGrouper(),
        context_builder=GenerationContextBuilder(),
    )
    sections, _, _ = dispatcher.generate_action_sections(
        flowgroup,
        actions,
        EnhancedSubstitutionManager(),
        {},
        None,
        None,
        "dev",
        include_tests=False,
    )
    reads: Dict[str, Set[str]] = {}
    for node in ast.walk(ast.parse("\n".join(sections))):
        if isinstance(node, ast.FunctionDef):
            reads[node.name] = {
                {"read": "batch", "readStream": "stream"}[attr.attr]
                for attr in ast.walk(node)
                if isinstance(attr, ast.Attribute)
                and isinstance(attr.value, ast.Name)
                and attr.value.id == "spark"
                and attr.attr in ("read", "readStream")
            }
    return reads
