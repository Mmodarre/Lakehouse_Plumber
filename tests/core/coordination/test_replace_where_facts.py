"""Per-flowgroup fact collection for the project-wide VAL-072 check (#346).

``collect_flowgroup_facts`` runs in the worker on the resolved (and, under
``--sandbox``, rewritten) flowgroup. It records the tables every write
produces and the tables the flowgroup reads as a stream, reading SQL the way
the generators do.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from lhp.core.coordination import _flowgroup_pool as fp
from lhp.core.coordination import _replace_where_facts as facts_module
from lhp.core.coordination._flowgroup_pool import (
    _FlowgroupWorkerState,
    _process_one_flowgroup,
)
from lhp.core.coordination._replace_where_facts import collect_flowgroup_facts
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.core.sandbox import (
    TableRenameStrategy,
    build_sandbox_table_renames,
    rewrite_flowgroup_tables,
)
from lhp.models import Action, ActionType, FlowGroup, FlowGroupContext, TransformType
from lhp.models.processing import StreamReadFact, TableProducerFact

pytestmark = pytest.mark.unit


def _rw_write(table: str = "rw") -> Action:
    return Action(
        name="write_rw",
        type=ActionType.WRITE,
        source="v_src",
        write_target={
            "type": "streaming_table",
            "mode": "replace_where",
            "catalog": "c",
            "schema": "s",
            "table": table,
            "replace_where_config": {"replace_where": "d >= '2024-01-01'"},
        },
    )


def _delta(table: str, *, read_mode: str = "stream", target: str = "v_in") -> Action:
    return Action(
        name=f"load_{target}",
        type=ActionType.LOAD,
        source={"type": "delta", "catalog": "c", "schema": "s", "table": table},
        target=target,
        readMode=read_mode,
    )


def _sql_transform(target: str, **fields: Any) -> Action:
    return Action(
        name=f"transform_{target}",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.SQL,
        target=target,
        **fields,
    )


def _collect(fg: FlowGroup, **kwargs: Any):
    kwargs.setdefault("registry", ActionRegistry())
    kwargs.setdefault("substitution_mgr", None)
    kwargs.setdefault("project_root", None)
    kwargs.setdefault("table_renames", None)
    return collect_flowgroup_facts(fg, fg.pipeline, **kwargs)


def _substitution_mgr(
    root: Path, mappings: Dict[str, str]
) -> EnhancedSubstitutionManager:
    path = root / "substitutions" / "dev.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"dev": mappings}), encoding="utf-8")
    return EnhancedSubstitutionManager(path, env="dev")


def test_sql_path_file_stream_read_collected(tmp_path: Path):
    """The ``sql_path`` file is loaded and substituted as the generator does it."""
    sql_file = tmp_path / "sql" / "recent.sql"
    sql_file.parent.mkdir()
    sql_file.write_text(
        "SELECT * FROM STREAM(${catalog}.silver.t) WHERE id > 0\n", encoding="utf-8"
    )
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[_sql_transform("v_recent", sql_path="sql/recent.sql")],
    )

    _, readers = _collect(
        fg,
        substitution_mgr=_substitution_mgr(tmp_path, {"catalog": "main"}),
        project_root=tmp_path,
    )

    assert readers == (
        StreamReadFact(
            pipeline="p",
            flowgroup="fg",
            action="transform_v_recent",
            table_ref="main.silver.t",
            via="sql",
        ),
    )


def test_sandbox_renamed_delta_and_sql_readers_collected():
    """The structured rewrite renames the write and the delta load; the SQL body
    is renamed the way generate's text pass renames it."""
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _delta("src", read_mode="batch", target="v_src"),
            _rw_write(),
            _delta("rw", target="v_delta_reader"),
            _sql_transform("v_sql_reader", sql="SELECT * FROM STREAM(c.s.rw)"),
        ],
    )
    renames = build_sandbox_table_renames(
        [fg],
        TableRenameStrategy(namespace="dev_me", table_pattern="{namespace}_{table}"),
    )
    rewritten = rewrite_flowgroup_tables(fg, renames)

    producers, readers = _collect(rewritten, table_renames=renames)

    assert producers == (
        TableProducerFact(
            pipeline="p",
            flowgroup="fg",
            action="write_rw",
            table="c.s.dev_me_rw",
            is_replace_where=True,
        ),
    )
    assert {(r.action, r.table_ref, r.via) for r in readers} == {
        ("load_v_delta_reader", "c.s.dev_me_rw", "delta"),
        ("transform_v_sql_reader", "c.s.dev_me_rw", "sql"),
    }


class _SpyRegistry(ActionRegistry):
    """A real registry that records the reads it is asked to declare."""

    def __init__(self) -> None:
        super().__init__()
        self.asked: List[str] = []

    def consumer_read_mode(self, action: Action):
        self.asked.append(action.name)
        return super().consumer_read_mode(action)


def test_delta_read_mode_comes_from_the_given_registry():
    """The collector asks the registry it is handed: a spawned worker's default
    ``ActionRegistry()`` is empty, so the worker's registry must reach it."""
    registry = _SpyRegistry()
    fg = FlowGroup(pipeline="p", flowgroup="fg", actions=[_delta("rw")])

    _, readers = _collect(fg, registry=registry)

    assert "load_v_in" in registry.asked
    assert [(r.table_ref, r.via) for r in readers] == [("c.s.rw", "delta")]


def test_every_write_produces_and_only_replace_where_is_marked():
    sink = Action(
        name="write_sink",
        type=ActionType.WRITE,
        source="v_in",
        write_target={
            "type": "sink",
            "sink_type": "delta",
            "sink_name": "s1",
            "options": {"tableName": "c.s.sunk"},
        },
    )
    standard = Action(
        name="write_st",
        type=ActionType.WRITE,
        source="v_in",
        write_target={
            "type": "streaming_table",
            "catalog": "c",
            "schema": "s",
            "table": "st",
        },
    )
    fg = FlowGroup(pipeline="p", flowgroup="fg", actions=[_rw_write(), standard, sink])

    producers, readers = _collect(fg)

    assert [(p.action, p.table, p.is_replace_where) for p in producers] == [
        ("write_rw", "c.s.rw", True),
        ("write_st", "c.s.st", False),
        ("write_sink", "c.s.sunk", False),
    ]
    assert readers == ()


def test_unparseable_stream_sql_is_read_lexically():
    """sqlglot cannot parse WATERMARK; the lexical fallback names the table."""
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _sql_transform(
                "v_x",
                sql="SELECT * FROM STREAM(c.s.rw) WATERMARK ts DELAY OF INTERVAL 1 MINUTE",
            )
        ],
    )

    _, readers = _collect(fg)

    assert readers == (
        StreamReadFact(
            pipeline="p",
            flowgroup="fg",
            action="transform_v_x",
            table_ref="c.s.rw",
            via="sql",
        ),
    )


def test_unextractable_stream_read_is_an_unverified_reader():
    sql = "SELECT * FROM STREAM((SELECT * FROM c.s.rw)) WATERMARK ts DELAY OF INTERVAL 1 MINUTE"
    fg = FlowGroup(
        pipeline="p", flowgroup="fg", actions=[_sql_transform("v_x", sql=sql)]
    )

    _, readers = _collect(fg)

    assert readers == (
        StreamReadFact(
            pipeline="p",
            flowgroup="fg",
            action="transform_v_x",
            table_ref=None,
            via="sql",
        ),
    )


def test_a_collection_error_fails_the_flowgroup(monkeypatch):
    """An exception while collecting fails the flowgroup on the per-flowgroup
    error path (fail-closed), never crossing the worker boundary."""
    fg = FlowGroup(pipeline="p", flowgroup="fg", actions=[_delta("rw")])
    _init_worker_state(monkeypatch, fg, ActionRegistry())

    def _boom(*args: Any, **kwargs: Any):
        raise RuntimeError("collector exploded")

    monkeypatch.setattr(facts_module, "collect_flowgroup_facts", _boom)
    outcome = _process_one_flowgroup(_context(fg), mode="validate")

    assert outcome.success is False
    assert "collector exploded" in outcome.errors[0]


def test_worker_outcome_carries_facts_collected_with_the_state_registry(monkeypatch):
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            _delta("src", read_mode="batch", target="v_src"),
            _rw_write(),
            _delta("rw"),
        ],
    )
    registry = _SpyRegistry()
    _init_worker_state(monkeypatch, fg, registry)

    outcome = _process_one_flowgroup(_context(fg), mode="validate")

    assert outcome.success, outcome.errors
    assert [p.table for p in outcome.producer_facts] == ["c.s.rw"]
    assert [(r.action, r.table_ref) for r in outcome.stream_read_facts] == [
        ("load_v_in", "c.s.rw")
    ]
    assert "load_v_in" in registry.asked


class _PassThroughProcessor:
    """Resolution stand-in: returns the context's flowgroup unchanged."""

    def process_flowgroup(self, ctx, substitution_mgr, include_tests=True):
        return ctx


def _context(fg: FlowGroup) -> FlowGroupContext:
    return FlowGroupContext(flowgroup=fg, source_yaml=None)


def _init_worker_state(monkeypatch, fg: FlowGroup, registry: ActionRegistry) -> None:
    """In-process analogue of ``_init_flowgroup_worker`` (logging untouched)."""
    state = _FlowgroupWorkerState(
        processor=_PassThroughProcessor(),
        substitution_managers={fg.pipeline: EnhancedSubstitutionManager()},
        include_tests=False,
        code_generator=None,
        pipeline_output_dirs={},
        environment="dev",
        action_registry=registry,
        project_root=None,
    )
    monkeypatch.setattr(fp, "_flowgroup_state", state)
