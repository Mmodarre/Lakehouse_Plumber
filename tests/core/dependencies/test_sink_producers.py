"""Acceptance tests: sink-destination producer registration.

A ``type: sink`` WRITE action stores its destination in ``options.tableName``,
NOT in ``write_target.catalog/schema/table``. Before this fix, only the
catalog/schema/table triple was registered as a producer, so every table
written by a delta sink had NO producer and every downstream read of it was
wrongly classified "external".

These tests build a real action graph over in-memory flowgroups and assert:

- a delta sink's ``options.tableName`` IS registered -> a downstream reader
  gets an INTERNAL edge (not external);
- 2-part<->3-part reconciliation still applies to a sink-registered producer;
- kafka / custom / foreachbatch sinks (and a delta sink writing to ``path``)
  register NO producer -> the downstream read stays EXTERNAL;
- non-sink (streaming_table) registration is unchanged (regression guard).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.core.dependencies._producers import build_producer_indexes
from lhp.core.dependencies.builder import DependencyGraphBuilder
from lhp.models import Action, ActionType, FlowGroup


def _delta_sink_action(name: str, source: str, table_name: str) -> Action:
    """A delta-sink WRITE reading `source`, writing to options.tableName."""
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "sink",
            "sink_type": "delta",
            "options": {"tableName": table_name},
        },
    )


def _delta_sink_path_action(name: str, source: str, path: str) -> Action:
    """A delta-sink WRITE to a filesystem path (no tableName)."""
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "sink",
            "sink_type": "delta",
            "options": {"path": path},
        },
    )


def _kafka_sink_action(name: str, source: str) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "sink",
            "sink_type": "kafka",
            "options": {"kafka.bootstrap.servers": "broker:9092", "topic": "t"},
        },
    )


def _custom_sink_action(name: str, source: str) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "sink",
            "sink_type": "custom",
            "module_path": "sinks/my_sink.py",
            "custom_sink_class": "MySink",
        },
    )


def _foreachbatch_sink_action(name: str, source: str) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "sink",
            "sink_type": "foreachbatch",
            "batch_handler": "handlers/h.py",
        },
    )


def _streaming_table_action(
    name: str, source: str, catalog: str, schema: str, table: str
) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=source,
        write_target={
            "type": "streaming_table",
            "catalog": catalog,
            "schema": schema,
            "table": table,
        },
    )


def _transform_reader(name: str, source: str, target: str) -> Action:
    """A TRANSFORM action reading `source` and producing a (view) `target`."""
    return Action(name=name, type=ActionType.TRANSFORM, source=source, target=target)


def _flowgroup(name: str, pipeline: str, actions: list) -> FlowGroup:
    return FlowGroup(pipeline=pipeline, flowgroup=name, actions=actions)


def _build(flowgroups: list[FlowGroup]):
    builder = DependencyGraphBuilder(project_root=Path("/tmp/nonexistent_lhp_root"))
    return builder.build_from_flowgroups(flowgroups, file_paths={}).action_graph


@pytest.mark.unit
class TestDeltaSinkProducerRegistration:
    def test_delta_sink_table_name_registers_producer_internal_edge(self):
        """Delta sink to `cat.stg.events`; downstream read -> INTERNAL edge."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _delta_sink_action("sink_events", "v_prep", "cat.stg.events"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_events", "cat.stg.events", "v_out")],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge(
            "p1.producer_fg.sink_events", "p2.consumer_fg.read_events"
        )
        edge = graph.edges["p1.producer_fg.sink_events", "p2.consumer_fg.read_events"]
        assert edge["dependency_type"] == "internal"
        assert "external_sources" not in graph.nodes["p2.consumer_fg.read_events"]

    def test_delta_sink_case_and_backtick_variant_match(self):
        """Sink tableName `Cat.Stg.Events`; reader `` `cat`.`stg`.`events` ``."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _delta_sink_action("sink_events", "v_prep", "Cat.Stg.Events"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_events", "`cat`.`stg`.`events`", "v_out")],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge(
            "p1.producer_fg.sink_events", "p2.consumer_fg.read_events"
        )

    def test_delta_sink_two_part_reader_unique_catalog_match(self):
        """Sink to `cat.stg.events`; 2-part reader `stg.events` (unique) -> edge."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _delta_sink_action("sink_events", "v_prep", "cat.stg.events"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_events", "stg.events", "v_out")],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge(
            "p1.producer_fg.sink_events", "p2.consumer_fg.read_events"
        )
        assert "external_sources" not in graph.nodes["p2.consumer_fg.read_events"]


@pytest.mark.unit
class TestNonTableSinksStayExternal:
    def test_kafka_sink_registers_no_producer(self):
        """A kafka sink is a terminal external sink: no producer registered."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _kafka_sink_action("sink_kafka", "v_prep"),
            ],
        )
        # Nothing downstream can read a kafka sink, but assert the index has no
        # producer by reading the would-be table-shaped name as external.
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_x", "cat.stg.events", "v_out")],
        )

        graph = _build([producer, consumer])

        assert not graph.has_edge("p1.producer_fg.sink_kafka", "p2.consumer_fg.read_x")
        assert graph.nodes["p2.consumer_fg.read_x"].get("external_sources") == [
            "cat.stg.events"
        ]

    def test_custom_sink_registers_no_producer(self):
        """A custom sink has no table destination -> empty producer index.

        Asserted directly against ``build_producer_indexes`` (the producer side
        under test) rather than the full graph: custom/foreachbatch sinks carry
        a ``module_path``/``batch_handler`` that the source parser would try to
        resolve from disk during edge extraction — irrelevant to producer
        registration.
        """
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [_custom_sink_action("sink_custom", "v_prep")],
        )

        table_producers, table_short_to_catalogs = build_producer_indexes([producer])

        assert table_producers == {}
        assert table_short_to_catalogs == {}

    def test_foreachbatch_sink_registers_no_producer(self):
        """A foreachbatch sink has no table destination -> empty producer index."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [_foreachbatch_sink_action("sink_feb", "v_prep")],
        )

        table_producers, table_short_to_catalogs = build_producer_indexes([producer])

        assert table_producers == {}
        assert table_short_to_catalogs == {}

    def test_delta_sink_to_path_registers_no_producer(self):
        """A delta sink writing to `path` (no tableName) has no table producer."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _delta_sink_path_action("sink_path", "v_prep", "/Volumes/x/y/z"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_x", "cat.stg.events", "v_out")],
        )

        graph = _build([producer, consumer])

        assert not graph.has_edge("p1.producer_fg.sink_path", "p2.consumer_fg.read_x")
        assert graph.nodes["p2.consumer_fg.read_x"].get("external_sources") == [
            "cat.stg.events"
        ]


@pytest.mark.unit
class TestNonSinkRegistrationUnchanged:
    def test_streaming_table_producer_still_matches(self):
        """Regression guard: catalog/schema/table registration is unchanged."""
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep", "raw.src", "v_prep"),
                _streaming_table_action("write_t", "v_prep", "cat", "stg", "events"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [_transform_reader("read_events", "cat.stg.events", "v_out")],
        )

        graph = _build([producer, consumer])

        assert graph.has_edge("p1.producer_fg.write_t", "p2.consumer_fg.read_events")
        assert "external_sources" not in graph.nodes["p2.consumer_fg.read_events"]
