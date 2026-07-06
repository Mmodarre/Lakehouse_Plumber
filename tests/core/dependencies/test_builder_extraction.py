"""Builder end-to-end: binding-driven edges + extraction-warning threading.

Exercises :meth:`DependencyGraphBuilder.build_from_flowgroups` over real
on-disk Python modules so the YAML parameter bindings seed the AST extractor
exactly the way codegen applies them at runtime:

- snapshot_cdc ``source_function`` parameters naming a produced table -> INTERNAL edge,
- python transform ``parameters: {tables: [a, b]}`` looped over -> one edge per element,
- python LOAD ``source.parameters`` naming a produced table -> INTERNAL edge,
- opaque helper-routed read -> stamped LHP-DEP-002 on ``graphs.extraction_warnings``, NO edge,
- warnings aggregate per SITE (code/file/line/message): duplicates from one
  action collapse to one record, and a shared helper referenced by many
  flowgroups collapses to one record enumerating the affected actions.
"""

from __future__ import annotations

import pytest

from lhp.core.dependencies.builder import DependencyGraphBuilder
from lhp.models import Action, ActionType, FlowGroup

DEP_002_CODE = "LHP-DEP-002"


def _write_action(name: str, source: str, catalog: str, schema: str, table: str):
    """A WRITE action that reads `source` and produces catalog.schema.table."""
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


def _transform_reader(name: str, source: str, target: str):
    return Action(name=name, type=ActionType.TRANSFORM, source=source, target=target)


def _flowgroup(name: str, pipeline: str, actions: list) -> FlowGroup:
    return FlowGroup(pipeline=pipeline, flowgroup=name, actions=actions)


def _producer_fg() -> FlowGroup:
    """A flowgroup producing cat.sch.orders via `write_orders`."""
    return _flowgroup(
        "producer_fg",
        "p1",
        [
            _transform_reader("prep", "raw.src", "v_prep"),
            _write_action("write_orders", "v_prep", "cat", "sch", "orders"),
        ],
    )


def _build(tmp_path, flowgroups):
    builder = DependencyGraphBuilder(project_root=tmp_path)
    return builder.build_from_flowgroups(flowgroups, file_paths={})


def _write_module(tmp_path, relative: str, code: str) -> None:
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(code)


@pytest.mark.unit
class TestBindingDrivenEdges:
    def test_snapshot_cdc_parameters_produce_internal_edge(self, tmp_path):
        """source_function kwonly parameters name a produced table -> edge."""
        _write_module(
            tmp_path,
            "cdc/source.py",
            "def next_snapshot(latest_version, *, table_name):\n"
            "    if latest_version is None:\n"
            "        return (spark.read.table(table_name), 1)\n"
            "    return None\n",
        )
        consumer = _flowgroup(
            "cdc_fg",
            "p2",
            [
                Action(
                    name="apply_cdc",
                    type=ActionType.WRITE,
                    write_target={
                        "type": "streaming_table",
                        "catalog": "cat",
                        "schema": "sch",
                        "table": "orders_silver",
                        "snapshot_cdc_config": {
                            "source_function": {
                                "file": "cdc/source.py",
                                "function": "next_snapshot",
                                "parameters": {"table_name": "cat.sch.orders"},
                            }
                        },
                    },
                )
            ],
        )

        graphs = _build(tmp_path, [_producer_fg(), consumer])

        assert graphs.action_graph.has_edge(
            "producer_fg.write_orders", "cdc_fg.apply_cdc"
        )
        assert graphs.extraction_warnings == []

    def test_transform_parameters_list_produce_edge_per_element(self, tmp_path):
        """`for t in parameters["tables"]: spark.read.table(t)` -> edge per element."""
        _write_module(
            tmp_path,
            "transforms/combine.py",
            "def combine(df, spark, parameters):\n"
            "    for t in parameters['tables']:\n"
            "        df = df.union(spark.read.table(t))\n"
            "    return df\n",
        )
        producer = _flowgroup(
            "producer_fg",
            "p1",
            [
                _transform_reader("prep_a", "raw.a", "v_a"),
                _write_action("write_a", "v_a", "cat", "sch", "a"),
                _transform_reader("prep_b", "raw.b", "v_b"),
                _write_action("write_b", "v_b", "cat", "sch", "b"),
            ],
        )
        consumer = _flowgroup(
            "consumer_fg",
            "p2",
            [
                Action(
                    name="combine_act",
                    type=ActionType.TRANSFORM,
                    transform_type="python",
                    module_path="transforms/combine.py",
                    function_name="combine",
                    source="v_in",
                    target="v_combined",
                    parameters={"tables": ["cat.sch.a", "cat.sch.b"]},
                )
            ],
        )

        graphs = _build(tmp_path, [producer, consumer])

        assert graphs.action_graph.has_edge(
            "producer_fg.write_a", "consumer_fg.combine_act"
        )
        assert graphs.action_graph.has_edge(
            "producer_fg.write_b", "consumer_fg.combine_act"
        )
        assert graphs.extraction_warnings == []

    def test_python_load_parameters_produce_internal_edge(self, tmp_path):
        """A python LOAD's source.parameters naming a produced table -> edge."""
        _write_module(
            tmp_path,
            "loaders/l.py",
            "def get_df(spark, parameters):\n"
            "    return spark.read.table(parameters['table'])\n",
        )
        consumer = _flowgroup(
            "load_fg",
            "p2",
            [
                Action(
                    name="load_orders",
                    type=ActionType.LOAD,
                    target="v_orders",
                    source={
                        "type": "python",
                        "module_path": "loaders/l.py",
                        "parameters": {"table": "cat.sch.orders"},
                    },
                )
            ],
        )

        graphs = _build(tmp_path, [_producer_fg(), consumer])

        assert graphs.action_graph.has_edge(
            "producer_fg.write_orders", "load_fg.load_orders"
        )
        assert graphs.extraction_warnings == []


@pytest.mark.unit
class TestExtractionWarningThreading:
    def test_opaque_read_yields_stamped_warning_and_no_edge(self, tmp_path):
        _write_module(
            tmp_path,
            "transforms/opaque.py",
            "def do_it(df, spark, parameters):\n"
            "    return spark.read.table(helper())\n",
        )
        consumer = _flowgroup(
            "opaque_fg",
            "p2",
            [
                Action(
                    name="opaque_act",
                    type=ActionType.TRANSFORM,
                    transform_type="python",
                    module_path="transforms/opaque.py",
                    function_name="do_it",
                    source="v_in",
                    target="v_out",
                )
            ],
        )

        graphs = _build(tmp_path, [_producer_fg(), consumer])

        [warning] = graphs.extraction_warnings
        assert warning.code == DEP_002_CODE
        assert warning.flowgroup == "opaque_fg"
        assert warning.action == "opaque_act"
        assert warning.file_path == str(tmp_path / "transforms/opaque.py")
        # Opaque read forms NO edge — never speculate.
        assert graphs.action_graph.in_degree("opaque_fg.opaque_act") == 0

    def test_duplicate_identical_warnings_dedupe_to_one(self, tmp_path):
        # Two identical opaque reads on the SAME line share one warning SITE
        # (code/message/file/line), so the builder's site aggregation folds
        # them into a single record with one affected action.
        _write_module(
            tmp_path,
            "transforms/dup.py",
            "def do_it(df, spark, parameters):\n"
            "    return [spark.read.table(h()), spark.read.table(h())]\n",
        )
        consumer = _flowgroup(
            "dup_fg",
            "p2",
            [
                Action(
                    name="dup_act",
                    type=ActionType.TRANSFORM,
                    transform_type="python",
                    module_path="transforms/dup.py",
                    function_name="do_it",
                    source="v_in",
                    target="v_out",
                )
            ],
        )

        graphs = _build(tmp_path, [consumer])

        assert len(graphs.extraction_warnings) == 1
        [warning] = graphs.extraction_warnings
        assert warning.code == DEP_002_CODE
        assert warning.affected_count == 1
        assert [(a.flowgroup, a.action) for a in warning.affected_actions] == [
            ("dup_fg", "dup_act")
        ]

    def test_shared_helper_site_aggregates_across_flowgroups(self, tmp_path):
        """Three flowgroups routing through ONE opaque helper site collapse
        to a single aggregated record: affected actions sorted by
        flowgroup/action, the first sorted pair as the representative, each
        pair stamped with its own flowgroup YAML as the depends_on edit
        path, and ``affected_count`` counting the distinct actions."""
        _write_module(
            tmp_path,
            "transforms/shared.py",
            "def do_it(df, spark, parameters):\n"
            "    return spark.read.table(helper())\n",
        )

        def consumer(idx: int) -> FlowGroup:
            return _flowgroup(
                f"fg_{idx}",
                "p2",
                [
                    Action(
                        name=f"act_{idx}",
                        type=ActionType.TRANSFORM,
                        transform_type="python",
                        module_path="transforms/shared.py",
                        function_name="do_it",
                        source="v_in",
                        target="v_out",
                    )
                ],
            )

        # Deliberately out-of-order input to prove the sorted representative.
        flowgroups = [consumer(2), consumer(0), consumer(1)]
        file_paths = {f"fg_{i}": tmp_path / f"pipelines/fg_{i}.yaml" for i in range(3)}

        builder = DependencyGraphBuilder(project_root=tmp_path)
        graphs = builder.build_from_flowgroups(flowgroups, file_paths=file_paths)

        [warning] = graphs.extraction_warnings
        assert warning.code == DEP_002_CODE
        assert warning.affected_count == 3
        assert [(a.flowgroup, a.action) for a in warning.affected_actions] == [
            ("fg_0", "act_0"),
            ("fg_1", "act_1"),
            ("fg_2", "act_2"),
        ]
        assert (warning.flowgroup, warning.action) == ("fg_0", "act_0")
        assert warning.edit_yaml_path == (tmp_path / "pipelines/fg_0.yaml").as_posix()
        for i, affected in enumerate(warning.affected_actions):
            expected = (tmp_path / f"pipelines/fg_{i}.yaml").as_posix()
            assert affected.edit_yaml_path == expected
