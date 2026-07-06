"""Per-run parse-cache behavior: one read/parse per distinct content.

Memoization is asserted through object identity (the same cached artifact
comes back) and through spies at the cache's construction seams — plus one
builder-level end-to-end proving a helper module shared by several actions
parses once while the bindings-dependent visitor still runs per action.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from lhp.core.dependencies._function_index import FunctionIndex
from lhp.core.dependencies._parse_cache import ParseCache
from lhp.core.dependencies.builder import DependencyGraphBuilder
from lhp.models import Action, ActionType, FlowGroup


@pytest.mark.unit
class TestParseCache:
    def test_read_text_reads_each_path_once(self, tmp_path):
        target = tmp_path / "helper.py"
        target.write_text("x = 1\n")
        cache = ParseCache()

        first = cache.read_text(target)
        target.write_text("x = 2\n")  # a re-read would observe this
        second = cache.read_text(target)

        assert first == second == "x = 1\n"

    def test_parse_python_memoizes_by_content(self):
        cache = ParseCache()
        code = 'def f():\n    return "cat.sch.t"\n'

        first = cache.parse_python(code)
        second = cache.parse_python(code)

        assert first is not None
        assert first is second  # same (tree, index) pair, not a re-parse
        assert cache.parse_python("other = 1\n") is not first

    def test_parse_python_caches_unparseable_as_none(self):
        cache = ParseCache()
        assert cache.parse_python("def broken(:\n") is None
        assert cache.parse_python("def broken(:\n") is None

    def test_extract_sql_memoizes_by_content(self):
        cache = ParseCache()

        first = cache.extract_sql("SELECT * FROM cat.sch.orders")
        second = cache.extract_sql("SELECT * FROM cat.sch.orders")

        assert first is second
        assert first.tables == ["cat.sch.orders"]


@pytest.mark.unit
class TestBuilderParsesSharedHelperOnce:
    def test_shared_module_parses_once_across_actions(self, tmp_path):
        module = tmp_path / "transforms/shared.py"
        module.parent.mkdir(parents=True)
        module.write_text(
            "def do_it(df, spark, parameters):\n"
            '    return spark.read.table("cat.sch.orders")\n'
        )

        def consumer(idx: int) -> FlowGroup:
            return FlowGroup(
                pipeline="p1",
                flowgroup=f"fg_{idx}",
                actions=[
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

        builder = DependencyGraphBuilder(project_root=tmp_path)
        with patch(
            "lhp.core.dependencies._parse_cache.FunctionIndex",
            wraps=FunctionIndex,
        ) as index_spy:
            graphs = builder.build_from_flowgroups(
                [consumer(i) for i in range(3)], file_paths={}
            )

        # One parse + index for the shared body...
        assert index_spy.call_count == 1
        # ...while extraction still ran per action (each records the read).
        for i in range(3):
            node = graphs.action_graph.nodes[f"fg_{i}.act_{i}"]
            assert "cat.sch.orders" in node["external_sources"]
