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

    def test_read_text_captures_stat_at_read_time(self, tmp_path):
        """The recorded ``(mtime_ns, size)`` is the state AT READ TIME, not a
        later stat. This is the graph-cache invariant: a body edited after the
        build read it must drift the body manifest so a later load MISSES
        instead of serving a stale graph."""
        target = tmp_path / "helper.py"
        target.write_text("x = 1\n")
        st_before = target.stat()
        cache = ParseCache()

        cache.read_text(target)
        # An edit landing DURING the build, after the read (longer -> new size).
        target.write_text("x = 2  # edited mid-build, changes size\n")
        st_after = target.stat()

        recorded = cache.recorded_reads()
        assert recorded[target] == (st_before.st_mtime_ns, st_before.st_size)
        assert recorded[target] != (st_after.st_mtime_ns, st_after.st_size)

    def test_recorded_stat_survives_memo_hit_after_edit(self, tmp_path):
        """A re-read that hits the memo returns the ORIGINAL bytes and keeps the
        ORIGINAL read-time stat. Recording a fresh stat on the memo hit would
        stamp a post-edit stat over a graph built from pre-edit bytes -- exactly
        the stale HIT this capture prevents."""
        target = tmp_path / "helper.py"
        target.write_text("x = 1\n")
        st_before = target.stat()
        cache = ParseCache()

        first = cache.read_text(target)
        target.write_text("x = 2  # edited mid-build, changes size\n")
        second = cache.read_text(target)  # memo hit -> original bytes

        assert first == second == "x = 1\n"
        assert cache.recorded_reads()[target] == (
            st_before.st_mtime_ns,
            st_before.st_size,
        )

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
            node = graphs.action_graph.nodes[f"p1.fg_{i}.act_{i}"]
            assert "cat.sch.orders" in node["external_sources"]


@pytest.mark.unit
class TestPythonExtractionResultCache:
    """extract_python memoizes per (content hash, frozen bindings)."""

    _CODE = 'df = spark.table("cat.sch.t")\n'

    def test_same_code_and_bindings_share_one_result(self):
        cache = ParseCache()
        first = cache.extract_python(self._CODE, None)
        second = cache.extract_python(self._CODE, None)
        assert first is second
        assert first.tables == ["cat.sch.t"]

    def test_equal_bindings_instances_hit_the_same_entry(self):
        from lhp.core.dependencies._bindings import (
            DictValue,
            ParameterBindings,
        )

        code = "def fn(*, table_name):\n    return spark.table(table_name)\n"

        def make() -> ParameterBindings:
            return ParameterBindings(
                function_name="fn",
                kwonly=DictValue({"table_name": frozenset({"cat.sch.orders"})}),
            )

        cache = ParseCache()
        first = cache.extract_python(code, make())
        second = cache.extract_python(code, make())
        assert first is second
        assert first.tables == ["cat.sch.orders"]

    def test_different_bindings_get_distinct_entries(self):
        from lhp.core.dependencies._bindings import (
            DictValue,
            ParameterBindings,
        )

        code = "def fn(*, table_name):\n    return spark.table(table_name)\n"

        def bindings_for(value: str) -> ParameterBindings:
            return ParameterBindings(
                function_name="fn",
                kwonly=DictValue({"table_name": frozenset({value})}),
            )

        cache = ParseCache()
        a = cache.extract_python(code, bindings_for("cat.sch.a"))
        b = cache.extract_python(code, bindings_for("cat.sch.b"))
        assert a.tables == ["cat.sch.a"]
        assert b.tables == ["cat.sch.b"]

    def test_unparseable_code_caches_empty_result(self):
        cache = ParseCache()
        result = cache.extract_python("def broken(:\n", None)
        assert result.tables == []
        assert result.warnings == []
