"""Unit tests for SourceParser: per-shape parameter bindings + warning stamping.

Pins the exact :class:`ParameterBindings` `_iter_python_bodies` attaches for
each of the four Python body shapes — each mirroring its generator/template
call convention byte-for-byte (``${token}`` values preserved verbatim) — and
the stamping of parser-emitted advisories with flowgroup/action/file context.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.core.dependencies._bindings import DictValue, ListValue, ParameterBindings
from lhp.core.dependencies.source_parsing import ActionSources, SourceParser
from lhp.models import Action, ActionType

DEP_002_CODE = "LHP-DEP-002"
DEP_003_CODE = "LHP-DEP-003"


def _parser(
    file_paths: dict[str, Path] | None = None,
    project_root: Path = Path("/tmp/nonexistent_lhp_root"),
) -> SourceParser:
    return SourceParser(file_paths or {}, project_root)


def _bodies(action: Action) -> list[tuple]:
    return list(_parser()._iter_python_bodies(action))


@pytest.mark.unit
class TestSnapshotCdcBindings:
    """source_function parameters bind KWONLY (functools.partial in codegen)."""

    def _action(self, source_function: dict) -> Action:
        return Action(
            name="apply_cdc",
            type=ActionType.WRITE,
            write_target={
                "type": "streaming_table",
                "catalog": "cat",
                "schema": "sch",
                "table": "orders_silver",
                "snapshot_cdc_config": {"source_function": source_function},
            },
        )

    def test_parameters_bind_kwonly_with_token_bytes(self):
        action = self._action(
            {
                "file": "cdc/source.py",
                "function": "next_snapshot",
                "parameters": {
                    "table_name": "${catalog}.bronze.orders",
                    "tables": ["a.b.c", "a.b.d"],
                    "batch_size": 100,  # non-str scalar: unbindable, dropped
                },
            }
        )

        bindings = [b for _, path, b in _bodies(action) if path == "cdc/source.py"]

        assert bindings == [
            ParameterBindings(
                function_name="next_snapshot",
                kwonly=DictValue(
                    {
                        "table_name": frozenset({"${catalog}.bronze.orders"}),
                        "tables": ListValue(("a.b.c", "a.b.d")),
                    }
                ),
            )
        ]

    def test_no_parameters_binds_empty_kwonly(self):
        """Codegen emits the bare function reference (no kwargs) when no
        parameters are declared — an empty kwonly DictValue seeds nothing,
        mirroring that."""
        action = self._action({"file": "cdc/source.py", "function": "next_snapshot"})

        bindings = [b for _, path, b in _bodies(action) if path == "cdc/source.py"]

        assert bindings == [
            ParameterBindings(function_name="next_snapshot", kwonly=DictValue({}))
        ]

    def test_missing_function_name_binds_nothing(self):
        action = self._action({"file": "cdc/source.py"})

        bindings = [b for _, path, b in _bodies(action) if path == "cdc/source.py"]

        assert bindings == [None]


@pytest.mark.unit
class TestPythonTransformBindings:
    """Transform parameters dict is POSITIONAL: index 2 with source views, 1 without."""

    def _action(self, **overrides) -> Action:
        config = {
            "name": "t_act",
            "type": ActionType.TRANSFORM,
            "transform_type": "python",
            "module_path": "transforms/t.py",
            "function_name": "do_it",
            "source": "v_in",
            "target": "v_out",
            "parameters": {"table": "${env}.sch.t"},
        }
        config.update(overrides)
        return Action(**config)

    def test_single_source_view_binds_dict_at_index_2(self):
        bindings = [b for _, path, b in _bodies(self._action()) if path]

        assert bindings == [
            ParameterBindings(
                function_name="do_it",
                dict_arg_index=2,
                dict_value=DictValue({"table": frozenset({"${env}.sch.t"})}),
            )
        ]

    def test_multiple_source_views_bind_dict_at_index_2(self):
        action = self._action(source=["v_a", "v_b"])

        [binding] = [b for _, path, b in _bodies(action) if path]

        assert binding.dict_arg_index == 2

    def test_no_source_views_bind_dict_at_index_1(self):
        action = self._action(source=[])

        [binding] = [b for _, path, b in _bodies(action) if path]

        assert binding == ParameterBindings(
            function_name="do_it",
            dict_arg_index=1,
            dict_value=DictValue({"table": frozenset({"${env}.sch.t"})}),
        )

    def test_none_source_binds_nothing(self):
        """Codegen raises VAL_014 on a None source — no call to mirror."""
        action = self._action(source=None)

        assert [b for _, path, b in _bodies(action) if path] == [None]

    def test_undeclared_parameters_bind_empty_dict(self):
        """The template passes `parameters` positionally regardless."""
        action = self._action(parameters=None)

        [binding] = [b for _, path, b in _bodies(action) if path]

        assert binding == ParameterBindings(
            function_name="do_it", dict_arg_index=2, dict_value=DictValue({})
        )

    def test_non_python_transform_binds_nothing(self):
        action = self._action(transform_type="sql")

        assert [b for _, path, b in _bodies(action) if path] == [None]


@pytest.mark.unit
class TestPythonLoadBindings:
    """Python-load module files are analyzed; parameters dict positional at index 1."""

    def _action(self, source: dict) -> Action:
        return Action(
            name="load_act", type=ActionType.LOAD, target="v_raw", source=source
        )

    def test_load_module_is_yielded_with_default_get_df(self):
        action = self._action(
            {
                "type": "python",
                "module_path": "loaders/l.py",
                "parameters": {"table": "${catalog}.sch.orders"},
            }
        )

        results = _bodies(action)

        assert results == [
            (
                None,
                "loaders/l.py",
                ParameterBindings(
                    function_name="get_df",
                    dict_arg_index=1,
                    dict_value=DictValue(
                        {"table": frozenset({"${catalog}.sch.orders"})}
                    ),
                ),
            )
        ]

    def test_explicit_function_name_is_honored(self):
        action = self._action(
            {
                "type": "python",
                "module_path": "loaders/l.py",
                "function_name": "load_data",
            }
        )

        [(_, _, binding)] = _bodies(action)

        assert binding == ParameterBindings(
            function_name="load_data", dict_arg_index=1, dict_value=DictValue({})
        )

    def test_non_python_source_dict_is_not_yielded(self):
        action = self._action({"type": "cloudfiles", "path": "/mnt/raw"})

        assert _bodies(action) == []


@pytest.mark.unit
class TestNoBindingShapes:
    """Custom-sink / batch-handler bodies have no codegen parameters mechanism."""

    def test_custom_sink_module_path_has_no_bindings(self):
        action = Action(
            name="sink_act",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "my_sink",
                "module_path": "sinks/custom.py",
                "custom_sink_class": "MySink",
            },
        )

        assert (None, "sinks/custom.py", None) in _bodies(action)

    def test_batch_handler_inline_has_no_bindings(self):
        handler = "df.write.format('delta').save('/path')"
        action = Action(
            name="feb_act",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "batch_handler": handler,
            },
        )

        assert (handler, None, None) in _bodies(action)


@pytest.mark.unit
class TestWarningStamping:
    """Parser advisories come out stamped with flowgroup/action/file, line kept."""

    def test_file_body_warning_stamped_with_resolved_module_path(self, tmp_path):
        yaml_file = tmp_path / "pipelines" / "fg.yaml"
        yaml_file.parent.mkdir(parents=True)
        yaml_file.write_text("flowgroup: fg\n")
        module = yaml_file.parent / "transforms" / "t.py"
        module.parent.mkdir(parents=True)
        module.write_text(
            "def do_it(df, spark, parameters):\n    return spark.read.table(helper())\n"
        )
        action = Action(
            name="t_act",
            type=ActionType.TRANSFORM,
            transform_type="python",
            module_path="transforms/t.py",
            function_name="do_it",
            source="v_in",
            target="v_out",
        )

        result = _parser({"fg": yaml_file}, tmp_path).extract_action_sources(
            action, "fg"
        )

        assert isinstance(result, ActionSources)
        [warning] = result.warnings
        assert warning.code == DEP_002_CODE
        assert warning.flowgroup == "fg"
        assert warning.action == "t_act"
        assert warning.file_path == str(module)
        assert warning.line == 2  # the parser's own line survives stamping

    def test_inline_body_warning_stamped_with_yaml_path(self, tmp_path):
        yaml_file = tmp_path / "pipelines" / "fg.yaml"
        yaml_file.parent.mkdir(parents=True)
        yaml_file.write_text("flowgroup: fg\n")
        action = Action(
            name="feb_act",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "batch_handler": (
                    "def handler(df, batch_id):\n    spark.table(pick())\n"
                ),
            },
        )

        result = _parser({"fg": yaml_file}, tmp_path).extract_action_sources(
            action, "fg"
        )

        [warning] = result.warnings
        assert warning.code == DEP_002_CODE
        assert warning.flowgroup == "fg"
        assert warning.action == "feb_act"
        assert warning.file_path == str(yaml_file)
        assert warning.line == 2

    def test_unparseable_sql_file_body_yields_single_stamped_dep_003(self, tmp_path):
        """Garbage SQL never raises: zero sources + exactly ONE LHP-DEP-003,
        stamped with flowgroup/action and the resolved ``.sql`` file path."""
        yaml_file = tmp_path / "pipelines" / "fg.yaml"
        yaml_file.parent.mkdir(parents=True)
        yaml_file.write_text("flowgroup: fg\n")
        sql_file = yaml_file.parent / "sql" / "garbage.sql"
        sql_file.parent.mkdir(parents=True)
        sql_file.write_text("THIS IS NOT VALID SQL !!!")
        action = Action(
            name="bad_sql",
            type=ActionType.TRANSFORM,
            sql_path="sql/garbage.sql",
            target="v_out",
        )

        result = _parser({"fg": yaml_file}, tmp_path).extract_action_sources(
            action, "fg"
        )

        assert result.sources == []
        [warning] = result.warnings
        assert warning.code == DEP_003_CODE
        assert warning.flowgroup == "fg"
        assert warning.action == "bad_sql"
        assert warning.file_path == str(sql_file)

    def test_unparseable_inline_sql_yields_single_dep_003_with_yaml_path(
        self, tmp_path
    ):
        """Inline SQL bodies stamp the flowgroup YAML path instead."""
        yaml_file = tmp_path / "pipelines" / "fg.yaml"
        yaml_file.parent.mkdir(parents=True)
        yaml_file.write_text("flowgroup: fg\n")
        action = Action(
            name="bad_inline",
            type=ActionType.TRANSFORM,
            sql="THIS IS NOT VALID SQL !!!",
            target="v_out",
        )

        result = _parser({"fg": yaml_file}, tmp_path).extract_action_sources(
            action, "fg"
        )

        assert result.sources == []
        [warning] = result.warnings
        assert warning.code == DEP_003_CODE
        assert warning.flowgroup == "fg"
        assert warning.action == "bad_inline"
        assert warning.file_path == str(yaml_file)

    def test_sources_keep_existing_element_type(self, tmp_path):
        """ActionSources.sources stays a plain list of reference strings."""
        action = Action(
            name="reader",
            type=ActionType.TRANSFORM,
            source="cat.sch.orders",
            target="v_out",
        )

        result = _parser({}, tmp_path).extract_action_sources(action, "fg")

        assert result.sources == ["cat.sch.orders"]
        assert result.warnings == []
