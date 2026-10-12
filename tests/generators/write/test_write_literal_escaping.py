"""Regression tests for #296 — user strings in generated write code.

The streaming-table, materialized-view and sink templates wrapped config
strings in hand-written ``"..."`` / ``\"\"\"...\"\"\"`` literals and ``#``
comments, so quotes, backslashes, newlines and line separators produced
invalid Python, silently altered values, or (in a sink comment) executable
code. Each test generates the action, normalizes the code exactly as the write
funnel does (:func:`lhp.utils.file_header.normalize_content`), parses it and
checks the values with :func:`ast.literal_eval`.
"""

import ast
from pathlib import Path

import pytest

from lhp.generators.write.materialized_view import MaterializedViewWriteGenerator
from lhp.generators.write.sinks import (
    CustomSinkWriteGenerator,
    DeltaSinkWriteGenerator,
    KafkaSinkWriteGenerator,
)
from lhp.generators.write.sinks.foreachbatch_sink import (
    ForEachBatchSinkWriteGenerator,
)
from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
)
from lhp.utils.file_header import normalize_content

UNC_PATH = "\\\\srv\\new"
REGEX = "code RLIKE '^\\\\d+$'"
ODD_METADATA_COLUMNS = ("it's", "C:\\new\\x41")


def _parse(code: str) -> ast.Module:
    """Parse the generated code as it is written to disk."""
    return ast.parse(normalize_content(code))


def _calls(tree: ast.Module, name: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == name
    ]


def _kwargs(tree: ast.Module, name: str) -> dict[str, object]:
    """Literal keyword arguments of the only ``<x>.<name>(...)`` call."""
    (call,) = _calls(tree, name)
    values: dict[str, object] = {}
    for keyword in call.keywords:
        try:
            values[str(keyword.arg)] = ast.literal_eval(keyword.value)
        except ValueError:
            continue
    return values


def _docstrings(tree: ast.Module) -> list[str | None]:
    return [
        ast.get_docstring(node, clean=False)
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    ]


def _with_column_names(tree: ast.Module) -> list[object]:
    return [ast.literal_eval(call.args[0]) for call in _calls(tree, "withColumn")]


def _write(sub_type: str, description: str | None = None, **write_target) -> Action:
    return Action(
        name="write_orders",
        type=ActionType.WRITE,
        source="v_orders",
        description=description,
        write_target={
            "type": sub_type,
            "catalog": "main",
            "schema": "silver",
            "table": "orders",
            **write_target,
        },
    )


def _streaming_table(context: dict | None = None, **kwargs) -> ast.Module:
    code = StreamingTableWriteGenerator().generate(
        _write("streaming_table", **kwargs), {"expectations": [], **(context or {})}
    )
    return _parse(code)


@pytest.mark.unit
class TestStreamingTable:
    def test_comment_with_quote_and_unc_path(self):
        comment = f'say "hi" {UNC_PATH}'

        tree = _streaming_table(comment=comment)

        assert _kwargs(tree, "create_streaming_table")["comment"] == comment

    def test_ddl_file_with_backslash_u(self, tmp_path):
        ddl = "id BIGINT,\npath STRING COMMENT 'C:\\Users\\x'"
        (tmp_path / "orders.ddl").write_text(ddl + "\n", encoding="utf-8")

        tree = _streaming_table({"project_root": tmp_path}, table_schema="orders.ddl")

        assert _kwargs(tree, "create_streaming_table")["schema"] == ddl

    @pytest.mark.parametrize(
        "field", ["path", "row_filter"], ids=["path", "row_filter"]
    )
    def test_table_string_options_round_trip(self, field):
        value = f'{UNC_PATH} "q"'

        tree = _streaming_table(**{field: value})

        assert _kwargs(tree, "create_streaming_table")[field] == value

    def test_flow_description_comment_and_docstring(self):
        description = 'Append "orders" from C:\\new """x"""'

        tree = _streaming_table(description=description)

        assert _kwargs(tree, "append_flow")["comment"] == description
        assert _docstrings(tree) == [description]

    def test_cdc_strings_round_trip(self):
        deletes = f'op = "D" OR {REGEX}'
        truncates = 'op = "T"'
        sequence_by = '`event "ts"`'

        tree = _streaming_table(
            mode="cdc",
            cdc_config={
                "keys": ['id"x'],
                "sequence_by": sequence_by,
                "apply_as_deletes": deletes,
                "apply_as_truncates": truncates,
            },
        )

        cdc = _kwargs(tree, "create_auto_cdc_flow")
        assert cdc["apply_as_deletes"] == deletes
        assert cdc["apply_as_truncates"] == truncates
        assert cdc["sequence_by"] == sequence_by
        assert cdc["keys"] == ['id"x']

    def test_cdc_struct_sequence_by_keeps_json_spelling(self):
        code = StreamingTableWriteGenerator().generate(
            _write(
                "streaming_table",
                mode="cdc",
                cdc_config={"keys": ["id"], "sequence_by": ["a", 'b"c']},
            ),
            {"expectations": []},
        )

        assert 'sequence_by=struct("a", "b\\"c"),' in code
        _parse(code)

    def test_snapshot_source_round_trips(self):
        source = 'raw."snap\\shots"'

        tree = _streaming_table(
            mode="snapshot_cdc",
            snapshot_cdc_config={"source": source, "keys": ["id"]},
        )

        assert _kwargs(tree, "create_auto_cdc_from_snapshot_flow")["source"] == source

    def test_replace_flow_strings_round_trip(self):
        description = 'Replace "orders"\\d'

        tree = _streaming_table(
            mode="replace",
            description=description,
            replace_config={"replace_using": ["id"], "sequence_by": '`ts"x`'},
        )

        flow = _kwargs(tree, "replace_flow")
        assert flow["sequence_by"] == '`ts"x`'
        assert flow["comment"] == description
        assert _docstrings(tree) == [description]

    def test_table_properties_with_constants_are_python(self):
        tree = _streaming_table(
            table_properties={"delta.enableChangeDataFeed": True, "x": None}
        )

        assert _kwargs(tree, "create_streaming_table")["table_properties"] == {
            "delta.enableChangeDataFeed": True,
            "x": None,
        }

    def test_ordinary_values_keep_their_exact_bytes(self):
        """Tabs, other control characters and non-ASCII text stay raw."""
        code = StreamingTableWriteGenerator().generate(
            _write(
                "streaming_table",
                description="caf\u00e9\tflow",
                comment="tab\there \x07",
                table_schema='id BIGINT COMMENT "a ""b"" c"',
            ),
            {"expectations": []},
        )

        assert '    comment="tab\there \x07",\n' in code
        assert '    schema="""id BIGINT COMMENT "a ""b"" c\\"""")\n' in code
        assert '    comment="caf\u00e9\tflow"\n' in code
        assert '    """caf\u00e9\tflow"""\n' in code


@pytest.mark.unit
class TestMaterializedView:
    def _generate(self, context: dict | None = None, **kwargs) -> ast.Module:
        code = MaterializedViewWriteGenerator().generate(
            _write("materialized_view", **kwargs), context or {}
        )
        return _parse(code)

    def test_quoted_strings_round_trip(self):
        values = {
            "comment": 'MV "orders"',
            "row_filter": 'region = "EU"',
            "refresh_schedule": 'CRON "0 0 * * *"',
            "path": UNC_PATH,
        }

        tree = self._generate(**values)

        kwargs = _kwargs(tree, "materialized_view")
        assert {key: kwargs[key] for key in values} == values

    def test_multi_line_ddl_schema(self, tmp_path):
        ddl = 'id BIGINT,\nname STRING COMMENT "n\\d"'
        (tmp_path / "mv.ddl").write_text(ddl, encoding="utf-8")

        tree = self._generate({"project_root": tmp_path}, table_schema="mv.ddl")

        assert _kwargs(tree, "materialized_view")["schema"] == ddl

    @pytest.mark.parametrize(
        "sql",
        [f"SELECT * FROM t WHERE {REGEX}", 'SELECT "x"', 'SELECT \'""""\' AS q'],
        ids=["regex", "ends-in-quote", "quadruple-quote"],
    )
    def test_sql_reaches_spark_exactly(self, sql):
        tree = self._generate(sql=sql, description='MV "doc"')

        assert [ast.literal_eval(c.args[0]) for c in _calls(tree, "sql")] == [sql]
        assert _docstrings(tree) == ['MV "doc"']


def _sink_context(tmp_path: Path) -> dict:
    return {
        "spec_dir": tmp_path,
        "output_dir": tmp_path / "generated",
        "flowgroup": FlowGroup(pipeline="p", flowgroup="fg", actions=[]),
        "preset_config": {},
        "project_config": ProjectConfig(
            name="p",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    name: MetadataColumnConfig(
                        expression="F.current_timestamp()", applies_to=["view"]
                    )
                    for name in ODD_METADATA_COLUMNS
                }
            ),
        ),
    }


def _sink(
    sink_type: str, comment: str, description: str | None, **write_target
) -> Action:
    return Action(
        name="write_sink",
        type=ActionType.WRITE,
        source="v_orders",
        description=description,
        operational_metadata=list(ODD_METADATA_COLUMNS),
        write_target={
            "type": "sink",
            "sink_type": sink_type,
            "sink_name": "orders_sink",
            "comment": comment,
            **write_target,
        },
    )


_SINKS = {
    "delta": (DeltaSinkWriteGenerator, {"options": {"tableName": "c.s.t"}}),
    "kafka": (
        KafkaSinkWriteGenerator,
        {"bootstrap_servers": "localhost:9092", "topic": "events"},
    ),
    "custom": (
        CustomSinkWriteGenerator,
        {"module_path": "my_sink.py", "custom_sink_class": "MySink"},
    ),
    "foreachbatch": (ForEachBatchSinkWriteGenerator, {"batch_handler": "pass"}),
}


@pytest.mark.unit
@pytest.mark.parametrize("sink_type", sorted(_SINKS))
class TestSinks:
    def _generate(
        self,
        tmp_path: Path,
        sink_type: str,
        comment: str,
        description: str | None = None,
    ) -> str:
        (tmp_path / "my_sink.py").write_text(
            "class MySink(DataSink):\n"
            "    @classmethod\n"
            "    def name(cls):\n"
            '        return "my_sink"\n'
        )
        generator, write_target = _SINKS[sink_type]
        return generator().generate(
            _sink(sink_type, comment, description, **write_target),
            _sink_context(tmp_path),
        )

    def test_newline_in_comment_cannot_inject_code(self, tmp_path, sink_type):
        code = self._generate(tmp_path, sink_type, "line1\nINJECTED = 1")

        tree = _parse(code)
        assigned = {
            target.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        assert "INJECTED" not in assigned
        assert "# line1\n# INJECTED = 1\n" in normalize_content(code)

    @pytest.mark.parametrize(
        "comment",
        ["Stream metrics\n", "Stream metrics\n# second line\n", "a\n\n  # b\n\t\n"],
        ids=["block-scalar", "comment-line", "blank-lines"],
    )
    def test_multi_line_comment_that_was_valid_keeps_its_bytes(
        self, tmp_path, sink_type, comment
    ):
        code = self._generate(tmp_path, sink_type, comment, description="d")

        assert code.startswith(f"# {comment}\n\n")
        _parse(code)

    def test_flow_comment_and_metadata_columns_round_trip(self, tmp_path, sink_type):
        description = f'Sink "orders" {UNC_PATH}\nsecond line'

        tree = _parse(self._generate(tmp_path, sink_type, "c", description))

        assert _kwargs(tree, "append_flow")["comment"] == description
        assert sorted(_with_column_names(tree)) == sorted(ODD_METADATA_COLUMNS)
