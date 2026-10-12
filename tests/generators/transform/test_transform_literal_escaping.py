"""Regression tests for #296 — user strings in generated transform code.

The transform templates wrapped SQL, descriptions, view and column names in
hand-written ``"..."`` / ``\"\"\"...\"\"\"`` literals, so quotes, backslashes and
line separators produced invalid Python or silently altered values (a
``'^\\\\d+$'`` regex in SQL reached Spark as ``'^\\d+$'``). Each test generates
the action, normalizes the code exactly as the write funnel does, parses it and
checks the values with :func:`ast.literal_eval`.
"""

import ast
from pathlib import Path

import pytest
import yaml

from lhp.generators.transform import (
    DataQualityTransformGenerator,
    PythonTransformGenerator,
    SchemaTransformGenerator,
    SQLTransformGenerator,
    TempTableTransformGenerator,
)
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
    QuarantineConfig,
    TransformType,
)
from lhp.utils.file_header import normalize_content

REGEX_SQL = "SELECT * FROM t WHERE code RLIKE '^\\\\d+$'"
ODD_METADATA_COLUMNS = ("it's", "C:\\new\\x41")
TRICKY_SQL = [
    REGEX_SQL,
    'SELECT "x"',
    'SELECT \'""""\' AS q',
    "SELECT 1 -- trailing backslash \\",
    "SELECT '\\N{BULLET}' AS b",
    "SELECT 'a\u2028b' AS s",
]
TRICKY_SQL_IDS = [
    "regex",
    "ends-in-quote",
    "quote-run",
    "backslash",
    "N-escape",
    "u2028",
]


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


def _first_args(tree: ast.Module, name: str) -> list[object]:
    return [ast.literal_eval(call.args[0]) for call in _calls(tree, name) if call.args]


def _docstrings(tree: ast.Module) -> list[str | None]:
    return [
        ast.get_docstring(node, clean=False)
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    ]


def _assignments(tree: ast.Module) -> dict[str, object]:
    values: dict[str, object] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            try:
                values[node.targets[0].id] = ast.literal_eval(node.value)
            except ValueError:
                continue
    return values


def _metadata_names(tree: ast.Module) -> list[object]:
    """Column names passed to ``withColumn`` or as ``withColumns`` dict keys."""
    names = [ast.literal_eval(call.args[0]) for call in _calls(tree, "withColumn")]
    for call in _calls(tree, "withColumns"):
        if isinstance(call.args[0], ast.Dict):
            names += [ast.literal_eval(key) for key in call.args[0].keys if key]
    return names


def _metadata_context(tmp_path: Path) -> dict:
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


def _transform(
    transform_type: TransformType, source: str = "v_orders", **fields
) -> Action:
    return Action(
        name="transform_orders",
        type=ActionType.TRANSFORM,
        transform_type=transform_type,
        source=source,
        target="v_orders_out",
        **fields,
    )


@pytest.mark.unit
class TestSqlTransform:
    @pytest.mark.parametrize("sql", TRICKY_SQL, ids=TRICKY_SQL_IDS)
    def test_inline_sql_reaches_spark_exactly(self, sql):
        code = SQLTransformGenerator().generate(
            _transform(TransformType.SQL, sql=sql), {}
        )

        assert _first_args(_parse(code), "sql") == [sql]

    def test_sql_path_regex_keeps_its_backslashes(self, tmp_path):
        sql = "SELECT * FROM t WHERE zip RLIKE '^\\\\d{3}$'"
        (tmp_path / "q.sql").write_text(sql + "\n", encoding="utf-8")

        code = SQLTransformGenerator().generate(
            _transform(TransformType.SQL, sql_path="q.sql"),
            {"project_root": tmp_path},
        )

        assert _first_args(_parse(code), "sql") == [sql]

    def test_description_comment_and_docstring(self):
        description = 'Clean "orders" from C:\\new """x"""'

        tree = _parse(
            SQLTransformGenerator().generate(
                _transform(TransformType.SQL, sql="SELECT 1", description=description),
                {},
            )
        )

        (view,) = _calls(tree, "temporary_view")
        assert ast.literal_eval(view.keywords[0].value) == description
        assert _docstrings(tree) == [description]

    def test_ordinary_values_keep_their_exact_bytes(self):
        sql = 'SELECT "" AS e,\t"a""b" AS q, \'caf\u00e9\''

        code = SQLTransformGenerator().generate(
            _transform(TransformType.SQL, sql=sql, description="tab\there"), {}
        )

        assert f'    df = spark.sql("""{sql}""")\n' in code
        assert '@dp.temporary_view(comment="tab\there")\n' in code


@pytest.mark.unit
class TestTempTable:
    @pytest.mark.parametrize("sql", TRICKY_SQL, ids=TRICKY_SQL_IDS)
    def test_sql_reaches_spark_exactly(self, sql):
        code = TempTableTransformGenerator().generate(
            _transform(TransformType.TEMP_TABLE, sql=sql, description='T "doc"'), {}
        )

        tree = _parse(code)
        assert _first_args(tree, "sql") == [sql]
        assert _docstrings(tree) == ['T "doc"']

    def test_source_view_name_round_trips(self):
        action = _transform(TransformType.TEMP_TABLE, source='v_"odd"\\view')

        code = TempTableTransformGenerator().generate(action, {})

        assert _first_args(_parse(code), "table") == ['v_"odd"\\view']


@pytest.mark.unit
class TestPythonTransform:
    def _generate(self, tmp_path: Path, **fields) -> ast.Module:
        (tmp_path / "fns.py").write_text(
            "def enrich(df, spark, parameters):\n    return df\n", encoding="utf-8"
        )
        action = _transform(
            TransformType.PYTHON,
            module_path="fns.py",
            function_name="enrich",
            operational_metadata=list(ODD_METADATA_COLUMNS),
            **fields,
        )
        return _parse(
            PythonTransformGenerator().generate(action, _metadata_context(tmp_path))
        )

    def test_parameters_are_python_literals(self, tmp_path):
        parameters = {"flag": True, "nothing": None, "path": "C:\\new", "q": '"'}

        tree = self._generate(tmp_path, parameters=parameters)

        assert _assignments(tree)["parameters"] == parameters

    def test_description_and_metadata_columns(self, tmp_path):
        tree = self._generate(tmp_path, description='Enrich """x""" C:\\new')

        assert _docstrings(tree) == ['Enrich """x""" C:\\new']
        assert sorted(_metadata_names(tree)) == sorted(ODD_METADATA_COLUMNS)


@pytest.mark.unit
class TestSchemaTransform:
    def test_legacy_mapping_and_cast_round_trip(self):
        schema = yaml.safe_dump(
            {
                "column_mapping": {'we"ird': "clean"},
                "type_casting": {"clean": 'STRUCT<n: STRING COMMENT "x\\d">'},
            }
        )
        action = _transform(
            TransformType.SCHEMA,
            schema_inline=schema,
            description="Schema C:\\new",
            readMode="batch",
        )

        tree = _parse(SchemaTransformGenerator().generate(action, {}))

        (rename,) = _calls(tree, "withColumnRenamed")
        assert [ast.literal_eval(arg) for arg in rename.args] == ['we"ird', "clean"]
        (cast,) = _calls(tree, "cast")
        assert ast.literal_eval(cast.args[0]) == 'STRUCT<n: STRING COMMENT "x\\d">'
        assert _docstrings(tree) == ["Schema C:\\new"]


def _expectations(tmp_path: Path) -> str:
    path = tmp_path / "expectations.yaml"
    path.write_text(
        yaml.safe_dump(
            {"code RLIKE '^\\\\d+$'": {"action": "drop", "name": 'digits "only"'}}
        ),
        encoding="utf-8",
    )
    return str(path)


@pytest.mark.unit
class TestDataQuality:
    def test_description_and_metadata_columns(self, tmp_path):
        action = _transform(
            TransformType.DATA_QUALITY,
            readMode="stream",
            expectations_file=_expectations(tmp_path),
            description='DQ """x""" C:\\new',
            operational_metadata=list(ODD_METADATA_COLUMNS),
        )

        tree = _parse(
            DataQualityTransformGenerator().generate(
                action, _metadata_context(tmp_path)
            )
        )

        assert _docstrings(tree) == ['DQ """x""" C:\\new']
        assert sorted(_metadata_names(tree)) == sorted(ODD_METADATA_COLUMNS)

    def test_quarantine_strings_round_trip(self, tmp_path):
        description = 'Quarantine """x""" ends with "'
        action = _transform(
            TransformType.DATA_QUALITY,
            readMode="stream",
            expectations_file=_expectations(tmp_path),
            description=description,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table='cat.sch."dlq"', source_table="cat.sch.src\\x"
            ),
            operational_metadata=list(ODD_METADATA_COLUMNS),
        )

        tree = _parse(
            DataQualityTransformGenerator().generate(
                action, _metadata_context(tmp_path)
            )
        )

        values = _assignments(tree)
        assert values["DLQ_TABLE_v_orders"] == 'cat.sch."dlq"'
        assert values["DLQ_OUTBOX_TABLE_v_orders"] == 'cat.sch."dlq"_outbox'
        assert values["SOURCE_TABLE_v_orders"] == "cat.sch.src\\x"
        docstrings = _docstrings(tree)
        assert f"{description} — clean records" in docstrings
        assert f"{description} — clean + recycled records" in docstrings
        assert set(ODD_METADATA_COLUMNS) <= set(_metadata_names(tree))


@pytest.mark.unit
@pytest.mark.parametrize(
    "transform_type, fields",
    [
        (TransformType.SQL, {"sql": "SELECT 1"}),
        (TransformType.TEMP_TABLE, {"sql": "SELECT 1"}),
    ],
    ids=["sql", "temp_table"],
)
def test_operational_metadata_column_names_round_trip(tmp_path, transform_type, fields):
    generator = {
        TransformType.SQL: SQLTransformGenerator,
        TransformType.TEMP_TABLE: TempTableTransformGenerator,
    }[transform_type]
    action = _transform(
        transform_type, operational_metadata=list(ODD_METADATA_COLUMNS), **fields
    )

    tree = _parse(generator().generate(action, _metadata_context(tmp_path)))

    assert sorted(_metadata_names(tree)) == sorted(ODD_METADATA_COLUMNS)
