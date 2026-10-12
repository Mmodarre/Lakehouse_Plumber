"""Regression tests for #296 — user strings in generated load code.

Every load template used to wrap raw config strings in hand-written ``"..."``
/ ``\"\"\"...\"\"\"`` literals, so quotes, backslashes, newlines and line
separators produced invalid Python or silently altered values. Each test
generates a load action, normalizes the code exactly as the write funnel does
(:func:`lhp.utils.file_header.normalize_content`), parses it, and asserts the
literal evaluates to the original value.
"""

import ast
from pathlib import Path

import pytest

from lhp.generators.load.cloudfiles import CloudFilesLoadGenerator
from lhp.generators.load.custom_datasource import CustomDataSourceLoadGenerator
from lhp.generators.load.delta import DeltaLoadGenerator
from lhp.generators.load.jdbc import JDBCLoadGenerator
from lhp.generators.load.kafka import KafkaLoadGenerator
from lhp.generators.load.python import PythonLoadGenerator
from lhp.generators.load.sql import SQLLoadGenerator
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
)
from lhp.utils.file_header import normalize_content

WINDOWS_PATH = "C:\\new\\table\\x41"
REGEX_SQL = "SELECT * FROM t WHERE code RLIKE '^\\\\d+$'"


def _parse(code: str) -> ast.Module:
    """Parse the generated code as it is written to disk."""
    return ast.parse(normalize_content(code))


def _calls(tree: ast.Module, method: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == method
    ]


def _first_arg_values(tree: ast.Module, method: str) -> list[object]:
    return [ast.literal_eval(call.args[0]) for call in _calls(tree, method)]


def _options(tree: ast.Module) -> dict[object, object]:
    return {
        ast.literal_eval(call.args[0]): ast.literal_eval(call.args[1])
        for call in _calls(tree, "option")
        if isinstance(call.args[1], ast.Constant)
    }


def _docstring(tree: ast.Module) -> str | None:
    function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef))
    return ast.get_docstring(function, clean=False)


def _string_constants(tree: ast.Module) -> list[str]:
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]


def _flowgroup_context(tmp_path: Path) -> dict:
    return {
        "spec_dir": tmp_path,
        "output_dir": tmp_path / "generated",
        "flowgroup": FlowGroup(pipeline="p", flowgroup="fg", actions=[]),
        "preset_config": {},
        "project_config": None,
    }


def _cloudfiles(tmp_path: Path, description: str | None = None, **source) -> str:
    action = Action(
        name="load_files",
        type=ActionType.LOAD,
        target="v_files",
        description=description,
        source={"type": "cloudfiles", "path": "/data/in", "format": "csv", **source},
    )
    return CloudFilesLoadGenerator().generate(action, {"spec_dir": tmp_path})


@pytest.mark.unit
class TestCloudFiles:
    def test_path_with_quote_and_backslash_escapes(self, tmp_path):
        path = f'{WINDOWS_PATH} "quoted"'

        tree = _parse(_cloudfiles(tmp_path, path=path))

        assert _first_arg_values(tree, "load") == [path]
        assert _docstring(tree) == f"Load data from csv files at {path}"

    def test_schema_hint_column_with_backslash(self, tmp_path):
        (tmp_path / "hints.ddl").write_text(
            "id BIGINT, `C:\\new` STRING", encoding="utf-8"
        )

        tree = _parse(
            _cloudfiles(tmp_path, options={"cloudFiles.schemaHints": "hints.ddl"})
        )

        (hints,) = [s for s in _string_constants(tree) if "BIGINT" in s]
        assert hints.strip().replace("\n", " ") == "id BIGINT,     `C:\\new` STRING"

    def test_schema_hint_columns_ending_in_quotes_keep_their_bytes(self, tmp_path):
        """A hint line is followed by ``,`` or a newline, never by the closing quotes."""
        hints = 'id BIGINT, note STRING COMMENT "free text", other STRING COMMENT "two"'

        code = _cloudfiles(tmp_path, options={"cloudFiles.schemaHints": hints})

        assert '\n    note STRING COMMENT "free text",\n' in code
        assert '\n    other STRING COMMENT "two"\n""".strip()' in code
        (value,) = [s for s in _string_constants(_parse(code)) if "BIGINT" in s]
        assert value.strip().replace("\n", " ") == (
            'id BIGINT,     note STRING COMMENT "free text",     '
            'other STRING COMMENT "two"'
        )

    @pytest.mark.parametrize(
        "value",
        ["\n", "a\nb", "a\u2028b", 'tab\tquote" back\\slash', "\U0001f600"],
        ids=["newline", "embedded-newline", "u2028", "mixed", "emoji"],
    )
    def test_option_value_round_trips(self, tmp_path, value):
        tree = _parse(_cloudfiles(tmp_path, options={"lineSep": value}))

        assert _options(tree)["lineSep"] == value

    def test_option_key_is_a_string_literal(self, tmp_path):
        tree = _parse(_cloudfiles(tmp_path, options={'odd"key': "v"}))

        assert _options(tree)['odd"key'] == "v"

    @pytest.mark.parametrize(
        "description",
        ["C:\\Users\\me", 'ends with "', 'say """hi"""', "two\nlines"],
    )
    def test_description_docstring_round_trips(self, tmp_path, description):
        tree = _parse(_cloudfiles(tmp_path, description=description))

        assert _docstring(tree) == description


@pytest.mark.unit
class TestSql:
    def _generate(self, sql: str, description: str | None = None) -> ast.Module:
        action = Action(
            name="load_sql",
            type=ActionType.LOAD,
            target="v_sql",
            description=description,
            source={"type": "sql", "sql": sql},
        )
        return _parse(SQLLoadGenerator().generate(action, {}))

    @pytest.mark.parametrize(
        "sql",
        [
            REGEX_SQL,
            'SELECT "x"',
            'SELECT """odd""" AS q',
            "SELECT 1 -- trailing backslash \\",
            "SELECT '\\N{BULLET}' AS b",
            "SELECT 'a\u2028b' AS s",
        ],
    )
    def test_sql_reaches_spark_exactly(self, sql):
        assert _first_arg_values(self._generate(sql), "sql") == [sql]

    def test_sql_from_file_with_regex(self, tmp_path):
        (tmp_path / "q.sql").write_text(REGEX_SQL + "\n", encoding="utf-8")
        action = Action(
            name="load_sql",
            type=ActionType.LOAD,
            target="v_sql",
            source={"type": "sql", "sql_path": "q.sql"},
        )

        code = SQLLoadGenerator().generate(action, {"project_root": tmp_path})

        assert _first_arg_values(_parse(code), "sql") == [REGEX_SQL]

    def test_description_docstring_round_trips(self):
        tree = self._generate("SELECT 1", description='C:\\Users\\me "x"')

        assert _docstring(tree) == 'C:\\Users\\me "x"'


@pytest.mark.unit
class TestJdbc:
    def _generate(self, **source) -> ast.Module:
        action = Action(
            name="load_jdbc",
            type=ActionType.LOAD,
            target="v_jdbc",
            description="JDBC \\\\server\\share",
            source={
                "type": "jdbc",
                "url": "jdbc:sqlserver://h;database=d\nx",
                "user": 'us"er',
                "password": "p\\w",
                "driver": "com.Driver",
                **source,
            },
        )
        return _parse(JDBCLoadGenerator().generate(action, {}))

    def test_query_ending_in_quote(self):
        query = 'SELECT * FROM t WHERE name = "x"'

        tree = self._generate(query=query)

        assert _options(tree)["query"] == query

    def test_query_with_regex_and_triple_quotes(self):
        query = REGEX_SQL + ' AND note = """'

        assert _options(self._generate(query=query))["query"] == query

    def test_connection_options_and_table_round_trip(self):
        tree = self._generate(table='dbo."weird"\ntable')

        options = _options(tree)
        assert options["url"] == "jdbc:sqlserver://h;database=d\nx"
        assert options["user"] == 'us"er'
        assert options["password"] == "p\\w"
        assert options["dbtable"] == 'dbo."weird"\ntable'
        assert _docstring(tree) == "JDBC \\\\server\\share"


@pytest.mark.unit
class TestDelta:
    def _generate(self, **source) -> ast.Module:
        action = Action(
            name="load_delta",
            type=ActionType.LOAD,
            target="v_delta",
            source={
                "type": "delta",
                "catalog": "main",
                "schema": "bronze",
                "table": "orders",
                **source,
            },
        )
        return _parse(DeltaLoadGenerator().generate(action, {}))

    def test_where_clause_with_double_quotes(self):
        tree = self._generate(where_clause=['status = "active"', "c RLIKE '\\\\d'"])

        assert _first_arg_values(tree, "where") == [
            'status = "active"',
            "c RLIKE '\\\\d'",
        ]

    def test_table_ref_and_docstring(self):
        tree = self._generate()

        assert _first_arg_values(tree, "table") == ["main.bronze.orders"]
        assert _docstring(tree) == "Delta source: main.bronze.orders"

    @pytest.mark.parametrize("read_mode", ["stream", "batch"])
    def test_option_value_with_newline(self, read_mode):
        tree = self._generate(readMode=read_mode, options={"note": 'a\n"b"\\'})

        assert _options(tree)["note"] == 'a\n"b"\\'

    def test_select_columns_with_emoji_and_quote(self):
        columns = ["id", "\U0001f600", 'we"ird']

        tree = self._generate(select_columns=columns)

        assert _first_arg_values(tree, "select") == [columns]


@pytest.mark.unit
class TestPythonLoad:
    _MODULE = "def load_data(spark, parameters):\n    return None\n"

    def _generate(self, tmp_path: Path, parameters: object) -> str:
        (tmp_path / "loaders").mkdir(exist_ok=True)
        (tmp_path / "loaders" / "my_loader.py").write_text(self._MODULE)
        action = Action(
            name="load_py",
            type=ActionType.LOAD,
            target="v_py",
            description="Python \\d loader",
            source={
                "type": "python",
                "module_path": "loaders/my_loader.py",
                "function_name": "load_data",
                "parameters": parameters,
            },
        )
        return PythonLoadGenerator().generate(action, _flowgroup_context(tmp_path))

    def _parameters(self, code: str) -> object:
        (assignment,) = [
            node
            for node in ast.walk(_parse(code))
            if isinstance(node, ast.Assign)
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "parameters"
        ]
        return ast.literal_eval(assignment.value)

    def test_json_constants_become_python_constants(self, tmp_path):
        code = self._generate(tmp_path, {"flag": True, "nothing": None})

        assert 'parameters = {"flag": True, "nothing": None}' in code
        assert self._parameters(code) == {"flag": True, "nothing": None}

    def test_emoji_and_escapes_round_trip(self, tmp_path):
        parameters = {"face": "\U0001f600", "path": WINDOWS_PATH, "nl": "a\nb"}

        code = self._generate(tmp_path, parameters)

        assert self._parameters(code) == parameters
        assert _docstring(_parse(code)) == "Python \\d loader"


@pytest.mark.unit
class TestKafka:
    def test_option_value_with_newline_and_quotes(self):
        jaas = 'PlainLoginModule required\nusername="u" password="p\\q";'
        action = Action(
            name="load_kafka",
            type=ActionType.LOAD,
            target="v_kafka",
            readMode="stream",
            description='Kafka "events"',
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "events",
                "options": {"kafka.sasl.jaas.config": jaas},
            },
        )

        tree = _parse(KafkaLoadGenerator().generate(action, {}))

        assert _options(tree)["kafka.sasl.jaas.config"] == jaas
        assert _docstring(tree) == 'Kafka "events"'


@pytest.mark.unit
class TestCustomDataSource:
    @pytest.mark.parametrize("read_mode", ["stream", "batch"])
    def test_option_value_and_description_round_trip(self, tmp_path, read_mode):
        (tmp_path / "src.py").write_text(
            "class MySource(DataSource):\n"
            "    @classmethod\n"
            "    def name(cls):\n"
            '        return "my_source"\n'
        )
        action = Action(
            name="load_custom",
            type=ActionType.LOAD,
            target="v_custom",
            readMode=read_mode,
            description="Custom C:\\Users\\me",
            source={
                "type": "custom_datasource",
                "module_path": "src.py",
                "custom_datasource_class": "MySource",
                "options": {"auth": 'tok"en\n\\x41'},
            },
        )

        code = CustomDataSourceLoadGenerator().generate(
            action, _flowgroup_context(tmp_path)
        )
        tree = _parse(code)

        assert _options(tree)["auth"] == 'tok"en\n\\x41'
        assert _docstring(tree) == "Custom C:\\Users\\me"
        assert _first_arg_values(tree, "format") == ["my_source"]


_ODD_METADATA_COLUMNS = ("it's", "C:\\new\\x41")

_LOAD_GENERATORS = {
    "cloudfiles": CloudFilesLoadGenerator,
    "delta": DeltaLoadGenerator,
    "jdbc": JDBCLoadGenerator,
    "kafka": KafkaLoadGenerator,
    "custom_datasource": CustomDataSourceLoadGenerator,
    "python": PythonLoadGenerator,
    "sql": SQLLoadGenerator,
}

_LOAD_SOURCES = {
    "cloudfiles": {"type": "cloudfiles", "path": "/data/in", "format": "csv"},
    "delta": {"type": "delta", "catalog": "c", "schema": "s", "table": "t"},
    "jdbc": {
        "type": "jdbc",
        "url": "jdbc:x",
        "user": "u",
        "password": "p",
        "driver": "d",
        "table": "t",
    },
    "kafka": {
        "type": "kafka",
        "bootstrap_servers": "localhost:9092",
        "subscribe": "events",
    },
    "custom_datasource": {
        "type": "custom_datasource",
        "module_path": "src.py",
        "custom_datasource_class": "MySource",
    },
    "python": {
        "type": "python",
        "module_path": "loaders/my_loader.py",
        "function_name": "load_data",
    },
    "sql": {"type": "sql", "sql": "SELECT 1"},
}


@pytest.mark.unit
@pytest.mark.parametrize("source_type", sorted(_LOAD_GENERATORS))
def test_operational_metadata_column_names_round_trip(tmp_path, source_type):
    """A metadata column name with a quote or backslash reaches withColumn intact."""
    (tmp_path / "src.py").write_text(
        "class MySource(DataSource):\n"
        "    @classmethod\n"
        "    def name(cls):\n"
        '        return "my_source"\n'
    )
    (tmp_path / "loaders").mkdir()
    (tmp_path / "loaders" / "my_loader.py").write_text(
        "def load_data(spark, parameters):\n    return None\n"
    )
    context = _flowgroup_context(tmp_path)
    context["project_config"] = ProjectConfig(
        name="p",
        operational_metadata=ProjectOperationalMetadataConfig(
            columns={
                name: MetadataColumnConfig(
                    expression="F.current_timestamp()", applies_to=["view"]
                )
                for name in _ODD_METADATA_COLUMNS
            }
        ),
    )
    action = Action(
        name="load_with_metadata",
        type=ActionType.LOAD,
        target="v_with_metadata",
        readMode="stream",
        source=_LOAD_SOURCES[source_type],
        operational_metadata=list(_ODD_METADATA_COLUMNS),
    )

    code = _LOAD_GENERATORS[source_type]().generate(action, context)

    assert sorted(_first_arg_values(_parse(code), "withColumn")) == sorted(
        _ODD_METADATA_COLUMNS
    )


@pytest.mark.unit
class TestRawCharactersStayRaw:
    """Tabs and harmless control characters keep their pre-#296 bytes.

    The old templates emitted these raw (or, at ``tojson`` sites, as json
    escapes); only characters that broke the code are escaped now.
    """

    def test_cloudfiles_option_path_and_description(self, tmp_path):
        code = _cloudfiles(
            tmp_path,
            description="Tab\there\x01",
            path="/data/in\tx",
            options={"sep": "\t", "k\x07": "v\x1f"},
        )

        assert '"""Tab\there\x01"""' in code
        assert '.load("/data/in\tx")' in code
        assert '.option("sep", "\t")' in code
        assert '.option("k\x07", "v\x1f")' in code

    def test_sql_query(self):
        action = Action(
            name="load_sql",
            type=ActionType.LOAD,
            target="v_sql",
            source={"type": "sql", "sql": "SELECT\t1 AS a -- \x01"},
        )

        code = SQLLoadGenerator().generate(action, {})

        assert 'spark.sql("""SELECT\t1 AS a -- \x01""")' in code

    def test_delta_where_and_select(self):
        action = Action(
            name="load_delta",
            type=ActionType.LOAD,
            target="v_delta",
            source={
                "type": "delta",
                "table": "t",
                "where_clause": ["a =\t1"],
                "select_columns": ["c\t1"],
            },
        )

        code = DeltaLoadGenerator().generate(action, {})

        assert 'df.where("a =\t1")' in code
        assert 'df.select(["c\\t1"])' in code

    def test_python_load_parameters_keep_the_tojson_spelling(self, tmp_path):
        (tmp_path / "loaders").mkdir()
        (tmp_path / "loaders" / "my_loader.py").write_text(
            "def load_data(spark, parameters):\n    return None\n"
        )
        action = Action(
            name="load_py",
            type=ActionType.LOAD,
            target="v_py",
            source={
                "type": "python",
                "module_path": "loaders/my_loader.py",
                "function_name": "load_data",
                "parameters": {"sep": "\t", "name": "caf\xe9"},
            },
        )

        code = PythonLoadGenerator().generate(action, _flowgroup_context(tmp_path))

        assert 'parameters = {"sep": "\\t", "name": "caf\\u00e9"}' in code
