"""Tests for custom data source load generator."""

from pathlib import Path

import pytest

from lhp.generators.load.custom_datasource import CustomDataSourceLoadGenerator
from lhp.models import Action, ActionType, FlowGroup


class TestCustomDataSourceGenerator:
    def test_basic_generation_no_parameters(self, tmp_path):
        custom_source_file = tmp_path / "test_source.py"
        custom_source_file.write_text("""
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

class TestDataSource(DataSource):
    @classmethod
    def name(cls):
        return "test_datasource"
    
    def schema(self):
        return "id int, name string"
    
    def reader(self, schema: StructType):
        return TestDataSourceReader(schema, self.options)

class TestDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
    
    def read(self, partition):
        yield (1, "test")

# Register the data source
spark.dataSource.register(TestDataSource)
""")

        action = Action(
            name="test_load",
            type=ActionType.LOAD,
            target="v_test_data",
            readMode="stream",
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "TestDataSource",
        }

        generator = CustomDataSourceLoadGenerator()

        context = {
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            "preset_config": {},
            "project_config": None,
        }

        result = generator.generate(action, context)

        # Verify format is correct (uses name() method, not class name)
        assert '.format("test_datasource")' in result
        # Verify no .option() calls since no parameters
        assert ".option(" not in result
        # Verify stream mode
        assert "spark.readStream" in result
        # Verify target view name
        assert "def v_test_data():" in result

        # Verify the new copy-and-import shape: imports + pre-pipeline statement
        imports = generator.imports
        assert "import custom_python_functions" in imports
        assert "from pyspark import cloudpickle as _lhp_cloudpickle" in imports
        assert (
            "from custom_python_functions.test_source import TestDataSource" in imports
        )
        assert generator.get_pre_pipeline_statements() == [
            "_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)"
        ]

    def test_generation_with_parameters(self, tmp_path):
        custom_source_file = tmp_path / "api_source.py"
        custom_source_file.write_text("""
class APIDataSource(DataSource):
    @classmethod
    def name(cls):
        return "api_datasource"
""")

        action = Action(
            name="test_api_load",
            type=ActionType.LOAD,
            target="v_api_data",
            readMode="batch",
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "APIDataSource",
            "options": {
                "apiKey": "test-key-123",
                "endpoint": "https://api.example.com",
                "timeout": 30,
                "retries": 3,
                "enabled": True,
            },
        }

        generator = CustomDataSourceLoadGenerator()

        context = {
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            "preset_config": {},
            "project_config": None,
        }

        result = generator.generate(action, context)

        # Verify format is correct (uses name() method, not class name)
        assert '.format("api_datasource")' in result
        # Verify options are present
        assert '.option("apiKey", "test-key-123")' in result
        assert '.option("endpoint", "https://api.example.com")' in result
        assert '.option("timeout", 30)' in result  # Number without quotes
        assert '.option("retries", 3)' in result  # Number without quotes
        assert '.option("enabled", True)' in result  # Boolean
        # Verify batch mode
        assert "spark.read" in result
        assert "spark.readStream" not in result

    def test_missing_module_path_error(self):
        action = Action(name="test_load", type=ActionType.LOAD, target="v_test_data")
        action.source = {
            "type": "custom_datasource",
            "custom_datasource_class": "TestDataSource",
        }

        generator = CustomDataSourceLoadGenerator()
        context = {"spec_dir": Path.cwd()}

        with pytest.raises(Exception) as exc_info:
            generator.generate(action, context)

        assert "module_path" in str(exc_info.value)

    def test_missing_custom_datasource_class_error(self, tmp_path):
        custom_source_file = tmp_path / "test_source.py"
        custom_source_file.write_text("# test file")

        action = Action(name="test_load", type=ActionType.LOAD, target="v_test_data")
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
        }

        generator = CustomDataSourceLoadGenerator()
        context = {"spec_dir": tmp_path}

        with pytest.raises(Exception) as exc_info:
            generator.generate(action, context)

        assert "custom_datasource_class" in str(exc_info.value)

    def test_missing_file_error(self, tmp_path):
        action = Action(name="test_load", type=ActionType.LOAD, target="v_test_data")
        action.source = {
            "type": "custom_datasource",
            "module_path": "nonexistent_file.py",
            "custom_datasource_class": "TestDataSource",
        }

        generator = CustomDataSourceLoadGenerator()
        context = {"spec_dir": tmp_path}

        with pytest.raises(FileNotFoundError) as exc_info:
            generator.generate(action, context)

        assert "Custom data source file not found" in str(exc_info.value)

    def test_values_with_quotes_escaped(self, tmp_path):
        custom_source_file = tmp_path / "test_source.py"
        custom_source_file.write_text("""
class TestDataSource(DataSource):
    @classmethod
    def name(cls):
        return "test_datasource"
""")

        action = Action(
            name="test_quotes",
            type=ActionType.LOAD,
            target="v_test_quotes",
            readMode="stream",
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "TestDataSource",
            "options": {
                # Value with embedded quotes
                "authConfig": 'token="secret123"',
            },
        }

        generator = CustomDataSourceLoadGenerator()
        context = {
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            "preset_config": {},
            "project_config": None,
        }

        result = generator.generate(action, context)

        # Check that quotes are escaped
        assert '\\"secret123\\"' in result or 'token=\\"secret123\\"' in result

        # Verify it's valid Python by compiling
        compile(result, "<string>", "exec")

    def test_values_with_backslashes_escaped(self, tmp_path):
        custom_source_file = tmp_path / "test_source.py"
        custom_source_file.write_text("""
class TestDataSource(DataSource):
    @classmethod
    def name(cls):
        return "test_datasource"
""")

        action = Action(
            name="test_backslashes",
            type=ActionType.LOAD,
            target="v_test_backslashes",
            readMode="batch",
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "TestDataSource",
            "options": {
                # Value with backslashes (Windows path)
                "dataPath": r"C:\data\files",
            },
        }

        generator = CustomDataSourceLoadGenerator()
        context = {
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            "preset_config": {},
            "project_config": None,
        }

        result = generator.generate(action, context)

        # Check that backslashes are escaped
        assert "\\\\data\\\\files" in result or r"C:\\data\\files" in result

        # Verify no SyntaxWarning
        import warnings

        warnings.simplefilter("error", SyntaxWarning)
        compile(result, "<string>", "exec")
        warnings.simplefilter("default", SyntaxWarning)

    def test_json_config_with_quotes(self, tmp_path):
        custom_source_file = tmp_path / "api_source.py"
        custom_source_file.write_text("""
class APIDataSource(DataSource):
    @classmethod
    def name(cls):
        return "api_datasource"
""")

        action = Action(
            name="test_json",
            type=ActionType.LOAD,
            target="v_api_json",
            readMode="stream",
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "APIDataSource",
            "options": {
                # JSON-like configuration
                "config": '{"key": "value", "nested": {"field": "data"}}',
            },
        }

        generator = CustomDataSourceLoadGenerator()
        context = {
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            "preset_config": {},
            "project_config": None,
        }

        result = generator.generate(action, context)

        # Verify valid Python (quotes should be escaped)
        compile(result, "<string>", "exec")


class TestCustomDataSourcePEP236:
    """``from __future__`` lives in the user's file, not the assembled module.

    Under the new copy-and-import pattern, the user's PySpark ``DataSource``
    file is copied verbatim into a sibling ``custom_python_functions/``
    directory. The pipeline file imports the class by name; it never inlines
    the user source. Therefore the user's ``from __future__ import …`` lines
    stay at the top of the user's *own* file (the natural PEP 236 location)
    and never need cross-file hoisting.

    These tests verify that invariant.
    """

    def _generate_with_output_dir(self, tmp_path, custom_source_text):
        from lhp.models import FlowGroup

        custom_source_file = tmp_path / "future_source.py"
        custom_source_file.write_text(custom_source_text)

        output_dir = tmp_path / "generated"
        output_dir.mkdir()

        action = Action(
            name="load_future",
            type=ActionType.LOAD,
            target="v_future",
            readMode="batch",
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "FutureDataSource",
        }

        generator = CustomDataSourceLoadGenerator()
        flowgroup = FlowGroup(pipeline="p_test", flowgroup="fg_test")
        context = {
            "spec_dir": tmp_path,
            "output_dir": output_dir,
            "flowgroup": flowgroup,
            "preset_config": {},
            "project_config": None,
        }
        generated = generator.generate(action, context)
        return generated, output_dir, generator

    def test_future_import_lives_in_copied_user_file(self, tmp_path):
        """``from __future__ import annotations`` is preserved in the copy."""
        source_text = """from __future__ import annotations

from pyspark.sql.datasource import DataSource

class FutureDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "future_format"
"""
        generated, output_dir, _ = self._generate_with_output_dir(tmp_path, source_text)

        # The generated pipeline file does NOT contain the user's class body.
        assert "class FutureDataSource" not in generated
        # The user's __future__ import lives in the copied file, not in the
        # generated pipeline file.
        copied = (
            output_dir / "custom_python_functions" / "future_source.py"
        ).read_text()
        assert "from __future__ import annotations" in copied

    def test_no_future_import_unchanged(self, tmp_path):
        """No ``__future__`` import → file copied verbatim, generated file unaffected."""
        source_text = """from pyspark.sql.datasource import DataSource

class FutureDataSource(DataSource):
    @classmethod
    def name(cls):
        return "no_future"
"""
        generated, output_dir, generator = self._generate_with_output_dir(
            tmp_path, source_text
        )

        # The generator emits the import for the user's class via add_import
        # (collected by the assembler into the file header), not by inlining
        # the class body.
        assert (
            "from custom_python_functions.future_source import FutureDataSource"
            in generator.imports
        )
        assert "class FutureDataSource" not in generated

        copied = (
            output_dir / "custom_python_functions" / "future_source.py"
        ).read_text()
        assert "from __future__" not in copied


@pytest.mark.unit
class TestCustomDataSourceGoldenOutput:
    def test_custom_datasource_golden(self, golden, tmp_path):
        custom_source_file = tmp_path / "test_source.py"
        custom_source_file.write_text(
            "from pyspark.sql.datasource import DataSource, DataSourceReader\n"
            "from pyspark.sql.types import StructType\n"
            "\n"
            "class TestDataSource(DataSource):\n"
            "    @classmethod\n"
            "    def name(cls):\n"
            '        return "test_datasource"\n'
            "\n"
            "    def schema(self):\n"
            '        return "id int, name string"\n'
            "\n"
            "    def reader(self, schema: StructType):\n"
            "        return TestDataSourceReader(schema, self.options)\n"
            "\n"
            "class TestDataSourceReader(DataSourceReader):\n"
            "    def __init__(self, schema, options):\n"
            "        self.schema = schema\n"
            "        self.options = options\n"
            "\n"
            "    def read(self, partition):\n"
            '        yield (1, "test")\n'
            "\n"
            "spark.dataSource.register(TestDataSource)\n"
        )

        action = Action(
            name="test_load",
            type=ActionType.LOAD,
            target="v_test_data",
            readMode="stream",
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "TestDataSource",
        }

        generator = CustomDataSourceLoadGenerator()
        context = {
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            "preset_config": {},
            "project_config": None,
        }

        code = generator.generate(action, context)
        golden(code, "load_custom_datasource")


@pytest.mark.unit
class TestCustomDataSourceCopyAndImportInvariants:
    """Cross-action invariants: boilerplate dedup across multiple custom actions and class-name collision detection."""

    @staticmethod
    def _write_source(path, class_name, format_name):
        path.write_text(f"""from pyspark.sql.datasource import DataSource

class {class_name}(DataSource):
    @classmethod
    def name(cls):
        return "{format_name}"
""")

    def test_multiple_custom_actions_dedupe_boilerplate(self, tmp_path):
        """Two custom_datasources in one flowgroup → boilerplate appears once.

        ``import custom_python_functions``, the cloudpickle alias import, and
        the ``register_pickle_by_value`` statement each must appear exactly
        once even though two actions independently call ``add_import`` /
        ``add_pre_pipeline_statement``. Dedup is provided by ``ImportManager``
        (set-based) and the assembler's pre-pipeline statements set.
        """
        from lhp.core.codegen.coordinator import CodeGenerationService
        from lhp.models import FlowGroup

        # Two distinct user files exporting two distinct classes.
        src_a = tmp_path / "source_a.py"
        src_b = tmp_path / "source_b.py"
        self._write_source(src_a, "ClassA", "format_a")
        self._write_source(src_b, "ClassB", "format_b")

        flowgroup = FlowGroup(
            pipeline="p_test",
            flowgroup="fg_test",
            actions=[
                Action(
                    name="load_a",
                    type=ActionType.LOAD,
                    target="v_a",
                    readMode="batch",
                    source={
                        "type": "custom_datasource",
                        "module_path": "source_a.py",
                        "custom_datasource_class": "ClassA",
                    },
                ),
                Action(
                    name="load_b",
                    type=ActionType.LOAD,
                    target="v_b",
                    readMode="batch",
                    source={
                        "type": "custom_datasource",
                        "module_path": "source_b.py",
                        "custom_datasource_class": "ClassB",
                    },
                ),
            ],
        )

        # Run two independent generator instances and merge their outputs the
        # way the assembler does.
        all_imports: set = set()
        all_pre_stmts: set = set()
        for action in flowgroup.actions:
            gen = CustomDataSourceLoadGenerator()
            gen.generate(
                action,
                {
                    "spec_dir": tmp_path,
                    "flowgroup": flowgroup,
                    "preset_config": {},
                    "project_config": None,
                },
            )
            all_imports.update(gen.imports)
            all_pre_stmts.update(gen.get_pre_pipeline_statements())

        # ImportManager dedupe: the package import and cloudpickle alias
        # import each appear exactly once across the merged set.
        assert (
            sum(1 for imp in all_imports if imp == "import custom_python_functions")
            == 1
        )
        assert (
            sum(
                1
                for imp in all_imports
                if imp == "from pyspark import cloudpickle as _lhp_cloudpickle"
            )
            == 1
        )

        # Two distinct from-imports (one per class).
        from_imports = [
            imp
            for imp in all_imports
            if imp.startswith("from custom_python_functions.")
        ]
        assert len(from_imports) == 2
        assert "from custom_python_functions.source_a import ClassA" in all_imports
        assert "from custom_python_functions.source_b import ClassB" in all_imports

        # Pre-pipeline registration appears exactly once.
        assert all_pre_stmts == {
            "_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)"
        }

    def test_class_name_collision_raises(self, tmp_path):
        """Two actions exporting the same class name from different files → LHPValidationError."""
        from lhp.errors import LHPValidationError

        # Two files both exporting a class named ``Conflict``.
        src_a = tmp_path / "alpha.py"
        src_b = tmp_path / "beta.py"
        self._write_source(src_a, "Conflict", "alpha_format")
        self._write_source(src_b, "Conflict", "beta_format")

        action_a = Action(
            name="load_alpha",
            type=ActionType.LOAD,
            target="v_alpha",
            readMode="batch",
            source={
                "type": "custom_datasource",
                "module_path": "alpha.py",
                "custom_datasource_class": "Conflict",
            },
        )
        action_b = Action(
            name="load_beta",
            type=ActionType.LOAD,
            target="v_beta",
            readMode="batch",
            source={
                "type": "custom_datasource",
                "module_path": "beta.py",
                "custom_datasource_class": "Conflict",
            },
        )

        # Both actions share the same generator's ImportManager. The first
        # add_import succeeds; the second raises because ``Conflict`` would
        # bind to a different module path.
        generator = CustomDataSourceLoadGenerator()
        ctx = {
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            "preset_config": {},
            "project_config": None,
        }
        generator.generate(action_a, ctx)

        with pytest.raises(LHPValidationError) as excinfo:
            generator.generate(action_b, ctx)
        assert "Conflict" in str(excinfo.value)
        assert "collision" in str(excinfo.value).lower()
