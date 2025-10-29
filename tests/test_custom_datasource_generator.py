"""Tests for custom data source load generator."""

import pytest
from pathlib import Path
from lhp.generators.load.custom_datasource import CustomDataSourceLoadGenerator
from lhp.models.config import Action, ActionType


class TestCustomDataSourceGenerator:
    """Test the custom data source load generator."""

    def test_basic_generation_no_parameters(self, tmp_path):
        """Test basic generation with no parameters."""
        # Create a simple custom data source file
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

        # Create action
        action = Action(
            name="test_load",
            type=ActionType.LOAD,
            target="v_test_data",
            readMode="stream"
        )
        # Set source configuration (new structure)
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "TestDataSource"
        }

        # Create generator
        generator = CustomDataSourceLoadGenerator()
        
        # Create context
        context = {
            "spec_dir": tmp_path,
            "flowgroup": None,
            "preset_config": {},
            "project_config": None
        }

        # Generate code
        result = generator.generate(action, context)

        # Verify format is correct (uses name() method, not class name)
        assert '.format("test_datasource")' in result
        # Verify no .option() calls since no parameters
        assert '.option(' not in result
        # Verify stream mode
        assert 'spark.readStream' in result
        # Verify target view name
        assert 'def v_test_data():' in result
        # Verify custom source code is stored
        assert generator.custom_source_code is not None
        assert "TestDataSource" in generator.custom_source_code

    def test_generation_with_parameters(self, tmp_path):
        """Test generation with parameters."""
        # Create a simple custom data source file
        custom_source_file = tmp_path / "api_source.py"
        custom_source_file.write_text("""
class APIDataSource(DataSource):
    @classmethod
    def name(cls):
        return "api_datasource"
""")

        # Create action with parameters
        action = Action(
            name="test_api_load",
            type=ActionType.LOAD,
            target="v_api_data",
            readMode="batch"
        )
        # Set source configuration with options (new structure)
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "APIDataSource",
            "options": {
                "apiKey": "test-key-123",
                "endpoint": "https://api.example.com",
                "timeout": 30,
                "retries": 3,
                "enabled": True
            }
        }

        # Create generator
        generator = CustomDataSourceLoadGenerator()
        
        # Create context
        context = {
            "spec_dir": tmp_path,
            "flowgroup": None,
            "preset_config": {},
            "project_config": None
        }

        # Generate code
        result = generator.generate(action, context)

        # Verify format is correct (uses name() method, not class name)
        assert '.format("api_datasource")' in result
        # Verify options are present
        assert '.option("apiKey", "test-key-123")' in result
        assert '.option("endpoint", "https://api.example.com")' in result
        assert '.option("timeout", 30)' in result  # Number without quotes
        assert '.option("retries", 3)' in result   # Number without quotes
        assert '.option("enabled", True)' in result  # Boolean
        # Verify batch mode
        assert 'spark.read' in result
        assert 'spark.readStream' not in result

    def test_missing_module_path_error(self):
        """Test error when module_path is missing."""
        action = Action(
            name="test_load",
            type=ActionType.LOAD,
            target="v_test_data"
        )
        # Set source with missing module_path
        action.source = {
            "type": "custom_datasource",
            "custom_datasource_class": "TestDataSource"
        }

        generator = CustomDataSourceLoadGenerator()
        context = {"spec_dir": Path.cwd()}

        with pytest.raises(Exception) as exc_info:
            generator.generate(action, context)
        
        assert "module_path" in str(exc_info.value)

    def test_missing_custom_datasource_class_error(self, tmp_path):
        """Test error when custom_datasource_class is missing."""
        custom_source_file = tmp_path / "test_source.py"
        custom_source_file.write_text("# test file")

        action = Action(
            name="test_load",
            type=ActionType.LOAD,
            target="v_test_data"
        )
        # Set source with missing custom_datasource_class
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path))
        }

        generator = CustomDataSourceLoadGenerator()
        context = {"spec_dir": tmp_path}

        with pytest.raises(Exception) as exc_info:
            generator.generate(action, context)
        
        assert "custom_datasource_class" in str(exc_info.value)

    def test_missing_file_error(self, tmp_path):
        """Test error when module file doesn't exist."""
        action = Action(
            name="test_load",
            type=ActionType.LOAD,
            target="v_test_data"
        )
        # Set source with nonexistent file
        action.source = {
            "type": "custom_datasource",
            "module_path": "nonexistent_file.py",
            "custom_datasource_class": "TestDataSource"
        }

        generator = CustomDataSourceLoadGenerator()
        context = {"spec_dir": tmp_path}

        with pytest.raises(FileNotFoundError) as exc_info:
            generator.generate(action, context)
        
        assert "Custom data source file not found" in str(exc_info.value)

    def test_values_with_quotes_escaped(self, tmp_path):
        """Test that option values containing quotes are properly escaped."""
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
            readMode="stream"
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "TestDataSource",
            "options": {
                # Value with embedded quotes
                "authConfig": 'token="secret123"',
            }
        }

        generator = CustomDataSourceLoadGenerator()
        context = {
            "spec_dir": tmp_path,
            "flowgroup": None,
            "preset_config": {},
            "project_config": None
        }

        result = generator.generate(action, context)

        # Check that quotes are escaped
        assert '\\"secret123\\"' in result or 'token=\\"secret123\\"' in result
        
        # Verify it's valid Python by compiling
        try:
            compile(result, '<string>', 'exec')
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code with quotes is not valid Python syntax: {e}")

    def test_values_with_backslashes_escaped(self, tmp_path):
        """Test that option values containing backslashes are properly escaped."""
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
            readMode="batch"
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "TestDataSource",
            "options": {
                # Value with backslashes (Windows path)
                "dataPath": r"C:\data\files",
            }
        }

        generator = CustomDataSourceLoadGenerator()
        context = {
            "spec_dir": tmp_path,
            "flowgroup": None,
            "preset_config": {},
            "project_config": None
        }

        result = generator.generate(action, context)

        # Check that backslashes are escaped
        assert '\\\\data\\\\files' in result or r'C:\\data\\files' in result
        
        # Verify no SyntaxWarning
        import warnings
        warnings.simplefilter('error', SyntaxWarning)
        try:
            compile(result, '<string>', 'exec')
            assert True
        except SyntaxWarning as e:
            pytest.fail(f"Generated code has invalid escape sequences: {e}")
        except SyntaxError as e:
            pytest.fail(f"Generated code is not valid Python syntax: {e}")
        finally:
            warnings.simplefilter('default', SyntaxWarning)

    def test_json_config_with_quotes(self, tmp_path):
        """Test JSON configuration strings with quotes."""
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
            readMode="stream"
        )
        action.source = {
            "type": "custom_datasource",
            "module_path": str(custom_source_file.relative_to(tmp_path)),
            "custom_datasource_class": "APIDataSource",
            "options": {
                # JSON-like configuration
                "config": '{"key": "value", "nested": {"field": "data"}}',
            }
        }

        generator = CustomDataSourceLoadGenerator()
        context = {
            "spec_dir": tmp_path,
            "flowgroup": None,
            "preset_config": {},
            "project_config": None
        }

        result = generator.generate(action, context)

        # Verify valid Python (quotes should be escaped)
        try:
            compile(result, '<string>', 'exec')
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code with JSON config is not valid Python syntax: {e}") 