"""Tests for substitution support in Python Load generator."""

import pytest
from lhp.models.config import Action, ActionType
from lhp.generators.load.python import PythonLoadGenerator
from lhp.utils.substitution import EnhancedSubstitutionManager


class TestPythonLoadSubstitution:
    """Test substitution in Python Load actions."""

    def test_python_load_parameters_basic_substitution(self):
        """Test basic {token} substitution in parameters."""
        # Create substitution manager with test values
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "catalog": "test_catalog",
            "schema": "test_schema"
        })
        
        # Create action with parameters using {token} syntax
        action = Action(
            name="load_custom_data",
            type=ActionType.LOAD,
            target="v_custom_data",
            source={
                "type": "python",
                "module_path": "py_functions/custom_loader.py",
                "function_name": "load_data",
                "parameters": {
                    "table_name": "{catalog}.{schema}.customers",
                    "limit": 1000
                }
            }
        )
        
        # Create context with substitution manager
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify substitution occurred
        assert "test_catalog.test_schema.customers" in code
        assert "{catalog}" not in code
        assert "{schema}" not in code
        assert '"limit": 1000' in code

    def test_python_load_parameters_dollar_substitution(self):
        """Test ${token} substitution in parameters."""
        # Create substitution manager with test values
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "catalog": "prod_catalog",
            "bronze_schema": "bronze_layer"
        })
        
        # Create action with parameters using ${token} syntax
        action = Action(
            name="load_orders",
            type=ActionType.LOAD,
            target="v_orders",
            source={
                "type": "python",
                "module_path": "loaders/order_loader.py",
                "function_name": "get_orders",
                "parameters": {
                    "source_table": "${catalog}.${bronze_schema}.orders",
                    "batch_size": 500
                }
            }
        )
        
        # Create context with substitution manager
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify substitution occurred
        assert "prod_catalog.bronze_layer.orders" in code
        assert "${catalog}" not in code
        assert "${bronze_schema}" not in code
        assert '"batch_size": 500' in code

    def test_python_load_nested_parameters_substitution(self):
        """Test substitution in nested parameter dictionaries."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "env": "dev",
            "catalog": "dev_catalog",
            "api_endpoint": "https://api-dev.example.com"
        })
        
        # Create action with nested parameters
        action = Action(
            name="load_api_data",
            type=ActionType.LOAD,
            target="v_api_data",
            source={
                "type": "python",
                "module_path": "loaders/api_loader.py",
                "function_name": "load_from_api",
                "parameters": {
                    "config": {
                        "endpoint": "${api_endpoint}",
                        "environment": "${env}",
                        "target_table": "{catalog}.raw.api_data"
                    },
                    "retry_count": 3
                }
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify nested substitution occurred
        assert "https://api-dev.example.com" in code
        assert '"environment": "dev"' in code
        assert "dev_catalog.raw.api_data" in code
        assert '"retry_count": 3' in code

    def test_python_load_module_path_substitution(self):
        """Test substitution in module_path."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "py_functions_dir": "custom_python/loaders"
        })
        
        # Create action with substitution in module_path
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "${py_functions_dir}/data_loader.py",
                "function_name": "load_data",
                "parameters": {}
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify module_path substitution occurred
        # The function should be called (module name extracted from path)
        assert "load_data(spark, parameters)" in code
        assert "${py_functions_dir}" not in code or "custom_python/loaders" in code

    def test_python_load_secret_in_parameters(self):
        """Test ${secret:scope/key} substitution in parameters."""
        # Create substitution manager with secret support
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.default_secret_scope = "default_scope"
        
        # Create action with secret in parameters
        action = Action(
            name="load_secure_data",
            type=ActionType.LOAD,
            target="v_secure_data",
            source={
                "type": "python",
                "module_path": "loaders/secure_loader.py",
                "function_name": "load_secure",
                "parameters": {
                    "api_key": "${secret:api_secrets/service_key}",
                    "database_password": "${secret:password}"
                }
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify secrets are converted to placeholders (not dbutils calls yet)
        assert "__SECRET_" in code
        assert "api_secrets" in code or "service_key" in code
        assert "${secret:" not in code

    def test_python_load_no_substitution_manager(self):
        """Test graceful handling when no substitution manager is available."""
        # Create action with tokens (but no substitution manager)
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "loaders/data_loader.py",
                "function_name": "load_data",
                "parameters": {
                    "table": "${catalog}.${schema}.table"
                }
            }
        )
        
        # Create context WITHOUT substitution manager
        context = {
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify tokens remain unchanged
        assert "${catalog}.${schema}.table" in code or "{catalog}.{schema}.table" in code

    def test_python_load_function_name_substitution(self):
        """Test substitution in function_name."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "loader_function": "load_customer_data"
        })
        
        # Create action with substitution in function_name
        action = Action(
            name="load_customers",
            type=ActionType.LOAD,
            target="v_customers",
            source={
                "type": "python",
                "module_path": "loaders/customer_loader.py",
                "function_name": "${loader_function}",
                "parameters": {}
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify function_name substitution occurred
        assert "load_customer_data" in code
        assert "${loader_function}" not in code

    def test_python_load_mixed_syntax_substitution(self):
        """Test that both {} and ${} syntax work together."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "catalog": "mixed_catalog",
            "schema": "mixed_schema",
            "env": "test"
        })
        
        # Create action mixing both syntaxes
        action = Action(
            name="load_mixed",
            type=ActionType.LOAD,
            target="v_mixed",
            source={
                "type": "python",
                "module_path": "loaders/loader.py",
                "function_name": "load_data",
                "parameters": {
                    "table1": "${catalog}.{schema}.table1",
                    "environment": "{env}"
                }
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify both syntaxes are substituted
        assert "mixed_catalog.mixed_schema.table1" in code
        assert '"environment": "test"' in code


class TestPythonLoadModulePathParsing:
    """Test module_path parsing with different formats after substitution."""

    def test_module_path_substitution_with_py_extension(self):
        """Test file path with .py extension after substitution."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "loader_dir": "custom_python/loaders"
        })
        
        # Create action with substitution that resolves to file path
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "${loader_dir}/loader.py",
                "function_name": "load_data",
                "parameters": {}
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify import statement is valid Python (no forward slashes) - check imports property
        assert "from custom_python.loaders.loader import load_data" in generator.imports
        # Ensure no path separators in imports
        for import_stmt in generator.imports:
            assert "custom_python/loaders" not in import_stmt
            assert "custom_python\\loaders" not in import_stmt
        # Verify function is called correctly in code
        assert "load_data(spark, parameters)" in code

    def test_module_path_nested_directory_with_py(self):
        """Test nested directory path with .py extension after substitution."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "base_dir": "my_project/extractors"
        })
        
        # Create action with nested path substitution
        action = Action(
            name="extract_customers",
            type=ActionType.LOAD,
            target="v_customers",
            source={
                "type": "python",
                "module_path": "${base_dir}/subdir/customer_loader.py",
                "function_name": "extract",
                "parameters": {}
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify import statement is valid Python with proper dotted notation - check imports property
        assert "from my_project.extractors.subdir.customer_loader import extract" in generator.imports
        # Ensure no path separators in imports
        for import_stmt in generator.imports:
            assert "my_project/extractors" not in import_stmt
            assert "my_project\\extractors" not in import_stmt
        # Verify function call
        assert "extract(spark, parameters)" in code

    def test_module_path_dotted_import_no_extension(self):
        """Test dotted import path without .py extension (no substitution needed)."""
        # Create action with dotted import path
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "my_project.loaders.customer_loader",
                "function_name": "load_customers",
                "parameters": {}
            }
        )
        
        # Create context (no substitution manager needed)
        context = {
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify dotted import is preserved as-is - check imports property
        assert "from my_project.loaders.customer_loader import load_customers" in generator.imports
        # Ensure no path separators in imports
        for import_stmt in generator.imports:
            assert "my_project/loaders" not in import_stmt
            assert "my_project\\loaders" not in import_stmt

    def test_module_path_simple_module_name(self):
        """Test simple module name without path or extension."""
        # Create action with simple module name
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "loader",
                "function_name": "get_data",
                "parameters": {}
            }
        )
        
        # Create context
        context = {
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify simple import - check imports property
        assert "from loader import get_data" in generator.imports
        # Ensure no path separators in imports
        for import_stmt in generator.imports:
            assert "/" not in import_stmt or "://" in import_stmt  # Allow URLs
            assert "\\" not in import_stmt

    def test_module_path_windows_path_separator(self):
        """Test Windows-style path separator in module_path."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "loader_base": "loaders"
        })
        
        # Create action with Windows-style path
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "${loader_base}\\data_loader.py",
                "function_name": "load_data",
                "parameters": {}
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set()
        }
        
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        
        # Verify Windows backslash is converted to dot notation - check imports property
        assert "from loaders.data_loader import load_data" in generator.imports
        # Ensure no path separators in imports
        for import_stmt in generator.imports:
            assert "loaders\\data_loader" not in import_stmt
            assert "loaders/data_loader" not in import_stmt
        # Verify function call
        assert "load_data(spark, parameters)" in code

