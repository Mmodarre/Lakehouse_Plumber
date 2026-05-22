"""Tests for substitution support in Python Load generator."""

from pathlib import Path

import pytest

from lhp.generators.load.python import PythonLoadGenerator
from lhp.models.config import Action, ActionType
from lhp.utils.error_formatter import LHPError
from lhp.utils.substitution import EnhancedSubstitutionManager


@pytest.fixture
def stub_copy_helper(monkeypatch):
    """Stub copy_user_module_for_pipeline so substitution tests do not need
    real .py files on disk or a flowgroup in context. Returns the leaf
    module name — same behaviour as the helper's dry-run path."""

    def _stub(module_path: str, context: dict, *, component_label: str) -> str:
        return Path(module_path).stem

    monkeypatch.setattr(
        "lhp.generators.load.python.copy_user_module_for_pipeline", _stub
    )
    return _stub


class TestPythonLoadSubstitution:
    """Test substitution in Python Load actions."""

    @pytest.fixture(autouse=True)
    def _autouse_stub_copy_helper(self, stub_copy_helper):
        return stub_copy_helper

    def test_python_load_parameters_basic_substitution(self):
        """Test basic {token} substitution in parameters."""
        # Create substitution manager with test values
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {"catalog": "test_catalog", "schema": "test_schema"}
        )

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
                    "limit": 1000,
                },
            },
        )

        # Create context with substitution manager
        context = {"substitution_manager": substitution_mgr, "secret_references": set()}

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
        substitution_mgr.mappings.update(
            {"catalog": "prod_catalog", "bronze_schema": "bronze_layer"}
        )

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
                    "batch_size": 500,
                },
            },
        )

        # Create context with substitution manager
        context = {"substitution_manager": substitution_mgr, "secret_references": set()}

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
        substitution_mgr.mappings.update(
            {
                "env": "dev",
                "catalog": "dev_catalog",
                "api_endpoint": "https://api-dev.example.com",
            }
        )

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
                        "target_table": "{catalog}.raw.api_data",
                    },
                    "retry_count": 3,
                },
            },
        )

        # Create context
        context = {"substitution_manager": substitution_mgr, "secret_references": set()}

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
        substitution_mgr.mappings.update({"py_functions_dir": "custom_python/loaders"})

        # Create action with substitution in module_path
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "${py_functions_dir}/data_loader.py",
                "function_name": "load_data",
                "parameters": {},
            },
        )

        # Create context
        context = {"substitution_manager": substitution_mgr, "secret_references": set()}

        generator = PythonLoadGenerator()
        code = generator.generate(action, context)

        # Verify module_path substitution occurred
        # The function should be called (module name extracted from path)
        assert "load_data(spark, parameters)" in code
        assert "${py_functions_dir}" not in code or "custom_python/loaders" in code

    def test_python_load_secret_in_parameters(self):
        """Test ${secret:scope/key} substitution in parameters.

        Substitution emits sentinel placeholders at the generator layer;
        the post-pass (`SecretCodeGenerator`, invoked from
        `CodeGenerator._apply_secret_substitutions`) rewrites placeholders
        to bare ``dbutils.secrets.get(...)`` calls or f-strings depending
        on string-literal context. Bare-call form is asserted at the
        integration layer in `tests/test_integration.py`.
        """
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
                    "database_password": "${secret:password}",
                },
            },
        )

        # Create context
        context = {"substitution_manager": substitution_mgr, "secret_references": set()}

        generator = PythonLoadGenerator()
        code = generator.generate(action, context)

        # Placeholder is emitted at this layer; explicit and default scopes
        # both resolve to ``__SECRET_<scope>_<key>__``.
        assert "__SECRET_api_secrets_service_key__" in code
        assert "__SECRET_default_scope_password__" in code

        # ${secret:...} tokens must be fully replaced by substitution.
        assert "${secret:" not in code

        # No bare dbutils calls at the generator level — that happens in
        # the post-pass on assembled flowgroup code.
        assert "dbutils.secrets.get" not in code

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
                "parameters": {"table": "${catalog}.${schema}.table"},
            },
        )

        # Create context WITHOUT substitution manager
        context = {"secret_references": set()}

        generator = PythonLoadGenerator()
        code = generator.generate(action, context)

        # Verify tokens remain unchanged
        assert (
            "${catalog}.${schema}.table" in code or "{catalog}.{schema}.table" in code
        )

    def test_python_load_function_name_substitution(self):
        """Test substitution in function_name."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({"loader_function": "load_customer_data"})

        # Create action with substitution in function_name
        action = Action(
            name="load_customers",
            type=ActionType.LOAD,
            target="v_customers",
            source={
                "type": "python",
                "module_path": "loaders/customer_loader.py",
                "function_name": "${loader_function}",
                "parameters": {},
            },
        )

        # Create context
        context = {"substitution_manager": substitution_mgr, "secret_references": set()}

        generator = PythonLoadGenerator()
        code = generator.generate(action, context)

        # Verify function_name substitution occurred
        assert "load_customer_data" in code
        assert "${loader_function}" not in code

    def test_python_load_mixed_syntax_substitution(self):
        """Test that both {} and ${} syntax work together."""
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {"catalog": "mixed_catalog", "schema": "mixed_schema", "env": "test"}
        )

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
                    "environment": "{env}",
                },
            },
        )

        # Create context
        context = {"substitution_manager": substitution_mgr, "secret_references": set()}

        generator = PythonLoadGenerator()
        code = generator.generate(action, context)

        # Verify both syntaxes are substituted
        assert "mixed_catalog.mixed_schema.table1" in code
        assert '"environment": "test"' in code


class TestPythonLoadModulePathParsing:
    """Test module_path parsing with different formats after substitution."""

    @pytest.fixture(autouse=True)
    def _autouse_stub_copy_helper(self, stub_copy_helper):
        return stub_copy_helper

    def test_module_path_substitution_with_py_extension(self):
        """File path with .py extension after substitution → leaf-name import."""
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({"loader_dir": "custom_python/loaders"})

        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "${loader_dir}/loader.py",
                "function_name": "load_data",
                "parameters": {},
            },
        )
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
        }

        generator = PythonLoadGenerator()
        code = generator.generate(action, context)

        # Helper now copies into custom_python_functions/<leaf>.py and imports
        # the function from there, regardless of the user's directory layout.
        assert (
            "from custom_python_functions.loader import load_data" in generator.imports
        )
        for import_stmt in generator.imports:
            assert "custom_python/loaders" not in import_stmt
            assert "custom_python\\loaders" not in import_stmt
        assert "load_data(spark, parameters)" in code

    def test_module_path_nested_directory_with_py(self):
        """Nested directory path → leaf-name import via custom_python_functions/."""
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({"base_dir": "my_project/extractors"})

        action = Action(
            name="extract_customers",
            type=ActionType.LOAD,
            target="v_customers",
            source={
                "type": "python",
                "module_path": "${base_dir}/subdir/customer_loader.py",
                "function_name": "extract",
                "parameters": {},
            },
        )
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
        }

        generator = PythonLoadGenerator()
        code = generator.generate(action, context)

        assert (
            "from custom_python_functions.customer_loader import extract"
            in generator.imports
        )
        for import_stmt in generator.imports:
            assert "my_project/extractors" not in import_stmt
            assert "my_project\\extractors" not in import_stmt
        assert "extract(spark, parameters)" in code


class TestPythonLoadHardError:
    """Python LOAD now hard-rejects non-.py module_path values (no fallback)."""

    def test_dotted_module_path_rejected(self):
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "my_project.loaders.customer_loader",
                "function_name": "load_customers",
                "parameters": {},
            },
        )
        generator = PythonLoadGenerator()
        with pytest.raises(LHPError) as exc_info:
            generator.generate(action, {})
        # Surfaced via ErrorFormatter.file_not_found.
        assert "Python load action module file" in str(exc_info.value)

    def test_bare_module_name_rejected(self):
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "loader",
                "function_name": "get_data",
                "parameters": {},
            },
        )
        generator = PythonLoadGenerator()
        with pytest.raises(LHPError) as exc_info:
            generator.generate(action, {})
        assert "Python load action module file" in str(exc_info.value)

    def test_dry_run_returns_leaf_name(self, tmp_path):
        """With output_dir=None and a real .py file, helper returns the stem
        and the import line uses that stem (matches TRANSFORM dry-run)."""
        from lhp.models.config import FlowGroup

        loader_dir = tmp_path / "loaders"
        loader_dir.mkdir()
        (loader_dir / "my_loader.py").write_text(
            "def get_df(spark, parameters):\n    return None\n"
        )

        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "python",
                "module_path": "loaders/my_loader.py",
                "function_name": "get_df",
                "parameters": {},
            },
        )
        context = {
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(pipeline="p_test", flowgroup="fg_test"),
            # output_dir omitted → dry-run path inside the helper.
        }
        generator = PythonLoadGenerator()
        code = generator.generate(action, context)
        assert (
            "from custom_python_functions.my_loader import get_df" in generator.imports
        )
        assert "get_df(spark, parameters)" in code
