"""Tests for substitution support in Python Transform generator."""

import tempfile
from pathlib import Path

import pytest

from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.generators.transform.python import PythonTransformGenerator
from lhp.models import Action, ActionType, FlowGroup


class TestPythonTransformSubstitution:
    """Test substitution in Python Transform actions."""

    def test_python_transform_parameters_substitution(self):
        """Test that parameters dict is substituted."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
def transform_data(df, spark, parameters):
    table_name = parameters.get('table_name')
    limit = parameters.get('limit', 100)
    return df.limit(limit)
""")
            transform_file = Path(f.name)

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {"catalog": "test_catalog", "schema": "test_schema"}
        )

        action = Action(
            name="transform_customers",
            type=ActionType.TRANSFORM,
            source="v_customers_raw",
            target="v_customers_transformed",
            module_path=str(transform_file),
            function_name="transform_data",
            parameters={"table_name": "${catalog}.${schema}.customers", "limit": 1000},
        )

        flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": flowgroup,
        }

        generator = PythonTransformGenerator()
        code = generator.generate(action, context)

        assert "test_catalog.test_schema.customers" in code
        assert "${catalog}" not in code
        assert "${schema}" not in code
        assert '"limit": 1000' in code

        transform_file.unlink()

    def test_python_transform_parameters_with_secrets(self):
        """Test that secrets work in parameters.

        Generator layer emits ``__SECRET_scope_key__`` placeholders; the
        post-pass rewrites them later. See module docstring on
        SecretCodeGenerator for the architectural rationale.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
def transform_with_auth(df, spark, parameters):
    api_key = parameters.get('api_key')
    return df
""")
            transform_file = Path(f.name)

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.default_secret_scope = "default_scope"
        substitution_mgr.mappings.update({"endpoint": "https://api.example.com"})

        action = Action(
            name="transform_secure",
            type=ActionType.TRANSFORM,
            source="v_data",
            target="v_transformed",
            module_path=str(transform_file),
            function_name="transform_with_auth",
            parameters={
                "api_key": "${secret:api_secrets/service_key}",
                "endpoint": "${endpoint}",
            },
        )

        flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": flowgroup,
        }

        generator = PythonTransformGenerator()
        code = generator.generate(action, context)

        # Secret becomes a placeholder at the generator layer; non-secret
        # ${endpoint} substitutes normally.
        assert "__SECRET_api_secrets_service_key__" in code
        assert "https://api.example.com" in code
        assert "${secret:" not in code
        # Bare dbutils calls only appear after the post-pass.
        assert "dbutils.secrets.get" not in code

        transform_file.unlink()

    def test_python_transform_nested_parameters_substitution(self):
        """Test substitution in nested parameter structures."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
def complex_transform(df, spark, parameters):
    config = parameters.get('config', {})
    return df
""")
            transform_file = Path(f.name)

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {"env": "dev", "catalog": "dev_catalog", "region": "us-west-2"}
        )

        action = Action(
            name="transform_complex",
            type=ActionType.TRANSFORM,
            source="v_input",
            target="v_output",
            module_path=str(transform_file),
            function_name="complex_transform",
            parameters={
                "config": {
                    "environment": "${env}",
                    "target": {"catalog": "${catalog}", "region": "{region}"},
                },
                "batch_size": 500,
            },
        )

        flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": flowgroup,
        }

        generator = PythonTransformGenerator()
        code = generator.generate(action, context)

        assert '"environment": "dev"' in code
        assert '"catalog": "dev_catalog"' in code
        assert '"region": "us-west-2"' in code
        assert '"batch_size": 500' in code

        transform_file.unlink()

    def test_python_transform_no_parameters(self):
        """Test that transform works without parameters."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
def simple_transform(df, spark, parameters):
    return df.select("*")
""")
            transform_file = Path(f.name)

        substitution_mgr = EnhancedSubstitutionManager()

        action = Action(
            name="transform_simple",
            type=ActionType.TRANSFORM,
            source="v_data",
            target="v_transformed",
            module_path=str(transform_file),
            function_name="simple_transform",
        )

        flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": flowgroup,
        }

        generator = PythonTransformGenerator()
        code = generator.generate(action, context)

        assert "simple_transform" in code
        assert "parameters =" in code  # May be {} or null depending on how it's handled

        transform_file.unlink()

    def test_python_transform_no_substitution_manager(self):
        """Test graceful handling when no substitution manager is available."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
def transform_data(df, spark, parameters):
    return df
""")
            transform_file = Path(f.name)

        action = Action(
            name="transform_data",
            type=ActionType.TRANSFORM,
            source="v_data",
            target="v_transformed",
            module_path=str(transform_file),
            function_name="transform_data",
            parameters={"table": "${catalog}.${schema}.table"},
        )

        flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

        context = {
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": flowgroup,
        }

        generator = PythonTransformGenerator()
        code = generator.generate(action, context)

        assert (
            "${catalog}.${schema}.table" in code or "{catalog}.{schema}.table" in code
        )

        transform_file.unlink()

    def test_python_transform_mixed_syntax_parameters(self):
        """Test that both {} and ${} syntax work together in parameters."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
def transform_mixed(df, spark, parameters):
    return df
""")
            transform_file = Path(f.name)

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update(
            {"catalog": "mixed_catalog", "schema": "mixed_schema", "env": "test"}
        )

        action = Action(
            name="transform_mixed",
            type=ActionType.TRANSFORM,
            source="v_input",
            target="v_output",
            module_path=str(transform_file),
            function_name="transform_mixed",
            parameters={"table1": "${catalog}.{schema}.table1", "environment": "{env}"},
        )

        flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": flowgroup,
        }

        generator = PythonTransformGenerator()
        code = generator.generate(action, context)

        assert "mixed_catalog.mixed_schema.table1" in code
        assert '"environment": "test"' in code

        transform_file.unlink()


class TestPythonTransformModulePathFunctionNameSubstitution:
    """Test substitution of module_path and function_name in Python Transform actions."""

    def test_module_path_substitution(self):
        """Test that module_path is substituted before file loading."""
        # Create a temporary directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            transform_dir = tmpdir_path / "custom_transforms"
            transform_dir.mkdir()

            transform_file = transform_dir / "customer_transform.py"
            transform_file.write_text("""
def transform_customers(df, spark, parameters):
    return df.limit(100)
""")

            substitution_mgr = EnhancedSubstitutionManager()
            substitution_mgr.mappings.update({"transform_dir": "custom_transforms"})

            action = Action(
                name="transform_customers",
                type=ActionType.TRANSFORM,
                source="v_customers",
                target="v_customers_transformed",
                module_path="${transform_dir}/customer_transform.py",
                function_name="transform_customers",
                parameters={},
            )

            flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

            context = {
                "substitution_manager": substitution_mgr,
                "secret_references": set(),
                "spec_dir": tmpdir_path,
                "output_dir": tmpdir_path / "output",
                "flowgroup": flowgroup,
            }

            generator = PythonTransformGenerator()
            code = generator.generate(action, context)

            assert "transform_customers(v_customers_df, spark, parameters)" in code

    def test_function_name_substitution(self):
        """Test that function_name is substituted."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
def process_customer_data(df, spark, parameters):
    return df.limit(100)
""")
            transform_file = Path(f.name)

        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({"func_name": "process_customer_data"})

        action = Action(
            name="process_customers",
            type=ActionType.TRANSFORM,
            source="v_customers",
            target="v_customers_processed",
            module_path=str(transform_file),
            function_name="${func_name}",
            parameters={},
        )

        flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": flowgroup,
        }

        generator = PythonTransformGenerator()
        code = generator.generate(action, context)

        assert "process_customer_data(v_customers_df, spark, parameters)" in code
        assert "${func_name}" not in code

        transform_file.unlink()

    def test_module_path_with_nested_substitution(self):
        """Test module_path with multiple substitution tokens."""
        # Create temporary directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            base_dir = tmpdir_path / "my_project"
            transform_dir = base_dir / "transforms"
            transform_dir.mkdir(parents=True)

            transform_file = transform_dir / "processor.py"
            transform_file.write_text("""
def process_data(df, spark, parameters):
    return df
""")

            substitution_mgr = EnhancedSubstitutionManager()
            substitution_mgr.mappings.update(
                {
                    "base_dir": "my_project",
                    "transform_subdir": "transforms",
                    "module_name": "processor",
                }
            )

            action = Action(
                name="process_data",
                type=ActionType.TRANSFORM,
                source="v_input",
                target="v_output",
                module_path="${base_dir}/${transform_subdir}/${module_name}.py",
                function_name="process_data",
                parameters={},
            )

            flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

            context = {
                "substitution_manager": substitution_mgr,
                "secret_references": set(),
                "spec_dir": tmpdir_path,
                "output_dir": tmpdir_path / "output",
                "flowgroup": flowgroup,
            }

            generator = PythonTransformGenerator()
            code = generator.generate(action, context)

            assert "process_data(v_input_df, spark, parameters)" in code

    def test_module_path_and_function_name_both_substituted(self):
        """Test that both module_path and function_name are substituted together."""
        # Create temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            loaders_dir = tmpdir_path / "loaders"
            loaders_dir.mkdir()

            loader_file = loaders_dir / "data_loader.py"
            loader_file.write_text("""
def load_and_transform(df, spark, parameters):
    return df.limit(50)
""")

            substitution_mgr = EnhancedSubstitutionManager()
            substitution_mgr.mappings.update(
                {
                    "loader_dir": "loaders",
                    "loader_file": "data_loader",
                    "func_name": "load_and_transform",
                }
            )

            action = Action(
                name="load_transform",
                type=ActionType.TRANSFORM,
                source="v_raw",
                target="v_processed",
                module_path="${loader_dir}/${loader_file}.py",
                function_name="${func_name}",
                parameters={"limit": 50},
            )

            flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

            context = {
                "substitution_manager": substitution_mgr,
                "secret_references": set(),
                "spec_dir": tmpdir_path,
                "output_dir": tmpdir_path / "output",
                "flowgroup": flowgroup,
            }

            generator = PythonTransformGenerator()
            code = generator.generate(action, context)

            assert "load_and_transform(v_raw_df, spark, parameters)" in code
            assert "${func_name}" not in code
            assert "${loader_dir}" not in code

    def test_no_substitution_when_no_manager(self):
        """Test that transform works when no substitution manager is available."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("""
def simple_transform(df, spark, parameters):
    return df
""")
            transform_file = Path(f.name)

        action = Action(
            name="simple_transform",
            type=ActionType.TRANSFORM,
            source="v_input",
            target="v_output",
            module_path=str(transform_file),
            function_name="simple_transform",
            parameters={},
        )

        flowgroup = FlowGroup(pipeline="test_pipeline", flowgroup="test_flowgroup")

        context = {
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": flowgroup,
        }

        generator = PythonTransformGenerator()
        code = generator.generate(action, context)

        assert "simple_transform(v_input_df, spark, parameters)" in code

        transform_file.unlink()
