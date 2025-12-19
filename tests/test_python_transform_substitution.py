"""Tests for substitution support in Python Transform generator."""

import tempfile
import pytest
from pathlib import Path
from lhp.models.config import Action, ActionType
from lhp.generators.transform.python import PythonTransformGenerator
from lhp.utils.substitution import EnhancedSubstitutionManager


class TestPythonTransformSubstitution:
    """Test substitution in Python Transform actions."""

    def test_python_transform_parameters_substitution(self):
        """Test that parameters dict is substituted."""
        # Create a temporary Python file for the transform
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
def transform_data(df, spark, parameters):
    table_name = parameters.get('table_name')
    limit = parameters.get('limit', 100)
    return df.limit(limit)
""")
            transform_file = Path(f.name)
        
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "catalog": "test_catalog",
            "schema": "test_schema"
        })
        
        # Create action with parameters using substitution tokens
        action = Action(
            name="transform_customers",
            type=ActionType.TRANSFORM,
            source="v_customers_raw",
            target="v_customers_transformed",
            module_path=str(transform_file),
            function_name="transform_data",
            parameters={
                "table_name": "${catalog}.${schema}.customers",
                "limit": 1000
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": "test_flowgroup"
        }
        
        generator = PythonTransformGenerator()
        code = generator.generate(action, context)
        
        # Verify substitution in parameters
        assert "test_catalog.test_schema.customers" in code
        assert "${catalog}" not in code
        assert "${schema}" not in code
        assert '"limit": 1000' in code
        
        # Cleanup
        transform_file.unlink()

    def test_python_transform_parameters_with_secrets(self):
        """Test that secrets work in parameters."""
        # Create a temporary Python file for the transform
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
def transform_with_auth(df, spark, parameters):
    api_key = parameters.get('api_key')
    return df
""")
            transform_file = Path(f.name)
        
        # Create substitution manager with secret support
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.default_secret_scope = "default_scope"
        substitution_mgr.mappings.update({
            "endpoint": "https://api.example.com"
        })
        
        # Create action with secret in parameters
        action = Action(
            name="transform_secure",
            type=ActionType.TRANSFORM,
            source="v_data",
            target="v_transformed",
            module_path=str(transform_file),
            function_name="transform_with_auth",
            parameters={
                "api_key": "${secret:api_secrets/service_key}",
                "endpoint": "${endpoint}"
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": "test_flowgroup"
        }
        
        generator = PythonTransformGenerator()
        code = generator.generate(action, context)
        
        # Verify secret placeholder and regular substitution
        assert "__SECRET_" in code
        assert "https://api.example.com" in code
        assert "${secret:" not in code
        
        # Cleanup
        transform_file.unlink()

    def test_python_transform_nested_parameters_substitution(self):
        """Test substitution in nested parameter structures."""
        # Create a temporary Python file for the transform
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
def complex_transform(df, spark, parameters):
    config = parameters.get('config', {})
    return df
""")
            transform_file = Path(f.name)
        
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "env": "dev",
            "catalog": "dev_catalog",
            "region": "us-west-2"
        })
        
        # Create action with nested parameters
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
                    "target": {
                        "catalog": "${catalog}",
                        "region": "{region}"
                    }
                },
                "batch_size": 500
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": "test_flowgroup"
        }
        
        generator = PythonTransformGenerator()
        code = generator.generate(action, context)
        
        # Verify nested substitution
        assert '"environment": "dev"' in code
        assert '"catalog": "dev_catalog"' in code
        assert '"region": "us-west-2"' in code
        assert '"batch_size": 500' in code
        
        # Cleanup
        transform_file.unlink()

    def test_python_transform_no_parameters(self):
        """Test that transform works without parameters."""
        # Create a temporary Python file for the transform
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
def simple_transform(df, spark, parameters):
    return df.select("*")
""")
            transform_file = Path(f.name)
        
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        
        # Create action without parameters
        action = Action(
            name="transform_simple",
            type=ActionType.TRANSFORM,
            source="v_data",
            target="v_transformed",
            module_path=str(transform_file),
            function_name="simple_transform"
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": "test_flowgroup"
        }
        
        generator = PythonTransformGenerator()
        code = generator.generate(action, context)
        
        # Verify code is generated without errors
        assert "simple_transform" in code
        assert "parameters = {}" in code
        
        # Cleanup
        transform_file.unlink()

    def test_python_transform_no_substitution_manager(self):
        """Test graceful handling when no substitution manager is available."""
        # Create a temporary Python file for the transform
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
def transform_data(df, spark, parameters):
    return df
""")
            transform_file = Path(f.name)
        
        # Create action with tokens (but no substitution manager)
        action = Action(
            name="transform_data",
            type=ActionType.TRANSFORM,
            source="v_data",
            target="v_transformed",
            module_path=str(transform_file),
            function_name="transform_data",
            parameters={
                "table": "${catalog}.${schema}.table"
            }
        )
        
        # Create context WITHOUT substitution manager
        context = {
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
        }
        
        generator = PythonTransformGenerator()
        code = generator.generate(action, context)
        
        # Verify tokens remain unchanged
        assert "${catalog}.${schema}.table" in code or "{catalog}.{schema}.table" in code
        
        # Cleanup
        transform_file.unlink()

    def test_python_transform_mixed_syntax_parameters(self):
        """Test that both {} and ${} syntax work together in parameters."""
        # Create a temporary Python file for the transform
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
def transform_mixed(df, spark, parameters):
    return df
""")
            transform_file = Path(f.name)
        
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.mappings.update({
            "catalog": "mixed_catalog",
            "schema": "mixed_schema",
            "env": "test"
        })
        
        # Create action mixing both syntaxes
        action = Action(
            name="transform_mixed",
            type=ActionType.TRANSFORM,
            source="v_input",
            target="v_output",
            module_path=str(transform_file),
            function_name="transform_mixed",
            parameters={
                "table1": "${catalog}.{schema}.table1",
                "environment": "{env}"
            }
        )
        
        # Create context
        context = {
            "substitution_manager": substitution_mgr,
            "secret_references": set(),
            "spec_dir": transform_file.parent,
            "output_dir": transform_file.parent / "output",
            "flowgroup": "test_flowgroup"
        }
        
        generator = PythonTransformGenerator()
        code = generator.generate(action, context)
        
        # Verify both syntaxes are substituted
        assert "mixed_catalog.mixed_schema.table1" in code
        assert '"environment": "test"' in code
        
        # Cleanup
        transform_file.unlink()

