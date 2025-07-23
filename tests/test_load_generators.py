"""Tests for load action generators of LakehousePlumber."""

import pytest
from lhp.models.config import Action, ActionType
from lhp.generators.load import (
    CloudFilesLoadGenerator,
    DeltaLoadGenerator,
    SQLLoadGenerator,
    JDBCLoadGenerator,
    PythonLoadGenerator
)
from lhp.utils.substitution import EnhancedSubstitutionManager


class TestLoadGenerators:
    """Test load action generators."""
    
    def test_cloudfiles_generator(self):
        """Test CloudFiles load generator."""
        generator = CloudFilesLoadGenerator()
        action = Action(
            name="load_raw_files",
            type=ActionType.LOAD,
            target="v_raw_files",
            source={
                "type": "cloudfiles",
                "path": "/mnt/data/raw",
                "format": "json",
                "readMode": "stream",
                "schema_evolution_mode": "addNewColumns",
                "reader_options": {
                    "multiLine": "true"
                }
            },
            description="Load raw JSON files"
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_raw_files" in code
        assert "spark.readStream" in code
        assert 'cloudFiles.format", "json"' in code
        assert 'multiLine", "true"' in code
    
    def test_delta_generator(self):
        """Test Delta load generator."""
        generator = DeltaLoadGenerator()
        action = Action(
            name="load_customers",
            type=ActionType.LOAD,
            target="v_customers",
            source={
                "type": "delta",
                "catalog": "main",
                "database": "bronze",
                "table": "customers",
                "readMode": "stream",
                "read_change_feed": True,
                "where_clause": ["active = true"],
                "select_columns": ["id", "name", "email"]
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_customers" in code
        assert "spark.readStream" in code
        assert "readChangeFeed" in code
        assert "main.bronze.customers" in code
        assert 'where("active = true")' in code
        assert "select([" in code
    
    def test_sql_generator(self):
        """Test SQL load generator."""
        generator = SQLLoadGenerator()
        action = Action(
            name="load_metrics",
            type=ActionType.LOAD,
            target="v_metrics",
            source="SELECT * FROM metrics WHERE date > current_date() - 7"
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_metrics" in code
        assert "spark.sql" in code
        assert "SELECT * FROM metrics" in code
    
    def test_jdbc_generator_with_secrets(self):
        """Test JDBC load generator with secret substitution generates valid Python code."""
        generator = JDBCLoadGenerator()
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.default_secret_scope = "db_secrets"
        
        action = Action(
            name="load_external",
            type=ActionType.LOAD,
            target="v_external_data",
            source={
                "type": "jdbc",
                "url": "jdbc:postgresql://${secret:db/host}:5432/mydb",
                "user": "${secret:db/username}",
                "password": "${secret:db/password}",
                "driver": "org.postgresql.Driver",
                "table": "external_table"
            }
        )
        
        code = generator.generate(action, {"substitution_manager": substitution_mgr})
        
        # The generator should produce placeholders, not f-strings (conversion happens in orchestrator)
        # Check for placeholder patterns
        assert '__SECRET_db_host__' in code or '__SECRET_database_secrets_host__' in code
        assert '__SECRET_db_username__' in code or '__SECRET_database_secrets_username__' in code
        assert '__SECRET_db_password__' in code or '__SECRET_database_secrets_password__' in code
        
        # Verify placeholder patterns are in the expected format
        assert 'jdbc:postgresql://' in code
        assert '"__SECRET_' in code or "'__SECRET_" in code
        
        # Most importantly, verify the generated code is syntactically valid
        try:
            compile(code, '<string>', 'exec')
            # If compilation succeeds, the code is valid
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code with secrets is not valid Python syntax: {e}")
    
    def test_python_generator(self):
        """Test Python load generator."""
        generator = PythonLoadGenerator()
        action = Action(
            name="load_custom",
            type=ActionType.LOAD,
            target="v_custom_data",
            source={
                "type": "python",
                "module_path": "custom_loaders",
                "function_name": "load_custom_data",
                "parameters": {
                    "start_date": "2024-01-01",
                    "batch_size": 1000
                }
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_custom_data" in code
        assert "load_custom_data(spark, parameters)" in code
        assert '"start_date": "2024-01-01"' in code
        assert "from custom_loaders import load_custom_data" in generator.imports


def test_generator_imports():
    """Test that generators manage imports correctly."""
    # Load generator
    load_gen = CloudFilesLoadGenerator()
    assert "import dlt" in load_gen.imports


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 