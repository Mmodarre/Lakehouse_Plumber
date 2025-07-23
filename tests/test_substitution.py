"""Tests for substitution functionality of LakehousePlumber."""

import pytest
from pathlib import Path
import tempfile
from lhp.utils.substitution import SecretReference, EnhancedSubstitutionManager


class TestEnhancedSubstitutionManager:
    """Test the enhanced substitution manager."""
    
    def test_token_substitution(self):
        """Test basic token substitution."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config = """
dev:
  catalog: dev_catalog
  database: dev_bronze
global:
  company: acme_corp
"""
            f.write(config)
            f.flush()
            
            try:
                mgr = EnhancedSubstitutionManager(Path(f.name), env="dev")
                
                # Test token replacement
                result = mgr._replace_tokens_in_string("Use {catalog}.{database} from {company}")
                assert result == "Use dev_catalog.dev_bronze from acme_corp"
                
                # Test dollar-sign tokens
                result = mgr._replace_tokens_in_string("${catalog}_table")
                assert result == "dev_catalog_table"
            finally:
                Path(f.name).unlink()
    
    def test_secret_substitution(self):
        """Test secret reference handling."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config = """
dev:
  database: dev_db
secrets:
  default_scope: dev_secrets
  scopes:
    db: dev_database_secrets
    storage: dev_storage_secrets
"""
            f.write(config)
            f.flush()
            
            try:
                mgr = EnhancedSubstitutionManager(Path(f.name), env="dev")
                
                # Test secret with explicit scope
                result = mgr._process_string("jdbc://${secret:db/host}:5432/${database}")
                assert "__SECRET_dev_database_secrets_host__" in result
                assert "dev_db" in result
                
                # Test secret with default scope
                result = mgr._process_string("password=${secret:admin_password}")
                assert "__SECRET_dev_secrets_admin_password__" in result
                
                # Verify secret references were collected
                assert len(mgr.get_secret_references()) == 2
            finally:
                Path(f.name).unlink()
    
    def test_yaml_substitution(self):
        """Test substitution in YAML data structures."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {"env": "dev", "catalog": "main"}
        
        data = {
            "database": "{env}_bronze",
            "table": "{catalog}.users",
            "config": {
                "path": "/mnt/{env}/data",
                "secret": "${secret:storage/key}"
            }
        }
        
        result = mgr.substitute_yaml(data)
        
        assert result["database"] == "dev_bronze"
        assert result["table"] == "main.users"
        assert result["config"]["path"] == "/mnt/dev/data"
        assert "__SECRET_" in result["config"]["secret"]
    
    def test_secret_placeholder_replacement(self):
        """Test replacing secret placeholders with valid f-string Python code."""
        mgr = EnhancedSubstitutionManager()
        mgr.secret_references.add(SecretReference("prod_secrets", "db_password"))
        
        # Test case: secret embedded in a connection string (should become f-string)
        code = 'connection_string = "user=admin;password=__SECRET_prod_secrets_db_password__;timeout=30"'
        
        # Use SecretCodeGenerator to convert to valid Python
        from lhp.utils.secret_code_generator import SecretCodeGenerator
        generator = SecretCodeGenerator()
        result = generator.generate_python_code(code, mgr.get_secret_references())
        
        # Expected: f-string with dbutils call
        expected = 'connection_string = f"user=admin;password={dbutils.secrets.get(scope=\'prod_secrets\', key=\'db_password\')};timeout=30"'
        assert result == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 