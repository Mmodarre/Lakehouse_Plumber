"""Tests for Phase 2 components of LakehousePlumber."""

import pytest
from pathlib import Path
import tempfile
import yaml

from lhp.parsers.yaml_parser import YAMLParser
from lhp.utils.substitution import SecretReference, EnhancedSubstitutionManager
from lhp.presets.preset_manager import PresetManager
from lhp.core.secret_validator import SecretValidator
from lhp.utils.dqe import DQEParser
from lhp.utils.formatter import format_code, organize_imports, format_sql
from lhp.models.config import FlowGroup, Action, ActionType, Preset


class TestYAMLParser:
    """Test the YAML parser functionality."""
    
    def test_parse_file(self):
        """Test basic YAML file parsing."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_data
    type: load
    target: raw_data
"""
            f.write(yaml_content)
            f.flush()
            
            try:
                data = parser.parse_file(Path(f.name))
                assert data['pipeline'] == 'test_pipeline'
                assert data['flowgroup'] == 'test_flowgroup'
                assert len(data['actions']) == 1
            finally:
                Path(f.name).unlink()
    
    def test_parse_flowgroup(self):
        """Test FlowGroup parsing."""
        parser = YAMLParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
actions:
  - name: load_data
    type: load
    target: raw_data
    description: Load raw data
"""
            f.write(yaml_content)
            f.flush()
            
            try:
                flowgroup = parser.parse_flowgroup(Path(f.name))
                assert isinstance(flowgroup, FlowGroup)
                assert flowgroup.pipeline == 'test_pipeline'
                assert flowgroup.presets == ['bronze_layer']
                assert len(flowgroup.actions) == 1
                assert flowgroup.actions[0].name == 'load_data'
            finally:
                Path(f.name).unlink()
    
    def test_discover_flowgroups(self):
        """Test discovering multiple flowgroups."""
        parser = YAMLParser()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            pipelines_dir = Path(temp_dir) / "pipelines"
            pipelines_dir.mkdir()
            
            # Create test flowgroup files
            (pipelines_dir / "flow1.yaml").write_text("""
pipeline: pipeline1
flowgroup: flow1
actions:
  - name: action1
    type: load
    target: table1
""")
            
            (pipelines_dir / "flow2.yaml").write_text("""
pipeline: pipeline1
flowgroup: flow2
actions:
  - name: action2
    type: transform
    source: table1
    target: table2
""")
            
            flowgroups = parser.discover_flowgroups(pipelines_dir)
            assert len(flowgroups) == 2
            assert {fg.flowgroup for fg in flowgroups} == {'flow1', 'flow2'}


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


class TestPresetManager:
    """Test preset management and inheritance."""
    
    def test_preset_loading(self):
        """Test loading presets from directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)
            
            # Create test preset
            (presets_dir / "bronze.yaml").write_text("""
name: bronze
version: "1.0"
description: Bronze layer preset
defaults:
  quality: bronze
  checkpoint: true
""")
            
            mgr = PresetManager(presets_dir)
            assert "bronze" in mgr.presets
            
            preset = mgr.get_preset("bronze")
            assert preset.name == "bronze"
            assert preset.defaults["quality"] == "bronze"
    
    def test_preset_inheritance(self):
        """Test preset inheritance resolution."""
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)
            
            # Create base preset
            (presets_dir / "base.yaml").write_text("""
name: base
version: "1.0"
defaults:
  quality: base
  checkpoint: false
  common_setting: true
""")
            
            # Create child preset
            (presets_dir / "bronze.yaml").write_text("""
name: bronze
version: "1.0"
extends: base
defaults:
  quality: bronze
  checkpoint: true
""")
            
            mgr = PresetManager(presets_dir)
            
            # Resolve inheritance
            config = mgr._resolve_preset_inheritance("bronze")
            
            # Child overrides parent
            assert config["quality"] == "bronze"
            assert config["checkpoint"] is True
            # Parent settings are inherited
            assert config["common_setting"] is True
    
    def test_preset_chain_resolution(self):
        """Test resolving a chain of presets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)
            
            # Create presets
            (presets_dir / "preset1.yaml").write_text("""
name: preset1
defaults:
  setting1: value1
  setting2: original
""")
            
            (presets_dir / "preset2.yaml").write_text("""
name: preset2
defaults:
  setting2: overridden
  setting3: value3
""")
            
            mgr = PresetManager(presets_dir)
            
            # Resolve chain
            config = mgr.resolve_preset_chain(["preset1", "preset2"])
            
            assert config["setting1"] == "value1"
            assert config["setting2"] == "overridden"  # preset2 overrides preset1
            assert config["setting3"] == "value3"


class TestSecretValidator:
    """Test secret validation."""
    
    def test_validate_secret_references(self):
        """Test validating secret references."""
        validator = SecretValidator(available_scopes={'prod_secrets', 'dev_secrets'})
        
        refs = {
            SecretReference('prod_secrets', 'db_password'),
            SecretReference('dev_secrets', 'api-key'),
            SecretReference('unknown_scope', 'some_key')
        }
        
        errors = validator.validate_secret_references(refs)
        
        assert len(errors) == 1
        assert "unknown_scope" in errors[0]
    
    def test_key_format_validation(self):
        """Test secret key format validation."""
        validator = SecretValidator()
        
        # Valid formats
        assert validator._is_valid_key_format('db_password')
        assert validator._is_valid_key_format('api-key-123')
        assert validator._is_valid_key_format('TOKEN123')
        
        # Invalid formats
        assert not validator._is_valid_key_format('db password')
        assert not validator._is_valid_key_format('key@123')
        assert not validator._is_valid_key_format('key!value')


class TestDQEParser:
    """Test data quality expectations parser."""
    
    def test_parse_expectations(self):
        """Test parsing expectations into categories."""
        parser = DQEParser()
        
        expectations = [
            {'constraint': 'id IS NOT NULL', 'type': 'expect', 'message': 'ID required'},
            {'constraint': 'age > 0', 'type': 'expect_or_drop', 'message': 'Invalid age'},
            {'constraint': 'COUNT(*) > 0', 'type': 'expect_or_fail', 'message': 'No data'}
        ]
        
        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)
        
        assert 'ID required' in expect_all
        assert expect_all['ID required'] == 'id IS NOT NULL'
        
        assert 'Invalid age' in expect_drop
        assert 'No data' in expect_fail
    
    def test_load_expectations_from_file(self):
        """Test loading expectations from YAML file."""
        parser = DQEParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({
                'expectations': [
                    {'constraint': 'col1 IS NOT NULL', 'type': 'expect'},
                    {'constraint': 'col2 > 0', 'type': 'expect_or_drop'}
                ]
            }, f)
            f.flush()
            
            try:
                expectations = parser.load_expectations_from_file(Path(f.name))
                assert len(expectations) == 2
                assert expectations[0]['constraint'] == 'col1 IS NOT NULL'
            finally:
                Path(f.name).unlink()


class TestCodeFormatter:
    """Test code formatting utilities."""
    
    def test_format_code(self):
        """Test Python code formatting."""
        unformatted = """def hello(  name   ):
    return    f"Hello {name}"
"""
        formatted = format_code(unformatted)
        assert 'def hello(name):' in formatted
        assert '    return f"Hello {name}"' in formatted
    
    def test_organize_imports(self):
        """Test import organization."""
        code = """import os
from pathlib import Path
import dlt
from pyspark.sql import DataFrame
from mymodule import helper
"""
        
        organized = organize_imports(code)
        
        # Check order: stdlib, third-party, local
        lines = organized.strip().split('\n')
        
        # Find positions of imports
        os_pos = next(i for i, line in enumerate(lines) if 'import os' in line)
        dlt_pos = next(i for i, line in enumerate(lines) if 'import dlt' in line)
        
        # Standard library should come before third-party
        assert os_pos < dlt_pos
    
    def test_format_sql(self):
        """Test SQL formatting."""
        sql = "SELECT id, name FROM users WHERE age > 18 ORDER BY name"
        formatted = format_sql(sql)
        
        assert 'SELECT' in formatted
        assert 'FROM' in formatted
        assert 'WHERE' in formatted
        assert 'ORDER BY' in formatted
        assert formatted.count('\n') >= 3  # Should be multi-line


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 