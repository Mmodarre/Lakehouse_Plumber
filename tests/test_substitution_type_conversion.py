"""
Tests for EnhancedSubstitutionManager type conversion functionality.

This module tests that the substitution manager correctly converts
primitive types (bool, int, float) to strings for text-based token
replacement while preserving nested structures.
"""

import pytest
import yaml
from pathlib import Path
from tempfile import TemporaryDirectory

from lhp.utils.substitution import EnhancedSubstitutionManager


class TestTypeConversion:
    """Test type conversion in substitution manager."""

    def test_boolean_true_conversion(self, tmp_path):
        """Test that boolean true is converted to string 'true'."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  my_flag: true
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert "my_flag" in mgr.mappings
        assert mgr.mappings["my_flag"] == "true"
        assert isinstance(mgr.mappings["my_flag"], str)

    def test_boolean_false_conversion(self, tmp_path):
        """Test that boolean false is converted to string 'false'."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  my_flag: false
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert "my_flag" in mgr.mappings
        assert mgr.mappings["my_flag"] == "false"
        assert isinstance(mgr.mappings["my_flag"], str)

    def test_integer_conversion(self, tmp_path):
        """Test that integers are converted to strings."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  min_workers: 1
  max_workers: 42
  zero_value: 0
  negative: -5
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert mgr.mappings["min_workers"] == "1"
        assert mgr.mappings["max_workers"] == "42"
        assert mgr.mappings["zero_value"] == "0"
        assert mgr.mappings["negative"] == "-5"
        assert all(isinstance(v, str) for k, v in mgr.mappings.items() 
                  if k in ["min_workers", "max_workers", "zero_value", "negative"])

    def test_float_conversion(self, tmp_path):
        """Test that floats are converted to strings."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  pi: 3.14
  rate: 0.05
  scientific: 1.5e-3
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert mgr.mappings["pi"] == "3.14"
        assert mgr.mappings["rate"] == "0.05"
        assert mgr.mappings["scientific"] == "0.0015"
        assert all(isinstance(v, str) for k, v in mgr.mappings.items() 
                  if k in ["pi", "rate", "scientific"])

    def test_string_passthrough(self, tmp_path):
        """Test that strings remain strings."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  catalog: my_catalog
  schema: my_schema
  node_type: Standard_D4ds_v5
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert mgr.mappings["catalog"] == "my_catalog"
        assert mgr.mappings["schema"] == "my_schema"
        assert mgr.mappings["node_type"] == "Standard_D4ds_v5"
        assert all(isinstance(v, str) for k, v in mgr.mappings.items() 
                  if k in ["catalog", "schema", "node_type"])

    def test_mixed_types_conversion(self, tmp_path):
        """Test that mixed types are all converted to strings."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  catalog: acme_dev
  continuous: true
  photon: false
  min_workers: 1
  max_workers: 10
  timeout: 3.5
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert mgr.mappings["catalog"] == "acme_dev"
        assert mgr.mappings["continuous"] == "true"
        assert mgr.mappings["photon"] == "false"
        assert mgr.mappings["min_workers"] == "1"
        assert mgr.mappings["max_workers"] == "10"
        assert mgr.mappings["timeout"] == "3.5"
        
        # All should be strings
        for key in ["catalog", "continuous", "photon", "min_workers", "max_workers", "timeout"]:
            assert isinstance(mgr.mappings[key], str), f"{key} should be string, got {type(mgr.mappings[key])}"

    def test_nested_dict_preserved(self, tmp_path):
        """Test that nested dictionaries are preserved (for prefix_suffix handling)."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  simple_value: test
  nested_config:
    key1: value1
    key2: value2
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert mgr.mappings["simple_value"] == "test"
        assert isinstance(mgr.mappings["simple_value"], str)
        
        assert "nested_config" in mgr.mappings
        assert isinstance(mgr.mappings["nested_config"], dict)
        assert mgr.mappings["nested_config"]["key1"] == "value1"

    def test_nested_list_preserved(self, tmp_path):
        """Test that nested lists are preserved."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  simple_value: test
  my_list:
    - item1
    - item2
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert mgr.mappings["simple_value"] == "test"
        assert isinstance(mgr.mappings["simple_value"], str)
        
        assert "my_list" in mgr.mappings
        assert isinstance(mgr.mappings["my_list"], list)
        assert mgr.mappings["my_list"] == ["item1", "item2"]


class TestTokenReplacementWithConvertedTypes:
    """Test that token replacement works with converted types."""

    def test_boolean_token_replacement(self, tmp_path):
        """Test token replacement with boolean values."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  continuous: true
  photon: false
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        # Test replacement in strings
        result1 = mgr._process_string("continuous: {continuous}")
        result2 = mgr._process_string("photon: {photon}")
        
        assert result1 == "continuous: true"
        assert result2 == "photon: false"

    def test_integer_token_replacement(self, tmp_path):
        """Test token replacement with integer values."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  min_workers: 1
  max_workers: 10
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        result = mgr._process_string("min_workers: {min_workers}, max_workers: {max_workers}")
        
        assert result == "min_workers: 1, max_workers: 10"

    def test_yaml_dict_substitution(self, tmp_path):
        """Test substitute_yaml with a dictionary containing various types."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  catalog: my_catalog
  serverless: false
  min_workers: 2
  timeout: 30.5
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        data = {
            "catalog": "{catalog}",
            "serverless": "{serverless}",
            "autoscale": {
                "min_workers": "{min_workers}"
            },
            "timeout": "{timeout}"
        }
        
        result = mgr.substitute_yaml(data)
        
        assert result["catalog"] == "my_catalog"
        assert result["serverless"] == "false"
        assert result["autoscale"]["min_workers"] == "2"
        assert result["timeout"] == "30.5"


class TestYAMLTypeRestoration:
    """
    Test type restoration behavior.
    
    Note: In the actual LHP flow, type restoration happens via Jinja2 template rendering:
    1. Token substitution produces strings (e.g., "true", "1")
    2. Jinja2 renders these unquoted in the output YAML (e.g., `continuous: true`)
    3. When Databricks parses the final YAML, it correctly interprets types
    
    We convert booleans to lowercase ("true"/"false") specifically so they are
    recognized as booleans when rendered unquoted and then parsed by YAML.
    """

    def test_lowercase_boolean_strings_enable_yaml_type_restoration(self, tmp_path):
        """Test that lowercase boolean strings can be recognized as booleans by YAML."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  continuous: true
  photon: false
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        # After substitution, we have lowercase strings
        assert mgr.mappings["continuous"] == "true"
        assert mgr.mappings["photon"] == "false"
        
        # When rendered by Jinja2 template UNQUOTED and then parsed,
        # YAML recognizes them as booleans:
        simulated_yaml = "continuous: true\nphoton: false"  # Unquoted in template output
        parsed = yaml.safe_load(simulated_yaml)
        assert parsed["continuous"] is True
        assert parsed["photon"] is False
        assert isinstance(parsed["continuous"], bool)
        assert isinstance(parsed["photon"], bool)


class TestGlobalTokensConversion:
    """Test that global tokens are also converted to strings."""

    def test_global_tokens_converted(self, tmp_path):
        """Test that global tokens undergo type conversion."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
global:
  company: acme
  default_workers: 5
  enabled: true

dev:
  env: dev
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        # Global tokens should be converted
        assert mgr.mappings["company"] == "acme"
        assert mgr.mappings["default_workers"] == "5"
        assert mgr.mappings["enabled"] == "true"
        assert all(isinstance(v, str) for k, v in mgr.mappings.items() 
                  if k in ["company", "default_workers", "enabled"])

    def test_env_overrides_global_with_conversion(self, tmp_path):
        """Test that environment-specific values override global with type conversion."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
global:
  workers: 10
  debug: false

dev:
  workers: 2
  debug: true
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        # Dev values should override global
        assert mgr.mappings["workers"] == "2"  # Not "10"
        assert mgr.mappings["debug"] == "true"  # Not "false"
        assert isinstance(mgr.mappings["workers"], str)
        assert isinstance(mgr.mappings["debug"], str)


class TestEdgeCases:
    """Test edge cases in type conversion."""

    def test_none_value_conversion(self, tmp_path):
        """Test that None values are converted to string 'None'."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  null_value: null
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        # YAML null becomes Python None, should be converted to string
        assert "null_value" in mgr.mappings
        # Python None should be converted to "None"
        assert mgr.mappings["null_value"] == "None"

    def test_empty_string_preserved(self, tmp_path):
        """Test that empty strings are preserved."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  empty: ""
  non_empty: "value"
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        assert mgr.mappings["empty"] == ""
        assert mgr.mappings["non_empty"] == "value"
        assert isinstance(mgr.mappings["empty"], str)

    def test_numeric_strings_preserved(self, tmp_path):
        """Test that numeric strings (quoted in YAML) remain as strings."""
        sub_file = tmp_path / "sub.yaml"
        sub_file.write_text("""
dev:
  quoted_number: "123"
  unquoted_number: 123
""")
        
        mgr = EnhancedSubstitutionManager(sub_file, "dev")
        
        # Both should end up as strings
        assert mgr.mappings["quoted_number"] == "123"
        assert mgr.mappings["unquoted_number"] == "123"
        assert isinstance(mgr.mappings["quoted_number"], str)
        assert isinstance(mgr.mappings["unquoted_number"], str)

