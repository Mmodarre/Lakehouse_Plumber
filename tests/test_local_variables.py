"""Tests for local variable resolution."""

import pytest
from lhp.utils.local_variables import LocalVariableResolver
from lhp.utils.error_formatter import LHPError


class TestBasicSubstitution:
    """Test basic variable substitution."""
    
    def test_simple_variable_substitution(self):
        """Test basic %{var} substitution."""
        resolver = LocalVariableResolver({"table": "customers"})
        result = resolver.resolve({"name": "%{table}"})
        assert result == {"name": "customers"}
    
    def test_inline_prefix(self):
        """Test prefix_%{var} substitution."""
        resolver = LocalVariableResolver({"table": "customers"})
        result = resolver.resolve({"name": "load_%{table}"})
        assert result == {"name": "load_customers"}
    
    def test_inline_suffix(self):
        """Test %{var}_suffix substitution."""
        resolver = LocalVariableResolver({"table": "customers"})
        result = resolver.resolve({"name": "%{table}_raw"})
        assert result == {"name": "customers_raw"}
    
    def test_inline_both_sides(self):
        """Test prefix_%{var}_suffix substitution."""
        resolver = LocalVariableResolver({"table": "customers"})
        result = resolver.resolve({"name": "v_%{table}_bronze"})
        assert result == {"name": "v_customers_bronze"}
    
    def test_multiple_vars_in_string(self):
        """Test multiple variables in one string."""
        resolver = LocalVariableResolver({"catalog": "main", "schema": "bronze"})
        result = resolver.resolve({"database": "%{catalog}.%{schema}"})
        assert result == {"database": "main.bronze"}
    
    def test_no_variables_defined(self):
        """Test with empty variables dict."""
        resolver = LocalVariableResolver({})
        result = resolver.resolve({"name": "static_value"})
        assert result == {"name": "static_value"}


class TestNestedStructures:
    """Test variable substitution in nested data structures."""
    
    def test_nested_dict(self):
        """Test substitution in nested dictionaries."""
        resolver = LocalVariableResolver({"table": "customers"})
        data = {
            "action": {
                "source": {
                    "table": "%{table}"
                }
            }
        }
        result = resolver.resolve(data)
        assert result["action"]["source"]["table"] == "customers"
    
    def test_list_of_strings(self):
        """Test substitution in lists."""
        resolver = LocalVariableResolver({"schema": "bronze"})
        data = {
            "schemas": ["%{schema}_a", "%{schema}_b", "%{schema}_c"]
        }
        result = resolver.resolve(data)
        assert result["schemas"] == ["bronze_a", "bronze_b", "bronze_c"]
    
    def test_deeply_nested(self):
        """Test substitution in deeply nested structures."""
        resolver = LocalVariableResolver({"entity": "customer"})
        data = {
            "level1": {
                "level2": {
                    "level3": [
                        {"name": "%{entity}_action"},
                        {"target": "v_%{entity}"}
                    ]
                }
            }
        }
        result = resolver.resolve(data)
        assert result["level1"]["level2"]["level3"][0]["name"] == "customer_action"
        assert result["level1"]["level2"]["level3"][1]["target"] == "v_customer"


class TestRecursiveVariables:
    """Test recursive variable references."""
    
    def test_variable_references_another(self):
        """Test variable that references another variable."""
        resolver = LocalVariableResolver({
            "schema": "bronze",
            "full_path": "%{schema}_customers"
        })
        result = resolver.resolve({"path": "%{full_path}"})
        assert result == {"path": "bronze_customers"}
    
    def test_chain_of_references(self):
        """Test chain of variable references."""
        resolver = LocalVariableResolver({
            "env": "dev",
            "catalog": "acme_%{env}",
            "full_db": "%{catalog}.bronze"
        })
        result = resolver.resolve({"database": "%{full_db}"})
        assert result == {"database": "acme_dev.bronze"}
    
    def test_circular_reference_detected(self):
        """Test that circular references don't cause infinite loops."""
        resolver = LocalVariableResolver({
            "a": "%{b}",
            "b": "%{a}"
        })
        # Should not hang - max iterations prevents infinite loop
        # Circular refs will remain unresolved and caught by validation
        # Since both a and b reference each other, they stay as %{b} and %{a}
        # When we try to use them, validation should catch the unresolved pattern
        with pytest.raises(LHPError) as exc_info:
            resolver.resolve({"value": "%{a}"})
        error = exc_info.value
        assert "Undefined local variable" in error.title or "b" in error.details


class TestUndefinedVariables:
    """Test strict validation of undefined variables."""
    
    def test_undefined_variable_raises_error(self):
        """Test that undefined variable raises LHPError."""
        resolver = LocalVariableResolver({"table": "customers"})
        with pytest.raises(LHPError) as exc_info:
            resolver.resolve({"name": "%{missing}"})
        
        error = exc_info.value
        assert "LHP-CFG-011" in error.code
        assert "Undefined local variable" in error.title
        assert "%{missing}" in error.details
    
    def test_multiple_undefined_variables(self):
        """Test error message with multiple undefined variables."""
        resolver = LocalVariableResolver({})
        with pytest.raises(LHPError) as exc_info:
            resolver.resolve({
                "name": "%{var1}",
                "target": "%{var2}"
            })
        
        error = exc_info.value
        assert "var1" in error.details or "var2" in error.details
    
    def test_error_includes_available_variables(self):
        """Test that error message lists available variables."""
        resolver = LocalVariableResolver({"table": "customers", "schema": "bronze"})
        with pytest.raises(LHPError) as exc_info:
            resolver.resolve({"name": "%{missing}"})
        
        error = exc_info.value
        # Check suggestions contain available variables
        suggestions_text = " ".join(error.suggestions)
        assert "table" in suggestions_text
        assert "schema" in suggestions_text
    
    def test_error_includes_path_information(self):
        """Test that error message includes path to undefined variable."""
        resolver = LocalVariableResolver({})
        with pytest.raises(LHPError) as exc_info:
            resolver.resolve({
                "actions": [
                    {"source": {"table": "%{undefined}"}}
                ]
            })
        
        error = exc_info.value
        # Should include path information
        assert "actions[0]" in error.details or "source" in error.details


class TestMixedSubstitution:
    """Test local variables alongside environment substitution."""
    
    def test_local_and_env_vars_together(self):
        """Test that local vars resolve but env vars are preserved."""
        resolver = LocalVariableResolver({"table": "customers"})
        result = resolver.resolve({
            "database": "{catalog}.{schema}",  # Env vars preserved
            "table": "%{table}"  # Local var resolved
        })
        assert result["database"] == "{catalog}.{schema}"  # Unchanged
        assert result["table"] == "customers"  # Resolved
    
    def test_mixed_in_same_string(self):
        """Test local and env vars in same string."""
        resolver = LocalVariableResolver({"entity": "customer"})
        result = resolver.resolve({
            "path": "{catalog}.{schema}.%{entity}"
        })
        # Local var resolved, env vars preserved
        assert result["path"] == "{catalog}.{schema}.customer"


class TestFlowgroupIsolation:
    """Test that variables are scoped to single flowgroup."""
    
    def test_separate_resolvers_dont_share_variables(self):
        """Test that each resolver has its own variable scope."""
        resolver1 = LocalVariableResolver({"table": "customers"})
        resolver2 = LocalVariableResolver({"table": "orders"})
        
        result1 = resolver1.resolve({"name": "%{table}"})
        result2 = resolver2.resolve({"name": "%{table}"})
        
        assert result1["name"] == "customers"
        assert result2["name"] == "orders"


class TestEdgeCases:
    """Test edge cases and special scenarios."""
    
    def test_empty_string_variable(self):
        """Test variable with empty string value."""
        resolver = LocalVariableResolver({"empty": ""})
        result = resolver.resolve({"name": "prefix_%{empty}_suffix"})
        assert result["name"] == "prefix__suffix"
    
    def test_numeric_values_in_variables(self):
        """Test that numeric values are converted to strings."""
        # Variables should be strings, but test robustness
        resolver = LocalVariableResolver({"table": "customers"})
        result = resolver.resolve({"count": 42})  # Non-string value
        assert result["count"] == 42  # Unchanged
    
    def test_variable_name_with_underscores(self):
        """Test variable names with underscores."""
        resolver = LocalVariableResolver({"source_table": "raw_customers"})
        result = resolver.resolve({"table": "%{source_table}"})
        assert result["table"] == "raw_customers"
    
    def test_variable_name_with_numbers(self):
        """Test variable names with numbers."""
        resolver = LocalVariableResolver({"table_v2": "customers_v2"})
        result = resolver.resolve({"table": "%{table_v2}"})
        assert result["table"] == "customers_v2"
    
    def test_percent_sign_without_braces(self):
        """Test that % without braces is not treated as variable."""
        resolver = LocalVariableResolver({"table": "customers"})
        result = resolver.resolve({"query": "SELECT * WHERE value > 50%"})
        assert result["query"] == "SELECT * WHERE value > 50%"
    
    def test_braces_without_percent(self):
        """Test that {var} without % is not treated as local variable."""
        resolver = LocalVariableResolver({"table": "customers"})
        result = resolver.resolve({"database": "{catalog}.{schema}"})
        # Environment substitution syntax should be preserved
        assert result["database"] == "{catalog}.{schema}"
    
    def test_none_variables_treated_as_empty(self):
        """Test that None variables parameter is treated as empty dict."""
        resolver = LocalVariableResolver(None)
        # Should work without errors, just no substitutions
        result = resolver.resolve({"name": "test"})
        assert result["name"] == "test"
    
    def test_boolean_values_pass_through(self):
        """Test that boolean values pass through unchanged."""
        resolver = LocalVariableResolver({"var": "value"})
        result = resolver.resolve({"flag": True, "disabled": False})
        assert result["flag"] is True
        assert result["disabled"] is False
    
    def test_none_values_pass_through(self):
        """Test that None values pass through unchanged."""
        resolver = LocalVariableResolver({"var": "value"})
        result = resolver.resolve({"optional": None})
        assert result["optional"] is None
    
    def test_empty_dict_resolves_without_error(self):
        """Test resolving empty dictionary."""
        resolver = LocalVariableResolver({"var": "value"})
        result = resolver.resolve({})
        assert result == {}
    
    def test_empty_list_resolves_without_error(self):
        """Test resolving empty list."""
        resolver = LocalVariableResolver({"var": "value"})
        result = resolver.resolve([])
        assert result == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
