"""Tests for secret code execution - verifying generated code works with actual secret values."""

import pytest
from unittest.mock import Mock

from lhp.utils.secret_code_generator import SecretCodeGenerator
from lhp.utils.substitution import SecretReference


class MockDbutils:
    """Mock dbutils object for testing secret calls."""
    
    def __init__(self, secrets_dict):
        """Initialize with a dictionary of secrets."""
        self.secrets = Mock()
        self.secrets.get = Mock(side_effect=lambda scope, key: secrets_dict.get(f"{scope}.{key}", f"MOCK_{scope}_{key}"))


class TestSecretCodeExecution:
    """Test that generated secret code executes correctly with mock secret values."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SecretCodeGenerator()
        
        # Mock secret values
        self.secret_values = {
            "dev_secrets.host": "localhost",
            "dev_secrets.port": "5432", 
            "dev_secrets.database": "testdb",
            "dev_secrets.username": "testuser",
            "dev_secrets.password": "testpass123",
            "prod_secrets.host": "prod-db.company.com",
            "prod_secrets.username": "prod_user",
            "prod_secrets.password": "prod_pass456"
        }
        
        # Create mock dbutils
        self.dbutils = MockDbutils(self.secret_values)

    def test_single_secret_f_string_execution(self):
        """Test execution of f-string with single secret."""
        # Generate code with secret placeholder
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:5432/mydb")'
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        generated_code = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string format
        expected_pattern = 'f"jdbc://{dbutils.secrets.get('
        assert expected_pattern in generated_code
        
        # Execute the generated code and verify result
        # Extract just the f-string part for execution
        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]
        
        # Create execution context with mock dbutils
        exec_context = {"dbutils": self.dbutils}
        result = eval(f_string_code, exec_context)
        
        # Verify the secret was resolved correctly
        assert result == "jdbc://localhost:5432/mydb"

    def test_multiple_secrets_f_string_execution(self):
        """Test execution of f-string with multiple secrets."""
        # Generate code with multiple secret placeholders
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/__SECRET_dev_secrets_database__")'
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port"),
            SecretReference("dev_secrets", "database")
        }
        
        generated_code = self.generator.generate_python_code(input_code, secret_refs)
        
        # Execute the generated code
        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]
        
        exec_context = {"dbutils": self.dbutils}
        result = eval(f_string_code, exec_context)
        
        # Verify all secrets were resolved correctly
        assert result == "jdbc://localhost:5432/testdb"

    def test_direct_dbutils_call_execution(self):
        """Test execution of direct dbutils call (entire string is secret)."""
        # Generate code where entire string is a secret
        input_code = '.option("password", "__SECRET_dev_secrets_password__")'
        secret_refs = {SecretReference("dev_secrets", "password")}
        
        generated_code = self.generator.generate_python_code(input_code, secret_refs)
        
        # Should be a direct dbutils call
        assert "dbutils.secrets.get" in generated_code
        assert "f\"" not in generated_code  # No f-string for entire secret
        
        # Extract the dbutils call more precisely
        start = generated_code.find('dbutils.secrets.get')
        # Find the matching closing parenthesis
        paren_count = 0
        pos = start
        while pos < len(generated_code):
            if generated_code[pos] == '(':
                paren_count += 1
            elif generated_code[pos] == ')':
                paren_count -= 1
                if paren_count == 0:
                    end = pos + 1
                    break
            pos += 1
        
        dbutils_call = generated_code[start:end]
        
        exec_context = {"dbutils": self.dbutils}
        result = eval(dbutils_call, exec_context)
        
        # Verify the secret was resolved correctly
        assert result == "testpass123"

    def test_complex_jdbc_url_execution(self):
        """Test execution of complex JDBC URL with multiple secrets."""
        # Generate complex JDBC connection string
        input_code = '.option("url", "jdbc:postgresql://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/__SECRET_dev_secrets_database__?user=__SECRET_dev_secrets_username__&password=__SECRET_dev_secrets_password__&ssl=true")'
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port"),
            SecretReference("dev_secrets", "database"),
            SecretReference("dev_secrets", "username"),
            SecretReference("dev_secrets", "password")
        }
        
        generated_code = self.generator.generate_python_code(input_code, secret_refs)
        
        # Execute the f-string
        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]
        
        exec_context = {"dbutils": self.dbutils}
        result = eval(f_string_code, exec_context)
        
        # Verify complete JDBC URL construction
        expected = "jdbc:postgresql://localhost:5432/testdb?user=testuser&password=testpass123&ssl=true"
        assert result == expected

    def test_mixed_single_and_multiple_secrets_execution(self):
        """Test execution of code with both single and multiple secret patterns."""
        # Generate code with mixed patterns
        input_code = '''spark.read \\
    .option("url", "jdbc://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/mydb") \\
    .option("user", "__SECRET_dev_secrets_username__") \\
    .option("password", "__SECRET_dev_secrets_password__")'''
        
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port"),
            SecretReference("dev_secrets", "username"),
            SecretReference("dev_secrets", "password")
        }
        
        generated_code = self.generator.generate_python_code(input_code, secret_refs)
        
        # Test the URL f-string (multiple secrets)
        url_start = generated_code.find('f"jdbc:')
        url_end = generated_code.find('mydb"') + 5
        url_f_string = generated_code[url_start:url_end]
        
        exec_context = {"dbutils": self.dbutils}
        url_result = eval(url_f_string, exec_context)
        assert url_result == "jdbc://localhost:5432/mydb"
        
        # Test the user dbutils call (single secret)
        user_calls = [line.strip() for line in generated_code.split('\n') if 'option("user"' in line]
        assert len(user_calls) == 1
        user_line = user_calls[0]
        
        # Extract dbutils call from user line
        start = user_line.find('dbutils.secrets.get')
        # Find the matching closing parenthesis
        paren_count = 0
        pos = start
        while pos < len(user_line):
            if user_line[pos] == '(':
                paren_count += 1
            elif user_line[pos] == ')':
                paren_count -= 1
                if paren_count == 0:
                    end = pos + 1
                    break
            pos += 1
        
        user_dbutils_call = user_line[start:end]
        
        user_result = eval(user_dbutils_call, exec_context)
        assert user_result == "testuser"

    def test_quote_handling_in_execution(self):
        """Test that intelligent quote selection doesn't break execution."""
        # Test double-quoted string (should use single quotes for dbutils)
        input_code_double = '.option("query", "SELECT * FROM users WHERE id=__SECRET_dev_secrets_username__")'
        secret_refs = {SecretReference("dev_secrets", "username")}
        
        generated_double = self.generator.generate_python_code(input_code_double, secret_refs)
        
        # Should use single quotes in dbutils call
        assert "scope='dev_secrets'" in generated_double
        assert "key='username'" in generated_double
        
        # Execute and verify
        start = generated_double.find('f"')
        end = generated_double.rfind('"') + 1
        f_string_code = generated_double[start:end]
        
        exec_context = {"dbutils": self.dbutils}
        result = eval(f_string_code, exec_context)
        assert result == "SELECT * FROM users WHERE id=testuser"
        
        # Test single-quoted string (should use double quotes for dbutils)
        input_code_single = ".option('query', 'SELECT * FROM users WHERE id=__SECRET_dev_secrets_username__')"
        
        generated_single = self.generator.generate_python_code(input_code_single, secret_refs)
        
        # Should use double quotes in dbutils call
        assert 'scope="dev_secrets"' in generated_single
        assert 'key="username"' in generated_single
        
        # Execute and verify
        start = generated_single.find("f'")
        end = generated_single.rfind("'") + 1
        f_string_code = generated_single[start:end]
        
        result = eval(f_string_code, exec_context)
        assert result == "SELECT * FROM users WHERE id=testuser"

    def test_error_handling_missing_secret(self):
        """Test behavior when a secret is not found."""
        # Create dbutils that returns a default for missing secrets
        missing_secrets_dbutils = MockDbutils({})  # Empty secrets dict
        
        input_code = '.option("url", "jdbc://__SECRET_missing_scope_missing_key__:5432/mydb")'
        secret_refs = {SecretReference("missing_scope", "missing_key")}
        
        generated_code = self.generator.generate_python_code(input_code, secret_refs)
        
        # Execute with missing secret
        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]
        
        exec_context = {"dbutils": missing_secrets_dbutils}
        result = eval(f_string_code, exec_context)
        
        # Should get the mock default value
        assert result == "jdbc://MOCK_missing_scope_missing_key:5432/mydb"

    def test_special_characters_in_secrets(self):
        """Test handling of special characters in secret values."""
        # Setup secrets with special characters
        special_secrets = {
            "test_secrets.special_pass": "p@$$w0rd!@#$%^&*()_+{}[]|\\:;\"'<>,.?/~`",
            "test_secrets.host": "db-server.company.com"
        }
        special_dbutils = MockDbutils(special_secrets)
        
        input_code = '.option("connectionString", "host=__SECRET_test_secrets_host__;password=__SECRET_test_secrets_special_pass__;ssl=true")'
        secret_refs = {
            SecretReference("test_secrets", "host"),
            SecretReference("test_secrets", "special_pass")
        }
        
        generated_code = self.generator.generate_python_code(input_code, secret_refs)
        
        # Execute with special characters
        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]
        
        exec_context = {"dbutils": special_dbutils}
        result = eval(f_string_code, exec_context)
        
        # Verify special characters are preserved
        expected = "host=db-server.company.com;password=p@$$w0rd!@#$%^&*()_+{}[]|\\:;\"'<>,.?/~`;ssl=true"
        assert result == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 