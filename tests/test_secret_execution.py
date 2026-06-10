"""Tests for secret code execution."""

from unittest.mock import Mock

import pytest

from lhp.core.codegen.secret_code_generator import SecretCodeGenerator
from lhp.core.processing.substitution import SecretReference


class MockDbutils:
    def __init__(self, secrets_dict):
        self.secrets = Mock()
        self.secrets.get = Mock(
            side_effect=lambda scope, key: secrets_dict.get(
                f"{scope}.{key}", f"MOCK_{scope}_{key}"
            )
        )


class TestSecretCodeExecution:
    """Test that generated secret code executes correctly with mock secret values."""

    def setup_method(self):
        self.generator = SecretCodeGenerator()

        self.secret_values = {
            "dev_secrets.host": "localhost",
            "dev_secrets.port": "5432",
            "dev_secrets.database": "testdb",
            "dev_secrets.username": "testuser",
            "dev_secrets.password": "testpass123",
            "prod_secrets.host": "prod-db.company.com",
            "prod_secrets.username": "prod_user",
            "prod_secrets.password": "prod_pass456",
        }

        self.dbutils = MockDbutils(self.secret_values)

    def test_single_secret_f_string_execution(self):
        """Test execution of f-string with single secret."""
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:5432/mydb")'
        secret_refs = {SecretReference("dev_secrets", "host")}

        generated_code = self.generator.generate_python_code(input_code, secret_refs)

        expected_pattern = 'f"jdbc://{dbutils.secrets.get('
        assert expected_pattern in generated_code

        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]

        exec_context = {"dbutils": self.dbutils}
        result = eval(f_string_code, exec_context)

        assert result == "jdbc://localhost:5432/mydb"

    def test_multiple_secrets_f_string_execution(self):
        """Test execution of f-string with multiple secrets."""
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/__SECRET_dev_secrets_database__")'
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port"),
            SecretReference("dev_secrets", "database"),
        }

        generated_code = self.generator.generate_python_code(input_code, secret_refs)

        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]

        exec_context = {"dbutils": self.dbutils}
        result = eval(f_string_code, exec_context)

        assert result == "jdbc://localhost:5432/testdb"

    def test_direct_dbutils_call_execution(self):
        """Test execution of direct dbutils call (entire string is secret)."""
        input_code = '.option("password", "__SECRET_dev_secrets_password__")'
        secret_refs = {SecretReference("dev_secrets", "password")}

        generated_code = self.generator.generate_python_code(input_code, secret_refs)

        assert "dbutils.secrets.get" in generated_code
        assert 'f"' not in generated_code  # No f-string for entire secret

        start = generated_code.find("dbutils.secrets.get")
        # Find the matching closing parenthesis
        paren_count = 0
        pos = start
        while pos < len(generated_code):
            if generated_code[pos] == "(":
                paren_count += 1
            elif generated_code[pos] == ")":
                paren_count -= 1
                if paren_count == 0:
                    end = pos + 1
                    break
            pos += 1

        dbutils_call = generated_code[start:end]

        exec_context = {"dbutils": self.dbutils}
        result = eval(dbutils_call, exec_context)

        assert result == "testpass123"

    def test_complex_jdbc_url_execution(self):
        """Test execution of complex JDBC URL with multiple secrets."""
        input_code = '.option("url", "jdbc:postgresql://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/__SECRET_dev_secrets_database__?user=__SECRET_dev_secrets_username__&password=__SECRET_dev_secrets_password__&ssl=true")'
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port"),
            SecretReference("dev_secrets", "database"),
            SecretReference("dev_secrets", "username"),
            SecretReference("dev_secrets", "password"),
        }

        generated_code = self.generator.generate_python_code(input_code, secret_refs)

        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]

        exec_context = {"dbutils": self.dbutils}
        result = eval(f_string_code, exec_context)

        expected = "jdbc:postgresql://localhost:5432/testdb?user=testuser&password=testpass123&ssl=true"
        assert result == expected

    def test_mixed_single_and_multiple_secrets_execution(self):
        """Test execution of code with both single and multiple secret patterns."""
        input_code = """spark.read \\
    .option("url", "jdbc://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/mydb") \\
    .option("user", "__SECRET_dev_secrets_username__") \\
    .option("password", "__SECRET_dev_secrets_password__")"""

        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port"),
            SecretReference("dev_secrets", "username"),
            SecretReference("dev_secrets", "password"),
        }

        generated_code = self.generator.generate_python_code(input_code, secret_refs)

        url_start = generated_code.find('f"jdbc:')
        url_end = generated_code.find('mydb"') + 5
        url_f_string = generated_code[url_start:url_end]

        exec_context = {"dbutils": self.dbutils}
        url_result = eval(url_f_string, exec_context)
        assert url_result == "jdbc://localhost:5432/mydb"

        user_calls = [
            line.strip()
            for line in generated_code.split("\n")
            if 'option("user"' in line
        ]
        assert len(user_calls) == 1
        user_line = user_calls[0]

        start = user_line.find("dbutils.secrets.get")
        # Find the matching closing parenthesis
        paren_count = 0
        pos = start
        while pos < len(user_line):
            if user_line[pos] == "(":
                paren_count += 1
            elif user_line[pos] == ")":
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
        input_code_double = '.option("query", "SELECT * FROM users WHERE id=__SECRET_dev_secrets_username__")'
        secret_refs = {SecretReference("dev_secrets", "username")}

        generated_double = self.generator.generate_python_code(
            input_code_double, secret_refs
        )

        assert "scope='dev_secrets'" in generated_double
        assert "key='username'" in generated_double

        start = generated_double.find('f"')
        end = generated_double.rfind('"') + 1
        f_string_code = generated_double[start:end]

        exec_context = {"dbutils": self.dbutils}
        result = eval(f_string_code, exec_context)
        assert result == "SELECT * FROM users WHERE id=testuser"

        input_code_single = ".option('query', 'SELECT * FROM users WHERE id=__SECRET_dev_secrets_username__')"

        generated_single = self.generator.generate_python_code(
            input_code_single, secret_refs
        )

        assert 'scope="dev_secrets"' in generated_single
        assert 'key="username"' in generated_single

        start = generated_single.find("f'")
        end = generated_single.rfind("'") + 1
        f_string_code = generated_single[start:end]

        result = eval(f_string_code, exec_context)
        assert result == "SELECT * FROM users WHERE id=testuser"

    def test_error_handling_missing_secret(self):
        """Test behavior when a secret is not found."""
        missing_secrets_dbutils = MockDbutils({})

        input_code = (
            '.option("url", "jdbc://__SECRET_missing_scope_missing_key__:5432/mydb")'
        )
        secret_refs = {SecretReference("missing_scope", "missing_key")}

        generated_code = self.generator.generate_python_code(input_code, secret_refs)

        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]

        exec_context = {"dbutils": missing_secrets_dbutils}
        result = eval(f_string_code, exec_context)

        assert result == "jdbc://MOCK_missing_scope_missing_key:5432/mydb"

    def test_special_characters_in_secrets(self):
        """Test handling of special characters in secret values."""
        special_secrets = {
            "test_secrets.special_pass": "p@$$w0rd!@#$%^&*()_+{}[]|\\:;\"'<>,.?/~`",
            "test_secrets.host": "db-server.company.com",
        }
        special_dbutils = MockDbutils(special_secrets)

        input_code = '.option("connectionString", "host=__SECRET_test_secrets_host__;password=__SECRET_test_secrets_special_pass__;ssl=true")'
        secret_refs = {
            SecretReference("test_secrets", "host"),
            SecretReference("test_secrets", "special_pass"),
        }

        generated_code = self.generator.generate_python_code(input_code, secret_refs)

        start = generated_code.find('f"')
        end = generated_code.rfind('"') + 1
        f_string_code = generated_code[start:end]

        exec_context = {"dbutils": special_dbutils}
        result = eval(f_string_code, exec_context)

        expected = "host=db-server.company.com;password=p@$$w0rd!@#$%^&*()_+{}[]|\\:;\"'<>,.?/~`;ssl=true"
        assert result == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
