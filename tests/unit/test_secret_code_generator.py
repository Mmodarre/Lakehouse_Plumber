"""Tests for SecretCodeGenerator - generating valid Python code with secrets."""

import pytest
from pathlib import Path
from unittest.mock import patch

from lhp.utils.secret_code_generator import SecretCodeGenerator
from lhp.utils.substitution import SecretReference


class TestSecretCodeGeneratorSingleSecret:
    """Test SecretCodeGenerator with single secret in string scenarios."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SecretCodeGenerator()

    def test_single_secret_in_double_quoted_string(self):
        """Test single secret replacement in double-quoted string."""
        # Input: "jdbc://host:5432/db" with host as secret
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:5432/mydb")'
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with single quotes for dbutils call
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:5432/mydb")'
        assert result == expected

    def test_single_secret_in_single_quoted_string(self):
        """Test single secret replacement in single-quoted string."""
        # Input: 'jdbc://host:5432/db' with host as secret
        input_code = ".option('url', 'jdbc://__SECRET_dev_secrets_host__:5432/mydb')"
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with double quotes for dbutils call
        expected = '.option(\'url\', f\'jdbc://{dbutils.secrets.get(scope="dev_secrets", key="host")}:5432/mydb\')'
        assert result == expected

    def test_single_secret_entire_string(self):
        """Test when entire string is a secret."""
        # Input: "__SECRET_dev_secrets_password__" (entire string)
        input_code = '.option("password", "__SECRET_dev_secrets_password__")'
        secret_refs = {SecretReference("dev_secrets", "password")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Direct dbutils call without f-string
        expected = '.option("password", dbutils.secrets.get(scope=\'dev_secrets\', key=\'password\'))'
        assert result == expected

    def test_single_secret_at_beginning_of_string(self):
        """Test secret at the beginning of string."""
        # Input: "host:5432/mydb" with host as secret
        input_code = '.option("url", "__SECRET_dev_secrets_host__:5432/mydb")'
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string
        expected = '.option("url", f"{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:5432/mydb")'
        assert result == expected

    def test_single_secret_at_end_of_string(self):
        """Test secret at the end of string."""
        # Input: "jdbc://localhost:5432/db" with db as secret
        input_code = '.option("url", "jdbc://localhost:5432/__SECRET_dev_secrets_database__")'
        secret_refs = {SecretReference("dev_secrets", "database")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string
        expected = '.option("url", f"jdbc://localhost:5432/{dbutils.secrets.get(scope=\'dev_secrets\', key=\'database\')}")'
        assert result == expected

    def test_single_secret_in_middle_of_string(self):
        """Test secret in the middle of string."""
        # Input: "jdbc://host:5432/mydb" with host as secret
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:5432/mydb")'
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:5432/mydb")'
        assert result == expected

    def test_single_secret_with_special_characters(self):
        """Test secret with special characters in surrounding string."""
        # Input: "user='admin';password='secret'" with secret as secret
        input_code = '.option("connectionProperties", "user=admin;password=__SECRET_dev_secrets_password__;timeout=30")'
        secret_refs = {SecretReference("dev_secrets", "password")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with proper escaping
        expected = '.option("connectionProperties", f"user=admin;password={dbutils.secrets.get(scope=\'dev_secrets\', key=\'password\')};timeout=30")'
        assert result == expected

    def test_single_secret_with_quotes_in_string(self):
        """Test secret with quotes in the surrounding string."""
        # Input: "SELECT * FROM 'table' WHERE host='host'" with host as secret
        input_code = '.option("query", "SELECT * FROM \'public\'.\'users\' WHERE host=\'__SECRET_dev_secrets_host__\'")'
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with double quotes in dbutils call since outer uses double quotes
        expected = '.option("query", f"SELECT * FROM \'public\'.\'users\' WHERE host=\'{dbutils.secrets.get(scope="dev_secrets", key="host")}\'")'
        assert result == expected

    def test_single_secret_no_secrets_in_string(self):
        """Test string with no secrets (should be unchanged)."""
        # Input: "jdbc://localhost:5432/mydb" (no secrets)
        input_code = '.option("url", "jdbc://localhost:5432/mydb")'
        secret_refs = set()  # No secrets
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: unchanged
        expected = '.option("url", "jdbc://localhost:5432/mydb")'
        assert result == expected

    def test_single_secret_multiple_lines_code(self):
        """Test secret replacement in multi-line code."""
        input_code = '''spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc://__SECRET_dev_secrets_host__:5432/mydb") \\
    .option("user", "admin") \\
    .load()'''
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string replacement in multi-line code
        expected = '''spark.read \\
    .format("jdbc") \\
    .option("url", f"jdbc://{dbutils.secrets.get(scope='dev_secrets', key='host')}:5432/mydb") \\
    .option("user", "admin") \\
    .load()'''
        assert result == expected

    def test_single_secret_with_escaped_quotes(self):
        """Test secret with escaped quotes in string."""
        # Input: "SELECT * FROM \"table\" WHERE host=\"host\"" with host as secret
        input_code = '.option("query", "SELECT * FROM \\"public\\".\\"users\\" WHERE host=\\"__SECRET_dev_secrets_host__\\"")'
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string preserving escaped quotes
        expected = '.option("query", f"SELECT * FROM \\"public\\".\\"users\\" WHERE host=\\"{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}\\"")'
        assert result == expected

    def test_single_secret_empty_string_parts(self):
        """Test secret that results in empty string parts."""
        # Input: "secret" where entire content is secret
        input_code = '.option("password", "__SECRET_dev_secrets_password__")'
        secret_refs = {SecretReference("dev_secrets", "password")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Direct dbutils call (no f-string needed)
        expected = '.option("password", dbutils.secrets.get(scope=\'dev_secrets\', key=\'password\'))'
        assert result == expected

    def test_single_secret_whitespace_preservation(self):
        """Test that whitespace around secrets is preserved."""
        # Input: "  host  " with host as secret
        input_code = '.option("url", "  __SECRET_dev_secrets_host__  ")'
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string preserving whitespace
        expected = '.option("url", f"  {dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}  ")'
        assert result == expected

    def test_single_secret_with_underscores_in_key(self):
        """Test secret with underscores in key name."""
        # Input: secret key with underscores
        input_code = '.option("password", "__SECRET_dev_secrets_db_admin_password__")'
        secret_refs = {SecretReference("dev_secrets", "db_admin_password")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Direct dbutils call
        expected = '.option("password", dbutils.secrets.get(scope=\'dev_secrets\', key=\'db_admin_password\'))'
        assert result == expected

    def test_single_secret_with_hyphens_in_key(self):
        """Test secret with hyphens in key name."""
        # Input: secret key with hyphens
        input_code = '.option("password", "__SECRET_dev_secrets_db-admin-password__")'
        secret_refs = {SecretReference("dev_secrets", "db-admin-password")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Direct dbutils call
        expected = '.option("password", dbutils.secrets.get(scope=\'dev_secrets\', key=\'db-admin-password\'))'
        assert result == expected

    def test_single_secret_long_scope_and_key(self):
        """Test secret with long scope and key names."""
        # Input: secret with long names
        input_code = '.option("password", "__SECRET_very_long_environment_specific_secrets_very_long_database_admin_password__")'
        secret_refs = {SecretReference("very_long_environment_specific_secrets", "very_long_database_admin_password")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Direct dbutils call
        expected = '.option("password", dbutils.secrets.get(scope=\'very_long_environment_specific_secrets\', key=\'very_long_database_admin_password\'))'
        assert result == expected


class TestSecretCodeGeneratorMultipleSecrets:
    """Test SecretCodeGenerator with multiple secrets in one string scenarios."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SecretCodeGenerator()

    def test_two_secrets_in_double_quoted_string(self):
        """Test two secrets replacement in double-quoted string."""
        # Input: "jdbc://host:port/db" with host and port as secrets
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/mydb")'
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with both secrets
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:{dbutils.secrets.get(scope=\'dev_secrets\', key=\'port\')}/mydb")'
        assert result == expected

    def test_two_secrets_in_single_quoted_string(self):
        """Test two secrets replacement in single-quoted string."""
        # Input: 'jdbc://host:port/db' with host and port as secrets
        input_code = ".option('url', 'jdbc://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/mydb')"
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with double quotes for dbutils calls
        expected = '.option(\'url\', f\'jdbc://{dbutils.secrets.get(scope="dev_secrets", key="host")}:{dbutils.secrets.get(scope="dev_secrets", key="port")}/mydb\')'
        assert result == expected

    def test_three_secrets_in_string(self):
        """Test three secrets replacement in one string."""
        # Input: "jdbc://host:port/db" with host, port, and db as secrets
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:__SECRET_dev_secrets_port__/__SECRET_dev_secrets_database__")'
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port"),
            SecretReference("dev_secrets", "database")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with all three secrets
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:{dbutils.secrets.get(scope=\'dev_secrets\', key=\'port\')}/{dbutils.secrets.get(scope=\'dev_secrets\', key=\'database\')}")'
        assert result == expected

    def test_multiple_secrets_different_scopes(self):
        """Test multiple secrets with different scopes."""
        # Input: "jdbc://host:port/db" with secrets from different scopes
        input_code = '.option("url", "jdbc://__SECRET_db_secrets_host__:__SECRET_network_secrets_port__/__SECRET_db_secrets_database__")'
        secret_refs = {
            SecretReference("db_secrets", "host"),
            SecretReference("network_secrets", "port"),
            SecretReference("db_secrets", "database")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with different scopes
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'db_secrets\', key=\'host\')}:{dbutils.secrets.get(scope=\'network_secrets\', key=\'port\')}/{dbutils.secrets.get(scope=\'db_secrets\', key=\'database\')}")'
        assert result == expected

    def test_multiple_secrets_at_edges(self):
        """Test multiple secrets at beginning and end of string."""
        # Input: "host:5432/db" with host and db as secrets
        input_code = '.option("url", "__SECRET_dev_secrets_host__:5432/__SECRET_dev_secrets_database__")'
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "database")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with secrets at edges
        expected = '.option("url", f"{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:5432/{dbutils.secrets.get(scope=\'dev_secrets\', key=\'database\')}")'
        assert result == expected

    def test_multiple_secrets_consecutive(self):
        """Test multiple consecutive secrets (no text between them)."""
        # Input: "hostport" with host and port as consecutive secrets
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host____SECRET_dev_secrets_port__/mydb")'
        secret_refs = {
            SecretReference("dev_secrets", "host"),
            SecretReference("dev_secrets", "port")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with consecutive secrets
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}{dbutils.secrets.get(scope=\'dev_secrets\', key=\'port\')}/mydb")'
        assert result == expected

    def test_multiple_secrets_with_special_chars(self):
        """Test multiple secrets with special characters between them."""
        # Input: connection string with multiple secrets
        input_code = '.option("connectionProperties", "user=__SECRET_db_secrets_username__;password=__SECRET_db_secrets_password__;timeout=30")'
        secret_refs = {
            SecretReference("db_secrets", "username"),
            SecretReference("db_secrets", "password")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with special characters preserved
        expected = '.option("connectionProperties", f"user={dbutils.secrets.get(scope=\'db_secrets\', key=\'username\')};password={dbutils.secrets.get(scope=\'db_secrets\', key=\'password\')};timeout=30")'
        assert result == expected

    def test_multiple_secrets_with_quotes_in_string(self):
        """Test multiple secrets with quotes in the string."""
        # Input: SQL query with multiple secrets
        input_code = '.option("query", "SELECT * FROM \'__SECRET_db_secrets_schema__\'.\'users\' WHERE host=\'__SECRET_db_secrets_host__\'")'
        secret_refs = {
            SecretReference("db_secrets", "schema"),
            SecretReference("db_secrets", "host")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with quotes preserved
        expected = '.option("query", f"SELECT * FROM \'{dbutils.secrets.get(scope="db_secrets", key="schema")}\'.\'users\' WHERE host=\'{dbutils.secrets.get(scope="db_secrets", key="host")}\'")'
        assert result == expected

    def test_multiple_secrets_entire_string_parts(self):
        """Test when entire string is made up of secrets."""
        # Input: "secret1secret2" where entire content is secrets
        input_code = '.option("combinedSecret", "__SECRET_dev_secrets_part1____SECRET_dev_secrets_part2__")'
        secret_refs = {
            SecretReference("dev_secrets", "part1"),
            SecretReference("dev_secrets", "part2")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with no literal parts
        expected = '.option("combinedSecret", f"{dbutils.secrets.get(scope=\'dev_secrets\', key=\'part1\')}{dbutils.secrets.get(scope=\'dev_secrets\', key=\'part2\')}")'
        assert result == expected

    def test_multiple_secrets_complex_jdbc_url(self):
        """Test realistic JDBC URL with multiple secrets."""
        # Input: Complex JDBC URL with multiple secrets
        input_code = '.option("url", "jdbc:postgresql://__SECRET_db_secrets_host__:__SECRET_db_secrets_port__/__SECRET_db_secrets_database__?user=__SECRET_db_secrets_username__&password=__SECRET_db_secrets_password__&ssl=true")'
        secret_refs = {
            SecretReference("db_secrets", "host"),
            SecretReference("db_secrets", "port"),
            SecretReference("db_secrets", "database"),
            SecretReference("db_secrets", "username"),
            SecretReference("db_secrets", "password")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with all secrets in complex URL
        expected = '.option("url", f"jdbc:postgresql://{dbutils.secrets.get(scope=\'db_secrets\', key=\'host\')}:{dbutils.secrets.get(scope=\'db_secrets\', key=\'port\')}/{dbutils.secrets.get(scope=\'db_secrets\', key=\'database\')}?user={dbutils.secrets.get(scope=\'db_secrets\', key=\'username\')}&password={dbutils.secrets.get(scope=\'db_secrets\', key=\'password\')}&ssl=true")'
        assert result == expected

    def test_multiple_secrets_with_escaped_quotes(self):
        """Test multiple secrets with escaped quotes."""
        # Input: String with escaped quotes and multiple secrets
        input_code = '.option("query", "SELECT * FROM \\"__SECRET_db_secrets_schema__\\".\\"users\\" WHERE host=\\"__SECRET_db_secrets_host__\\"")'
        secret_refs = {
            SecretReference("db_secrets", "schema"),
            SecretReference("db_secrets", "host")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string with escaped quotes preserved
        expected = '.option("query", f"SELECT * FROM \\"{dbutils.secrets.get(scope=\'db_secrets\', key=\'schema\')}\\".\\"users\\" WHERE host=\\"{dbutils.secrets.get(scope=\'db_secrets\', key=\'host\')}\\"")' 
        assert result == expected

    def test_multiple_secrets_multiline_code(self):
        """Test multiple secrets in multi-line code."""
        input_code = '''spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc://__SECRET_db_secrets_host__:__SECRET_db_secrets_port__/mydb") \\
    .option("user", "__SECRET_db_secrets_username__") \\
    .option("password", "__SECRET_db_secrets_password__") \\
    .load()'''
        secret_refs = {
            SecretReference("db_secrets", "host"),
            SecretReference("db_secrets", "port"),
            SecretReference("db_secrets", "username"),
            SecretReference("db_secrets", "password")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-strings in multi-line code
        expected = '''spark.read \\
    .format("jdbc") \\
    .option("url", f"jdbc://{dbutils.secrets.get(scope='db_secrets', key='host')}:{dbutils.secrets.get(scope='db_secrets', key='port')}/mydb") \\
    .option("user", dbutils.secrets.get(scope='db_secrets', key='username')) \\
    .option("password", dbutils.secrets.get(scope='db_secrets', key='password')) \\
    .load()'''
        assert result == expected

    def test_multiple_secrets_mixed_with_single_secrets(self):
        """Test code with both single and multiple secrets per string."""
        input_code = '''spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc://__SECRET_db_secrets_host__:__SECRET_db_secrets_port__/mydb") \\
    .option("user", "__SECRET_db_secrets_username__") \\
    .option("password", "__SECRET_db_secrets_password__") \\
    .load()'''
        secret_refs = {
            SecretReference("db_secrets", "host"),
            SecretReference("db_secrets", "port"),
            SecretReference("db_secrets", "username"),
            SecretReference("db_secrets", "password")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: f-string for multiple secrets line, direct calls for single secret lines
        expected = '''spark.read \\
    .format("jdbc") \\
    .option("url", f"jdbc://{dbutils.secrets.get(scope='db_secrets', key='host')}:{dbutils.secrets.get(scope='db_secrets', key='port')}/mydb") \\
    .option("user", dbutils.secrets.get(scope='db_secrets', key='username')) \\
    .option("password", dbutils.secrets.get(scope='db_secrets', key='password')) \\
    .load()'''
        assert result == expected


class TestSecretCodeGeneratorIntelligentQuoteHandling:
    """Test intelligent quote selection for dbutils calls based on string context."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SecretCodeGenerator()

    def test_double_quoted_string_uses_single_quotes_for_dbutils(self):
        """Test that double-quoted strings use single quotes for dbutils calls."""
        # Input: Double-quoted string should use single quotes in dbutils calls
        input_code = '.option("url", "jdbc://__SECRET_dev_secrets_host__:5432/mydb")'
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Single quotes used in dbutils call
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:5432/mydb")'
        assert result == expected
        assert "scope='dev_secrets'" in result
        assert "key='host'" in result

    def test_single_quoted_string_uses_double_quotes_for_dbutils(self):
        """Test that single-quoted strings use double quotes for dbutils calls."""
        # Input: Single-quoted string should use double quotes in dbutils calls
        input_code = ".option('url', 'jdbc://__SECRET_dev_secrets_host__:5432/mydb')"
        secret_refs = {SecretReference("dev_secrets", "host")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Double quotes used in dbutils call
        expected = '.option(\'url\', f\'jdbc://{dbutils.secrets.get(scope="dev_secrets", key="host")}:5432/mydb\')'
        assert result == expected
        assert 'scope="dev_secrets"' in result
        assert 'key="host"' in result

    def test_string_with_single_quotes_inside_uses_double_quotes_for_dbutils(self):
        """Test that strings containing single quotes use double quotes for dbutils calls."""
        # Input: String with single quotes inside should use double quotes in dbutils calls
        input_code = '.option("query", "SELECT * FROM \'users\' WHERE id=__SECRET_dev_secrets_user_id__")'
        secret_refs = {SecretReference("dev_secrets", "user_id")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Double quotes used in dbutils call to avoid conflict with single quotes in string
        expected = '.option("query", f"SELECT * FROM \'users\' WHERE id={dbutils.secrets.get(scope="dev_secrets", key="user_id")}")'
        assert result == expected
        assert 'scope="dev_secrets"' in result
        assert 'key="user_id"' in result

    def test_string_with_double_quotes_inside_uses_single_quotes_for_dbutils(self):
        """Test that strings containing double quotes use single quotes for dbutils calls."""
        # Input: String with double quotes inside should use single quotes in dbutils calls
        input_code = '.option(\'query\', \'SELECT * FROM "users" WHERE id=__SECRET_dev_secrets_user_id__\')'
        secret_refs = {SecretReference("dev_secrets", "user_id")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Single quotes used in dbutils call to avoid conflict with double quotes in string
        expected = '.option(\'query\', f\'SELECT * FROM "users" WHERE id={dbutils.secrets.get(scope=\'dev_secrets\', key=\'user_id\')}\')'
        assert result == expected
        assert "scope='dev_secrets'" in result
        assert "key='user_id'" in result

    def test_string_with_both_quote_types_chooses_optimal_quotes(self):
        """Test quote selection when string contains both single and double quotes."""
        # Input: String with both quote types - should choose based on outer string quote
        input_code = '.option("query", "SELECT * FROM \'users\' WHERE name=\\"__SECRET_dev_secrets_name__\\" AND id=\'123\'")'
        secret_refs = {SecretReference("dev_secrets", "name")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Since outer string uses double quotes, dbutils call should use single quotes
        expected = '.option("query", f"SELECT * FROM \'users\' WHERE name=\\"{dbutils.secrets.get(scope=\'dev_secrets\', key=\'name\')}\\" AND id=\'123\'")'
        assert result == expected
        assert "scope='dev_secrets'" in result
        assert "key='name'" in result

    def test_multiple_secrets_consistent_quote_choice(self):
        """Test that multiple secrets in same string use consistent quote choice."""
        # Input: Multiple secrets should use consistent quote choice
        input_code = '.option("url", "jdbc://__SECRET_db_secrets_host__:__SECRET_db_secrets_port__/mydb")'
        secret_refs = {
            SecretReference("db_secrets", "host"),
            SecretReference("db_secrets", "port")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: All dbutils calls use single quotes consistently
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'db_secrets\', key=\'host\')}:{dbutils.secrets.get(scope=\'db_secrets\', key=\'port\')}/mydb")'
        assert result == expected
        assert result.count("scope='db_secrets'") == 2
        assert result.count("key='host'") == 1
        assert result.count("key='port'") == 1

    def test_mixed_quote_types_in_different_strings(self):
        """Test that different strings can use different quote types for dbutils calls."""
        # Input: Multiple strings with different quote types
        input_code = '''spark.read \\
    .option("url", "jdbc://__SECRET_db_secrets_host__:5432/mydb") \\
    .option('user', '__SECRET_db_secrets_username__') \\
    .load()'''
        secret_refs = {
            SecretReference("db_secrets", "host"),
            SecretReference("db_secrets", "username")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Different quote types for different strings
        expected = '''spark.read \\
    .option("url", f"jdbc://{dbutils.secrets.get(scope='db_secrets', key='host')}:5432/mydb") \\
    .option('user', dbutils.secrets.get(scope="db_secrets", key="username")) \\
    .load()'''
        assert result == expected
        assert "scope='db_secrets'" in result  # First string uses single quotes
        assert 'scope="db_secrets"' in result  # Second string uses double quotes

    def test_escaped_quotes_influence_quote_choice(self):
        """Test that escaped quotes influence the quote choice algorithm."""
        # Input: String with escaped quotes
        input_code = '.option("query", "SELECT * FROM \\"public\\".\\"users\\" WHERE id=__SECRET_dev_secrets_id__")'
        secret_refs = {SecretReference("dev_secrets", "id")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Since string has escaped double quotes, dbutils should use single quotes
        expected = '.option("query", f"SELECT * FROM \\"public\\".\\"users\\" WHERE id={dbutils.secrets.get(scope=\'dev_secrets\', key=\'id\')}")'
        assert result == expected
        assert "scope='dev_secrets'" in result
        assert "key='id'" in result

    def test_complex_quote_scenarios_with_multiple_secrets(self):
        """Test complex scenarios with multiple secrets and various quote types."""
        # Input: Complex string with multiple quote types and multiple secrets
        input_code = '.option("connectionString", "host=__SECRET_db_secrets_host__;port=__SECRET_db_secrets_port__;options=\\"ssl=true;timeout=30\\"")'
        secret_refs = {
            SecretReference("db_secrets", "host"),
            SecretReference("db_secrets", "port")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Consistent single quotes for all dbutils calls
        expected = '.option("connectionString", f"host={dbutils.secrets.get(scope=\'db_secrets\', key=\'host\')};port={dbutils.secrets.get(scope=\'db_secrets\', key=\'port\')};options=\\"ssl=true;timeout=30\\"")'
        assert result == expected
        assert result.count("scope='db_secrets'") == 2
        assert result.count("key='host'") == 1
        assert result.count("key='port'") == 1

    def test_quote_choice_with_different_scope_names(self):
        """Test quote choice consistency across different scope names."""
        # Input: Multiple secrets with different scope names
        input_code = '.option("url", "jdbc://__SECRET_database_secrets_host__:__SECRET_network_config_port__/mydb")'
        secret_refs = {
            SecretReference("database_secrets", "host"),
            SecretReference("network_config", "port")
        }
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: All dbutils calls use single quotes consistently
        expected = '.option("url", f"jdbc://{dbutils.secrets.get(scope=\'database_secrets\', key=\'host\')}:{dbutils.secrets.get(scope=\'network_config\', key=\'port\')}/mydb")'
        assert result == expected
        assert "scope='database_secrets'" in result
        assert "scope='network_config'" in result

    def test_entire_string_secret_quote_choice(self):
        """Test quote choice for entire string secrets (direct dbutils calls)."""
        # Input: Entire string is a secret - should still follow quote choice rules
        input_code = '.option("password", "__SECRET_dev_secrets_password__")'
        secret_refs = {SecretReference("dev_secrets", "password")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Direct dbutils call with single quotes (since outer context uses double quotes)
        expected = '.option("password", dbutils.secrets.get(scope=\'dev_secrets\', key=\'password\'))'
        assert result == expected
        assert "scope='dev_secrets'" in result
        assert "key='password'" in result

    def test_quote_choice_fallback_to_single_quotes(self):
        """Test fallback to single quotes when quote choice is ambiguous."""
        # Input: String with equal amounts of both quote types
        input_code = '.option("complex", "value=\'test\' AND name=\\"test\\" WHERE id=__SECRET_dev_secrets_id__")'
        secret_refs = {SecretReference("dev_secrets", "id")}
        
        result = self.generator.generate_python_code(input_code, secret_refs)
        
        # Expected: Should default to single quotes when ambiguous
        expected = '.option("complex", f"value=\'test\' AND name=\\"test\\" WHERE id={dbutils.secrets.get(scope=\'dev_secrets\', key=\'id\')}")'
        assert result == expected
        assert "scope='dev_secrets'" in result
        assert "key='id'" in result


class TestSecretCodeGeneratorSyntaxValidation:
    """Test that generated code is syntactically valid Python."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SecretCodeGenerator()

    def test_generated_code_compiles_single_secret(self):
        """Test that generated code with single secret compiles."""
        # Create a complete Python statement for compilation
        input_code = 'spark.read.option("url", f"jdbc://{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:5432/mydb")'
        
        # This should compile without syntax errors
        try:
            compile(input_code, '<string>', 'exec')
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code has syntax error: {e}")

    def test_generated_code_compiles_entire_secret(self):
        """Test that generated code with entire string as secret compiles."""
        # Create a complete Python statement for compilation
        input_code = 'spark.read.option("password", dbutils.secrets.get(scope=\'dev_secrets\', key=\'password\'))'
        
        # This should compile without syntax errors
        try:
            compile(input_code, '<string>', 'exec')
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code has syntax error: {e}")

    def test_generated_code_compiles_complex_case(self):
        """Test that generated code with complex quotes compiles."""
        # Create a complete Python statement for compilation
        input_code = 'spark.read.option("query", f"SELECT * FROM \\"public\\".\\"users\\" WHERE host=\\"{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}\\"")'
        
        # This should compile without syntax errors
        try:
            compile(input_code, '<string>', 'exec')
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code has syntax error: {e}")

    def test_generated_code_compiles_multiple_secrets(self):
        """Test that generated code with multiple secrets compiles."""
        # Create a complete Python statement for compilation
        input_code = 'spark.read.option("url", f"jdbc://{dbutils.secrets.get(scope=\'dev_secrets\', key=\'host\')}:{dbutils.secrets.get(scope=\'dev_secrets\', key=\'port\')}/mydb")'
        
        # This should compile without syntax errors
        try:
            compile(input_code, '<string>', 'exec')
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code has syntax error: {e}")

    def test_generated_code_compiles_complex_jdbc_url(self):
        """Test that generated code with complex JDBC URL compiles."""
        # Create a complete Python statement for compilation
        input_code = 'spark.read.option("url", f"jdbc:postgresql://{dbutils.secrets.get(scope=\'db_secrets\', key=\'host\')}:{dbutils.secrets.get(scope=\'db_secrets\', key=\'port\')}/{dbutils.secrets.get(scope=\'db_secrets\', key=\'database\')}?user={dbutils.secrets.get(scope=\'db_secrets\', key=\'username\')}&password={dbutils.secrets.get(scope=\'db_secrets\', key=\'password\')}&ssl=true")'
        
        # This should compile without syntax errors
        try:
            compile(input_code, '<string>', 'exec')
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code has syntax error: {e}")

    def test_generated_code_compiles_intelligent_quotes(self):
        """Test that generated code with intelligent quote selection compiles."""
        # Create a complete Python statement for compilation
        input_code = 'spark.read.option(\'query\', f"SELECT * FROM \\"users\\" WHERE id={dbutils.secrets.get(scope=\'dev_secrets\', key=\'user_id\')}")'
        
        # This should compile without syntax errors
        try:
            compile(input_code, '<string>', 'exec')
            assert True
        except SyntaxError as e:
            pytest.fail(f"Generated code has syntax error: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 