"""Tests for Python parser utility."""

import pytest
from unittest.mock import patch, MagicMock
from lhp.utils.python_parser import PythonParser, extract_tables_from_python, extract_sql_from_python


class TestPythonParser:
    """Test Python parser functionality."""

    def setup_method(self):
        """Set up test instance."""
        self.parser = PythonParser()

    def test_basic_spark_sql_call(self):
        """Test basic spark.sql() call extraction."""
        python_code = """
        spark.sql("SELECT * FROM bronze.customers")
        """
        result = self.parser.extract_tables_from_python(python_code)
        assert result == ["bronze.customers"]

    def test_multiple_spark_sql_calls(self):
        """Test multiple spark.sql() calls in the same code."""
        python_code = """
        df1 = spark.sql("SELECT * FROM bronze.customers")
        df2 = spark.sql("SELECT * FROM silver.orders")
        result = spark.sql("SELECT c.*, o.* FROM gold.customer_summary c JOIN silver.products p")
        """
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["bronze.customers", "gold.customer_summary", "silver.orders", "silver.products"]

    def test_spark_table_method(self):
        """Test spark.table() method detection."""
        python_code = """
        df = spark.table("bronze.customers")
        """
        result = self.parser.extract_tables_from_python(python_code)
        assert result == ["bronze.customers"]

    def test_spark_read_table_method(self):
        """Test spark.read.table() method detection."""
        python_code = """
        df = spark.read.table("silver.processed_orders")
        """
        result = self.parser.extract_tables_from_python(python_code)
        assert result == ["silver.processed_orders"]

    def test_catalog_methods(self):
        """Test spark.catalog methods that reference tables."""
        python_code = """
        if spark.catalog.tableExists("bronze.temp_data"):
            spark.catalog.dropTempView("bronze.temp_data")
        """
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["bronze.temp_data"]

    def test_f_string_sql_with_substitution_tokens(self):
        """Test f-string SQL with substitution tokens."""
        python_code = '''
        df = spark.sql(f"""
        SELECT * FROM {catalog}.{schema}.customers
        WHERE created_date >= '{start_date}'
        """)
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert result == ["{catalog}.{schema}.customers"]

    def test_f_string_with_variables(self):
        """Test f-string handling with various variable types."""
        python_code = '''
        table_name = "customers"
        df = spark.sql(f"""
        SELECT * FROM {bronze_schema}.{table_name}
        JOIN {silver_schema}.orders ON customers.id = orders.customer_id
        """)
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["{bronze_schema}.{var}", "{silver_schema}.orders"]

    def test_complex_sql_in_python(self):
        """Test complex SQL queries embedded in Python."""
        python_code = '''
        def process_customer_data():
            # Get base customer data
            customers_df = spark.sql("""
                WITH recent_customers AS (
                    SELECT * FROM bronze.raw_customers
                    WHERE signup_date >= '2023-01-01'
                )
                SELECT rc.*, prof.profile_score
                FROM recent_customers rc
                LEFT JOIN silver.customer_profiles prof ON rc.id = prof.customer_id
            """)

            # Enrich with order data
            enriched_df = spark.sql("""
                SELECT c.*, COUNT(o.id) as order_count
                FROM temp_customers c
                LEFT JOIN gold.order_summary o ON c.id = o.customer_id
                GROUP BY c.id, c.name, c.profile_score
            """)

            return enriched_df
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["bronze.raw_customers", "gold.order_summary", "silver.customer_profiles", "temp_customers"]

    def test_multiline_sql_strings(self):
        """Test multiline SQL string handling."""
        python_code = '''
        result_df = spark.sql("""
        SELECT
            c.id,
            c.name,
            COUNT(o.id) as total_orders
        FROM bronze.customers c
        LEFT JOIN silver.orders o
            ON c.id = o.customer_id
        WHERE c.active = true
        GROUP BY c.id, c.name
        ORDER BY total_orders DESC
        """)
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["bronze.customers", "silver.orders"]

    def test_mixed_string_types(self):
        """Test mixed string types (single quotes, double quotes, triple quotes)."""
        python_code = '''
        df1 = spark.sql('SELECT * FROM bronze.table1')
        df2 = spark.sql("SELECT * FROM silver.table2")
        df3 = spark.sql("""SELECT * FROM gold.table3""")
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["bronze.table1", "gold.table3", "silver.table2"]

    def test_variable_sql_strings(self):
        """Test SQL strings stored in variables."""
        python_code = '''
        base_query = "SELECT * FROM bronze.events"
        enriched_query = "SELECT e.*, u.name FROM temp_events e JOIN silver.users u ON e.user_id = u.id"

        df1 = spark.sql(base_query)
        df2 = spark.sql(enriched_query)
        '''
        # Variable resolution is not supported, so these won't be extracted
        result = self.parser.extract_tables_from_python(python_code)
        assert result == []

    def test_function_parameter_sql(self):
        """Test SQL passed as function parameters."""
        python_code = '''
        def execute_query():
            return spark.sql("SELECT * FROM bronze.test_data")

        result = execute_query()
        '''
        # Only literal SQL strings in spark.sql() calls are extracted
        result = self.parser.extract_tables_from_python(python_code)
        assert result == ["bronze.test_data"]

    def test_nested_function_calls(self):
        """Test nested function calls."""
        python_code = '''
        df = spark.sql("SELECT * FROM bronze.raw_data").cache()
        processed = spark.table("silver.processed_data").select("*")
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["bronze.raw_data", "silver.processed_data"]

    def test_comments_in_python_code(self):
        """Test Python code with comments."""
        python_code = '''
        # Load customer data
        customers = spark.sql("SELECT * FROM bronze.customers")  # Main customer table

        """
        This is a docstring, not a SQL query
        FROM should_not_be_extracted
        """

        orders = spark.sql("SELECT * FROM silver.orders")  # Order data
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["bronze.customers", "silver.orders"]

    def test_class_methods(self):
        """Test extraction from class methods."""
        python_code = '''
        class DataProcessor:
            def __init__(self, spark_session):
                self.spark = spark_session

            def load_customers(self):
                return self.spark.sql("SELECT * FROM bronze.customers")

            def load_orders(self):
                return self.spark.table("silver.orders")
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert sorted(result) == ["bronze.customers", "silver.orders"]

    def test_invalid_python_syntax(self):
        """Test handling of invalid Python syntax."""
        python_code = '''
        this is not valid python syntax
        spark.sql("SELECT * FROM bronze.customers"
        '''
        # Should handle syntax errors gracefully
        result = self.parser.extract_tables_from_python(python_code)
        assert result == []

    def test_empty_and_none_input(self):
        """Test handling of empty and None input."""
        assert self.parser.extract_tables_from_python("") == []
        assert self.parser.extract_tables_from_python(None) == []
        assert self.parser.extract_tables_from_python("   ") == []

    def test_no_spark_references(self):
        """Test Python code without Spark references."""
        python_code = '''
        import pandas as pd

        def process_data():
            df = pd.read_csv("data.csv")
            return df.groupby("category").sum()
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert result == []

    def test_spark_object_variations(self):
        """Test different Spark object reference patterns."""
        python_code = '''
        # This should work
        df1 = spark.sql("SELECT * FROM bronze.table1")

        # These should NOT work (not the 'spark' object)
        df2 = my_spark.sql("SELECT * FROM bronze.table2")
        df3 = spark_session.sql("SELECT * FROM bronze.table3")
        '''
        result = self.parser.extract_tables_from_python(python_code)
        assert result == ["bronze.table1"]

    def test_complex_f_string_scenarios(self):
        """Test complex f-string scenarios."""
        python_code = '''
        # Standard substitution tokens should be preserved
        query1 = f"SELECT * FROM {catalog}.{schema}.table1"
        df1 = spark.sql(query1)

        # Mixed tokens and literals
        query2 = f"SELECT * FROM {catalog}.bronze.{table}"
        df2 = spark.sql(query2)

        # Other variables should become generic placeholders
        query3 = f"SELECT * FROM {database_name}.{table_suffix}_data"
        df3 = spark.sql(query3)
        '''
        result = self.parser.extract_tables_from_python(python_code)
        # Only the f-strings will be processed, variables won't be resolved
        assert result == []

    def test_extract_sql_from_python_separately(self):
        """Test SQL extraction without table parsing."""
        python_code = '''
        sql1 = "SELECT * FROM bronze.customers WHERE active = true"
        sql2 = "SELECT COUNT(*) FROM silver.orders"

        df1 = spark.sql(sql1)
        df2 = spark.sql(sql2)
        '''
        sql_queries = self.parser.extract_sql_from_python(python_code)
        # Only literal strings passed to spark.sql should be extracted
        assert len(sql_queries) == 0  # Variables are not resolved

    def test_direct_sql_strings_in_spark_sql(self):
        """Test direct SQL strings passed to spark.sql()."""
        python_code = '''
        df = spark.sql("SELECT * FROM bronze.events WHERE event_date >= '2023-01-01'")
        '''
        sql_queries = self.parser.extract_sql_from_python(python_code)
        assert len(sql_queries) == 1
        assert "bronze.events" in sql_queries[0]

    def test_substitution_token_preservation(self):
        """Test that substitution tokens are properly preserved."""
        python_code = f'''
        df = spark.sql(f"SELECT * FROM {{catalog}}.{{bronze_schema}}.raw_data")
        '''
        result = self.parser.extract_tables_from_python(python_code)
        # F-string processing should preserve known substitution tokens
        assert result == ["{catalog}.{bronze_schema}.raw_data"]

    @patch('lhp.utils.python_parser.extract_tables_from_sql')
    def test_sql_extraction_integration(self, mock_extract):
        """Test integration with SQL parser."""
        mock_extract.return_value = ["bronze.customers", "silver.orders"]

        python_code = '''
        df = spark.sql("SELECT c.*, o.* FROM bronze.customers c JOIN silver.orders o")
        '''

        result = self.parser.extract_tables_from_python(python_code)

        # Verify that SQL parser was called
        mock_extract.assert_called()
        assert result == ["bronze.customers", "silver.orders"]


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_extract_tables_from_python_function(self):
        """Test the standalone convenience function."""
        python_code = '''
        df = spark.sql("SELECT * FROM bronze.customers")
        '''
        result = extract_tables_from_python(python_code)
        assert result == ["bronze.customers"]

    def test_extract_sql_from_python_function(self):
        """Test the SQL extraction convenience function."""
        python_code = '''
        df = spark.sql("SELECT * FROM bronze.customers WHERE active = true")
        '''
        result = extract_sql_from_python(python_code)
        assert len(result) == 1
        assert "bronze.customers" in result[0]

    def test_convenience_functions_with_none(self):
        """Test convenience functions with None input."""
        assert extract_tables_from_python(None) == []
        assert extract_sql_from_python(None) == []


@pytest.mark.parametrize("python_code,expected_tables", [
    # Basic spark.sql calls
    ('spark.sql("SELECT * FROM table1")', ["table1"]),
    ('spark.sql("SELECT * FROM schema.table1")', ["schema.table1"]),

    # spark.table calls
    ('spark.table("table1")', ["table1"]),
    ('spark.read.table("schema.table1")', ["schema.table1"]),

    # Multiple calls
    ('spark.sql("SELECT * FROM t1"); spark.table("t2")', ["t1", "t2"]),

    # No Spark calls
    ('print("Hello world")', []),
    ('df = pd.read_csv("file.csv")', []),

    # Empty/None cases
    ('', []),
])
def test_python_parser_parametrized(python_code, expected_tables):
    """Parametrized tests for various Python code patterns."""
    parser = PythonParser()
    result = parser.extract_tables_from_python(python_code)
    assert sorted(result) == sorted(expected_tables)


class TestASTHandling:
    """Test specific AST node handling."""

    def test_ast_call_node_processing(self):
        """Test that AST Call nodes are processed correctly."""
        parser = PythonParser()

        # This should extract tables
        python_code = '''
        result = spark.sql("SELECT * FROM bronze.test_table")
        '''

        result = parser.extract_tables_from_python(python_code)
        assert result == ["bronze.test_table"]

    def test_ast_constant_vs_str_nodes(self):
        """Test handling of both Constant and Str AST nodes."""
        parser = PythonParser()

        # Modern Python uses Constant nodes
        python_code = '''
        df = spark.sql("SELECT * FROM bronze.modern_table")
        '''

        result = parser.extract_tables_from_python(python_code)
        assert result == ["bronze.modern_table"]

    def test_f_string_ast_processing(self):
        """Test f-string AST processing."""
        parser = PythonParser()

        # F-strings create JoinedStr AST nodes
        python_code = '''
        table = "customers"
        df = spark.sql(f"SELECT * FROM bronze.{table}")
        '''

        # F-strings with unknown variables preserve variable names as placeholders
        result = parser.extract_tables_from_python(python_code)
        assert result == ["bronze.{table}"]