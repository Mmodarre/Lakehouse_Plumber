"""Tests for SQL parser utility."""

import pytest
from lhp.utils.sql_parser import SQLParser, extract_tables_from_sql


class TestSQLParser:
    """Test SQL parser functionality."""

    def setup_method(self):
        """Set up test instance."""
        self.parser = SQLParser()

    def test_basic_from_clause(self):
        """Test basic FROM clause table extraction."""
        sql = "SELECT * FROM customers"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["customers"]

    def test_schema_qualified_tables(self):
        """Test schema-qualified table references."""
        sql = "SELECT * FROM bronze.customers"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["bronze.customers"]

    def test_fully_qualified_tables(self):
        """Test fully-qualified table references with catalog."""
        sql = "SELECT * FROM catalog.schema.table"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["catalog.schema.table"]

    def test_substitution_tokens(self):
        """Test handling of substitution tokens."""
        sql = "SELECT * FROM {catalog}.{schema}.customers"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["{catalog}.{schema}.customers"]

    def test_mixed_substitution_tokens(self):
        """Test mixed substitution tokens and literals."""
        sql = "SELECT * FROM {catalog}.bronze.{table_name}"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["{catalog}.bronze.{table_name}"]

    def test_join_clauses(self):
        """Test JOIN clause table extraction."""
        sql = """
        SELECT * FROM customers c
        INNER JOIN orders o ON c.id = o.customer_id
        LEFT JOIN products p ON o.product_id = p.id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["customers", "orders", "products"]

    def test_join_with_schema(self):
        """Test JOIN clauses with schema-qualified tables."""
        sql = """
        SELECT * FROM bronze.customers c
        JOIN silver.orders o ON c.id = o.customer_id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.customers", "silver.orders"]

    def test_multiple_joins(self):
        """Test multiple JOIN types."""
        sql = """
        SELECT * FROM base_table bt
        INNER JOIN table1 t1 ON bt.id = t1.base_id
        LEFT OUTER JOIN table2 t2 ON bt.id = t2.base_id
        RIGHT JOIN table3 t3 ON bt.id = t3.base_id
        FULL OUTER JOIN table4 t4 ON bt.id = t4.base_id
        CROSS JOIN table5 t5
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["base_table", "table1", "table2", "table3", "table4", "table5"]

    def test_cte_with_tables(self):
        """Test CTE (WITH clause) handling."""
        sql = """
        WITH customer_orders AS (
            SELECT customer_id, COUNT(*) as order_count
            FROM bronze.orders
            GROUP BY customer_id
        ),
        high_value_customers AS (
            SELECT * FROM silver.customers
            WHERE value_score > 80
        )
        SELECT co.customer_id, co.order_count, hvc.name
        FROM customer_orders co
        JOIN high_value_customers hvc ON co.customer_id = hvc.id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.orders", "silver.customers"]

    def test_nested_cte(self):
        """Test nested CTE structures."""
        sql = """
        WITH base_data AS (
            SELECT * FROM raw.events
            WHERE event_date >= '2023-01-01'
        ),
        aggregated AS (
            SELECT user_id, COUNT(*) as event_count
            FROM base_data
            WHERE event_type = 'click'
            GROUP BY user_id
        )
        SELECT a.user_id, a.event_count, u.name
        FROM aggregated a
        JOIN bronze.users u ON a.user_id = u.id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.users", "raw.events"]

    def test_stream_function_wrapper(self):
        """Test stream() function wrapper detection."""
        sql = "SELECT * FROM stream(bronze.events)"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["bronze.events"]

    def test_multiple_function_wrappers(self):
        """Test multiple function wrappers."""
        sql = """
        SELECT * FROM stream(bronze.events) e
        JOIN live(silver.users) u ON e.user_id = u.id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.events", "silver.users"]

    def test_snapshot_function(self):
        """Test snapshot() function wrapper."""
        sql = "SELECT * FROM snapshot(gold.customer_summary)"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["gold.customer_summary"]

    def test_comma_separated_from_clause(self):
        """Test comma-separated tables in FROM clause."""
        sql = "SELECT * FROM table1, table2, table3 WHERE table1.id = table2.id"
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["table1", "table2", "table3"]

    def test_comma_separated_with_schema(self):
        """Test comma-separated tables with schema."""
        sql = "SELECT * FROM bronze.table1, silver.table2 WHERE bronze.table1.id = silver.table2.id"
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.table1", "silver.table2"]

    def test_table_aliases(self):
        """Test tables with aliases."""
        sql = """
        SELECT * FROM customers AS c
        JOIN orders o ON c.id = o.customer_id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["customers", "orders"]

    def test_sql_comments_removal(self):
        """Test SQL comments are properly removed."""
        sql = """
        -- This is a comment
        SELECT * FROM bronze.customers  -- Another comment
        /* Multi-line
           comment */
        JOIN silver.orders ON customers.id = orders.customer_id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.customers", "silver.orders"]

    def test_case_insensitive_keywords(self):
        """Test case insensitive SQL keywords."""
        sql = """
        select * from bronze.customers c
        inner join silver.orders o on c.id = o.customer_id
        left outer join gold.products p on o.product_id = p.id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.customers", "gold.products", "silver.orders"]

    def test_complex_query_with_subqueries(self):
        """Test complex query with multiple constructs."""
        sql = """
        WITH recent_orders AS (
            SELECT * FROM bronze.orders
            WHERE order_date >= '2023-01-01'
        )
        SELECT c.name, COUNT(ro.id) as order_count
        FROM silver.customers c
        LEFT JOIN recent_orders ro ON c.id = ro.customer_id
        WHERE c.id IN (
            SELECT DISTINCT customer_id
            FROM gold.high_value_transactions
            WHERE amount > 1000
        )
        GROUP BY c.name
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.orders", "gold.high_value_transactions", "silver.customers"]

    def test_invalid_table_filtering(self):
        """Test filtering of SQL keywords and invalid references."""
        # This shouldn't extract SQL keywords as tables
        sql = "SELECT * FROM customers WHERE name IS NOT NULL"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["customers"]
        assert "IS" not in result
        assert "NOT" not in result
        assert "NULL" not in result

    def test_empty_and_none_input(self):
        """Test handling of empty and None input."""
        assert self.parser.extract_tables_from_sql("") == []
        assert self.parser.extract_tables_from_sql(None) == []
        assert self.parser.extract_tables_from_sql("   ") == []

    def test_function_calls_not_extracted(self):
        """Test that function calls are not extracted as tables."""
        sql = """
        SELECT * FROM customers
        WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["customers", "orders"]

    def test_substitution_tokens_complex(self):
        """Test complex substitution token scenarios."""
        sql = """
        SELECT * FROM {catalog}.{bronze_schema}.raw_events
        JOIN {catalog}.{silver_schema}.processed_events
        ON raw_events.id = processed_events.raw_id
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == [
            "{catalog}.{bronze_schema}.raw_events",
            "{catalog}.{silver_schema}.processed_events"
        ]

    def test_mixed_quoted_identifiers(self):
        """Test mixed quoted and unquoted identifiers."""
        # Note: The current parser doesn't handle quoted identifiers,
        # but this test documents the current behavior
        sql = 'SELECT * FROM "bronze"."customers" JOIN orders'
        result = self.parser.extract_tables_from_sql(sql)
        # This will not extract quoted identifiers correctly with current implementation
        assert "orders" in result

    def test_union_queries(self):
        """Test UNION queries."""
        sql = """
        SELECT * FROM bronze.customers_2022
        UNION ALL
        SELECT * FROM bronze.customers_2023
        """
        result = self.parser.extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.customers_2022", "bronze.customers_2023"]

    def test_very_long_table_names(self):
        """Test very long table names."""
        long_table = "very_very_very_long_table_name_that_exceeds_normal_limits"
        sql = f"SELECT * FROM {long_table}"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == [long_table]

    def test_special_characters_in_tokens(self):
        """Test special characters in substitution tokens."""
        sql = "SELECT * FROM {catalog_env}.{schema_v2}.{table_2023_01}"
        result = self.parser.extract_tables_from_sql(sql)
        assert result == ["{catalog_env}.{schema_v2}.{table_2023_01}"]


class TestConvenienceFunction:
    """Test the convenience function."""

    def test_extract_tables_from_sql_function(self):
        """Test the standalone convenience function."""
        sql = "SELECT * FROM bronze.customers JOIN silver.orders"
        result = extract_tables_from_sql(sql)
        assert sorted(result) == ["bronze.customers", "silver.orders"]

    def test_convenience_function_with_none(self):
        """Test convenience function with None input."""
        result = extract_tables_from_sql(None)
        assert result == []


@pytest.mark.parametrize("sql,expected", [
    # Basic cases
    ("SELECT * FROM table1", ["table1"]),
    ("SELECT * FROM schema.table1", ["schema.table1"]),
    ("SELECT * FROM catalog.schema.table1", ["catalog.schema.table1"]),

    # JOIN cases
    ("SELECT * FROM t1 JOIN t2", ["t1", "t2"]),
    ("SELECT * FROM t1 LEFT JOIN t2", ["t1", "t2"]),

    # Function wrappers
    ("SELECT * FROM stream(table1)", ["table1"]),
    ("SELECT * FROM live(schema.table1)", ["schema.table1"]),

    # Substitution tokens
    ("SELECT * FROM {catalog}.{schema}.{table}", ["{catalog}.{schema}.{table}"]),

    # Empty cases
    ("", []),
    ("SELECT 1", []),
])
def test_sql_parser_parametrized(sql, expected):
    """Parametrized tests for various SQL patterns."""
    parser = SQLParser()
    result = parser.extract_tables_from_sql(sql)
    assert sorted(result) == sorted(expected)