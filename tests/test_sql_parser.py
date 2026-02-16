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
        assert sorted(result) == [
            "base_table",
            "table1",
            "table2",
            "table3",
            "table4",
            "table5",
        ]

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
        assert sorted(result) == [
            "bronze.orders",
            "gold.high_value_transactions",
            "silver.customers",
        ]

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
            "{catalog}.{silver_schema}.processed_events",
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


class TestCTENameLeakFix:
    """Tests verifying CTE names do not leak as external table references.

    When one CTE references another CTE defined earlier in the same WITH block,
    the referenced CTE name must NOT appear as an external table dependency.
    """

    def setup_method(self):
        """Set up test instance."""
        self.parser = SQLParser()

    def test_cte_b_references_cte_a(self):
        """CTE 'b' references CTE 'a' -- 'a' should NOT appear as external table."""
        sql = """
        WITH a AS (
            SELECT id, value FROM raw.source_data
        ),
        b AS (
            SELECT id, SUM(value) AS total
            FROM a
            GROUP BY id
        )
        SELECT * FROM b
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert "a" not in tables, "CTE name 'a' should not leak as an external table"
        assert "b" not in tables, "CTE name 'b' should not leak as an external table"
        assert tables == ["raw.source_data"]

    def test_multi_level_cte_chain(self):
        """Multi-level CTE chain: c references both a and b, b references a."""
        sql = """
        WITH a AS (
            SELECT id, name FROM bronze.users
        ),
        b AS (
            SELECT a.id, a.name, o.amount
            FROM a
            JOIN bronze.orders o ON a.id = o.user_id
        ),
        c AS (
            SELECT b.id, b.name, b.amount, a.id AS original_id
            FROM b
            JOIN a ON b.id = a.id
        )
        SELECT * FROM c
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert "a" not in tables, "CTE name 'a' should not leak"
        assert "b" not in tables, "CTE name 'b' should not leak"
        assert "c" not in tables, "CTE name 'c' should not leak"
        assert sorted(tables) == ["bronze.orders", "bronze.users"]

    def test_cte_with_multiple_inter_references(self):
        """Multiple CTEs with complex inter-references among them."""
        sql = """
        WITH raw AS (
            SELECT * FROM bronze.events
        ),
        filtered AS (
            SELECT * FROM raw WHERE event_type = 'click'
        ),
        enriched AS (
            SELECT f.*, r.timestamp
            FROM filtered f
            JOIN raw r ON f.id = r.id
        ),
        final AS (
            SELECT e.*, u.name
            FROM enriched e
            JOIN silver.users u ON e.user_id = u.id
        )
        SELECT * FROM final
        """
        tables = self.parser.extract_tables_from_sql(sql)
        for cte_name in ["raw", "filtered", "enriched", "final"]:
            assert cte_name not in tables, f"CTE name '{cte_name}' should not leak"
        assert sorted(tables) == ["bronze.events", "silver.users"]


class TestSubqueryExtraction:
    """Tests for extracting table references from subqueries in WHERE, HAVING, and nested contexts."""

    def setup_method(self):
        """Set up test instance."""
        self.parser = SQLParser()

    def test_where_in_subquery(self):
        """WHERE clause with IN subquery should extract the subquery's table."""
        sql = """
        SELECT * FROM customers
        WHERE customer_id IN (
            SELECT customer_id FROM other_table WHERE active = 1
        )
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert "customers" in tables
        assert "other_table" in tables
        assert sorted(tables) == ["customers", "other_table"]

    def test_having_clause_subquery(self):
        """HAVING clause with subquery should extract the subquery's table."""
        sql = """
        SELECT department_id, COUNT(*) AS emp_count
        FROM employees
        GROUP BY department_id
        HAVING COUNT(*) > (
            SELECT AVG(dept_size)
            FROM department_stats
        )
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert "employees" in tables
        assert "department_stats" in tables
        assert sorted(tables) == ["department_stats", "employees"]

    def test_nested_subqueries(self):
        """Nested subqueries: WHERE x IN (SELECT ... WHERE y IN (SELECT ... FROM t2))."""
        sql = """
        SELECT * FROM orders
        WHERE customer_id IN (
            SELECT customer_id FROM customers
            WHERE region_id IN (
                SELECT region_id FROM regions WHERE country = 'US'
            )
        )
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert "orders" in tables
        assert "customers" in tables
        assert "regions" in tables
        assert sorted(tables) == ["customers", "orders", "regions"]


class TestSetOperations:
    """Tests for UNION, INTERSECT, and EXCEPT set operations."""

    def setup_method(self):
        """Set up test instance."""
        self.parser = SQLParser()

    def test_union_all_two_tables(self):
        """UNION ALL of two tables should extract both."""
        sql = """
        SELECT * FROM bronze.events_2022
        UNION ALL
        SELECT * FROM bronze.events_2023
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert sorted(tables) == ["bronze.events_2022", "bronze.events_2023"]

    def test_intersect_two_tables(self):
        """INTERSECT of two tables should extract both."""
        sql = """
        SELECT id FROM bronze.active_users
        INTERSECT
        SELECT id FROM bronze.premium_users
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert sorted(tables) == ["bronze.active_users", "bronze.premium_users"]

    def test_triple_union(self):
        """Three-way UNION should extract all three tables."""
        sql = """
        SELECT * FROM bronze.events_2021
        UNION
        SELECT * FROM bronze.events_2022
        UNION
        SELECT * FROM bronze.events_2023
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert sorted(tables) == [
            "bronze.events_2021",
            "bronze.events_2022",
            "bronze.events_2023",
        ]

    def test_union_with_join(self):
        """UNION ALL where the first branch contains a JOIN should extract all tables."""
        sql = """
        SELECT a.*, b.score
        FROM bronze.customers a
        JOIN silver.scores b ON a.id = b.customer_id
        UNION ALL
        SELECT *
        FROM bronze.legacy_customers
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert sorted(tables) == [
            "bronze.customers",
            "bronze.legacy_customers",
            "silver.scores",
        ]


class TestStreamWithCTEContext:
    """Tests for stream() function wrapper interactions with CTE names."""

    def setup_method(self):
        """Set up test instance."""
        self.parser = SQLParser()

    def test_stream_cte_name_filtered(self):
        """stream(cte_name) within CTE context should filter out the CTE name."""
        sql = """
        WITH raw_events AS (
            SELECT * FROM stream(bronze.event_log)
        )
        SELECT * FROM stream(raw_events)
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert (
            "raw_events" not in tables
        ), "CTE name 'raw_events' wrapped in stream() should be filtered out"
        assert "bronze.event_log" in tables
        assert tables == ["bronze.event_log"]

    def test_stream_external_table_not_filtered(self):
        """stream() wrapping an external table (not a CTE name) should be preserved."""
        sql = """
        WITH base AS (
            SELECT * FROM stream(bronze.raw_data)
        )
        SELECT b.*, u.name
        FROM base b
        JOIN stream(silver.users) u ON b.user_id = u.id
        """
        tables = self.parser.extract_tables_from_sql(sql)
        assert "base" not in tables, "CTE name 'base' should not leak"
        assert "bronze.raw_data" in tables
        assert "silver.users" in tables
        assert sorted(tables) == ["bronze.raw_data", "silver.users"]

    def test_stream_mixed_cte_and_external(self):
        """Mix of stream() on CTE names and external tables within the same query."""
        sql = """
        WITH incoming AS (
            SELECT * FROM stream(bronze.ingest)
        ),
        cleaned AS (
            SELECT * FROM stream(incoming)
            WHERE quality_flag = 'good'
        )
        SELECT * FROM stream(cleaned)
        JOIN stream(silver.reference_data) r ON cleaned.ref_id = r.id
        """
        tables = self.parser.extract_tables_from_sql(sql)
        for cte_name in ["incoming", "cleaned"]:
            assert (
                cte_name not in tables
            ), f"CTE name '{cte_name}' wrapped in stream() should be filtered"
        assert "bronze.ingest" in tables
        assert "silver.reference_data" in tables
        assert sorted(tables) == ["bronze.ingest", "silver.reference_data"]


@pytest.mark.parametrize(
    "sql,expected",
    [
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
    ],
)
def test_sql_parser_parametrized(sql, expected):
    """Parametrized tests for various SQL patterns."""
    parser = SQLParser()
    result = parser.extract_tables_from_sql(sql)
    assert sorted(result) == sorted(expected)
