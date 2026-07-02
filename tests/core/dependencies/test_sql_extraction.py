"""Unit tests for the sqlglot-based SQL table extractor.

Covers the NEW capabilities over the legacy regex parser — byte-exact
substitution-token fidelity via mask/unmask, write-target exclusion
(INSERT/CTAS/MERGE), strings containing FROM, quoted identifiers,
multi-statement bodies, and LHP-DEP-003 parse-failure advisories — plus a
sanity slice of the legacy output conventions (sorted, deduplicated dotted
names; CTE exclusion incl. through stream()).

The ``TestLegacyCorpus*`` classes port the full behavior corpus of the retired
regex parser (``tests/test_sql_parser.py``) so every legacy behavior group is
pinned against this extractor. Two documented strengthenings over legacy:

- Mid-segment substitution tokens are full-fidelity (``tbl${tok}`` stays
  ``tbl${tok}``; the regex parser truncated to ``tbl``).
- Double-quoted identifiers are extracted with quoting stripped
  (``"bronze"."customers"`` yields ``bronze.customers``; legacy missed it).

Bare ``$word`` placeholders (the documented ``$source`` of SQL transforms) are
masked with a separate drop class: any reference containing one is excluded
from the output entirely (``TestBareDollarPlaceholders``).
"""

from __future__ import annotations

import pytest

from lhp.core.dependencies.sql_extraction import (
    SqlExtractionResult,
    _mask_tokens,
    _unmask,
    extract_tables_from_sql,
)

DEP_003_CODE = "LHP-DEP-003"

# Adversarial inputs for the mask -> unmask identity property: tokens glued to
# identifiers, token-only identifiers, secrets, deprecated {x} tokens, multiple
# tokens, token text containing the salt seed, SQL already containing
# placeholder-shaped ``__...__`` identifiers, and bare-$ placeholders
# (drop class) alone and mixed with preserve-class tokens.
ADVERSARIAL_INPUTS = [
    "SELECT * FROM cat.sch.tbl${suffix}",
    "SELECT * FROM ${table_only}",
    "SELECT * FROM t WHERE k = '${secret:scope/key}'",
    "SELECT * FROM {catalog}.{schema}.customers",
    "SELECT * FROM ${a}.${b}.${c} JOIN x${d}y ON 1=1",
    "SELECT * FROM tbl${lhpmask}",
    "SELECT lhpmask FROM lhpmask_table JOIN t${a} ON 1=1",
    "SELECT * FROM __lhpmask_0__ JOIN tbl${a} ON 1=1",
    "SELECT * FROM __weird__ident__ WHERE x = '${secret:s/k}' AND y = ${z}",
    "${tok}",
    "no tokens at all",
    "",
    "SELECT a, b FROM $source",
    "SELECT * FROM $source JOIN ${cat}.sch.t ON $source.id = t.id",
    "SELECT * FROM cat.$schema.t WHERE x = '${secret:s/k}'",
    "$source",
]


class TestMaskUnmaskRoundTrip:
    """Masking then unmasking any input must be the identity function."""

    @pytest.mark.parametrize("raw", ADVERSARIAL_INPUTS)
    def test_mask_unmask_identity(self, raw):
        masked, replacements, _ = _mask_tokens(raw)
        assert _unmask(masked, replacements) == raw

    @pytest.mark.parametrize("raw", ADVERSARIAL_INPUTS)
    def test_masked_text_contains_no_tokens(self, raw):
        masked, replacements, _ = _mask_tokens(raw)
        for original in replacements.values():
            assert original not in masked

    def test_placeholders_unique_per_occurrence(self):
        masked, replacements, _ = _mask_tokens("SELECT * FROM ${a}.${a}.${b}")
        assert len(replacements) == 3
        assert len(set(replacements)) == 3
        for placeholder in replacements:
            assert masked.count(placeholder) == 1

    def test_drop_class_disjoint_from_preserve_class(self):
        # ${catalog} is preserve class; $source occurrences are drop class.
        _, replacements, dropped = _mask_tokens(
            "SELECT * FROM $source JOIN ${catalog}.sch.t ON $source.id = t.id"
        )
        assert len(replacements) == 3
        assert len(dropped) == 2
        assert {replacements[p] for p in dropped} == {"$source"}
        preserved = set(replacements) - dropped
        assert {replacements[p] for p in preserved} == {"${catalog}"}


@pytest.mark.unit
class TestTokenByteFidelity:
    """Output table names carry token bytes exactly as written."""

    def test_token_glued_to_identifier(self):
        result = extract_tables_from_sql("SELECT * FROM cat.sch.tbl${suffix}")
        assert result.tables == ["cat.sch.tbl${suffix}"]
        assert result.warnings == []

    def test_token_as_catalog_segment(self):
        result = extract_tables_from_sql("SELECT * FROM ${catalog}.sch.t")
        assert result.tables == ["${catalog}.sch.t"]

    def test_secret_token_in_string_never_surfaces(self):
        result = extract_tables_from_sql(
            "SELECT * FROM cat.${schema}.t WHERE x = '${secret:s/k}'"
        )
        assert result.tables == ["cat.${schema}.t"]
        assert all("secret" not in t for t in result.tables)

    def test_legacy_brace_tokens(self):
        result = extract_tables_from_sql("SELECT * FROM {catalog}.{schema}.customers")
        assert result.tables == ["{catalog}.{schema}.customers"]

    def test_token_text_containing_salt_seed(self):
        result = extract_tables_from_sql("SELECT * FROM tbl${lhpmask}")
        assert result.tables == ["tbl${lhpmask}"]

    def test_input_containing_placeholder_shaped_identifier(self):
        result = extract_tables_from_sql(
            "SELECT * FROM __lhpmask_0__ JOIN tbl${a} ON 1=1"
        )
        assert result.tables == ["__lhpmask_0__", "tbl${a}"]

    def test_multiple_tokens_across_tables(self):
        result = extract_tables_from_sql(
            "SELECT * FROM ${catalog}.${bronze_schema}.raw_events "
            "JOIN ${catalog}.${silver_schema}.processed_events ON 1=1"
        )
        assert result.tables == [
            "${catalog}.${bronze_schema}.raw_events",
            "${catalog}.${silver_schema}.processed_events",
        ]


@pytest.mark.unit
class TestBareDollarPlaceholders:
    """Bare ``$word`` placeholders (e.g. ``$source``) never surface as tables.

    ``$source`` is replaced with the source view name by LHP before runtime
    and the action's declared ``source:`` already carries the dependency edge,
    so references built from bare-$ placeholders are dropped entirely.
    """

    def test_source_placeholder_alone(self):
        result = extract_tables_from_sql("SELECT a, b FROM $source")
        assert result.tables == []
        assert result.warnings == []

    def test_source_placeholder_alongside_real_table(self):
        result = extract_tables_from_sql(
            "SELECT * FROM $source "
            "JOIN cat.sch.real_table ON $source.id = real_table.id"
        )
        assert result.tables == ["cat.sch.real_table"]
        assert result.warnings == []

    def test_dollar_brace_token_unaffected_by_mask_order(self):
        # ${...} is masked BEFORE \$\w+ — ${catalog} must never be mis-eaten
        # by the bare-$ exclusion pattern.
        result = extract_tables_from_sql("SELECT * FROM ${catalog}.sch.t")
        assert result.tables == ["${catalog}.sch.t"]
        assert result.warnings == []

    def test_bare_dollar_mid_reference_drops_whole_reference(self):
        # A bare-$ segment is substituted by LHP before runtime; the
        # assembled name is not statically known, so the whole reference
        # is dropped.
        result = extract_tables_from_sql("SELECT * FROM cat.$schema.t")
        assert result.tables == []
        assert result.warnings == []

    def test_mixed_dropped_and_kept_references(self):
        result = extract_tables_from_sql(
            "SELECT * FROM cat.$schema.t JOIN ${catalog}.sch.u ON 1=1"
        )
        assert result.tables == ["${catalog}.sch.u"]
        assert result.warnings == []


@pytest.mark.unit
class TestWriteExclusion:
    """Write targets are excluded; only reads are extracted."""

    def test_insert_into_select(self):
        result = extract_tables_from_sql("INSERT INTO tgt.t SELECT * FROM src.s")
        assert result.tables == ["src.s"]

    def test_create_table_as_select(self):
        result = extract_tables_from_sql(
            "CREATE TABLE cat.sch.t AS SELECT * FROM src.a JOIN src.b ON a.id = b.id"
        )
        assert result.tables == ["src.a", "src.b"]

    def test_merge_yields_using_side_only(self):
        result = extract_tables_from_sql(
            "MERGE INTO tgt.t USING src.s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"
        )
        assert result.tables == ["src.s"]

    def test_merge_with_using_subquery(self):
        result = extract_tables_from_sql(
            "MERGE INTO tgt.t AS t USING (SELECT * FROM src.s) AS s "
            "ON t.id = s.id WHEN MATCHED THEN DELETE"
        )
        assert result.tables == ["src.s"]


@pytest.mark.unit
class TestDltWrappers:
    """stream()/live()/snapshot() unwrap to their first argument."""

    @pytest.mark.parametrize(
        "wrapper", ["stream", "live", "snapshot", "STREAM", "Stream"]
    )
    def test_wrapper_unwraps(self, wrapper):
        result = extract_tables_from_sql(f"SELECT * FROM {wrapper}(bronze.events)")
        assert result.tables == ["bronze.events"]

    def test_wrapper_with_token(self):
        result = extract_tables_from_sql(
            "SELECT * FROM stream(${catalog}.bronze.events${suffix})"
        )
        assert result.tables == ["${catalog}.bronze.events${suffix}"]

    def test_stream_on_cte_name_excluded(self):
        sql = """
        WITH raw_events AS (
            SELECT * FROM stream(bronze.event_log)
        )
        SELECT * FROM stream(raw_events)
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.event_log"]

    def test_stream_mixed_cte_and_external(self):
        sql = """
        WITH incoming AS (
            SELECT * FROM stream(bronze.ingest)
        ),
        cleaned AS (
            SELECT * FROM stream(incoming) WHERE quality_flag = 'good'
        )
        SELECT * FROM stream(cleaned)
        JOIN stream(silver.reference_data) r ON cleaned.ref_id = r.id
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.ingest", "silver.reference_data"]

    def test_non_dlt_table_function_not_extracted(self):
        result = extract_tables_from_sql(
            "SELECT * FROM read_files('/mnt/raw') JOIN bronze.t ON 1=1"
        )
        assert result.tables == ["bronze.t"]


@pytest.mark.unit
class TestDltWrapperNonNameArguments:
    """Wrapper arguments that are not plain names are opaque: excluded, no junk.

    A nested call, a string literal, or a subquery as the first argument of
    stream()/live()/snapshot() is not a statically known table reference —
    it must yield NO entry (and no warning), never a rendered-SQL junk name.
    """

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM stream(live(bronze.x))",
            "SELECT * FROM stream('bronze.x')",
            "SELECT * FROM snapshot((SELECT 1))",
            "SELECT * FROM stream(read_files('/mnt'))",
        ],
    )
    def test_non_name_argument_yields_no_entry(self, sql):
        result = extract_tables_from_sql(sql)
        assert result.tables == []
        assert result.warnings == []

    def test_non_name_argument_alongside_real_read(self):
        result = extract_tables_from_sql(
            "SELECT * FROM stream(read_files('/mnt/raw')) JOIN bronze.t ON 1=1"
        )
        assert result.tables == ["bronze.t"]
        assert result.warnings == []

    def test_fully_qualified_name_still_extracts(self):
        result = extract_tables_from_sql("SELECT * FROM stream(cat.sch.t)")
        assert result.tables == ["cat.sch.t"]


@pytest.mark.unit
class TestMultiStatement:
    """Multi-statement bodies union the reads of every statement."""

    def test_two_statements_unioned(self):
        result = extract_tables_from_sql("SELECT * FROM b.t2; SELECT * FROM a.t1")
        assert result.tables == ["a.t1", "b.t2"]

    def test_write_then_read_statement(self):
        result = extract_tables_from_sql(
            "INSERT INTO tgt.t SELECT * FROM src.s; SELECT * FROM other.u"
        )
        assert result.tables == ["other.u", "src.s"]


@pytest.mark.unit
class TestParseFailure:
    """Whole-body parse failure yields zero tables + exactly one DEP_003."""

    def test_garbage_sql(self):
        result = extract_tables_from_sql("this is not sql at all !!!")
        assert result.tables == []
        assert len(result.warnings) == 1
        warning = result.warnings[0]
        assert warning.code == DEP_003_CODE
        assert warning.message
        assert "depends_on" in warning.suggestion
        assert warning.flowgroup == ""
        assert warning.action == ""
        assert warning.file_path is None

    def test_local_var_token_fails_parsing(self):
        # %{local_var} is intentionally NOT masked — it never reaches .sql
        # files, so failing with DEP_003 is correct feedback.
        result = extract_tables_from_sql("SELECT * FROM %{schema}.t")
        assert result.tables == []
        assert len(result.warnings) == 1
        assert result.warnings[0].code == DEP_003_CODE

    def test_never_raises(self):
        for sql in ["SELECT * FROM FROM ((", ";;;", "WITH AS AS AS"]:
            result = extract_tables_from_sql(sql)
            assert isinstance(result, SqlExtractionResult)
            assert result.tables == []


@pytest.mark.unit
class TestQuotedIdentifiers:
    """Backtick-quoted identifier parts output as unquoted text."""

    def test_backticks_stripped(self):
        result = extract_tables_from_sql("SELECT * FROM `bronze`.`customers`")
        assert result.tables == ["bronze.customers"]

    def test_mixed_quoting(self):
        result = extract_tables_from_sql(
            "SELECT * FROM `cat`.sch.`my tbl` JOIN orders ON 1=1"
        )
        assert result.tables == ["cat.sch.my tbl", "orders"]


@pytest.mark.unit
class TestStringsContainingFrom:
    """String literals containing FROM are never extracted (regex could not)."""

    def test_string_only(self):
        result = extract_tables_from_sql("SELECT 'FROM fake.table' AS s")
        assert result.tables == []
        assert result.warnings == []

    def test_string_alongside_real_read(self):
        result = extract_tables_from_sql("SELECT 'FROM fake.table' AS s FROM real.t")
        assert result.tables == ["real.t"]


@pytest.mark.unit
class TestLegacyConventionsSanitySlice:
    """Sanity slice of legacy output conventions (full port is a later task)."""

    def test_basic_from_clause(self):
        assert extract_tables_from_sql("SELECT * FROM customers").tables == [
            "customers"
        ]

    def test_fully_qualified(self):
        result = extract_tables_from_sql("SELECT * FROM catalog.schema.table")
        assert result.tables == ["catalog.schema.table"]

    def test_joins_sorted_and_deduped(self):
        sql = """
        SELECT * FROM customers c
        INNER JOIN orders o ON c.id = o.customer_id
        LEFT JOIN customers c2 ON c.id = c2.id
        """
        assert extract_tables_from_sql(sql).tables == ["customers", "orders"]

    def test_cte_names_excluded(self):
        sql = """
        WITH a AS (SELECT id FROM raw.source_data),
        b AS (SELECT id FROM a)
        SELECT * FROM b
        """
        assert extract_tables_from_sql(sql).tables == ["raw.source_data"]

    def test_subquery_in_where(self):
        sql = """
        SELECT * FROM customers
        WHERE customer_id IN (SELECT customer_id FROM other_table)
        """
        assert extract_tables_from_sql(sql).tables == ["customers", "other_table"]

    def test_union_branches(self):
        sql = """
        SELECT * FROM bronze.events_2022
        UNION ALL
        SELECT * FROM bronze.events_2023
        """
        assert extract_tables_from_sql(sql).tables == [
            "bronze.events_2022",
            "bronze.events_2023",
        ]

    def test_comments_removed(self):
        sql = """
        -- comment FROM commented.table
        SELECT * FROM bronze.customers  -- trailing comment
        /* multi-line
           comment FROM another.fake */
        """
        assert extract_tables_from_sql(sql).tables == ["bronze.customers"]

    @pytest.mark.parametrize("empty", ["", "   ", "SELECT 1"])
    def test_empty_and_table_free(self, empty):
        result = extract_tables_from_sql(empty)
        assert result.tables == []
        assert result.warnings == []


# ---------------------------------------------------------------------------
# Legacy corpus port (tests/test_sql_parser.py).
#
# Every behavior group of the retired regex parser is pinned here against the
# sqlglot extractor. Expectations are verbatim from the legacy suite except
# the two documented strengthenings (token fidelity, double-quoted
# identifiers) — see the module docstring.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLegacyCorpusFrom:
    """FROM-clause variants: qualification, commas, aliases, edge inputs."""

    def test_schema_qualified_table(self):
        result = extract_tables_from_sql("SELECT * FROM bronze.customers")
        assert result.tables == ["bronze.customers"]

    def test_comma_separated_from_clause(self):
        sql = "SELECT * FROM table1, table2, table3 WHERE table1.id = table2.id"
        result = extract_tables_from_sql(sql)
        assert result.tables == ["table1", "table2", "table3"]

    def test_comma_separated_with_schema(self):
        sql = (
            "SELECT * FROM bronze.table1, silver.table2 "
            "WHERE bronze.table1.id = silver.table2.id"
        )
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.table1", "silver.table2"]

    def test_table_aliases(self):
        sql = """
        SELECT * FROM customers AS c
        JOIN orders o ON c.id = o.customer_id
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["customers", "orders"]

    def test_very_long_table_name(self):
        long_table = "very_very_very_long_table_name_that_exceeds_normal_limits"
        result = extract_tables_from_sql(f"SELECT * FROM {long_table}")
        assert result.tables == [long_table]

    def test_case_insensitive_keywords(self):
        sql = """
        select * from bronze.customers c
        inner join silver.orders o on c.id = o.customer_id
        left outer join gold.products p on o.product_id = p.id
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == [
            "bronze.customers",
            "gold.products",
            "silver.orders",
        ]

    def test_sql_keywords_never_extracted_as_tables(self):
        # Legacy pinned its regex keyword-blocklist; re-expressed here as a
        # parser sanity case — keywords never surface as table reads.
        result = extract_tables_from_sql(
            "SELECT * FROM customers WHERE name IS NOT NULL"
        )
        assert result.tables == ["customers"]
        for keyword in ("SELECT", "WHERE", "IS", "NOT", "NULL"):
            assert keyword not in result.tables
            assert keyword.lower() not in result.tables

    def test_none_input(self):
        result = extract_tables_from_sql(None)  # type: ignore[arg-type]
        assert result.tables == []
        assert result.warnings == []


@pytest.mark.unit
class TestLegacyCorpusJoins:
    """JOIN variants: all join types, schema qualification, bare JOIN."""

    def test_join_clauses(self):
        sql = """
        SELECT * FROM customers c
        INNER JOIN orders o ON c.id = o.customer_id
        LEFT JOIN products p ON o.product_id = p.id
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["customers", "orders", "products"]

    def test_join_with_schema(self):
        sql = """
        SELECT * FROM bronze.customers c
        JOIN silver.orders o ON c.id = o.customer_id
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.customers", "silver.orders"]

    def test_multiple_join_types(self):
        sql = """
        SELECT * FROM base_table bt
        INNER JOIN table1 t1 ON bt.id = t1.base_id
        LEFT OUTER JOIN table2 t2 ON bt.id = t2.base_id
        RIGHT JOIN table3 t3 ON bt.id = t3.base_id
        FULL OUTER JOIN table4 t4 ON bt.id = t4.base_id
        CROSS JOIN table5 t5
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == [
            "base_table",
            "table1",
            "table2",
            "table3",
            "table4",
            "table5",
        ]

    def test_join_without_on_condition(self):
        # Legacy exercised this via its module-level convenience function.
        result = extract_tables_from_sql(
            "SELECT * FROM bronze.customers JOIN silver.orders"
        )
        assert result.tables == ["bronze.customers", "silver.orders"]


@pytest.mark.unit
class TestLegacyCorpusCte:
    """CTE suites: external reads surface, CTE names never leak."""

    def test_cte_with_tables(self):
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
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.orders", "silver.customers"]

    def test_nested_cte(self):
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
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.users", "raw.events"]

    def test_multi_level_cte_chain(self):
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
        result = extract_tables_from_sql(sql)
        for cte_name in ("a", "b", "c"):
            assert cte_name not in result.tables
        assert result.tables == ["bronze.orders", "bronze.users"]

    def test_cte_with_multiple_inter_references(self):
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
        result = extract_tables_from_sql(sql)
        for cte_name in ("raw", "filtered", "enriched", "final"):
            assert cte_name not in result.tables
        assert result.tables == ["bronze.events", "silver.users"]


@pytest.mark.unit
class TestLegacyCorpusSubqueries:
    """Subqueries in WHERE/HAVING/EXISTS and nested contexts."""

    def test_having_clause_subquery(self):
        sql = """
        SELECT department_id, COUNT(*) AS emp_count
        FROM employees
        GROUP BY department_id
        HAVING COUNT(*) > (
            SELECT AVG(dept_size)
            FROM department_stats
        )
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["department_stats", "employees"]

    def test_nested_subqueries(self):
        sql = """
        SELECT * FROM orders
        WHERE customer_id IN (
            SELECT customer_id FROM customers
            WHERE region_id IN (
                SELECT region_id FROM regions WHERE country = 'US'
            )
        )
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["customers", "orders", "regions"]

    def test_exists_subquery(self):
        sql = """
        SELECT * FROM customers
        WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["customers", "orders"]

    def test_complex_query_with_cte_join_and_in_subquery(self):
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
        result = extract_tables_from_sql(sql)
        assert result.tables == [
            "bronze.orders",
            "gold.high_value_transactions",
            "silver.customers",
        ]


@pytest.mark.unit
class TestLegacyCorpusSetOperations:
    """UNION / INTERSECT / EXCEPT branches all contribute reads."""

    def test_intersect_two_tables(self):
        sql = """
        SELECT id FROM bronze.active_users
        INTERSECT
        SELECT id FROM bronze.premium_users
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.active_users", "bronze.premium_users"]

    def test_except_two_tables(self):
        sql = """
        SELECT id FROM bronze.all_users
        EXCEPT
        SELECT id FROM bronze.banned_users
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.all_users", "bronze.banned_users"]

    def test_triple_union(self):
        sql = """
        SELECT * FROM bronze.events_2021
        UNION
        SELECT * FROM bronze.events_2022
        UNION
        SELECT * FROM bronze.events_2023
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == [
            "bronze.events_2021",
            "bronze.events_2022",
            "bronze.events_2023",
        ]

    def test_union_with_join_branch(self):
        sql = """
        SELECT a.*, b.score
        FROM bronze.customers a
        JOIN silver.scores b ON a.id = b.customer_id
        UNION ALL
        SELECT *
        FROM bronze.legacy_customers
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == [
            "bronze.customers",
            "bronze.legacy_customers",
            "silver.scores",
        ]


@pytest.mark.unit
class TestLegacyCorpusWrappers:
    """stream()/live() combinations beyond the single-wrapper cases above."""

    def test_multiple_function_wrappers(self):
        sql = """
        SELECT * FROM stream(bronze.events) e
        JOIN live(silver.users) u ON e.user_id = u.id
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.events", "silver.users"]

    def test_stream_external_table_not_filtered(self):
        sql = """
        WITH base AS (
            SELECT * FROM stream(bronze.raw_data)
        )
        SELECT b.*, u.name
        FROM base b
        JOIN stream(silver.users) u ON b.user_id = u.id
        """
        result = extract_tables_from_sql(sql)
        assert "base" not in result.tables
        assert result.tables == ["bronze.raw_data", "silver.users"]


@pytest.mark.unit
class TestLegacyCorpusTokens:
    """Deprecated {token} syntax cases from the legacy suite."""

    def test_mixed_substitution_tokens(self):
        # STRENGTHENED vs legacy: the regex parser truncated mid-segment
        # tokens (tbl${tok} -> tbl); the sqlglot extractor is full-fidelity.
        result = extract_tables_from_sql("SELECT * FROM {catalog}.bronze.{table_name}")
        assert result.tables == ["{catalog}.bronze.{table_name}"]

        glued = extract_tables_from_sql("SELECT * FROM {catalog}.bronze.tbl${tok}")
        assert glued.tables == ["{catalog}.bronze.tbl${tok}"]

    def test_substitution_tokens_complex(self):
        sql = """
        SELECT * FROM {catalog}.{bronze_schema}.raw_events
        JOIN {catalog}.{silver_schema}.processed_events
        ON raw_events.id = processed_events.raw_id
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == [
            "{catalog}.{bronze_schema}.raw_events",
            "{catalog}.{silver_schema}.processed_events",
        ]

    def test_special_characters_in_tokens(self):
        result = extract_tables_from_sql(
            "SELECT * FROM {catalog_env}.{schema_v2}.{table_2023_01}"
        )
        assert result.tables == ["{catalog_env}.{schema_v2}.{table_2023_01}"]


@pytest.mark.unit
class TestLegacyCorpusQuotedIdentifiers:
    """Double-quoted identifiers (legacy could not extract them)."""

    def test_mixed_quoted_identifiers(self):
        # STRENGTHENED vs legacy: the regex parser only found `orders` here;
        # the sqlglot extractor also extracts the double-quoted read with
        # quoting stripped.
        result = extract_tables_from_sql(
            'SELECT * FROM "bronze"."customers" JOIN orders'
        )
        assert result.tables == ["bronze.customers", "orders"]


@pytest.mark.unit
class TestLegacyCorpusComments:
    """Comments interleaved with clauses never affect extraction."""

    def test_comments_interleaved_with_join(self):
        sql = """
        -- This is a comment
        SELECT * FROM bronze.customers  -- Another comment
        /* Multi-line
           comment */
        JOIN silver.orders ON customers.id = orders.customer_id
        """
        result = extract_tables_from_sql(sql)
        assert result.tables == ["bronze.customers", "silver.orders"]


@pytest.mark.unit
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
        # Substitution tokens (deprecated {x} syntax)
        ("SELECT * FROM {catalog}.{schema}.{table}", ["{catalog}.{schema}.{table}"]),
        # Empty cases
        ("", []),
        ("SELECT 1", []),
    ],
)
def test_legacy_corpus_parametrized(sql, expected):
    """Legacy parametrized matrix, pinned against the sqlglot extractor."""
    result = extract_tables_from_sql(sql)
    assert result.tables == sorted(expected)
    assert result.warnings == []
