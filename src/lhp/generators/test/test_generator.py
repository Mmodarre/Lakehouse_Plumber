"""Test action generator for Lakehouse Plumber.

Dispatcher — validates Action config, builds the per-test-type render
context, delegates to a Jinja2 template under ``templates/test/``.
"""

import logging
from typing import Any, Dict, List

from lhp.core.registry import BaseActionGenerator
from lhp.errors import ErrorCategory, LHPValidationError
from lhp.models import Action

logger = logging.getLogger(__name__)


class TestActionGenerator(BaseActionGenerator):
    """Generator for test actions — dispatches to per-test-type templates."""

    __test__ = False  # Tell pytest this is not a test class

    def __init__(
        self,
        config: Dict[str, Any] | None = None,
        context: Dict[str, Any] | None = None,
    ):
        """Initialize TestGenerator with config and context."""
        super().__init__()
        self.config = config or {}
        self.context = context or {}

        # Add basic imports
        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql.functions import *")

    def generate(
        self, action: Action = None, context: Dict[str, Any] | None = None
    ) -> str:
        """Generate test code by dispatching to per-test-type Jinja2 template."""
        # Use instance config/context if not provided
        if action:
            # Convert Action to config dict
            self.config = action.model_dump(mode="json", exclude_none=True)

        if context is None:
            context = self.context

        test_type = self.config.get("test_type", "row_count")
        target = self.config.get("target", f"tmp_test_{self.config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{self.config.get('name')}'"
        )
        on_violation = self.config.get("on_violation", "fail")
        logger.debug(
            f"Test '{self.config.get('name')}': on_violation='{on_violation}', source={self.config.get('source')}"
        )

        render_context = self._build_render_context(test_type, context)
        return self.render_template(f"test/{test_type}.py.j2", render_context)

    def _build_render_context(
        self, test_type: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the template-render context dict for the given test type.

        Produces all common keys (add_imports, target, description,
        fail_expectations, drop_expectations, warn_expectations) plus the
        per-test-type-specific fields each template expects.
        """
        target = self.config.get("target", f"tmp_test_{self.config.get('name')}")
        description = self.config.get("description", f"Test: {test_type}")
        # In standalone mode (no flowgroup), the template prepends imports.
        # In orchestrator mode, imports are added at file-level so we skip them.
        add_imports = not context or "flowgroup" not in context

        # Group expectations by violation action
        expectations = self._build_expectations(test_type)
        fail_expectations: Dict[str, str] = {}
        drop_expectations: Dict[str, str] = {}
        warn_expectations: Dict[str, str] = {}
        for exp in expectations:
            name = exp["name"]
            expression = exp["expression"]
            violation = exp.get("on_violation", "fail")
            if violation == "fail":
                fail_expectations[name] = expression
            elif violation == "drop":
                drop_expectations[name] = expression
            elif violation == "warn":
                warn_expectations[name] = expression

        ctx: Dict[str, Any] = {
            "add_imports": add_imports,
            "target": target,
            "description": description,
            "fail_expectations": fail_expectations,
            "drop_expectations": drop_expectations,
            "warn_expectations": warn_expectations,
        }

        # Per-test-type-specific fields
        if test_type == "row_count":
            source = self.config.get("source", [])
            ctx["source_table_0"] = source[0] if len(source) >= 1 else ""
            ctx["source_table_1"] = source[1] if len(source) >= 2 else ""
        elif test_type == "uniqueness":
            source = self.config.get("source")
            if isinstance(source, list):
                source = source[0] if source else "source_table"
            columns = self.config.get("columns", [])
            ctx["source"] = source
            ctx["columns_str"] = ", ".join(columns) if columns else "id"
            filter_clause = self.config.get("filter", "") or ""
            ctx["filter_clause"] = filter_clause.strip()
        elif test_type == "referential_integrity":
            source = self.config.get("source")
            if isinstance(source, list):
                source = source[0] if source else "source_table"
            reference = self.config.get("reference", "reference_table")
            source_cols = self.config.get("source_columns", ["id"])
            ref_cols = self.config.get("reference_columns", ["id"])
            join_conditions = [
                f"s.{s_col} = r.{r_col}" for s_col, r_col in zip(source_cols, ref_cols)
            ]
            ctx["source"] = source
            ctx["reference"] = reference
            ctx["ref_col"] = ref_cols[0] if ref_cols else "id"
            ctx["join_condition"] = " AND ".join(join_conditions)
        elif test_type == "completeness":
            source = self.config.get("source")
            if isinstance(source, list):
                source = source[0] if source else "source_table"
            required_columns = self.config.get("required_columns", [])
            ctx["source"] = source
            ctx["columns"] = ", ".join(required_columns) if required_columns else "*"
        elif test_type == "range":
            source = self.config.get("source")
            if isinstance(source, list):
                source = source[0] if source else "source_table"
            ctx["source"] = source
            ctx["column"] = self.config.get("column", "*")
        elif test_type == "all_lookups_found":
            source = self.config.get("source")
            if isinstance(source, list):
                source = source[0] if source else "source_table"
            lookup_cols = self.config.get("lookup_columns", ["id"])
            result_cols = self.config.get("lookup_result_columns", ["result"])
            join_conditions = [f"s.{col} = l.{col}" for col in lookup_cols]
            ctx["source"] = source
            ctx["lookup_table"] = self.config.get("lookup_table", "lookup_table")
            ctx["lookup_result"] = result_cols[0] if result_cols else "result"
            ctx["join_condition"] = " AND ".join(join_conditions)
        elif test_type == "schema_match":
            source = self.config.get("source")
            if isinstance(source, list):
                source = source[0] if source else ""
            reference = self.config.get("reference")
            src_catalog, src_schema, src_table = self._parse_three_part_fqn(
                source, "source"
            )
            ref_catalog, ref_schema, ref_table = self._parse_three_part_fqn(
                reference, "reference"
            )
            ctx["src_catalog"] = src_catalog
            ctx["src_schema"] = src_schema
            ctx["src_table"] = src_table
            ctx["ref_catalog"] = ref_catalog
            ctx["ref_schema"] = ref_schema
            ctx["ref_table"] = ref_table
        elif test_type == "custom_sql":
            ctx["sql"] = self.config.get("sql", "")
            # ``source`` is the pre-refactor fallback target — when no ``sql``
            # is supplied, the template emits ``return spark.table(<source>)``
            # (byte-matching the pre-Phase-9.3 generator's behaviour).
            source = self.config.get("source")
            if isinstance(source, list):
                source = source[0] if source else "source_table"
            ctx["source"] = source
        elif test_type == "custom_expectations":
            source = self.config.get("source")
            if isinstance(source, list):
                source = source[0] if source else "source_table"
            ctx["source"] = source

        return ctx

    def _parse_three_part_fqn(self, value: Any, field: str) -> tuple:
        """Parse and validate a 3-part Unity Catalog FQN.

        The schema_match SQL queries ``information_schema.columns``, which in
        Unity Catalog stores the *unqualified* table name in ``table_name``. To
        produce a SQL predicate that actually matches a single table we must
        split the user-supplied FQN into ``(catalog, schema, table)`` and use
        all three parts in the WHERE clause — and also catalog-qualify
        ``information_schema.columns`` so the query targets the right
        ``information_schema`` (each UC catalog has its own).

        Args:
            value: The FQN string from action config (already substituted).
            field: Action field name (``source`` or ``reference``), surfaced
                in the error context.

        Returns:
            Tuple of (catalog, schema, table) — all non-empty strings.

        Raises:
            LHPValidationError: When the input is not exactly three
                dot-separated non-empty parts, or contains backticks.
        """
        invalid = not isinstance(value, str) or "`" in str(value or "")
        parts: List[str] = []
        if not invalid:
            parts = value.split(".")
            if len(parts) != 3 or not all(parts):
                invalid = True
        if invalid:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="022",
                title="Invalid table identifier for schema_match",
                details=(
                    f"Field '{field}' for the schema_match test must be a "
                    f"fully-qualified Unity Catalog table name in the form "
                    f"'catalog.schema.table'. Got: {value!r}."
                ),
                suggestions=[
                    "Use a three-part name: '<catalog>.<schema>.<table>'",
                    "Resolve the FQN via substitutions if the parts come from "
                    "env (e.g. '${catalog}.${silver_schema}.fact_orders')",
                    "Avoid backticked identifiers; use plain dotted form",
                ],
                context={
                    "Provided": str(value),
                    "Field": field,
                    "Test type": "schema_match",
                },
            )
        return parts[0], parts[1], parts[2]

    def _build_expectations(self, test_type: str) -> List[Dict[str, Any]]:
        """Build expectations based on test type."""
        on_violation = self.config.get("on_violation", "fail")
        # Validate on_violation value, default to 'fail' if invalid
        if on_violation not in ["fail", "warn", "drop"]:
            on_violation = "fail"

        if test_type == "row_count":
            tolerance = self.config.get("tolerance", 0)
            return [
                {
                    "name": "row_count_match",
                    "expression": f"abs(source_count - target_count) <= {tolerance}",
                    "on_violation": on_violation,
                }
            ]

        elif test_type == "uniqueness":
            return [
                {
                    "name": "no_duplicates",
                    "expression": "duplicate_count == 0",
                    "on_violation": on_violation,
                }
            ]

        elif test_type == "referential_integrity":
            ref_cols = self.config.get("reference_columns", ["id"])
            ref_col = ref_cols[0] if ref_cols else "id"
            return [
                {
                    "name": "referential_integrity",
                    "expression": f"ref_{ref_col} IS NOT NULL",
                    "on_violation": on_violation,
                }
            ]

        elif test_type == "completeness":
            required_cols = self.config.get("required_columns", [])
            if required_cols:
                expressions = [f"{col} IS NOT NULL" for col in required_cols]
                return [
                    {
                        "name": "required_fields_complete",
                        "expression": " AND ".join(expressions),
                        "on_violation": on_violation,
                    }
                ]

        elif test_type == "range":
            column = self.config.get("column", "value")
            min_val = self.config.get("min_value")
            max_val = self.config.get("max_value")

            expressions = []
            if min_val is not None:
                expressions.append(f"{column} >= '{min_val}'")
            if max_val is not None:
                expressions.append(f"{column} <= '{max_val}'")

            if expressions:
                return [
                    {
                        "name": "value_in_range",
                        "expression": " AND ".join(expressions),
                        "on_violation": on_violation,
                    }
                ]

        elif test_type == "all_lookups_found":
            result_cols = self.config.get("lookup_result_columns", ["result"])
            result_col = result_cols[0] if result_cols else "result"
            return [
                {
                    "name": "all_lookups_found",
                    "expression": f"lookup_{result_col} IS NOT NULL",
                    "on_violation": on_violation,
                }
            ]

        elif test_type == "schema_match":
            return [
                {
                    "name": "schemas_match",
                    "expression": "false",  # Fails if any row exists (schema differences)
                    "on_violation": on_violation,
                }
            ]

        elif test_type == "custom_expectations":
            # Pass through custom expectations
            return self.config.get("expectations", [])
        elif test_type == "custom_sql":
            # Custom SQL can also have custom expectations
            return self.config.get("expectations", [])

        return []
