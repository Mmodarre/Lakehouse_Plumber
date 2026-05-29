"""Schema-match test action generator."""

import logging
from typing import Any, Dict, List

from lhp.errors import ErrorCategory, LHPValidationError
from lhp.models import Action

from ._base import BaseTestActionGenerator

logger = logging.getLogger(__name__)


class SchemaMatchTestGenerator(BaseTestActionGenerator):
    """Generate a schema_match test — diffs two tables' information_schema."""

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate schema_match test code."""
        config = action.model_dump(mode="json", exclude_none=True)
        test_type = "schema_match"

        target = config.get("target", f"tmp_test_{config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{config.get('name')}'"
        )

        on_violation = self._normalize_on_violation(config)
        expectations: List[Dict[str, Any]] = [
            {
                "name": "schemas_match",
                "expression": "false",  # Fails if any row exists (schema differences)
                "on_violation": on_violation,
            }
        ]

        ctx = self._common_render_context(config, test_type, context, expectations)
        source = config.get("source")
        if isinstance(source, list):
            source = source[0] if source else ""
        reference = config.get("reference")
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

        return self.render_template(f"test/{test_type}.py.j2", ctx)

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
