"""Referential-integrity test action generator."""

import logging
from typing import Any, Dict, List

from lhp.models import Action

from ._base import BaseTestActionGenerator

logger = logging.getLogger(__name__)


class ReferentialIntegrityTestGenerator(BaseTestActionGenerator):
    """Generate a referential_integrity test — source rows must match reference."""

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate referential_integrity test code."""
        config = action.model_dump(mode="json", exclude_none=True)
        test_type = "referential_integrity"

        target = config.get("target", f"tmp_test_{config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{config.get('name')}'"
        )

        on_violation = self._normalize_on_violation(config)
        ref_cols = config.get("reference_columns", ["id"])
        ref_col = ref_cols[0] if ref_cols else "id"
        expectations: List[Dict[str, Any]] = [
            {
                "name": "referential_integrity",
                "expression": f"ref_{ref_col} IS NOT NULL",
                "on_violation": on_violation,
            }
        ]

        ctx = self._common_render_context(config, test_type, context, expectations)
        source = config.get("source")
        if isinstance(source, list):
            source = source[0] if source else "source_table"
        reference = config.get("reference", "reference_table")
        source_cols = config.get("source_columns", ["id"])
        ref_cols = config.get("reference_columns", ["id"])
        join_conditions = [
            f"s.{s_col} = r.{r_col}"
            for s_col, r_col in zip(source_cols, ref_cols, strict=False)
        ]
        ctx["source"] = source
        ctx["reference"] = reference
        ctx["ref_col"] = ref_cols[0] if ref_cols else "id"
        ctx["join_condition"] = " AND ".join(join_conditions)

        return self.render_template(f"test/{test_type}.py.j2", ctx)
