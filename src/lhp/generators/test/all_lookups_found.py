import logging
from typing import Any, Dict, List

from lhp.models import Action

from ._base import BaseTestActionGenerator

logger = logging.getLogger(__name__)


class AllLookupsFoundTestGenerator(BaseTestActionGenerator):
    """Generate an all_lookups_found test — every source row must resolve."""

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        config = action.model_dump(mode="json", exclude_none=True)
        test_type = "all_lookups_found"

        target = config.get("target", f"tmp_test_{config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{config.get('name')}'"
        )

        on_violation = self._normalize_on_violation(config)
        result_cols = config.get("lookup_result_columns", ["result"])
        result_col = result_cols[0] if result_cols else "result"
        expectations: List[Dict[str, Any]] = [
            {
                "name": "all_lookups_found",
                "expression": f"lookup_{result_col} IS NOT NULL",
                "on_violation": on_violation,
            }
        ]

        ctx = self._common_render_context(config, test_type, context, expectations)
        source = config.get("source")
        if isinstance(source, list):
            source = source[0] if source else "source_table"
        lookup_cols = config.get("lookup_columns", ["id"])
        result_cols = config.get("lookup_result_columns", ["result"])
        join_conditions = [f"s.{col} = l.{col}" for col in lookup_cols]
        ctx["source"] = source
        ctx["lookup_table"] = config.get("lookup_table", "lookup_table")
        ctx["lookup_result"] = result_cols[0] if result_cols else "result"
        ctx["join_condition"] = " AND ".join(join_conditions)

        return self.render_template(f"test/{test_type}.py.j2", ctx)
