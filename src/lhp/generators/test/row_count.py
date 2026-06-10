import logging
from typing import Any, Dict, List

from lhp.models import Action

from ._base import BaseTestActionGenerator

logger = logging.getLogger(__name__)


class RowCountTestGenerator(BaseTestActionGenerator):
    """Generate a row_count test — compares source vs target row counts."""

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        config = action.model_dump(mode="json", exclude_none=True)
        test_type = "row_count"

        target = config.get("target", f"tmp_test_{config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{config.get('name')}'"
        )

        on_violation = self._normalize_on_violation(config)
        tolerance = config.get("tolerance", 0)
        expectations: List[Dict[str, Any]] = [
            {
                "name": "row_count_match",
                "expression": f"abs(source_count - target_count) <= {tolerance}",
                "on_violation": on_violation,
            }
        ]

        ctx = self._common_render_context(config, test_type, context, expectations)
        source = config.get("source", [])
        ctx["source_table_0"] = source[0] if len(source) >= 1 else ""
        ctx["source_table_1"] = source[1] if len(source) >= 2 else ""

        return self.render_template(f"test/{test_type}.py.j2", ctx)
