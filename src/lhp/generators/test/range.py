"""Range test action generator."""

import logging
from typing import Any, Dict, List

from lhp.models import Action

from ._base import BaseTestActionGenerator

logger = logging.getLogger(__name__)


class RangeTestGenerator(BaseTestActionGenerator):
    """Generate a range test — a column's values must fall within bounds."""

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate range test code."""
        config = action.model_dump(mode="json", exclude_none=True)
        test_type = "range"

        target = config.get("target", f"tmp_test_{config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{config.get('name')}'"
        )

        on_violation = self._normalize_on_violation(config)
        column = config.get("column", "value")
        min_val = config.get("min_value")
        max_val = config.get("max_value")

        expressions = []
        if min_val is not None:
            expressions.append(f"{column} >= '{min_val}'")
        if max_val is not None:
            expressions.append(f"{column} <= '{max_val}'")

        expectations: List[Dict[str, Any]] = []
        if expressions:
            expectations = [
                {
                    "name": "value_in_range",
                    "expression": " AND ".join(expressions),
                    "on_violation": on_violation,
                }
            ]

        ctx = self._common_render_context(config, test_type, context, expectations)
        source = config.get("source")
        if isinstance(source, list):
            source = source[0] if source else "source_table"
        ctx["source"] = source
        ctx["column"] = config.get("column", "*")

        return self.render_template(f"test/{test_type}.py.j2", ctx)
