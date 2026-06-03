import logging
from typing import Any, Dict, List

from lhp.models import Action

from ._base import BaseTestActionGenerator

logger = logging.getLogger(__name__)


class CompletenessTestGenerator(BaseTestActionGenerator):
    """Generate a completeness test — required columns must be non-null."""

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        config = action.model_dump(mode="json", exclude_none=True)
        test_type = "completeness"

        target = config.get("target", f"tmp_test_{config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{config.get('name')}'"
        )

        on_violation = self._normalize_on_violation(config)
        required_cols = config.get("required_columns", [])
        expectations: List[Dict[str, Any]] = []
        if required_cols:
            expressions = [f"{col} IS NOT NULL" for col in required_cols]
            expectations = [
                {
                    "name": "required_fields_complete",
                    "expression": " AND ".join(expressions),
                    "on_violation": on_violation,
                }
            ]

        ctx = self._common_render_context(config, test_type, context, expectations)
        source = config.get("source")
        if isinstance(source, list):
            source = source[0] if source else "source_table"
        required_columns = config.get("required_columns", [])
        ctx["source"] = source
        ctx["columns"] = ", ".join(required_columns) if required_columns else "*"

        return self.render_template(f"test/{test_type}.py.j2", ctx)
