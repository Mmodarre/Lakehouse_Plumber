import logging
from typing import Any, Dict, List

from lhp.models import Action

from ._base import BaseTestActionGenerator

logger = logging.getLogger(__name__)


class UniquenessTestGenerator(BaseTestActionGenerator):
    """Generate a uniqueness test — asserts no duplicate rows for columns."""

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        config = action.model_dump(mode="json", exclude_none=True)
        test_type = "uniqueness"

        target = config.get("target", f"tmp_test_{config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{config.get('name')}'"
        )

        on_violation = self._normalize_on_violation(config)
        expectations: List[Dict[str, Any]] = [
            {
                "name": "no_duplicates",
                "expression": "duplicate_count == 0",
                "on_violation": on_violation,
            }
        ]

        ctx = self._common_render_context(config, test_type, context, expectations)
        source = config.get("source")
        if isinstance(source, list):
            source = source[0] if source else "source_table"
        columns = config.get("columns", [])
        ctx["source"] = source
        ctx["columns_str"] = ", ".join(columns) if columns else "id"
        filter_clause = config.get("filter", "") or ""
        ctx["filter_clause"] = filter_clause.strip()

        return self.render_template(f"test/{test_type}.py.j2", ctx)
