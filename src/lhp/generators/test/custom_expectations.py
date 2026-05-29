"""Custom-expectations test action generator."""

import logging
from typing import Any, Dict, List

from lhp.models import Action

from ._base import BaseTestActionGenerator

logger = logging.getLogger(__name__)


class CustomExpectationsTestGenerator(BaseTestActionGenerator):
    """Generate a custom_expectations test — pass-through user expectations."""

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate custom_expectations test code."""
        config = action.model_dump(mode="json", exclude_none=True)
        test_type = "custom_expectations"

        target = config.get("target", f"tmp_test_{config.get('name')}")
        logger.debug(
            f"Generating test action: test_type='{test_type}', target='{target}', name='{config.get('name')}'"
        )

        expectations: List[Dict[str, Any]] = config.get("expectations", [])

        ctx = self._common_render_context(config, test_type, context, expectations)
        source = config.get("source")
        if isinstance(source, list):
            source = source[0] if source else "source_table"
        ctx["source"] = source

        return self.render_template(f"test/{test_type}.py.j2", ctx)
