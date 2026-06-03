"""Abstract base for per-test-type action generators.

Each concrete test type (row_count, uniqueness, …) is a stateless leaf
generator that subclasses :class:`BaseTestActionGenerator`, fuses its own
expectation + render-context construction, and renders a Jinja2 template
under ``templates/test/``. The shared scaffolding — the two universal
imports, ``on_violation`` normalization, fail/drop/warn expectation
bucketing, and common render-context assembly — lives here so it is
written once and reused by every leaf (constitution §3.6).
"""

import logging
from abc import abstractmethod
from typing import Any, Dict, List

from lhp.core.registry import BaseActionGenerator
from lhp.models import Action

logger = logging.getLogger(__name__)


class BaseTestActionGenerator(BaseActionGenerator):
    """Abstract base for per-test-type generators.

    Holds only the logic genuinely shared by two or more leaves. Per-type
    expression and render-context construction live on the leaves.
    """

    __test__ = False  # Tell pytest this is not a test class

    def __init__(self) -> None:
        super().__init__()

        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql.functions import *")

    @abstractmethod
    def generate(self, action: Action, context: Dict[str, Any]) -> str: ...

    def _normalize_on_violation(self, config: Dict[str, Any]) -> str:
        """Return a valid ``on_violation`` value, defaulting to ``fail``."""
        on_violation: str = config.get("on_violation", "fail")
        if on_violation not in ["fail", "warn", "drop"]:
            on_violation = "fail"
        return on_violation

    def _common_render_context(
        self,
        config: Dict[str, Any],
        test_type: str,
        context: Dict[str, Any],
        expectations: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build the render-context keys common to every test type.

        Produces ``add_imports``, ``target``, ``description`` and the three
        violation-bucketed expectation dicts. Leaves extend the returned dict
        with their per-test-type-specific fields.
        """
        target = config.get("target", f"tmp_test_{config.get('name')}")
        description = config.get("description", f"Test: {test_type}")
        # In standalone mode (no flowgroup), the template prepends imports.
        # In orchestrator mode, imports are added at file-level so we skip them.
        add_imports = not context or "flowgroup" not in context

        fail_expectations, drop_expectations, warn_expectations = (
            self._bucket_expectations(expectations)
        )

        return {
            "add_imports": add_imports,
            "target": target,
            "description": description,
            "fail_expectations": fail_expectations,
            "drop_expectations": drop_expectations,
            "warn_expectations": warn_expectations,
        }

    def _bucket_expectations(
        self, expectations: List[Dict[str, Any]]
    ) -> tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
        """Split expectations into (fail, drop, warn) name→expression dicts."""
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
        return fail_expectations, drop_expectations, warn_expectations
