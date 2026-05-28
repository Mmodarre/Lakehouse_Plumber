"""Parse ``test_reporting`` section of lhp.yaml."""

import logging
from typing import Any

from ...errors import ErrorCategory, LHPError
from lhp.models import TestReportingConfig

logger = logging.getLogger(__name__)


def parse_test_reporting_config(test_reporting_data: Any) -> TestReportingConfig:
    """Parse and validate the ``test_reporting`` mapping. ``module_path`` and ``function_name`` are required."""
    if not isinstance(test_reporting_data, dict):
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="009",
            title="Invalid test_reporting configuration",
            details=f"test_reporting must be a mapping, got {type(test_reporting_data).__name__}",
            suggestions=[
                "Define test_reporting as a YAML mapping with keys: module_path, function_name",
                "Example: test_reporting:\n  module_path: src/my_publisher.py\n  function_name: publish_results",
            ],
        )

    missing = []
    if "module_path" not in test_reporting_data:
        missing.append("module_path")
    if "function_name" not in test_reporting_data:
        missing.append("function_name")

    if missing:
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="009",
            title="Incomplete test_reporting configuration",
            details=f"test_reporting is missing required fields: {', '.join(missing)}",
            suggestions=[
                "Add the missing fields to test_reporting in lhp.yaml",
                "Required: module_path (path to provider Python file), function_name (callable name)",
                "Example: test_reporting:\n  module_path: src/my_publisher.py\n  function_name: publish_results",
            ],
        )

    try:
        return TestReportingConfig(
            module_path=test_reporting_data["module_path"],
            function_name=test_reporting_data["function_name"],
            config_file=test_reporting_data.get("config_file"),
        )
    except Exception as e:
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="009",
            title="Error parsing test_reporting configuration",
            details=f"Failed to parse test_reporting configuration: {e}",
            suggestions=[
                "Check test_reporting field types: module_path (string), function_name (string)",
                "config_file is optional and must be a string path",
            ],
        ) from e
