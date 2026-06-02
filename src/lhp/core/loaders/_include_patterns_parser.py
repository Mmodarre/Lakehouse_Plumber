"""Parse and validate ``include`` / ``blueprint_include`` / ``instance_include`` patterns."""

import logging
from typing import Any, Dict, List, Optional, Tuple

from lhp.errors import ErrorFactory, codes

logger = logging.getLogger(__name__)


def parse_include_patterns(
    config_data: Dict[str, Any],
) -> Tuple[Optional[List[str]], Optional[List[str]], Optional[List[str]]]:
    """Parse all three include-pattern sections from raw project config.

    Returns:
        Tuple of (include, blueprint_include, instance_include). Each element is
        ``None`` if the corresponding section is absent from ``config_data``.
    """
    include_patterns: Optional[List[str]] = None
    if "include" in config_data:
        include_patterns = _parse_include_list(config_data["include"])

    blueprint_include_patterns: Optional[List[str]] = None
    if "blueprint_include" in config_data:
        blueprint_include_patterns = _parse_include_list(
            config_data["blueprint_include"]
        )

    instance_include_patterns: Optional[List[str]] = None
    if "instance_include" in config_data:
        instance_include_patterns = _parse_include_list(config_data["instance_include"])

    return include_patterns, blueprint_include_patterns, instance_include_patterns


def _parse_include_list(include_data: Any) -> List[str]:
    """Parse and validate a single include-pattern list."""
    if not isinstance(include_data, list):
        raise ErrorFactory.config_error(
            codes.CFG_003,
            title="Invalid include field type",
            details=f"Include field must be a list of strings, got {type(include_data).__name__}",
            suggestions=[
                "Change include to a list format: include: ['*.yaml', 'bronze_*.yaml']",
                "Use array syntax in YAML with proper indentation",
            ],
        )

    validated_patterns = []
    for i, pattern in enumerate(include_data):
        if not isinstance(pattern, str):
            raise ErrorFactory.config_error(
                codes.CFG_004,
                title="Invalid include pattern type",
                details=f"Include pattern at index {i} must be a string, got {type(pattern).__name__}",
                suggestions=[
                    "Ensure all include patterns are strings",
                    "Quote patterns if they contain special characters",
                ],
            )

        if not _validate_include_pattern(pattern):
            raise ErrorFactory.config_error(
                codes.CFG_005,
                title="Invalid include pattern",
                details=f"Include pattern '{pattern}' is not a valid glob pattern",
                suggestions=[
                    "Use valid glob patterns like '*.yaml', 'bronze_*.yaml', 'dir/**/*.yaml'",
                    "Avoid empty patterns or invalid regex characters",
                    "Check pattern syntax for proper glob format",
                ],
            )

        validated_patterns.append(pattern)

    return validated_patterns


def _validate_include_pattern(pattern: str) -> bool:
    # Import here to avoid circular imports
    from ...utils.file_pattern_matcher import validate_pattern

    return validate_pattern(pattern)
