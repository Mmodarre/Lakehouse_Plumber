"""Parse ``uc_tagging`` section of lhp.yaml."""

import logging
from typing import Any

from lhp.errors import ErrorFactory, codes
from lhp.models import UCTaggingConfig

logger = logging.getLogger(__name__)

_BOOL_FIELDS = ("enabled", "remove_undeclared_tags")


def parse_uc_tagging_config(uc_tagging_data: Any) -> UCTaggingConfig:
    """Parse and validate the ``uc_tagging`` mapping.

    Both keys are optional and default to ``enabled: true`` /
    ``remove_undeclared_tags: false``. Any provided value must be a boolean.
    """
    if not isinstance(uc_tagging_data, dict):
        raise ErrorFactory.config_error(
            codes.CFG_009,
            title="Invalid uc_tagging configuration",
            details=f"uc_tagging must be a mapping, got {type(uc_tagging_data).__name__}",
            suggestions=[
                "Define uc_tagging as a YAML mapping with optional keys: enabled, remove_undeclared_tags",
                "Example: uc_tagging:\n  enabled: true\n  remove_undeclared_tags: false",
            ],
        )

    for field in _BOOL_FIELDS:
        if field in uc_tagging_data and not isinstance(uc_tagging_data[field], bool):
            raise ErrorFactory.config_error(
                codes.CFG_009,
                title="Invalid uc_tagging configuration",
                details=(
                    f"uc_tagging.{field} must be a boolean, got "
                    f"{type(uc_tagging_data[field]).__name__}"
                ),
                suggestions=[
                    f"Set uc_tagging.{field} to true or false",
                ],
            )

    return UCTaggingConfig(
        enabled=uc_tagging_data.get("enabled", True),
        remove_undeclared_tags=uc_tagging_data.get("remove_undeclared_tags", False),
    )
