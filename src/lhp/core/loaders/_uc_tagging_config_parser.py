"""Parse ``uc_tagging`` section of lhp.yaml."""

import logging
from typing import Any

from lhp.errors import ErrorFactory, codes
from lhp.models import UCTaggingConfig

logger = logging.getLogger(__name__)

_BOOL_FIELDS = ("enabled", "remove_undeclared_tags")


def parse_uc_tagging_config(uc_tagging_data: Any) -> UCTaggingConfig:
    """Parse and validate the ``uc_tagging`` mapping.

    A bare/empty block (``uc_tagging:`` with nothing under it → ``None``) is
    accepted and enables the feature with defaults. Otherwise it must be a mapping;
    all keys are optional. ``enabled`` / ``remove_undeclared_tags`` must be booleans
    and ``tag_update_concurrency`` a positive integer.
    """
    if uc_tagging_data is None:
        # `uc_tagging:` declared with no body → opt in with defaults.
        return UCTaggingConfig()

    if not isinstance(uc_tagging_data, dict):
        raise ErrorFactory.config_error(
            codes.CFG_009,
            title="Invalid uc_tagging configuration",
            details=f"uc_tagging must be a mapping, got {type(uc_tagging_data).__name__}",
            suggestions=[
                "Define uc_tagging as a YAML mapping with optional keys: enabled, remove_undeclared_tags, tag_update_concurrency",
                "Example: uc_tagging:\n  enabled: true\n  remove_undeclared_tags: false\n  tag_update_concurrency: 8",
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

    concurrency = uc_tagging_data.get("tag_update_concurrency", 16)
    # bool is an int subclass — reject it explicitly.
    if (
        not isinstance(concurrency, int)
        or isinstance(concurrency, bool)
        or concurrency < 1
    ):
        raise ErrorFactory.config_error(
            codes.CFG_009,
            title="Invalid uc_tagging configuration",
            details=(
                f"uc_tagging.tag_update_concurrency must be a positive integer, got "
                f"{concurrency!r}"
            ),
            suggestions=[
                "Set uc_tagging.tag_update_concurrency to a positive integer (e.g. 8)",
            ],
        )

    return UCTaggingConfig(
        enabled=uc_tagging_data.get("enabled", True),
        remove_undeclared_tags=uc_tagging_data.get("remove_undeclared_tags", False),
        tag_update_concurrency=concurrency,
    )
