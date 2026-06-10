"""Parse ``wheel`` section of lhp.yaml."""

import logging
from typing import Any

from lhp.errors import ErrorFactory, codes
from lhp.models import WheelConfig

logger = logging.getLogger(__name__)


def parse_wheel_config(wheel_data: Any) -> WheelConfig:
    """Parse and validate the ``wheel`` mapping. All fields are optional; ``artifact_volume`` (if present) must be a string.

    The ``/Volumes/...`` shape of ``artifact_volume`` is validated later,
    post-substitution; only the static type is checked here.
    """
    if not isinstance(wheel_data, dict):
        raise ErrorFactory.config_error(
            codes.CFG_060,
            title="Invalid wheel configuration",
            details=f"wheel must be a mapping, got {type(wheel_data).__name__}",
            suggestions=[
                "Define wheel as a YAML mapping with optional key: artifact_volume",
                "Example: wheel:\n  artifact_volume: /Volumes/catalog/schema/volume",
            ],
        )

    artifact_volume = wheel_data.get("artifact_volume")
    if artifact_volume is not None and not isinstance(artifact_volume, str):
        raise ErrorFactory.config_error(
            codes.CFG_060,
            title="Invalid wheel configuration",
            details=f"wheel.artifact_volume must be a string, got {type(artifact_volume).__name__}",
            suggestions=[
                "Set artifact_volume to a string path",
                "Example: wheel:\n  artifact_volume: /Volumes/catalog/schema/volume",
            ],
        )

    try:
        return WheelConfig(
            artifact_volume=artifact_volume,
        )
    except Exception as e:
        raise ErrorFactory.config_error(
            codes.CFG_060,
            title="Error parsing wheel configuration",
            details=f"Failed to parse wheel configuration: {e}",
            suggestions=[
                "Check wheel field types: artifact_volume (string)",
                "artifact_volume is optional and must be a string path",
            ],
        ) from e
