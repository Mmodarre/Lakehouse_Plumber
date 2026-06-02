"""Parse and validate ``operational_metadata`` section of lhp.yaml."""

import logging
from typing import Any, Dict

from lhp.errors import ErrorFactory, codes
from lhp.models import (
    MetadataColumnConfig,
    MetadataPresetConfig,
    ProjectOperationalMetadataConfig,
)

logger = logging.getLogger(__name__)


def parse_operational_metadata_config(
    metadata_config: Dict[str, Any],
) -> ProjectOperationalMetadataConfig:
    """Parse operational metadata configuration and verify preset references resolve."""
    columns = {}
    if "columns" in metadata_config:
        for col_name, col_config in metadata_config["columns"].items():
            try:
                if isinstance(col_config, str):
                    # Shorthand: bare string is the expression.
                    col_config = {"expression": col_config}
                elif not isinstance(col_config, dict):
                    raise ValueError(
                        f"Column configuration must be dict or string, got {type(col_config)}"
                    )

                columns[col_name] = MetadataColumnConfig(
                    expression=col_config.get("expression", ""),
                    description=col_config.get("description"),
                    applies_to=col_config.get(
                        "applies_to", ["streaming_table", "materialized_view"]
                    ),
                    additional_imports=col_config.get("additional_imports"),
                    enabled=col_config.get("enabled", True),
                )

            except Exception as e:
                error_msg = (
                    f"Error parsing column '{col_name}' in operational metadata: {e}"
                )
                logger.exception(error_msg)
                raise ErrorFactory.config_error(
                    codes.CFG_003,
                    title="Invalid operational metadata column configuration",
                    details=error_msg,
                    suggestions=[
                        "Check column configuration structure",
                        "Ensure 'expression' field is provided",
                        "Verify applies_to is a list of valid target types",
                    ],
                ) from e

    presets = {}
    if "presets" in metadata_config:
        for preset_name, preset_config in metadata_config["presets"].items():
            try:
                if isinstance(preset_config, list):
                    # Shorthand: bare list is the columns list.
                    preset_config = {"columns": preset_config}
                elif not isinstance(preset_config, dict):
                    raise ValueError(
                        f"Preset configuration must be dict or list, got {type(preset_config)}"
                    )

                presets[preset_name] = MetadataPresetConfig(
                    columns=preset_config.get("columns", []),
                    description=preset_config.get("description"),
                )

            except Exception as e:
                error_msg = (
                    f"Error parsing preset '{preset_name}' in operational metadata: {e}"
                )
                logger.exception(error_msg)
                raise ErrorFactory.config_error(
                    codes.CFG_004,
                    title="Invalid operational metadata preset configuration",
                    details=error_msg,
                    suggestions=[
                        "Check preset configuration structure",
                        "Ensure 'columns' field is a list of column names",
                        "Verify all referenced columns are defined",
                    ],
                ) from e

    defaults = metadata_config.get("defaults", {})

    operational_metadata_config = ProjectOperationalMetadataConfig(
        columns=columns,
        presets=presets if presets else None,
        defaults=defaults if defaults else None,
    )

    _validate_preset_references(operational_metadata_config)

    return operational_metadata_config


def _validate_preset_references(config: ProjectOperationalMetadataConfig) -> None:
    """Validate that every preset's referenced column is defined in ``columns``."""
    if not config.presets:
        return

    defined_columns = set(config.columns.keys())

    for preset_name, preset_config in config.presets.items():
        for column_name in preset_config.columns:
            if column_name not in defined_columns:
                error_msg = f"Preset '{preset_name}' references undefined column '{column_name}'"
                logger.error(error_msg)
                raise ErrorFactory.config_error(
                    codes.CFG_005,
                    title="Invalid preset column reference",
                    details=error_msg,
                    suggestions=[
                        f"Define column '{column_name}' in operational_metadata.columns",
                        f"Remove '{column_name}' from preset '{preset_name}'",
                        "Check for typos in column names",
                    ],
                )
