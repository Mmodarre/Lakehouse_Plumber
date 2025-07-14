"""Project configuration loader for LakehousePlumber.

Loads project-level configuration from lhp.yaml including operational metadata definitions.
"""

import yaml
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

from ..models.config import (
    ProjectConfig,
    ProjectOperationalMetadataConfig,
    MetadataColumnConfig,
    MetadataPresetConfig,
)
from ..utils.error_formatter import LHPError, ErrorCategory


class ProjectConfigLoader:
    """Loads project configuration from lhp.yaml."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        self.config_file = project_root / "lhp.yaml"

    def load_project_config(self) -> Optional[ProjectConfig]:
        """Load project configuration from lhp.yaml.

        Returns:
            ProjectConfig if file exists and is valid, None otherwise
        """
        if not self.config_file.exists():
            self.logger.info(
                f"No project configuration file found at {self.config_file}"
            )
            return None

        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            if not config_data:
                self.logger.warning(
                    f"Empty project configuration file: {self.config_file}"
                )
                return None

            # Parse the configuration
            project_config = self._parse_project_config(config_data)

            self.logger.info(f"Loaded project configuration from {self.config_file}")
            return project_config

        except yaml.YAMLError as e:
            error_msg = (
                f"Invalid YAML in project configuration file {self.config_file}: {e}"
            )
            self.logger.error(error_msg)
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="001",
                title="Invalid project configuration YAML",
                details=error_msg,
                suggestions=[
                    "Check YAML syntax in lhp.yaml",
                    "Ensure proper indentation and structure",
                    "Validate YAML online or with a linter",
                ],
            )

        except Exception as e:
            error_msg = (
                f"Error loading project configuration from {self.config_file}: {e}"
            )
            self.logger.error(error_msg)
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="002",
                title="Project configuration loading failed",
                details=error_msg,
                suggestions=[
                    "Check file permissions and accessibility",
                    "Verify file is not corrupted",
                    "Check project configuration structure",
                ],
            )

    def _parse_project_config(self, config_data: Dict[str, Any]) -> ProjectConfig:
        """Parse raw configuration data into ProjectConfig model.

        Args:
            config_data: Raw configuration data from YAML

        Returns:
            Parsed ProjectConfig
        """
        # Extract operational metadata configuration
        operational_metadata_config = None
        if "operational_metadata" in config_data:
            operational_metadata_config = self._parse_operational_metadata_config(
                config_data["operational_metadata"]
            )

        # Parse and validate include patterns
        include_patterns = None
        if "include" in config_data:
            include_patterns = self._parse_include_patterns(config_data["include"])

        # Create project config
        project_config = ProjectConfig(
            name=config_data.get("name", "unnamed_project"),
            version=config_data.get("version", "1.0"),
            description=config_data.get("description"),
            author=config_data.get("author"),
            created_date=config_data.get("created_date"),
            include=include_patterns,
            operational_metadata=operational_metadata_config,
        )

        return project_config

    def _parse_include_patterns(self, include_data: Any) -> List[str]:
        """Parse and validate include patterns from configuration.

        Args:
            include_data: Raw include data from YAML

        Returns:
            List of validated include patterns

        Raises:
            LHPError: If include patterns are invalid
        """
        # Validate that include is a list
        if not isinstance(include_data, list):
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="003",
                title="Invalid include field type",
                details=f"Include field must be a list of strings, got {type(include_data).__name__}",
                suggestions=[
                    "Change include to a list format: include: ['*.yaml', 'bronze_*.yaml']",
                    "Use array syntax in YAML with proper indentation",
                ],
            )

        # Validate each pattern
        validated_patterns = []
        for i, pattern in enumerate(include_data):
            if not isinstance(pattern, str):
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="004",
                    title="Invalid include pattern type",
                    details=f"Include pattern at index {i} must be a string, got {type(pattern).__name__}",
                    suggestions=[
                        "Ensure all include patterns are strings",
                        "Quote patterns if they contain special characters",
                    ],
                )

            # Validate pattern format
            if not self._validate_include_pattern(pattern):
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="005",
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

    def _validate_include_pattern(self, pattern: str) -> bool:
        """Validate a single include pattern.

        Args:
            pattern: The pattern to validate

        Returns:
            True if pattern is valid, False otherwise
        """
        # Import here to avoid circular imports
        from ..utils.file_pattern_matcher import validate_pattern
        
        return validate_pattern(pattern)

    def _parse_operational_metadata_config(
        self, metadata_config: Dict[str, Any]
    ) -> ProjectOperationalMetadataConfig:
        """Parse operational metadata configuration.

        Args:
            metadata_config: Raw operational metadata configuration

        Returns:
            Parsed ProjectOperationalMetadataConfig
        """
        # Parse column definitions
        columns = {}
        if "columns" in metadata_config:
            for col_name, col_config in metadata_config["columns"].items():
                try:
                    # Ensure col_config is a dict
                    if isinstance(col_config, str):
                        # Simple string expression - convert to full config
                        col_config = {"expression": col_config}
                    elif not isinstance(col_config, dict):
                        raise ValueError(
                            f"Column configuration must be dict or string, got {type(col_config)}"
                        )

                    # Parse column configuration
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
                    error_msg = f"Error parsing column '{col_name}' in operational metadata: {e}"
                    self.logger.error(error_msg)
                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="003",
                        title="Invalid operational metadata column configuration",
                        details=error_msg,
                        suggestions=[
                            "Check column configuration structure",
                            "Ensure 'expression' field is provided",
                            "Verify applies_to is a list of valid target types",
                        ],
                    )

        # Parse preset definitions
        presets = {}
        if "presets" in metadata_config:
            for preset_name, preset_config in metadata_config["presets"].items():
                try:
                    if isinstance(preset_config, list):
                        # Simple list of column names
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
                    error_msg = f"Error parsing preset '{preset_name}' in operational metadata: {e}"
                    self.logger.error(error_msg)
                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="004",
                        title="Invalid operational metadata preset configuration",
                        details=error_msg,
                        suggestions=[
                            "Check preset configuration structure",
                            "Ensure 'columns' field is a list of column names",
                            "Verify all referenced columns are defined",
                        ],
                    )

        # Parse defaults
        defaults = metadata_config.get("defaults", {})

        # Create operational metadata config
        operational_metadata_config = ProjectOperationalMetadataConfig(
            columns=columns,
            presets=presets if presets else None,
            defaults=defaults if defaults else None,
        )

        # Validate preset references
        self._validate_preset_references(operational_metadata_config)

        return operational_metadata_config

    def _validate_preset_references(self, config: ProjectOperationalMetadataConfig):
        """Validate that preset references point to defined columns.

        Args:
            config: Operational metadata configuration to validate
        """
        if not config.presets:
            return

        defined_columns = set(config.columns.keys())

        for preset_name, preset_config in config.presets.items():
            for column_name in preset_config.columns:
                if column_name not in defined_columns:
                    error_msg = f"Preset '{preset_name}' references undefined column '{column_name}'"
                    self.logger.error(error_msg)
                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="005",
                        title="Invalid preset column reference",
                        details=error_msg,
                        suggestions=[
                            f"Define column '{column_name}' in operational_metadata.columns",
                            f"Remove '{column_name}' from preset '{preset_name}'",
                            "Check for typos in column names",
                        ],
                    )

    def get_operational_metadata_config(
        self,
    ) -> Optional[ProjectOperationalMetadataConfig]:
        """Get operational metadata configuration from project config.

        Returns:
            ProjectOperationalMetadataConfig if available, None otherwise
        """
        project_config = self.load_project_config()
        if project_config and project_config.operational_metadata:
            return project_config.operational_metadata
        return None
