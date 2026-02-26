"""Project configuration loader for LakehousePlumber.

Loads project-level configuration from lhp.yaml including operational metadata definitions.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

from ..models.config import (
    EventLogConfig,
    MonitoringConfig,
    MonitoringMaterializedViewConfig,
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
            from ..utils.yaml_loader import load_yaml_file

            config_data = load_yaml_file(
                self.config_file,
                allow_empty=False,
                error_context="project configuration file",
            )

            if not config_data:
                self.logger.warning(
                    f"Empty project configuration file: {self.config_file}"
                )
                return None

            # Parse the configuration
            project_config = self._parse_project_config(config_data)

            self.logger.info(f"Loaded project configuration from {self.config_file}")
            return project_config

        except ValueError as e:
            # yaml_loader converts YAML and file errors to ValueError with clear context
            error_msg = str(e)
            self.logger.error(f"Project configuration loading failed: {error_msg}")

            # Determine if it's a YAML syntax error or file error
            if "Invalid YAML" in error_msg:
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

        # Parse event_log configuration
        event_log_config = None
        if "event_log" in config_data:
            event_log_config = self._parse_event_log_config(config_data["event_log"])

        # Parse monitoring configuration
        monitoring_config = None
        if "monitoring" in config_data:
            monitoring_config = self._parse_monitoring_config(
                config_data["monitoring"], event_log_config
            )

        # Create project config
        project_config = ProjectConfig(
            name=config_data.get("name", "unnamed_project"),
            version=config_data.get("version", "1.0"),
            description=config_data.get("description"),
            author=config_data.get("author"),
            created_date=config_data.get("created_date"),
            include=include_patterns,
            operational_metadata=operational_metadata_config,
            event_log=event_log_config,
            monitoring=monitoring_config,
            required_lhp_version=config_data.get("required_lhp_version"),
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

    def _parse_event_log_config(self, event_log_data: Any) -> EventLogConfig:
        """Parse event_log configuration from lhp.yaml.

        Args:
            event_log_data: Raw event_log data from YAML

        Returns:
            Parsed EventLogConfig

        Raises:
            LHPError: If event_log configuration is invalid
        """
        if not isinstance(event_log_data, dict):
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="006",
                title="Invalid event_log configuration",
                details=f"event_log must be a mapping, got {type(event_log_data).__name__}",
                suggestions=[
                    "Define event_log as a YAML mapping with keys: enabled, catalog, schema, name_prefix, name_suffix",
                    "Example: event_log:\\n  catalog: my_catalog\\n  schema: _meta",
                ],
            )

        try:
            config = EventLogConfig(
                enabled=event_log_data.get("enabled", True),
                catalog=event_log_data.get("catalog"),
                schema=event_log_data.get("schema"),
                name_prefix=event_log_data.get("name_prefix", ""),
                name_suffix=event_log_data.get("name_suffix", ""),
            )
        except Exception as e:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="006",
                title="Error parsing event_log configuration",
                details=f"Failed to parse event_log configuration: {e}",
                suggestions=[
                    "Check event_log field types: enabled (bool), catalog (string), schema (string)",
                    "name_prefix and name_suffix must be strings",
                ],
            )

        self._validate_event_log_config(config)
        return config

    def _validate_event_log_config(self, config: EventLogConfig) -> None:
        """Validate event_log configuration.

        If enabled, both catalog and schema must be provided.

        Args:
            config: EventLogConfig to validate

        Raises:
            LHPError: If enabled but catalog or schema missing
        """
        if not config.enabled:
            return

        if not config.catalog or not config.schema_:
            missing = []
            if not config.catalog:
                missing.append("catalog")
            if not config.schema_:
                missing.append("schema")
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="007",
                title="Incomplete event_log configuration",
                details=(
                    f"event_log is enabled but missing required fields: {', '.join(missing)}. "
                    f"Both 'catalog' and 'schema' are required when event_log is enabled."
                ),
                suggestions=[
                    "Add the missing fields to event_log in lhp.yaml",
                    "Or set 'enabled: false' to disable project-level event logging",
                    "Example: event_log:\\n  catalog: my_catalog\\n  schema: _meta",
                ],
            )

    def _parse_monitoring_config(
        self,
        monitoring_data: Any,
        event_log_config: Optional[EventLogConfig],
    ) -> MonitoringConfig:
        """Parse monitoring configuration from lhp.yaml.

        Handles `monitoring: {}` (all defaults) and explicit configuration.

        Args:
            monitoring_data: Raw monitoring data from YAML
            event_log_config: Parsed event_log config (for cross-validation)

        Returns:
            Parsed MonitoringConfig

        Raises:
            LHPError: If monitoring configuration is invalid
        """
        # Handle monitoring: {} (empty dict = all defaults)
        if monitoring_data is None:
            monitoring_data = {}

        if not isinstance(monitoring_data, dict):
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="008",
                title="Invalid monitoring configuration",
                details=f"monitoring must be a mapping, got {type(monitoring_data).__name__}",
                suggestions=[
                    "Define monitoring as a YAML mapping",
                    "Example: monitoring:\n  pipeline_name: my_monitor",
                    "Use 'monitoring: {}' for all defaults",
                ],
            )

        # Parse materialized_views list
        mv_configs = None
        raw_mvs = monitoring_data.get("materialized_views")
        if raw_mvs is not None:
            if not isinstance(raw_mvs, list):
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="008",
                    title="Invalid monitoring materialized_views",
                    details="materialized_views must be a list of view definitions",
                    suggestions=[
                        "Define materialized_views as a YAML list",
                        "Example:\n  materialized_views:\n    - name: events_summary\n      sql: 'SELECT ...'",
                    ],
                )
            mv_configs = []
            for mv_data in raw_mvs:
                if not isinstance(mv_data, dict):
                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="008",
                        title="Invalid materialized view entry",
                        details=f"Each materialized_view must be a mapping, got {type(mv_data).__name__}",
                        suggestions=[
                            "Define each view with at least a 'name' field",
                            "Example:\n  - name: events_summary\n    sql: 'SELECT ...'",
                        ],
                    )
                mv_configs.append(
                    MonitoringMaterializedViewConfig(
                        name=mv_data.get("name", ""),
                        sql=mv_data.get("sql"),
                        sql_path=mv_data.get("sql_path"),
                    )
                )

        try:
            config = MonitoringConfig(
                enabled=monitoring_data.get("enabled", True),
                pipeline_name=monitoring_data.get("pipeline_name"),
                catalog=monitoring_data.get("catalog"),
                schema=monitoring_data.get("schema"),
                streaming_table=monitoring_data.get(
                    "streaming_table", "all_pipelines_event_log"
                ),
                custom_actions=monitoring_data.get("custom_actions"),
                materialized_views=mv_configs,
            )
        except Exception as e:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="008",
                title="Error parsing monitoring configuration",
                details=f"Failed to parse monitoring configuration: {e}",
                suggestions=[
                    "Check monitoring field types: enabled (bool), pipeline_name (string)",
                    "catalog and schema must be strings",
                ],
            )

        self._validate_monitoring_config(config, event_log_config)
        return config

    def _validate_monitoring_config(
        self,
        config: MonitoringConfig,
        event_log_config: Optional[EventLogConfig],
    ) -> None:
        """Validate monitoring configuration.

        Checks:
        - If enabled, event_log must be present and enabled
        - MV names must be unique
        - MVs must not specify both sql and sql_path

        Args:
            config: MonitoringConfig to validate
            event_log_config: Project-level event log config

        Raises:
            LHPError: If validation fails
        """
        if not config.enabled:
            return

        # Monitoring requires event_log to be present and enabled
        if not event_log_config or not event_log_config.enabled:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="008",
                title="Monitoring requires event_log",
                details=(
                    "monitoring is enabled but event_log is either missing or disabled. "
                    "The monitoring pipeline needs event_log tables to function."
                ),
                suggestions=[
                    "Add an event_log section to lhp.yaml with catalog and schema",
                    "Or set 'monitoring: { enabled: false }' to disable monitoring",
                    "Example:\n  event_log:\n    catalog: my_catalog\n    schema: _meta",
                ],
            )

        # Validate materialized views
        if config.materialized_views:
            seen_names: Dict[str, int] = {}
            for i, mv in enumerate(config.materialized_views):
                # Name is required
                if not mv.name:
                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="008",
                        title="Materialized view missing name",
                        details=f"Materialized view at index {i} has no 'name' field",
                        suggestions=[
                            "Add a 'name' field to each materialized view",
                            "Example:\n  - name: events_summary\n    sql: 'SELECT ...'",
                        ],
                    )

                # Unique names
                if mv.name in seen_names:
                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="008",
                        title="Duplicate materialized view name",
                        details=(
                            f"Materialized view name '{mv.name}' appears at "
                            f"index {seen_names[mv.name]} and {i}"
                        ),
                        suggestions=[
                            "Each materialized view must have a unique name",
                        ],
                    )
                seen_names[mv.name] = i

                # Cannot specify both sql and sql_path
                if mv.sql and mv.sql_path:
                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="008",
                        title="Ambiguous materialized view SQL source",
                        details=(
                            f"Materialized view '{mv.name}' specifies both 'sql' and 'sql_path'. "
                            f"Only one is allowed."
                        ),
                        suggestions=[
                            "Use 'sql' for inline SQL or 'sql_path' for external file, not both",
                        ],
                    )

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
