"""Pipeline configuration loader with multi-document YAML support."""

import logging
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from ...utils.error_formatter import (
    ErrorCategory,
    LHPError,
    LHPFileError,
    LHPValidationError,
)

logger = logging.getLogger(__name__)


class PipelineConfigLoader:
    """
    Load and merge pipeline configurations from multi-document YAML.

    Supports project-level defaults and per-pipeline overrides.
    Config loaded once at initialization for efficiency.
    """

    DEFAULT_PIPELINE_CONFIG = {
        "serverless": True,
        "edition": "ADVANCED",
        "channel": "CURRENT",
        "continuous": False,
    }

    ALLOWED_EDITIONS = {"CORE", "PRO", "ADVANCED"}
    ALLOWED_CHANNELS = {"CURRENT", "PREVIEW"}
    MONITORING_ALIAS = "__eventlog_monitoring"

    def __init__(
        self,
        project_root: Path,
        config_file_path: Optional[str] = None,
        monitoring_pipeline_name: Optional[str] = None,
    ):
        """
        Initialize and load config.

        Args:
            project_root: Project root directory
            config_file_path: Config file path relative to project_root or absolute
            monitoring_pipeline_name: Resolved monitoring pipeline name for alias support

        Raises:
            FileNotFoundError: If explicit config_file_path doesn't exist
            yaml.YAMLError: If YAML syntax is invalid
            ValueError: If config validation fails
        """
        self.project_root = Path(project_root)
        self.logger = logger
        self.monitoring_pipeline_name = monitoring_pipeline_name

        # Load and parse config
        self.project_defaults, self.pipeline_configs = self._load_config(
            config_file_path
        )
        self._resolve_monitoring_alias()

    def get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Get merged config for a specific pipeline.

        Merge order: DEFAULT -> project_defaults -> pipeline_specific

        Args:
            pipeline_name: Name of the pipeline

        Returns:
            Merged configuration dictionary
        """
        # Start with defaults (deep copy to avoid mutation)
        config = deepcopy(self.DEFAULT_PIPELINE_CONFIG)

        # Deep merge with project_defaults
        if self.project_defaults:
            config = self._deep_merge(config, self.project_defaults)

        # Deep merge with pipeline-specific (if exists)
        if pipeline_name in self.pipeline_configs:
            config = self._deep_merge(config, self.pipeline_configs[pipeline_name])

        return config

    def _load_config(self, config_file_path: Optional[str]) -> Tuple[Dict, Dict]:
        """
        Load and parse multi-document YAML config.

        Returns:
            Tuple of (project_defaults, pipeline_configs)
            - project_defaults: Dict with project-level settings
            - pipeline_configs: Dict mapping pipeline names to their configs
        """
        # No config file specified - return empty defaults
        if config_file_path is None:
            self.logger.debug("No pipeline config file specified, using defaults only")
            return {}, {}

        # Resolve config file path
        config_path = Path(config_file_path)
        if not config_path.is_absolute():
            config_path = self.project_root / config_path

        # Check file exists
        if not config_path.exists():
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="001",
                title="Pipeline config file not found",
                details=f"Pipeline config file not found: {config_file_path}",
                suggestions=[
                    f"Ensure the file exists at: {config_path}",
                    "Check the file path for typos",
                    "Create the pipeline_config.yaml file if it doesn't exist",
                ],
                context={
                    "File Path": str(config_file_path),
                    "Resolved Path": str(config_path),
                },
            )

        self.logger.info(f"Loading pipeline config from: {config_path}")

        # Load all YAML documents (including empty ones for project_defaults handling)
        from ...utils.yaml_loader import load_yaml_documents_all

        documents = load_yaml_documents_all(
            config_path, error_context="pipeline configuration"
        )

        # Parse documents
        project_defaults = {}
        pipeline_configs = {}
        seen_pipelines = set()
        first_seen = {}  # Track which document first defined each pipeline

        for idx, doc in enumerate(documents):
            # Skip None/empty documents
            if doc is None:
                continue

            if not isinstance(doc, dict):
                self.logger.warning(f"Ignoring non-dict document: {doc}")
                continue

            # Check if it's project defaults
            if "project_defaults" in doc:
                project_defaults = doc["project_defaults"]
                self.logger.debug(
                    f"Loaded project defaults: {list(project_defaults.keys())}"
                )
                # Validate project defaults
                self._validate_config(project_defaults)

            # Check if it's a pipeline-specific config
            elif "pipeline" in doc:
                pipeline_names_raw = doc["pipeline"]

                # Normalize to list (support both string and list)
                if isinstance(pipeline_names_raw, str):
                    pipeline_names = [pipeline_names_raw]
                elif isinstance(pipeline_names_raw, list):
                    pipeline_names = pipeline_names_raw
                else:
                    self.logger.warning(
                        f"Document {idx+1} has invalid pipeline type: {type(pipeline_names_raw)}. "
                        f"Expected string or list. Skipping."
                    )
                    continue

                # Validate non-empty list
                if not pipeline_names:
                    raise LHPError(
                        category=ErrorCategory.VALIDATION,
                        code_number="005",
                        title="Empty pipeline list",
                        details=(
                            f"Document {idx+1} in pipeline config has an empty pipeline list. "
                            f"At least one pipeline name is required."
                        ),
                        suggestions=[
                            "Add at least one pipeline name to the list",
                            "Use 'pipeline: my_pipeline' for a single pipeline",
                            "Use 'pipeline: [pipeline1, pipeline2]' for multiple pipelines",
                        ],
                    )

                # Validate: monitoring alias cannot be in a pipeline list
                if self.MONITORING_ALIAS in pipeline_names and len(pipeline_names) > 1:
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="011",
                        title="Monitoring alias cannot be in a pipeline list",
                        details=(
                            f"'{self.MONITORING_ALIAS}' must be used as a standalone pipeline entry, "
                            f"not in a list with other pipelines."
                        ),
                        suggestions=[
                            f"Use 'pipeline: {self.MONITORING_ALIAS}' as its own YAML document",
                            "Create a separate document for the monitoring pipeline config",
                        ],
                        context={"Pipeline List": pipeline_names},
                    )

                # Extract all keys except 'pipeline'
                pipeline_config = {k: v for k, v in doc.items() if k != "pipeline"}

                # Validate the config before processing
                self._validate_config(pipeline_config)

                # Process each pipeline_name in the list
                for pipeline_name in pipeline_names:
                    # Validate for duplicates
                    if pipeline_name in seen_pipelines:
                        raise LHPError(
                            category=ErrorCategory.VALIDATION,
                            code_number="006",
                            title="Duplicate pipeline name",
                            details=(
                                f"pipeline '{pipeline_name}' in document {idx+1} was already defined "
                                f"in document {first_seen[pipeline_name]}. Each pipeline must be unique "
                                f"across all documents in the config file."
                            ),
                            suggestions=[
                                f"Remove the duplicate '{pipeline_name}' from one of the documents",
                                "Ensure each pipeline name appears only once in the entire config file",
                                "If you want to override a config, use the same pipeline name with different values",
                            ],
                            context={
                                "duplicate_pipeline": pipeline_name,
                                "first_defined_in_document": first_seen[pipeline_name],
                                "duplicate_in_document": idx + 1,
                            },
                        )

                    seen_pipelines.add(pipeline_name)
                    first_seen[pipeline_name] = idx + 1

                    # Deep copy config for each pipeline to ensure independence
                    pipeline_configs[pipeline_name] = deepcopy(pipeline_config)
                    self.logger.debug(
                        f"Loaded config for pipeline '{pipeline_name}': {list(pipeline_config.keys())}"
                    )

            else:
                self.logger.warning(
                    f"Document has neither 'project_defaults' nor 'pipeline' key, ignoring"
                )

        return project_defaults, pipeline_configs

    def _resolve_monitoring_alias(self) -> None:
        """Resolve __eventlog_monitoring alias to actual monitoring pipeline name.

        If the alias is found in pipeline_configs:
        - If monitoring_pipeline_name is None (monitoring not configured): warn and remove
        - If actual name also exists: raise error (collision)
        - Otherwise: rename alias key to actual monitoring pipeline name
        """
        if self.MONITORING_ALIAS not in self.pipeline_configs:
            return

        # Check: monitoring not configured → warn and ignore
        if self.monitoring_pipeline_name is None:
            self.logger.warning(
                f"'{self.MONITORING_ALIAS}' found in pipeline config but monitoring "
                f"is not configured or enabled in lhp.yaml. Ignoring this entry."
            )
            del self.pipeline_configs[self.MONITORING_ALIAS]
            return

        # Check: collision — both alias and actual name defined
        if self.monitoring_pipeline_name in self.pipeline_configs:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="010",
                title="Duplicate monitoring pipeline configuration",
                details=(
                    f"Both '{self.MONITORING_ALIAS}' alias and the actual monitoring pipeline "
                    f"name '{self.monitoring_pipeline_name}' are defined in pipeline config. "
                    f"Use one or the other, not both."
                ),
                suggestions=[
                    f"Remove either the '{self.MONITORING_ALIAS}' entry or the "
                    f"'{self.monitoring_pipeline_name}' entry",
                    f"The '{self.MONITORING_ALIAS}' alias automatically resolves to "
                    f"'{self.monitoring_pipeline_name}'",
                ],
                context={
                    "Alias": self.MONITORING_ALIAS,
                    "Actual Name": self.monitoring_pipeline_name,
                },
            )

        # Resolve: rename alias key to actual name
        config = self.pipeline_configs.pop(self.MONITORING_ALIAS)
        self.pipeline_configs[self.monitoring_pipeline_name] = config
        self.logger.debug(
            f"Resolved '{self.MONITORING_ALIAS}' alias to '{self.monitoring_pipeline_name}'"
        )

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        Deep merge override into base.

        - Nested dicts are merged recursively
        - Lists are REPLACED (not appended)
        - Other values are replaced

        Args:
            base: Base dictionary
            override: Override dictionary

        Returns:
            Merged dictionary (new dict, doesn't mutate inputs)
        """
        result = deepcopy(base)

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                # Recursively merge nested dicts
                result[key] = self._deep_merge(result[key], value)
            else:
                # Replace value (including lists - they don't merge)
                result[key] = deepcopy(value)

        return result

    def _validate_config(self, config: Dict) -> None:
        """
        Validate configuration values.

        Validates:
        - edition: Must be in ALLOWED_EDITIONS
        - channel: Must be in ALLOWED_CHANNELS

        Does NOT validate:
        - Complex structures (clusters, notifications, etc.)
        - Unknown keys (allows forward compatibility)

        Raises:
            ValueError: If validation fails with helpful message
        """
        # Validate edition
        if "edition" in config:
            if config["edition"] not in self.ALLOWED_EDITIONS:
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="009",
                    title="Invalid pipeline edition",
                    details=(
                        f"Invalid edition '{config['edition']}'. "
                        f"Allowed values: {', '.join(sorted(self.ALLOWED_EDITIONS))}"
                    ),
                    suggestions=[
                        f"Use one of: {', '.join(sorted(self.ALLOWED_EDITIONS))}",
                        "Check spelling and case sensitivity",
                    ],
                    context={"Provided": config["edition"]},
                )

        # Validate channel
        if "channel" in config:
            if config["channel"] not in self.ALLOWED_CHANNELS:
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="009",
                    title="Invalid pipeline channel",
                    details=(
                        f"Invalid channel '{config['channel']}'. "
                        f"Allowed values: {', '.join(sorted(self.ALLOWED_CHANNELS))}"
                    ),
                    suggestions=[
                        f"Use one of: {', '.join(sorted(self.ALLOWED_CHANNELS))}",
                        "Check spelling and case sensitivity",
                    ],
                    context={"Provided": config["channel"]},
                )

        # Validate environment (if present, must be a dict)
        if "environment" in config:
            if not isinstance(config["environment"], dict):
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="009",
                    title="Invalid 'environment' field type",
                    details=(
                        f"Invalid 'environment' value: expected a dictionary, "
                        f"got {type(config['environment']).__name__}."
                    ),
                    suggestions=[
                        "Use a dictionary format for the environment field",
                        "Example: environment: {dependencies: ['package==1.0.0']}",
                    ],
                    context={"Actual Type": type(config["environment"]).__name__},
                    example="environment:\n  dependencies:\n    - package==1.0.0",
                )

        # Validate configuration (if present, must be a dict with string values)
        if "configuration" in config:
            if not isinstance(config["configuration"], dict):
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="009",
                    title="Invalid 'configuration' field type",
                    details=(
                        f"Invalid 'configuration' value: expected a dictionary, "
                        f"got {type(config['configuration']).__name__}."
                    ),
                    suggestions=[
                        "Use a dictionary format for configuration entries",
                        'Example: configuration: {"pipelines.incompatibleViewCheck.enabled": "false"}',
                    ],
                    context={"Actual Type": type(config["configuration"]).__name__},
                )
            for key, value in config["configuration"].items():
                if not isinstance(value, str):
                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="009",
                        title=f"Invalid configuration value for key '{key}'",
                        details=(
                            f"Invalid configuration value for key '{key}': expected a string, "
                            f"got {type(value).__name__} ({value!r}). "
                            f"All Databricks pipeline configuration values must be strings."
                        ),
                        suggestions=[
                            f'Use: "{key}": "{value}"',
                            "All pipeline configuration values must be quoted strings",
                        ],
                        context={
                            "Key": key,
                            "Value": repr(value),
                            "Actual Type": type(value).__name__,
                        },
                    )

        # Note: We intentionally do NOT validate:
        # - cluster structures
        # - notification formats
        # - tag values
        # - other complex nested structures
        # This provides flexibility and forward compatibility
