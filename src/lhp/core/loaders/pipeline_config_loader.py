# JUSTIFIED: pipeline-config resolution is one substitution-pass over
# a unified document tree; splitting load/substitute/validate creates
# 3-pass overhead and a shared mutable state surface.

import logging
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

from lhp.errors import ErrorFactory, codes

logger = logging.getLogger(__name__)


class PipelineConfigLoader:
    """Load and merge pipeline configurations from multi-document YAML."""

    DEFAULT_PIPELINE_CONFIG = {
        "serverless": True,
        "edition": "ADVANCED",
        "channel": "CURRENT",
        "continuous": False,
    }

    ALLOWED_EDITIONS = {"CORE", "PRO", "ADVANCED"}
    ALLOWED_CHANNELS = {"CURRENT", "PREVIEW"}
    ALLOWED_PACKAGING_MODES = {"wheel", "source"}
    MONITORING_ALIAS = "__eventlog_monitoring"

    def __init__(
        self,
        project_root: Path,
        config_file_path: Optional[str] = None,
        monitoring_pipeline_name: Optional[str] = None,
    ):
        """
        Raises:
            FileNotFoundError: If explicit config_file_path doesn't exist
            yaml.YAMLError: If YAML syntax is invalid
            ValueError: If config validation fails
        """
        self.project_root = Path(project_root)
        self.logger = logger
        self.monitoring_pipeline_name = monitoring_pipeline_name

        self.project_defaults, self.pipeline_configs = self._load_config(
            config_file_path
        )
        self._resolve_monitoring_alias()

    def get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Merge order: DEFAULT -> project_defaults -> pipeline_specific."""
        config = deepcopy(self.DEFAULT_PIPELINE_CONFIG)

        if self.project_defaults:
            config = self._deep_merge(config, self.project_defaults)

        if pipeline_name in self.pipeline_configs:
            config = self._deep_merge(config, self.pipeline_configs[pipeline_name])

        return config

    def resolve_packaging_modes(
        self, pipeline_names: List[str]
    ) -> Dict[str, Literal["wheel", "source"]]:
        """Resolve each pipeline's packaging mode.

        Precedence (lowest to highest): hard default ``"source"`` ->
        ``project_defaults.packaging`` -> per-pipeline ``packaging``.

        The project_defaults -> per-pipeline layering is delegated to
        ``get_pipeline_config`` (the same deep-merge every consumer uses); the
        hard ``"source"`` default is applied here when neither layer sets the
        key. Any present value is guaranteed by ``_validate_config`` to be in
        ``ALLOWED_PACKAGING_MODES``.
        """
        modes: Dict[str, Literal["wheel", "source"]] = {}
        for name in pipeline_names:
            merged = self.get_pipeline_config(name)
            mode = merged.get("packaging", "source")
            modes[name] = "wheel" if mode == "wheel" else "source"
        return modes

    def _load_config(self, config_file_path: Optional[str]) -> Tuple[Dict, Dict]:
        """
        Returns:
            Tuple of (project_defaults, pipeline_configs)
            - project_defaults: Dict with project-level settings
            - pipeline_configs: Dict mapping pipeline names to their configs
        """
        if config_file_path is None:
            self.logger.debug("No pipeline config file specified, using defaults only")
            return {}, {}

        config_path = Path(config_file_path)
        if not config_path.is_absolute():
            config_path = self.project_root / config_path

        if not config_path.exists():
            raise ErrorFactory.io_error(
                codes.IO_001,
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

        from ...parsers.yaml_loader import load_yaml_documents_all

        documents = load_yaml_documents_all(
            config_path, error_context="pipeline configuration"
        )

        # Parse documents
        project_defaults = {}
        pipeline_configs = {}
        seen_pipelines = set()
        first_seen = {}  # document index where each pipeline was first defined

        for idx, doc in enumerate(documents):
            if doc is None:
                continue

            if not isinstance(doc, dict):
                self.logger.warning(f"Ignoring non-dict document: {doc}")
                continue

            if "project_defaults" in doc:
                project_defaults = doc["project_defaults"]
                self.logger.debug(
                    f"Loaded project defaults: {list(project_defaults.keys())}"
                )
                self._validate_config(project_defaults)

            elif "pipeline" in doc:
                pipeline_names_raw = doc["pipeline"]

                if isinstance(pipeline_names_raw, str):
                    pipeline_names = [pipeline_names_raw]
                elif isinstance(pipeline_names_raw, list):
                    pipeline_names = pipeline_names_raw
                else:
                    self.logger.warning(
                        f"Document {idx + 1} has invalid pipeline type: {type(pipeline_names_raw)}. "
                        f"Expected string or list. Skipping."
                    )
                    continue

                if not pipeline_names:
                    raise ErrorFactory.validation_error(
                        codes.VAL_005,
                        title="Empty pipeline list",
                        details=(
                            f"Document {idx + 1} in pipeline config has an empty pipeline list. "
                            f"At least one pipeline name is required."
                        ),
                        suggestions=[
                            "Add at least one pipeline name to the list",
                            "Use 'pipeline: my_pipeline' for a single pipeline",
                            "Use 'pipeline: [pipeline1, pipeline2]' for multiple pipelines",
                        ],
                    )

                if self.MONITORING_ALIAS in pipeline_names and len(pipeline_names) > 1:
                    raise ErrorFactory.validation_error(
                        codes.VAL_011,
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

                pipeline_config = {k: v for k, v in doc.items() if k != "pipeline"}

                self._validate_config(pipeline_config)

                for pipeline_name in pipeline_names:
                    if pipeline_name in seen_pipelines:
                        raise ErrorFactory.validation_error(
                            codes.VAL_006,
                            title="Duplicate pipeline name",
                            details=(
                                f"pipeline '{pipeline_name}' in document {idx + 1} was already defined "
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

                    pipeline_configs[pipeline_name] = deepcopy(pipeline_config)
                    self.logger.debug(
                        f"Loaded config for pipeline '{pipeline_name}': {list(pipeline_config.keys())}"
                    )

            else:
                self.logger.warning(
                    "Document has neither 'project_defaults' nor 'pipeline' key, ignoring"
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

        if self.monitoring_pipeline_name is None:
            self.logger.warning(
                f"'{self.MONITORING_ALIAS}' found in pipeline config but monitoring "
                f"is not configured or enabled in lhp.yaml. Ignoring this entry."
            )
            del self.pipeline_configs[self.MONITORING_ALIAS]
            return

        if self.monitoring_pipeline_name in self.pipeline_configs:
            raise ErrorFactory.validation_error(
                codes.VAL_010,
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

        config = self.pipeline_configs.pop(self.MONITORING_ALIAS)
        self.pipeline_configs[self.monitoring_pipeline_name] = config
        self.logger.debug(
            f"Resolved '{self.MONITORING_ALIAS}' alias to '{self.monitoring_pipeline_name}'"
        )

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        - Nested dicts are merged recursively
        - Lists are REPLACED (not appended)
        """
        result = deepcopy(base)

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = deepcopy(value)

        return result

    def _validate_config(self, config: Dict) -> None:
        """
        Does NOT validate unknown keys (allows forward compatibility).

        Raises:
            ValueError: If validation fails with helpful message
        """
        if "edition" in config:
            if config["edition"] not in self.ALLOWED_EDITIONS:
                raise ErrorFactory.validation_error(
                    codes.VAL_009,
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

        if "channel" in config:
            if config["channel"] not in self.ALLOWED_CHANNELS:
                raise ErrorFactory.validation_error(
                    codes.VAL_009,
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

        if "packaging" in config:
            if config["packaging"] not in self.ALLOWED_PACKAGING_MODES:
                raise ErrorFactory.validation_error(
                    codes.VAL_062,
                    title="Invalid pipeline packaging mode",
                    details=(
                        f"Invalid packaging '{config['packaging']}'. "
                        f"Allowed values: {', '.join(sorted(self.ALLOWED_PACKAGING_MODES))}"
                    ),
                    suggestions=[
                        f"Use one of: {', '.join(sorted(self.ALLOWED_PACKAGING_MODES))}",
                        "Check spelling and case sensitivity",
                    ],
                    context={"Provided": config["packaging"]},
                )

        if "environment" in config:
            if not isinstance(config["environment"], dict):
                raise ErrorFactory.validation_error(
                    codes.VAL_009,
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

        if "configuration" in config:
            if not isinstance(config["configuration"], dict):
                raise ErrorFactory.validation_error(
                    codes.VAL_009,
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
                    raise ErrorFactory.validation_error(
                        codes.VAL_009,
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

        # Catching permission errors at `lhp validate` time produces specific messages
        # instead of opaque failures at `databricks bundle deploy`.
        if "permissions" in config:
            if not isinstance(config["permissions"], list):
                raise ErrorFactory.validation_error(
                    codes.VAL_009,
                    title="Invalid 'permissions' field type",
                    details=(
                        f"Invalid 'permissions' value: expected a list, "
                        f"got {type(config['permissions']).__name__}."
                    ),
                    suggestions=[
                        "Use a list of permission entries",
                        "Each entry must have a 'level' and exactly one identity "
                        "(user_name, group_name, or service_principal_name)",
                    ],
                    context={"Actual Type": type(config["permissions"]).__name__},
                    example=(
                        "permissions:\n"
                        "  - level: CAN_MANAGE\n"
                        "    user_name: user@example.com"
                    ),
                )

            identity_keys = {"user_name", "group_name", "service_principal_name"}
            for idx, entry in enumerate(config["permissions"]):
                if not isinstance(entry, dict):
                    raise ErrorFactory.validation_error(
                        codes.VAL_009,
                        title=f"Invalid permissions entry at index {idx}",
                        details=(
                            f"Permissions entry {idx} must be a dict, "
                            f"got {type(entry).__name__}."
                        ),
                        suggestions=[
                            "Each permissions entry must be a mapping with 'level' "
                            "and one identity key"
                        ],
                        context={
                            "Index": idx,
                            "Actual Type": type(entry).__name__,
                        },
                    )
                if "level" not in entry or not isinstance(entry["level"], str):
                    raise ErrorFactory.validation_error(
                        codes.VAL_009,
                        title=f"Permissions entry {idx} missing 'level'",
                        details=(
                            f"Permissions entry {idx} must have a string 'level' "
                            f"field (e.g. CAN_MANAGE, CAN_VIEW, CAN_RUN, CAN_MANAGE_RUN)."
                        ),
                        suggestions=[
                            "Add a 'level' field to this permissions entry",
                            "Use one of the Databricks permission levels (CAN_VIEW, "
                            "CAN_RUN, CAN_MANAGE, CAN_MANAGE_RUN)",
                        ],
                        context={
                            "Index": idx,
                            "Entry": repr(entry),
                        },
                    )
                present = [k for k in identity_keys if k in entry]
                if len(present) != 1:
                    raise ErrorFactory.validation_error(
                        codes.VAL_009,
                        title=f"Permissions entry {idx} has invalid identity keys",
                        details=(
                            f"Permissions entry {idx} must have exactly one of "
                            f"{', '.join(sorted(identity_keys))}; found: {present}."
                        ),
                        suggestions=[
                            "Specify exactly one identity per permissions entry",
                            "Split into separate entries if multiple identities need "
                            "the same level",
                        ],
                        context={
                            "Index": idx,
                            "Identities Found": present,
                        },
                    )
