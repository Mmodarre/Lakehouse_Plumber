import logging
import warnings
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Set, Union

# Suppress Pydantic warning about 'schema' field shadowing BaseModel.schema() class method.
# This is deliberate: 'schema' is a UC namespace field, not related to Pydantic's schema().
warnings.filterwarnings(
    "ignore", message=r".*Field name \"schema\".*shadows an attribute.*"
)

from pydantic import (  # noqa: E402
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    model_validator,
)

from ..utils.error_formatter import (  # noqa: E402
    ErrorCategory,
    LHPValidationError,
)


class ActionType(str, Enum):
    LOAD = "load"
    TRANSFORM = "transform"
    WRITE = "write"
    TEST = "test"


class TestActionType(str, Enum):
    """Types of test actions available."""

    __test__ = False  # Tell pytest this is not a test class
    ROW_COUNT = "row_count"
    UNIQUENESS = "uniqueness"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    COMPLETENESS = "completeness"
    RANGE = "range"
    SCHEMA_MATCH = "schema_match"
    ALL_LOOKUPS_FOUND = "all_lookups_found"
    CUSTOM_SQL = "custom_sql"
    CUSTOM_EXPECTATIONS = "custom_expectations"


class ViolationAction(str, Enum):
    """Actions to take when test expectations are violated."""

    FAIL = "fail"
    WARN = "warn"


class LoadSourceType(str, Enum):
    CLOUDFILES = "cloudfiles"
    DELTA = "delta"
    SQL = "sql"
    PYTHON = "python"
    JDBC = "jdbc"
    CUSTOM_DATASOURCE = "custom_datasource"
    KAFKA = "kafka"


class TransformType(str, Enum):
    SQL = "sql"
    PYTHON = "python"
    DATA_QUALITY = "data_quality"
    TEMP_TABLE = "temp_table"
    SCHEMA = "schema"


class DQMode(str, Enum):
    """Data quality enforcement modes."""

    DQE = "dqe"
    QUARANTINE = "quarantine"


class WriteTargetType(str, Enum):
    STREAMING_TABLE = "streaming_table"
    MATERIALIZED_VIEW = "materialized_view"
    SINK = "sink"


class MetadataColumnConfig(BaseModel):
    """Configuration for a single metadata column."""

    expression: str
    description: Optional[str] = None
    applies_to: List[str] = ["streaming_table", "materialized_view"]
    additional_imports: Optional[List[str]] = None
    enabled: bool = True


class MetadataPresetConfig(BaseModel):
    """Configuration for a metadata column preset."""

    columns: List[str]
    description: Optional[str] = None


class OperationalMetadataSelection(BaseModel):
    """Operational metadata selection configuration (used in flowgroups/actions/presets)."""

    enabled: bool = True
    preset: Optional[str] = None  # Reference to project-defined preset
    columns: Optional[List[str]] = None  # Explicit column selection
    include_columns: Optional[List[str]] = None  # Alternative syntax
    exclude_columns: Optional[List[str]] = None  # Alternative syntax


class QuarantineConfig(BaseModel):
    """Configuration for quarantine mode in data quality transforms."""

    dlq_table: str = Field(..., description="Fully qualified DLQ table name")
    source_table: str = Field(
        ..., description="Fully qualified source table name for DLQ tagging"
    )


class ProjectOperationalMetadataConfig(BaseModel):
    """Project-level operational metadata configuration (definitions only)."""

    columns: Dict[str, MetadataColumnConfig]
    presets: Optional[Dict[str, MetadataPresetConfig]] = None
    defaults: Optional[Dict[str, Any]] = None


class EventLogConfig(BaseModel):
    """Project-level event log configuration for pipeline resource generation."""

    model_config = ConfigDict(populate_by_name=True)

    enabled: bool = True
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    name_prefix: str = ""
    name_suffix: str = ""


class MonitoringMaterializedViewConfig(BaseModel):
    """Configuration for a single monitoring materialized view."""

    name: str
    sql: Optional[str] = None
    sql_path: Optional[str] = None


class MonitoringConfig(BaseModel):
    """Project-level monitoring pipeline configuration.

    Generates two artifacts:

    1. A standalone notebook that runs N independent streaming queries (one per
       pipeline event log) appending into a user-created Delta table.
    2. A DLT pipeline with materialized views only, reading from that Delta table.

    A Databricks Workflow job chains: notebook_task (union) → pipeline_task (MVs).
    """

    model_config = ConfigDict(populate_by_name=True)

    enabled: bool = True
    pipeline_name: Optional[str] = None  # default: {project_name}_event_log_monitoring
    catalog: Optional[str] = None  # default: event_log.catalog
    schema_: Optional[str] = Field(None, alias="schema")  # default: event_log.schema
    streaming_table: str = "all_pipelines_event_log"  # user-created Delta table
    checkpoint_path: str = ""  # streaming checkpoint base path (required when enabled)
    job_config_path: Optional[str] = (
        None  # relative path to monitoring job config YAML (required when enabled)
    )
    max_concurrent_streams: int = 10  # ThreadPoolExecutor max_workers
    materialized_views: Optional[List[MonitoringMaterializedViewConfig]] = None
    enable_job_monitoring: bool = False


class TestReportingConfig(BaseModel):
    """Configuration for test result reporting to external systems."""

    __test__ = False  # Tell pytest this is not a test class

    module_path: str
    function_name: str
    config_file: Optional[str] = None


class ProjectConfig(BaseModel):
    """Project-level configuration loaded from lhp.yaml."""

    name: str
    version: str = "1.0"
    description: Optional[str] = None
    author: Optional[str] = None
    created_date: Optional[str] = None
    include: Optional[List[str]] = None
    blueprint_include: Optional[List[str]] = None
    instance_include: Optional[List[str]] = None
    operational_metadata: Optional[ProjectOperationalMetadataConfig] = None
    event_log: Optional[EventLogConfig] = None
    monitoring: Optional[MonitoringConfig] = None
    required_lhp_version: Optional[str] = None
    test_reporting: Optional[TestReportingConfig] = None


class WriteTarget(BaseModel):
    """Write target configuration for streaming tables, materialized views, and sinks."""

    model_config = ConfigDict(populate_by_name=True)

    type: WriteTargetType

    # Streaming table and materialized view fields
    catalog: Optional[str] = None
    schema: Optional[str] = (
        None  # UC namespace schema (not DDL — use table_schema for DDL)
    )
    database: Optional[str] = None  # REMOVE_AT_V1.0.0: deprecated, use catalog + schema
    table: Optional[str] = None
    create_table: bool = (
        True  # Default to True - optional, only set to False when needed
    )
    comment: Optional[str] = None
    table_properties: Optional[Dict[str, Any]] = None
    partition_columns: Optional[List[str]] = None
    cluster_columns: Optional[List[str]] = None
    spark_conf: Optional[Dict[str, Any]] = None
    table_schema: Optional[str] = None
    row_filter: Optional[str] = None
    temporary: bool = False
    path: Optional[str] = None
    # Materialized view specific
    refresh_schedule: Optional[str] = None
    sql: Optional[str] = None
    sql_path: Optional[str] = None

    # Sink-specific fields
    sink_type: Optional[str] = None  # delta, kafka, custom, foreachbatch
    sink_name: Optional[str] = None

    # Kafka/Event Hubs sink fields
    bootstrap_servers: Optional[str] = None
    topic: Optional[str] = None

    # Custom sink fields
    module_path: Optional[str] = None
    custom_sink_class: Optional[str] = None

    # ForEachBatch sink fields
    batch_handler: Optional[str] = None  # Inline batch handler code

    # Common sink options
    options: Optional[Dict[str, Any]] = None

    # NOTE: schema field now represents UC namespace, not DDL. Use table_schema for DDL.
    # The legacy schema→table_schema property was removed in v0.7.8.
    # The namespace_normalizer handles redirecting schema→table_schema when
    # schema appears alongside database (DDL collision case).


class Action(BaseModel):
    name: str
    type: ActionType
    source: Optional[Union[str, List[Union[str, Dict[str, Any]]], Dict[str, Any]]] = (
        None
    )
    target: Optional[str] = None
    description: Optional[str] = None
    readMode: Optional[str] = Field(
        None,
        description="Read mode: 'batch' or 'stream'. Controls spark.read vs spark.readStream",
    )
    # Write-specific target configuration
    write_target: Optional[Union[WriteTarget, Dict[str, Any]]] = None
    # Action-specific configurations
    transform_type: Optional[TransformType] = None
    sql: Optional[str] = None
    sql_path: Optional[str] = None
    operational_metadata: Optional[Union[bool, List[str]]] = (
        None  # Simplified: bool or list of column names
    )
    expectations_file: Optional[str] = None  # For data quality transforms
    mode: Optional[str] = Field(
        None,
        description="Data quality mode: 'dqe' (default) or 'quarantine' (DLQ recycling)",
    )
    quarantine: Optional[QuarantineConfig] = Field(
        None,
        description="Quarantine configuration (required when mode is 'quarantine')",
    )
    # Schema transform specific fields
    schema_inline: Optional[str] = (
        None  # Inline schema definition (arrow or YAML format)
    )
    schema_file: Optional[str] = None  # External schema file path
    enforcement: Optional[str] = None  # Schema enforcement mode: strict or permissive
    # Python transform specific fields
    module_path: Optional[str] = (
        None  # Path to Python module (relative to project root)
    )
    function_name: Optional[str] = None  # Python function name to call
    parameters: Optional[Dict[str, Any]] = None  # Parameters passed to Python function
    # Custom data source specific fields
    custom_datasource_class: Optional[str] = None  # Custom DataSource class name
    # Write action specific
    once: Optional[bool] = None  # For one-time flows/backfills
    # Test action specific fields
    test_type: Optional[str] = None  # Test type (row_count, uniqueness, etc.)
    on_violation: Optional[str] = None  # Action on violation (fail, warn)
    tolerance: Optional[int] = None  # Tolerance for row_count tests
    columns: Optional[List[str]] = None  # Columns for uniqueness/completeness tests
    filter: Optional[str] = None  # Optional WHERE clause filter for uniqueness tests
    reference: Optional[str] = None  # Reference table for referential integrity
    source_columns: Optional[List[str]] = None  # Source columns for joins
    reference_columns: Optional[List[str]] = None  # Reference columns for joins
    required_columns: Optional[List[str]] = None  # Required columns for completeness
    column: Optional[str] = None  # Column for range tests
    min_value: Optional[Any] = None  # Min value for range tests
    max_value: Optional[Any] = None  # Max value for range tests
    lookup_table: Optional[str] = None  # Lookup table for ALL_LOOKUPS_FOUND
    lookup_columns: Optional[List[str]] = None  # Lookup columns
    lookup_result_columns: Optional[List[str]] = None  # Expected result columns
    expectations: Optional[List[Dict[str, Any]]] = None  # Custom expectations
    test_id: Optional[str] = None  # External test management ID for reporting

    @property
    def resolved_test_target(self) -> str:
        """Canonical target name for test actions: explicit target or tmp_test_{name}."""
        return self.target or f"tmp_test_{self.name}"

    def model_post_init(self, __context: Any) -> None:
        """Post-initialization processing - normalize all path fields for cross-platform compatibility."""
        # List of path fields that need normalization
        path_fields = ["module_path", "sql_path", "expectations_file", "schema_file"]

        # Normalize direct path fields
        for field in path_fields:
            value = getattr(self, field, None)
            if value and isinstance(value, str):
                setattr(self, field, value.replace("\\", "/"))

        # Normalize paths in source dict if present
        if isinstance(self.source, dict):
            for field in path_fields:
                if field in self.source and isinstance(self.source[field], str):
                    self.source[field] = self.source[field].replace("\\", "/")

        # Normalize paths in write_target dict if present
        if isinstance(self.write_target, dict):
            # Handle snapshot_cdc source function file paths
            if "snapshot_cdc_config" in self.write_target:
                snapshot_config = self.write_target["snapshot_cdc_config"]
                if (
                    isinstance(snapshot_config, dict)
                    and "source_function" in snapshot_config
                ):
                    source_func = snapshot_config["source_function"]
                    if isinstance(source_func, dict) and "file" in source_func:
                        if isinstance(source_func["file"], str):
                            source_func["file"] = source_func["file"].replace("\\", "/")

            # Handle table_schema and schema paths
            for schema_field in ["table_schema", "schema", "sql_path", "module_path"]:
                if schema_field in self.write_target and isinstance(
                    self.write_target[schema_field], str
                ):
                    self.write_target[schema_field] = self.write_target[
                        schema_field
                    ].replace("\\", "/")


class FlowGroup(BaseModel):
    pipeline: str
    flowgroup: str
    job_name: Optional[str] = None
    variables: Optional[Dict[str, str]] = None  # Local variable definitions
    presets: List[str] = []
    use_template: Optional[str] = None
    template_parameters: Optional[Dict[str, Any]] = None
    actions: List[Action] = []
    operational_metadata: Optional[Union[bool, List[str]]] = (
        None  # Simplified: bool or list of column names
    )
    _synthetic: bool = PrivateAttr(default=False)
    _auxiliary_files: Dict[str, str] = PrivateAttr(default_factory=dict)
    _has_original_test_actions: bool = PrivateAttr(default=False)


class Template(BaseModel):
    name: str
    version: str = "1.0"
    description: Optional[str] = None
    presets: List[str] = []  # List of preset names to apply to template actions
    parameters: List[Dict[str, Any]] = []
    actions: Union[List[Action], List[Dict[str, Any]]] = []
    _raw_actions: bool = False  # Internal flag to track if actions are raw dictionaries

    def has_raw_actions(self) -> bool:
        """Check if template contains raw action dictionaries (not validated Action objects)."""
        return self._raw_actions

    def get_actions_as_dicts(self) -> List[Dict[str, Any]]:
        """Get actions as dictionaries, converting from Action objects if needed."""
        if self._raw_actions:
            return self.actions
        else:
            return [action.model_dump(mode="json") for action in self.actions]


class Preset(BaseModel):
    name: str
    version: str = "1.0"
    extends: Optional[str] = None
    description: Optional[str] = None
    defaults: Optional[Dict[str, Any]] = None


class BlueprintParameter(BaseModel):
    """A declared parameter on a blueprint, with optional default and required flag."""

    name: str
    required: bool = False
    default: Optional[Any] = None
    description: Optional[str] = None


class BlueprintFlowgroupSpec(BaseModel):
    """A flowgroup template inside a blueprint.

    Same shape as FlowGroup, but `pipeline` and `flowgroup` are templates that
    contain `%{var}` placeholders resolved at expansion time against instance
    parameter values.
    """

    pipeline: str
    flowgroup: str
    job_name: Optional[str] = None
    variables: Optional[Dict[str, str]] = None
    presets: List[str] = []
    use_template: Optional[str] = None
    template_parameters: Optional[Dict[str, Any]] = None
    actions: List[Action] = []
    operational_metadata: Optional[Union[bool, List[str]]] = None


class Blueprint(BaseModel):
    """A reusable collection of flowgroups instantiated once per BlueprintInstance.

    Distinguishing fields from a regular flowgroup file are `parameters` and
    `flowgroups` (the array of BlueprintFlowgroupSpec). `looks_like_blueprint()`
    keys on the presence of both fields together with the absence of `actions`.
    """

    name: str
    version: str = "1.0"
    description: Optional[str] = None
    parameters: List[BlueprintParameter] = []
    flowgroups: List[BlueprintFlowgroupSpec]


class BlueprintInstance(BaseModel):
    """An instance of a blueprint with concrete parameter values.

    Two input shapes are accepted:

    - **New (preferred):** ``use_blueprint:`` references the blueprint and a
      nested ``parameters:`` block holds parameter values; an optional
      ``overrides:`` block is reserved for future use. This shape mirrors the
      ``use_template:`` / ``template_parameters:`` pattern operators already
      know.
    - **Legacy (deprecated, removed in V0.9):** ``blueprint:`` plus flat
      top-level parameter keys. A deprecation warning is emitted once per
      file when this form is encountered.

    A `model_validator(mode='before')` is the single normalization point: it
    converts both shapes into the canonical (`use_blueprint`, `parameters`)
    form so all downstream code reads from one place. Mixing the two forms
    in the same file raises LHP-CFG-061.
    """

    model_config = ConfigDict(extra="forbid")

    use_blueprint: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    overrides: Optional[Dict[str, Any]] = None
    # Legacy field retained so existing readers (e.g. CLI display, expander)
    # continue to work; populated from `use_blueprint` after normalization.
    blueprint: Optional[str] = None

    # Tracks instance file paths for which we've already emitted the legacy
    # deprecation warning, so callers that re-parse the same file (e.g.
    # validate then generate) only see the warning once per process.
    _legacy_warned_paths: "ClassVar[Set[str]]" = set()

    @model_validator(mode="before")
    @classmethod
    def _normalize_syntax(cls, data: Any, info: Any) -> Any:
        if not isinstance(data, dict):
            return data

        has_use = "use_blueprint" in data
        has_legacy = "blueprint" in data

        file_path: Optional[str] = None
        if info is not None and getattr(info, "context", None):
            file_path = info.context.get("file_path")

        if has_use and has_legacy:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="061",
                title="Conflicting blueprint instance syntax",
                details=(
                    "Instance file uses both 'use_blueprint:' (new) and "
                    "'blueprint:' (legacy) keys. Pick exactly one."
                ),
                suggestions=[
                    "Use 'use_blueprint:' + nested 'parameters:' block (preferred)",
                    "Or use legacy 'blueprint:' with flat parameters "
                    "(deprecated, removed in V0.9)",
                ],
                context={"file": file_path or "<unknown>"},
            )

        if has_use:
            allowed = {"use_blueprint", "parameters", "overrides"}
            extras = sorted(k for k in data if k not in allowed)
            if extras:
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="061",
                    title="Mixing blueprint instance syntax forms",
                    details=(
                        "Instance file uses 'use_blueprint:' but also has "
                        f"flat top-level keys {extras}. With the new syntax, "
                        "all parameter values must live under 'parameters:'."
                    ),
                    suggestions=[
                        f"Move {extras} under the 'parameters:' block",
                    ],
                    context={"file": file_path or "<unknown>", "extras": extras},
                )
            return data

        if has_legacy:
            blueprint_name = data["blueprint"]
            flat_params = {k: v for k, v in data.items() if k != "blueprint"}

            warn_key = file_path or repr(sorted(data.items()))
            if warn_key not in cls._legacy_warned_paths:
                cls._legacy_warned_paths.add(warn_key)
                _legacy_logger = logging.getLogger("lhp.models.config")
                _legacy_logger.warning(
                    "Deprecated blueprint instance syntax in %s: the "
                    "'blueprint:' + flat parameters form will be removed in "
                    "V0.9. Migrate to 'use_blueprint:' + nested 'parameters:' "
                    "block.",
                    file_path or "<unknown>",
                )

            return {
                "use_blueprint": blueprint_name,
                "blueprint": blueprint_name,
                "parameters": flat_params,
            }

        return data

    @model_validator(mode="after")
    def _mirror_blueprint_field(self) -> "BlueprintInstance":
        if self.use_blueprint and not self.blueprint:
            object.__setattr__(self, "blueprint", self.use_blueprint)
        return self

    @property
    def blueprint_name(self) -> str:
        """Normalized blueprint name (works for both input shapes)."""
        return self.use_blueprint or self.blueprint or ""

    def parameter_values(self) -> Dict[str, Any]:
        """Return the parameter values supplied in this instance file."""
        return dict(self.parameters or {})
