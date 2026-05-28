"""Configuration models for LHP (project, action, flowgroup, blueprint, monitoring, test).

Each model is defined in an underscore-prefixed sub-module so the public surface is
explicit and the package contract is auditable. Import every public class from
`lhp.models` directly; sub-module paths are internal and may change.
"""

import warnings

# Suppress Pydantic warning about 'schema' field shadowing BaseModel.schema() class
# method. This is deliberate: 'schema' is a UC namespace field on WriteTarget /
# EventLogConfig / MonitoringConfig, not related to Pydantic's schema().
warnings.filterwarnings(
    "ignore", message=r".*Field name \"schema\".*shadows an attribute.*"
)

from ._action import Action, WriteTarget  # noqa: E402
from ._blueprint import (  # noqa: E402
    Blueprint,
    BlueprintFlowgroupSpec,
    BlueprintInstance,
    BlueprintParameter,
)
from ._enums import (  # noqa: E402
    ActionType,
    DQMode,
    LoadSourceType,
    TestActionType,
    TransformType,
    ViolationAction,
    WriteTargetType,
)
from ._flowgroup import FlowGroup, FlowGroupContext  # noqa: E402
from ._monitoring import (  # noqa: E402
    EventLogConfig,
    MonitoringConfig,
    MonitoringMaterializedViewConfig,
)
from ._operational_metadata import (  # noqa: E402
    MetadataColumnConfig,
    MetadataPresetConfig,
    OperationalMetadataSelection,
    ProjectOperationalMetadataConfig,
)
from ._project import ProjectConfig  # noqa: E402
from ._quarantine import QuarantineConfig  # noqa: E402
from ._template import Preset, Template  # noqa: E402
from ._test_reporting import TestReportingConfig  # noqa: E402

__all__ = [
    "Action",
    "ActionType",
    "Blueprint",
    "BlueprintFlowgroupSpec",
    "BlueprintInstance",
    "BlueprintParameter",
    "DQMode",
    "EventLogConfig",
    "FlowGroup",
    "FlowGroupContext",
    "LoadSourceType",
    "MetadataColumnConfig",
    "MetadataPresetConfig",
    "MonitoringConfig",
    "MonitoringMaterializedViewConfig",
    "OperationalMetadataSelection",
    "Preset",
    "ProjectConfig",
    "ProjectOperationalMetadataConfig",
    "QuarantineConfig",
    "Template",
    "TestActionType",
    "TestReportingConfig",
    "TransformType",
    "ViolationAction",
    "WriteTarget",
    "WriteTargetType",
]
