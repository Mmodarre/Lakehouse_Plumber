"""Compatibility validators (§1 home)."""

from .cdc_config import CdcConfigValidator
from .cdc_fanin import CdcFanInCompatibilityValidator
from .cdc_schema import CdcSchemaValidator
from .dlt_table_options import DltTableOptionsValidator
from .mv_expectations import MvExpectationsValidator
from .mv_fusion import dq_source_view, fusion_target
from .read_mode_lineage import ReadModeLineageValidator
from .replace_config import ReplaceFlowConfigValidator
from .replace_fanin import ReplaceFanInCompatibilityValidator
from .shared_dlq import SharedDlqSourceTableValidator
from .snapshot_cdc import SnapshotCdcConfigValidator
from .table_creation import TableCreationValidator, action_creates_table

__all__ = [
    "CdcConfigValidator",
    "CdcFanInCompatibilityValidator",
    "CdcSchemaValidator",
    "DltTableOptionsValidator",
    "MvExpectationsValidator",
    "ReadModeLineageValidator",
    "ReplaceFanInCompatibilityValidator",
    "ReplaceFlowConfigValidator",
    "SharedDlqSourceTableValidator",
    "SnapshotCdcConfigValidator",
    "TableCreationValidator",
    "action_creates_table",
    "dq_source_view",
    "fusion_target",
]
