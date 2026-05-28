"""Action validators package.

# isort: skip_file -- the import order below is load-bearing: ConfigValidator
# does ``from . import LoadActionValidator, ...`` and would hit a partially-
# initialized-module ImportError if isort moved it ahead of its dependencies.
"""

# isort: skip_file
from ._base import BaseActionValidator
from .cdc_fanin_compatibility_validator import CdcFanInCompatibilityValidator
from .load_validator import LoadActionValidator
from .transform_validator import TransformActionValidator
from .write_validator import WriteActionValidator
from .test_validator import TestActionValidator
from .table_creation_validator import TableCreationValidator, action_creates_table

# Domain-level validators (consolidated under §9.4 in Week 3)
from .config_field_validator import ConfigFieldValidator
from .secret_validator import SecretValidator
from .dlt_cdc_validators import (
    DltTableOptionsValidator,
    CdcConfigValidator,
    SnapshotCdcConfigValidator,
    CdcSchemaValidator,
)
from .pipeline_validator import PipelineValidator
from .job_name_validator import validate_job_names, validate_job_name_format
from .kafka_validator import KafkaOptionsValidator

# Project-wide aggregator (composes the action/structural validators above).
from .config_validator import ConfigValidator

__all__ = [
    "BaseActionValidator",
    "CdcFanInCompatibilityValidator",
    "LoadActionValidator",
    "TransformActionValidator",
    "WriteActionValidator",
    "TestActionValidator",
    "TableCreationValidator",
    "action_creates_table",
    "ConfigFieldValidator",
    "ConfigValidator",
    "SecretValidator",
    "DltTableOptionsValidator",
    "CdcConfigValidator",
    "SnapshotCdcConfigValidator",
    "CdcSchemaValidator",
    "PipelineValidator",
    "validate_job_names",
    "validate_job_name_format",
    "KafkaOptionsValidator",
]
