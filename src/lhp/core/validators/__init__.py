"""Action validators package.

# isort: skip_file -- the import order below is load-bearing: ConfigValidator
# does ``from . import LoadActionValidator, ...`` and would hit a partially-
# initialized-module ImportError if isort moved it ahead of its dependencies.
"""

# isort: skip_file
from ._base import BaseActionValidator
from .compatibility.cdc_fanin import CdcFanInCompatibilityValidator
from .action.load import LoadActionValidator
from .action.transform import TransformActionValidator
from .action.write import WriteActionValidator
from .action.test import TestActionValidator
from .compatibility.table_creation import TableCreationValidator, action_creates_table

# Domain-level validators (sorted into the action/pipeline/field/compatibility
# taxonomy under §9.4).
from .field.config_field import ConfigFieldValidator
from .field.secret_reference import SecretValidator
from .compatibility import (
    DltTableOptionsValidator,
    CdcConfigValidator,
    SnapshotCdcConfigValidator,
    CdcSchemaValidator,
)
from .pipeline.job_name import validate_job_names, validate_job_name_format
from .field.kafka_options import KafkaOptionsValidator

from .config_validator import ConfigValidator

__all__ = [
    "BaseActionValidator",
    "CdcConfigValidator",
    "CdcFanInCompatibilityValidator",
    "CdcSchemaValidator",
    "ConfigFieldValidator",
    "ConfigValidator",
    "DltTableOptionsValidator",
    "KafkaOptionsValidator",
    "LoadActionValidator",
    "SecretValidator",
    "SnapshotCdcConfigValidator",
    "TableCreationValidator",
    "TestActionValidator",
    "TransformActionValidator",
    "WriteActionValidator",
    "action_creates_table",
    "validate_job_name_format",
    "validate_job_names",
]
