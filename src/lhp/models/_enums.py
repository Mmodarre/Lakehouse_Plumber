"""Domain enums used across LHP configuration models."""

from enum import Enum


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
