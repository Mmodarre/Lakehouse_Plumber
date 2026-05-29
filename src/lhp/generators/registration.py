"""Wires per-action generator classes into the core ActionRegistry.

Importing this module registers every generator family. It lives in the
``generators`` layer (ABOVE ``core``), so ``generators -> core.registry`` is a
legal downward edge and ``core`` never imports ``generators``. The composition
root (``lhp/__init__.py``) imports this module so registration runs before any
``ActionRegistry`` is constructed.
"""
from lhp.core.registry import register_generators
from lhp.models import LoadSourceType, TransformType, WriteTargetType, TestActionType
from lhp.generators.load import (
    CloudFilesLoadGenerator,
    CustomDataSourceLoadGenerator,
    DeltaLoadGenerator,
    JDBCLoadGenerator,
    KafkaLoadGenerator,
    PythonLoadGenerator,
    SQLLoadGenerator,
)
from lhp.generators.transform import (
    DataQualityTransformGenerator,
    PythonTransformGenerator,
    SchemaTransformGenerator,
    SQLTransformGenerator,
    TempTableTransformGenerator,
)
from lhp.generators.write import (
    MaterializedViewWriteGenerator,
    SinkWriteGenerator,
    StreamingTableWriteGenerator,
)
from lhp.generators.test import TestActionGenerator


def register_all() -> None:
    """Register every per-action generator family into the core registry."""
    register_generators(
        "load",
        {
            LoadSourceType.CLOUDFILES: CloudFilesLoadGenerator,
            LoadSourceType.DELTA: DeltaLoadGenerator,
            LoadSourceType.SQL: SQLLoadGenerator,
            LoadSourceType.JDBC: JDBCLoadGenerator,
            LoadSourceType.PYTHON: PythonLoadGenerator,
            LoadSourceType.CUSTOM_DATASOURCE: CustomDataSourceLoadGenerator,
            LoadSourceType.KAFKA: KafkaLoadGenerator,
        },
    )
    register_generators(
        "transform",
        {
            TransformType.SQL: SQLTransformGenerator,
            TransformType.DATA_QUALITY: DataQualityTransformGenerator,
            TransformType.SCHEMA: SchemaTransformGenerator,
            TransformType.PYTHON: PythonTransformGenerator,
            TransformType.TEMP_TABLE: TempTableTransformGenerator,
        },
    )
    register_generators(
        "write",
        {
            WriteTargetType.STREAMING_TABLE: StreamingTableWriteGenerator,
            WriteTargetType.MATERIALIZED_VIEW: MaterializedViewWriteGenerator,
            WriteTargetType.SINK: SinkWriteGenerator,
        },
    )
    register_generators(
        "test",
        {
            TestActionType.ROW_COUNT: TestActionGenerator,
            TestActionType.UNIQUENESS: TestActionGenerator,
            TestActionType.REFERENTIAL_INTEGRITY: TestActionGenerator,
            TestActionType.COMPLETENESS: TestActionGenerator,
            TestActionType.RANGE: TestActionGenerator,
            TestActionType.SCHEMA_MATCH: TestActionGenerator,
            TestActionType.ALL_LOOKUPS_FOUND: TestActionGenerator,
            TestActionType.CUSTOM_SQL: TestActionGenerator,
            TestActionType.CUSTOM_EXPECTATIONS: TestActionGenerator,
        },
    )


register_all()
