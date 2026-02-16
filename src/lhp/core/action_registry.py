"""Action generator registry for LakehousePlumber."""

import logging
from typing import Dict, Type

from ..core.base_generator import BaseActionGenerator

# Import all generators
from ..generators.load import (
    CloudFilesLoadGenerator,
    CustomDataSourceLoadGenerator,
    DeltaLoadGenerator,
    JDBCLoadGenerator,
    KafkaLoadGenerator,
    PythonLoadGenerator,
    SQLLoadGenerator,
)
from ..generators.test import (
    TestActionGenerator,
)
from ..generators.transform import (
    DataQualityTransformGenerator,
    PythonTransformGenerator,
    SchemaTransformGenerator,
    SQLTransformGenerator,
    TempTableTransformGenerator,
)
from ..generators.write import (
    MaterializedViewWriteGenerator,
    SinkWriteGenerator,
    StreamingTableWriteGenerator,
)
from ..models.config import (
    ActionType,
    LoadSourceType,
    TestActionType,
    TransformType,
    WriteTargetType,
)
from ..utils.error_formatter import ErrorFormatter

logger = logging.getLogger(__name__)


class ActionRegistry:
    """Registry for action generators."""

    def __init__(self):
        # Create the registry structure
        self._load_generators: Dict[str, Type[BaseActionGenerator]] = {}
        self._transform_generators: Dict[str, Type[BaseActionGenerator]] = {}
        self._write_generators: Dict[str, Type[BaseActionGenerator]] = {}
        self._test_generators: Dict[str, Type[BaseActionGenerator]] = {}

        # Map action types to generators
        self._initialize_generators()

    def _initialize_generators(self):
        """Initialize generator mappings."""
        logger.debug("Initializing action generator registry")
        # Load generators
        self._load_generators = {
            LoadSourceType.CLOUDFILES: CloudFilesLoadGenerator,
            LoadSourceType.DELTA: DeltaLoadGenerator,
            LoadSourceType.SQL: SQLLoadGenerator,
            LoadSourceType.JDBC: JDBCLoadGenerator,
            LoadSourceType.PYTHON: PythonLoadGenerator,
            LoadSourceType.CUSTOM_DATASOURCE: CustomDataSourceLoadGenerator,
            LoadSourceType.KAFKA: KafkaLoadGenerator,
        }

        # Transform generators
        self._transform_generators = {
            TransformType.SQL: SQLTransformGenerator,
            TransformType.DATA_QUALITY: DataQualityTransformGenerator,
            TransformType.SCHEMA: SchemaTransformGenerator,
            TransformType.PYTHON: PythonTransformGenerator,
            TransformType.TEMP_TABLE: TempTableTransformGenerator,
        }

        # Write generators
        self._write_generators = {
            WriteTargetType.STREAMING_TABLE: StreamingTableWriteGenerator,
            WriteTargetType.MATERIALIZED_VIEW: MaterializedViewWriteGenerator,
            WriteTargetType.SINK: SinkWriteGenerator,
        }

        # Test generators - all test types use the same generator
        # The generator will handle different test types internally
        self._test_generators = {
            TestActionType.ROW_COUNT: TestActionGenerator,
            TestActionType.UNIQUENESS: TestActionGenerator,
            TestActionType.REFERENTIAL_INTEGRITY: TestActionGenerator,
            TestActionType.COMPLETENESS: TestActionGenerator,
            TestActionType.RANGE: TestActionGenerator,
            TestActionType.SCHEMA_MATCH: TestActionGenerator,
            TestActionType.ALL_LOOKUPS_FOUND: TestActionGenerator,
            TestActionType.CUSTOM_SQL: TestActionGenerator,
            TestActionType.CUSTOM_EXPECTATIONS: TestActionGenerator,
        }

        logger.debug(
            f"Registry initialized: {len(self._load_generators)} load, "
            f"{len(self._transform_generators)} transform, "
            f"{len(self._write_generators)} write, "
            f"{len(self._test_generators)} test generators"
        )

    def get_generator(
        self, action_type: ActionType, sub_type: str = None
    ) -> BaseActionGenerator:
        """Implement generator factory method."""
        logger.debug(
            f"Looking up generator for action_type={action_type}, sub_type={sub_type}"
        )
        # Add error handling and validation
        if not isinstance(action_type, ActionType):
            raise ValueError(
                f"Invalid action type: {action_type}. Must be an ActionType enum."
            )

        if action_type == ActionType.LOAD:
            if not sub_type:
                raise ValueError("Load actions require a sub_type")

            # Convert string to enum if needed
            if isinstance(sub_type, str):
                try:
                    sub_type = LoadSourceType(sub_type)
                except ValueError:
                    valid_types = [t.value for t in LoadSourceType]
                    raise ErrorFormatter.unknown_type_with_suggestion(
                        value_type="load sub_type",
                        provided_value=sub_type,
                        valid_values=valid_types,
                        example_usage="""actions:
  - name: load_csv_files
    type: load
    sub_type: cloudfiles  # ← Valid sub_type
    target: v_raw_data
    source:
      type: cloudfiles
      path: /path/to/files/*.csv
      format: csv""",
                    )

            if sub_type not in self._load_generators:
                raise ValueError(f"No generator registered for load type: {sub_type}")

            logger.debug(
                f"Resolved load generator: {self._load_generators[sub_type].__name__}"
            )
            return self._load_generators[sub_type]()

        elif action_type == ActionType.TRANSFORM:
            if not sub_type:
                raise ValueError("Transform actions require a sub_type")

            # Convert string to enum if needed
            if isinstance(sub_type, str):
                try:
                    sub_type = TransformType(sub_type)
                except ValueError:
                    valid_types = [t.value for t in TransformType]
                    raise ErrorFormatter.unknown_type_with_suggestion(
                        value_type="transform sub_type",
                        provided_value=sub_type,
                        valid_values=valid_types,
                        example_usage="""actions:
  - name: transform_data
    type: transform
    sub_type: sql  # ← Valid sub_type
    source: v_raw_data
    target: v_transformed_data
    sql: |
      SELECT * FROM $source WHERE active = true""",
                    )

            if sub_type not in self._transform_generators:
                raise ValueError(
                    f"No generator registered for transform type: {sub_type}"
                )

            logger.debug(
                f"Resolved transform generator: {self._transform_generators[sub_type].__name__}"
            )
            return self._transform_generators[sub_type]()

        elif action_type == ActionType.WRITE:
            if not sub_type:
                raise ValueError("Write actions require a sub_type")

            # Convert string to enum if needed
            if isinstance(sub_type, str):
                try:
                    sub_type = WriteTargetType(sub_type)
                except ValueError:
                    valid_types = [t.value for t in WriteTargetType]
                    raise ErrorFormatter.unknown_type_with_suggestion(
                        value_type="write sub_type",
                        provided_value=sub_type,
                        valid_values=valid_types,
                        example_usage="""actions:
  - name: write_to_table
    type: write
    sub_type: streaming_table  # ← Valid sub_type
    source: v_transformed_data
    write_target:
      type: streaming_table
      catalog: my_catalog
      database: my_db
      table: my_table""",
                    )

            if sub_type not in self._write_generators:
                raise ValueError(f"No generator registered for write type: {sub_type}")

            logger.debug(
                f"Resolved write generator: {self._write_generators[sub_type].__name__}"
            )
            return self._write_generators[sub_type]()

        elif action_type == ActionType.TEST:
            # For test actions, sub_type is the test_type
            if not sub_type:
                # Default to a basic test type if not specified
                sub_type = "row_count"

            # Convert string to enum if needed
            if isinstance(sub_type, str):
                try:
                    sub_type = TestActionType(sub_type)
                except ValueError:
                    valid_types = [t.value for t in TestActionType]
                    raise ErrorFormatter.unknown_type_with_suggestion(
                        value_type="test_type",
                        provided_value=sub_type,
                        valid_values=valid_types,
                        example_usage="""actions:
  - name: test_row_count
    type: test
    test_type: row_count  # ← Valid test_type
    source: [v_source, v_target]
    on_violation: fail""",
                    )

            if sub_type not in self._test_generators:
                raise ValueError(f"No generator registered for test type: {sub_type}")

            logger.debug(
                f"Resolved test generator: {self._test_generators[sub_type].__name__}"
            )
            return self._test_generators[sub_type]()

        else:
            raise ValueError(f"Unknown action type: {action_type}")

    def list_generators(self) -> Dict[str, list]:
        """List all available generators."""
        return {
            "load": [gen.value for gen in self._load_generators.keys()],
            "transform": [gen.value for gen in self._transform_generators.keys()],
            "write": [gen.value for gen in self._write_generators.keys()],
            "test": [gen.value for gen in self._test_generators.keys()],
        }

    def is_generator_available(self, action_type: ActionType, sub_type: str) -> bool:
        """Check if a generator is available for the given action and sub type."""
        try:
            if action_type == ActionType.LOAD:
                sub_type_enum = LoadSourceType(sub_type)
                return sub_type_enum in self._load_generators
            elif action_type == ActionType.TRANSFORM:
                sub_type_enum = TransformType(sub_type)
                return sub_type_enum in self._transform_generators
            elif action_type == ActionType.WRITE:
                sub_type_enum = WriteTargetType(sub_type)
                return sub_type_enum in self._write_generators
            else:
                return False
        except ValueError as e:
            logger.debug(
                f"Unknown sub_type '{sub_type}' for action type '{action_type}': {e}"
            )
            return False
