"""Action generator registry for LakehousePlumber."""

from typing import Dict, Type
from ..core.base_generator import BaseActionGenerator
from ..models.config import ActionType, LoadSourceType, TransformType, WriteTargetType
from ..utils.error_formatter import ErrorFormatter

# Import all generators
from ..generators.load import (
    CloudFilesLoadGenerator,
    DeltaLoadGenerator,
    SQLLoadGenerator,
    JDBCLoadGenerator,
    PythonLoadGenerator,
)
from ..generators.transform import (
    SQLTransformGenerator,
    DataQualityTransformGenerator,
    SchemaTransformGenerator,
    PythonTransformGenerator,
    TempTableTransformGenerator,
)
from ..generators.write import (
    StreamingTableWriteGenerator,
    MaterializedViewWriteGenerator,
)


class ActionRegistry:
    """Registry for action generators."""

    def __init__(self):
        # Step 4.1.1: Create the registry structure
        self._load_generators: Dict[str, Type[BaseActionGenerator]] = {}
        self._transform_generators: Dict[str, Type[BaseActionGenerator]] = {}
        self._write_generators: Dict[str, Type[BaseActionGenerator]] = {}

        # Step 4.1.2: Map action types to generators
        self._initialize_generators()

    def _initialize_generators(self):
        """Initialize generator mappings."""
        # Load generators
        self._load_generators = {
            LoadSourceType.CLOUDFILES: CloudFilesLoadGenerator,
            LoadSourceType.DELTA: DeltaLoadGenerator,
            LoadSourceType.SQL: SQLLoadGenerator,
            LoadSourceType.JDBC: JDBCLoadGenerator,
            LoadSourceType.PYTHON: PythonLoadGenerator,
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
        }

    def get_generator(
        self, action_type: ActionType, sub_type: str = None
    ) -> BaseActionGenerator:
        """Step 4.1.3: Implement generator factory method."""
        # Step 4.1.4: Add error handling and validation
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

            return self._write_generators[sub_type]()

        else:
            raise ValueError(f"Unknown action type: {action_type}")

    def list_generators(self) -> Dict[str, list]:
        """List all available generators."""
        return {
            "load": [gen.value for gen in self._load_generators.keys()],
            "transform": [gen.value for gen in self._transform_generators.keys()],
            "write": [gen.value for gen in self._write_generators.keys()],
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
        except ValueError:
            return False
