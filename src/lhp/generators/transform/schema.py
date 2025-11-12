"""Schema transformation generator."""

from pathlib import Path
from typing import Dict, Any
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.schema_transform_parser import SchemaTransformParser


class SchemaTransformGenerator(BaseActionGenerator):
    """Generate schema application transformations."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql import functions as F")
        self.add_import("from pyspark.sql.types import StructType")
        self.schema_parser = SchemaTransformParser()

    def generate(self, action: Action, context: dict) -> str:
        """Generate schema transform code."""
        # Handle string source (view name only, no schema config)
        if isinstance(action.source, str):
            # Simple case: just a view name, no schema transformations
            schema_config = {"pass_through_columns": []}
        elif isinstance(action.source, dict):
            # Check for both schema and schema_file (error)
            has_inline_schema = "schema" in action.source
            has_schema_file = "schema_file" in action.source
            
            if has_inline_schema and has_schema_file:
                raise ValueError(
                    "Cannot specify both 'schema' and 'schema_file' in source configuration. "
                    "Use either inline schema or external schema file, not both."
                )
            
            # Load schema configuration
            if has_schema_file:
                # Load from external file
                project_root = context.get("spec_dir", Path.cwd())
                if not isinstance(project_root, Path):
                    project_root = Path(project_root)
                
                schema_file_path = action.source["schema_file"]
                parsed_schema = self._load_schema_file(schema_file_path, project_root)
                
                # Extract config from parsed schema
                schema_config = {
                    "enforcement": parsed_schema.get("enforcement", "permissive"),
                    "column_mapping": parsed_schema.get("column_mapping", {}),
                    "type_casting": parsed_schema.get("type_casting", {}),
                    "pass_through_columns": parsed_schema.get("pass_through_columns", [])
                }
            else:
                # Use inline schema (legacy behavior - backward compatible)
                schema_config = action.source.get("schema", {})
                # Legacy inline format doesn't have pass_through_columns
                if "pass_through_columns" not in schema_config:
                    schema_config["pass_through_columns"] = []
        else:
            raise ValueError("Schema transform source must be either a string (view name) or a dictionary.")

        # Get readMode from action or default to batch
        readMode = action.readMode or "batch"

        # Get metadata columns to preserve from project config
        project_config = context.get("project_config")
        metadata_columns = set()
        if project_config and project_config.operational_metadata:
            metadata_columns = set(project_config.operational_metadata.columns.keys())

        # Filter out metadata columns from schema operations
        filtered_column_mapping = {}
        filtered_type_casting = {}

        # Only apply column mapping to non-metadata columns
        for old_col, new_col in schema_config.get("column_mapping", {}).items():
            if old_col not in metadata_columns:
                filtered_column_mapping[old_col] = new_col

        # Only apply type casting to non-metadata columns
        for col, new_type in schema_config.get("type_casting", {}).items():
            if col not in metadata_columns:
                filtered_type_casting[col] = new_type

        # Get pass-through columns (only supported in strict mode with arrow format)
        pass_through_columns = schema_config.get("pass_through_columns", [])

        # Build final column list for strict mode (in order)
        enforcement = schema_config.get("enforcement", "permissive")
        final_columns = []
        
        if enforcement == "strict":
            # Track which columns to include
            columns_to_include = set()
            
            # Add renamed columns (use target names)
            for source_col, target_col in filtered_column_mapping.items():
                columns_to_include.add(target_col)
            
            # Add cast-only columns (not renamed)
            for col in filtered_type_casting.keys():
                if col not in filtered_column_mapping.values():
                    # This is a cast-only column, not a renamed column
                    columns_to_include.add(col)
            
            # Build list of schema-defined columns (these MUST exist)
            schema_columns = []
            
            # First add columns from column_mapping (in their definition order)
            for target_col in filtered_column_mapping.values():
                if target_col not in schema_columns:
                    schema_columns.append(target_col)
            
            # Then add cast-only columns (in their definition order)
            for col in filtered_type_casting.keys():
                if col not in schema_columns:
                    schema_columns.append(col)
            
            # Finally add pass-through columns (no rename, no cast - just keep them)
            for col in pass_through_columns:
                if col not in schema_columns:
                    schema_columns.append(col)
            
            # Store both schema columns and metadata columns separately
            final_columns = schema_columns  # Schema columns go first (will fail if missing)
            # Metadata columns will be added conditionally in the template

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "source_view": self._extract_source_view(action.source),
            "readMode": readMode,
            "schema_enforcement": enforcement,
            "type_casting": filtered_type_casting,
            "column_mapping": filtered_column_mapping,
            "final_columns": final_columns,  # Schema-defined columns only
            "metadata_columns": metadata_columns,  # Operational metadata columns
            "description": action.description or f"Schema application: {action.name}",
        }

        return self.render_template("transform/schema.py.j2", template_context)

    def _extract_source_view(self, source) -> str:
        """Extract source view name from source configuration."""
        if isinstance(source, str):
            return source
        elif isinstance(source, dict):
            return source.get("view", source.get("source"))
        else:
            raise ValueError("Invalid source configuration for schema transform")
    
    def _load_schema_file(self, schema_file_path: str, project_root: Path) -> Dict[str, Any]:
        """Load and parse schema transform file from disk.
        
        Args:
            schema_file_path: Path to schema file (relative or absolute).
            project_root: Project root directory (from context['spec_dir']).
            
        Returns:
            Parsed schema configuration dict with column_mapping, type_casting, etc.
            
        Raises:
            FileNotFoundError: If schema file doesn't exist.
            ValueError: If schema format is invalid.
        """
        file_path = Path(schema_file_path)
        
        # Resolve path relative to project root if not absolute
        if not file_path.is_absolute():
            file_path = project_root / file_path
        
        # Parse the schema file
        return self.schema_parser.parse_file(file_path)
