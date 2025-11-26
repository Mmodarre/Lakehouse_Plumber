"""Schema transformation generator."""

from pathlib import Path
from typing import Dict, Any
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.schema_transform_parser import SchemaTransformParser
from ...utils.external_file_loader import resolve_external_file_path


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
        # Validate source format - must be a string (view name only)
        if isinstance(action.source, dict):
            # Old format detected - raise clear error
            raise ValueError(
                f"Schema transform action '{action.name}' uses deprecated nested format. "
                "The 'source' field must be a simple view name (string). "
                "Move 'schema' or 'schema_file' to top-level action fields. "
                "\nOld format: source: {{view: v_name, schema_file: path}} "
                "\nNew format: source: v_name\n            schema_file: path\n            enforcement: strict"
            )
        
        if not isinstance(action.source, str):
            raise ValueError(
                f"Schema transform action '{action.name}' must have a string source (view name). "
                f"Got: {type(action.source).__name__}"
            )
        
        # Validate exactly one of schema_inline or schema_file is specified
        has_schema_inline = action.schema_inline is not None
        has_schema_file = action.schema_file is not None
        
        if has_schema_inline and has_schema_file:
            raise ValueError(
                f"Schema transform action '{action.name}' cannot specify both 'schema_inline' and 'schema_file'. "
                "Use either inline schema or external schema file, not both."
            )
        
        if not has_schema_inline and not has_schema_file:
            raise ValueError(
                f"Schema transform action '{action.name}' must specify either 'schema_inline' (inline) or "
                "'schema_file' (external). Schema transforms require a schema definition."
            )
        
        # Load schema configuration
        if has_schema_file:
            # Load from external file
            project_root = context.get("spec_dir", Path.cwd())
            if not isinstance(project_root, Path):
                project_root = Path(project_root)
            
            parsed_schema = self._load_schema_file(action.schema_file, project_root)
        else:
            # Parse inline schema
            parsed_schema = self.schema_parser.parse_inline_schema(action.schema_inline)
        
        # Extract schema config (enforcement is no longer in schema files/inline)
        schema_config = {
            "column_mapping": parsed_schema.get("column_mapping", {}),
            "type_casting": parsed_schema.get("type_casting", {}),
            "pass_through_columns": parsed_schema.get("pass_through_columns", [])
        }
        
        # Get enforcement from action level (default: permissive)
        enforcement = action.enforcement or "permissive"
        
        # Validate enforcement value
        if enforcement not in ["strict", "permissive"]:
            raise ValueError(
                f"Schema transform action '{action.name}' has invalid enforcement '{enforcement}'. "
                "Must be 'strict' or 'permissive'."
            )

        # Get readMode from action or default to stream
        readMode = action.readMode or "stream"

        # Get metadata columns to preserve from project config
        project_config = context.get("project_config")
        metadata_columns = []  # Ordered list for template (preserves definition order)
        metadata_columns_set = set()  # Set for fast membership checks
        if project_config and project_config.operational_metadata:
            # Preserve insertion order from lhp.yaml (Python 3.7+ dicts maintain order)
            metadata_columns = list(project_config.operational_metadata.columns.keys())
            metadata_columns_set = set(metadata_columns)

        # Filter out metadata columns from schema operations
        filtered_column_mapping = {}
        filtered_type_casting = {}

        # Only apply column mapping to non-metadata columns
        for old_col, new_col in schema_config.get("column_mapping", {}).items():
            if old_col not in metadata_columns_set:
                filtered_column_mapping[old_col] = new_col

        # Only apply type casting to non-metadata columns
        for col, new_type in schema_config.get("type_casting", {}).items():
            if col not in metadata_columns_set:
                filtered_type_casting[col] = new_type

        # Get pass-through columns (only supported in strict mode with arrow format)
        pass_through_columns = schema_config.get("pass_through_columns", [])

        # Build final column list for strict mode (in order)
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
            "source_view": action.source,  # Now always a simple string
            "readMode": readMode,
            "schema_enforcement": enforcement,
            "type_casting": filtered_type_casting,
            "column_mapping": filtered_column_mapping,
            "final_columns": final_columns,  # Schema-defined columns only
            "metadata_columns": metadata_columns,  # Operational metadata columns
            "description": action.description or f"Schema application: {action.name}",
        }

        return self.render_template("transform/schema.py.j2", template_context)
    
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
        # Use common utility for path resolution
        resolved_path = resolve_external_file_path(
            schema_file_path,
            project_root,
            file_type="schema file"
        )
        
        # Parse the schema file
        return self.schema_parser.parse_file(resolved_path)
