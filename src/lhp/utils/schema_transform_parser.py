"""Schema transform parser for arrow format and legacy format."""

import re
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from ..parsers.yaml_parser import YAMLParser


class SchemaTransformParser:
    """Parse schema transform files supporting arrow and legacy formats.
    
    Arrow format (recommended):
        columns:
          - old_col -> new_col: TYPE    # Rename and cast
          - old_col -> new_col           # Rename only
          - col: TYPE                    # Cast only
          - col                          # Pass-through (strict mode only)
    
    Legacy format (deprecated):
        column_mapping:
          old_col: new_col
        type_casting:
          col: TYPE
    """
    
    def __init__(self):
        """Initialize the schema transform parser."""
        self.yaml_parser = YAMLParser()
        # Regex pattern for arrow syntax: "old -> new: TYPE" or variations
        self.arrow_pattern = re.compile(
            r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*->\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?::\s*(.+?))?\s*$'
        )
        # Regex pattern for type cast only: "col: TYPE"
        self.cast_pattern = re.compile(
            r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.+?)\s*$'
        )
        # Regex pattern for pass-through: "col"
        self.passthrough_pattern = re.compile(
            r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*$'
        )
    
    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        """Parse a schema transform file from disk.
        
        Args:
            file_path: Path to the schema transform YAML file.
            
        Returns:
            Parsed schema configuration dict with column_mapping, type_casting, etc.
            
        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If file format is invalid.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Schema transform file not found: {file_path}")
        
        data = self.yaml_parser.parse_file(file_path)
        return self.parse_file_data(data)
    
    def parse_file_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse schema transform data from a dict (already loaded YAML).
        
        Args:
            data: Dictionary containing schema transform configuration.
            
        Returns:
            Normalized schema configuration dict.
            
        Raises:
            ValueError: If format is invalid or mixed.
        """
        # Detect format
        has_columns = "columns" in data
        has_legacy = "column_mapping" in data or "type_casting" in data
        
        if has_columns and has_legacy:
            raise ValueError(
                "Cannot mix arrow format and legacy format. "
                "Use either 'columns' (arrow format) or 'column_mapping'/'type_casting' (legacy format)."
            )
        
        if has_columns:
            return self.parse_arrow_format(data)
        elif has_legacy:
            return self.parse_legacy_format(data)
        else:
            raise ValueError(
                "Unable to detect schema transform format. "
                "Expected either 'columns' (arrow format) or 'column_mapping'/'type_casting' (legacy format)."
            )
    
    def parse_arrow_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse arrow format schema transform.
        
        Arrow format examples:
            - "c_custkey -> customer_id: BIGINT"  # Rename + cast
            - "c_name -> customer_name"            # Rename only
            - "account_balance: DECIMAL(18,2)"     # Cast only
            - "address"                            # Pass-through (strict only)
        
        Args:
            data: Dictionary with 'columns' list and optional 'enforcement'.
            
        Returns:
            Normalized dict with column_mapping, type_casting, enforcement, etc.
            
        Raises:
            ValueError: If format is invalid, has duplicates, or violates rules.
        """
        columns = data.get("columns", [])
        enforcement = data.get("enforcement", "permissive")
        
        # Validate columns exist
        if not columns:
            if enforcement == "strict":
                raise ValueError("Strict enforcement requires at least one column definition.")
            else:
                raise ValueError("No columns defined in schema transform.")
        
        column_mapping: Dict[str, str] = {}
        type_casting: Dict[str, str] = {}
        pass_through_columns: List[str] = []
        
        # Track all column names to detect duplicates
        source_columns_seen: set[str] = set()
        target_columns_seen: set[str] = set()
        
        for col_def in columns:
            if not isinstance(col_def, str):
                raise ValueError(f"Invalid column definition: {col_def}. Must be a string.")
            
            # Try to match arrow syntax (rename + optional cast)
            arrow_match = self.arrow_pattern.match(col_def)
            if arrow_match:
                source_col = arrow_match.group(1)
                target_col = arrow_match.group(2)
                col_type = arrow_match.group(3)  # May be None
                
                # Check for duplicate source column
                if source_col in source_columns_seen:
                    raise ValueError(
                        f"Duplicate source column '{source_col}' in schema transform. "
                        "Each source column can only be mapped once."
                    )
                source_columns_seen.add(source_col)
                
                # Check for duplicate target column
                if target_col in target_columns_seen:
                    raise ValueError(
                        f"Duplicate target column '{target_col}' in schema transform. "
                        "Column '{target_col}' appears multiple times."
                    )
                target_columns_seen.add(target_col)
                
                # Add to column mapping
                column_mapping[source_col] = target_col
                
                # Add type casting if specified
                if col_type:
                    type_casting[target_col] = col_type
                
                continue
            
            # Try to match cast-only syntax
            cast_match = self.cast_pattern.match(col_def)
            if cast_match:
                col_name = cast_match.group(1)
                col_type = cast_match.group(2)
                
                # Check for duplicate
                if col_name in target_columns_seen:
                    raise ValueError(
                        f"Duplicate column '{col_name}' in schema transform. "
                        "Column '{col_name}' appears multiple times."
                    )
                target_columns_seen.add(col_name)
                
                # Check if this column was already renamed
                if col_name in source_columns_seen:
                    raise ValueError(
                        f"Duplicate column '{col_name}': cannot cast a column that is used as source in rename operation."
                    )
                
                # Add type casting
                type_casting[col_name] = col_type
                continue
            
            # Try to match pass-through syntax
            passthrough_match = self.passthrough_pattern.match(col_def)
            if passthrough_match:
                col_name = passthrough_match.group(1)
                
                # Pass-through only allowed in strict mode
                if enforcement != "strict":
                    raise ValueError(
                        f"Pass-through columns ('{col_name}') are only allowed in strict mode. "
                        "Set enforcement: strict or use column transformations."
                    )
                
                # Check for duplicate
                if col_name in target_columns_seen:
                    raise ValueError(f"Duplicate column '{col_name}' in schema transform.")
                target_columns_seen.add(col_name)
                
                pass_through_columns.append(col_name)
                continue
            
            # If no pattern matched, it's invalid syntax
            raise ValueError(
                f"Invalid arrow format syntax: '{col_def}'. "
                "Expected formats: 'old -> new: TYPE', 'old -> new', 'col: TYPE', or 'col' (strict mode only)."
            )
        
        return {
            "enforcement": enforcement,
            "column_mapping": column_mapping,
            "type_casting": type_casting,
            "pass_through_columns": pass_through_columns
        }
    
    def parse_legacy_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse legacy format schema transform.
        
        Legacy format:
            column_mapping:
              old_col: new_col
            type_casting:
              col: TYPE
            enforcement: strict|permissive
        
        Args:
            data: Dictionary with column_mapping and/or type_casting.
            
        Returns:
            Normalized dict with column_mapping, type_casting, enforcement.
            
        Raises:
            ValueError: If strict mode has no columns defined.
        """
        enforcement = data.get("enforcement", "permissive")
        column_mapping = data.get("column_mapping", {})
        type_casting = data.get("type_casting", {})
        
        # Validate strict mode has at least one column
        if enforcement == "strict":
            if not column_mapping and not type_casting:
                raise ValueError(
                    "Strict enforcement requires at least one column definition in column_mapping or type_casting."
                )
        
        return {
            "enforcement": enforcement,
            "column_mapping": column_mapping,
            "type_casting": type_casting,
            "pass_through_columns": []  # Not supported in legacy format
        }

